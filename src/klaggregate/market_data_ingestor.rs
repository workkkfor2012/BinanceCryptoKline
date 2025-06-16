//! 行情数据接入与解析模块
//! 
//! 负责通过WebSocket接收币安归集交易数据，并解析转发给路由器。

use crate::klaggregate::{AggregateConfig, TradeEventRouter};
use crate::klcommon::{Result, AppError, websocket::*};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{info, debug, warn, error, instrument, Instrument};

/// 行情数据接入器
pub struct MarketDataIngestor {
    /// 配置
    config: AggregateConfig,
    
    /// 交易事件路由器
    trade_router: Arc<TradeEventRouter>,
    
    /// WebSocket客户端
    websocket_client: Arc<RwLock<Option<Arc<AggTradeClient>>>>,
    
    /// 运行状态
    is_running: Arc<AtomicBool>,

    /// 连接计数
    connection_count: Arc<AtomicUsize>,

    /// 消息统计
    message_count: Arc<AtomicUsize>,

    /// 错误计数
    error_count: Arc<AtomicUsize>,
}

impl MarketDataIngestor {
    /// 创建新的行情数据接入器
    #[instrument(target = "MarketDataIngestor", name="new_ingestor", skip_all, err)]
    pub async fn new(
        config: AggregateConfig,
        trade_router: Arc<TradeEventRouter>,
    ) -> Result<Self> {
        info!(target: "MarketDataIngestor", event_name = "接入器初始化", "创建行情数据接入器");
        
        Ok(Self {
            config,
            trade_router,
            websocket_client: Arc::new(RwLock::new(None)),
            is_running: Arc::new(AtomicBool::new(false)),
            connection_count: Arc::new(AtomicUsize::new(0)),
            message_count: Arc::new(AtomicUsize::new(0)),
            error_count: Arc::new(AtomicUsize::new(0)),
        })
    }
    
    /// 启动数据接入
    #[instrument(target = "MarketDataIngestor", fields(symbols_count), skip(self), err)]
    pub async fn start(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            warn!(target: "MarketDataIngestor", event_name = "接入器已运行", "行情数据接入器已经在运行");
            return Ok(());
        }

        info!(target: "MarketDataIngestor", event_name = "接入器启动开始", "启动行情数据接入器");
        self.is_running.store(true, Ordering::Relaxed);

        // 获取需要订阅的品种列表
        let symbols = self.trade_router.get_registered_symbols().await;
        if symbols.is_empty() {
            error!(target: "MarketDataIngestor", event_name = "无注册品种", "没有注册的交易品种");
            return Err(AppError::ConfigError("没有注册的交易品种".to_string()));
        }

        tracing::Span::current().record("symbols_count", symbols.len());
        info!(target: "MarketDataIngestor", event_name = "品种订阅准备", symbols_count = symbols.len(), "准备订阅品种的归集交易数据: symbols_count={}", symbols.len());
        
        // 创建WebSocket配置
        let ws_config = AggTradeConfig {
            use_proxy: self.config.websocket.use_proxy,
            proxy_addr: self.config.websocket.proxy_host.clone(),
            proxy_port: self.config.websocket.proxy_port,
            symbols,
        };
        
        // 创建交易数据通道
        let (trade_sender, trade_receiver) = tokio::sync::mpsc::unbounded_channel();

        // 启动交易事件处理任务
        let trade_router = self.trade_router.clone();
        tokio::spawn(async move {
            let mut receiver = trade_receiver;
            while let Some(trade_data) = receiver.recv().await {
                if let Err(e) = trade_router.route_trade_event(trade_data).await {
                    error!(target: "MarketDataIngestor", event_name = "交易事件路由失败", error = %e, "路由交易事件失败");
                }
            }
        }.instrument(tracing::info_span!("trade_event_processor")));

        // 创建带有正确消息处理器的WebSocket客户端
        let message_handler = Arc::new(AggTradeMessageHandler::with_trade_sender(
            self.message_count.clone(),
            self.error_count.clone(),
            trade_sender,
        ));

        // 创建数据库连接（AggTradeClient需要）
        let db = Arc::new(crate::klcommon::Database::new(&std::path::PathBuf::from("./data/klines.db"))?);

        // 直接创建底层客户端，注入正确的消息处理器
        let mut client = AggTradeClient::new_with_handler(
            ws_config,
            db,
            vec!["1m".to_string()], // 临时使用，实际不需要
            message_handler,
        );

        // 启动WebSocket客户端
        client.start().await?;

        // 保存客户端引用
        *self.websocket_client.write().await = Some(Arc::new(client));

        // 启动统计输出任务
        self.start_statistics_task().await;

        info!(target: "MarketDataIngestor", event_name = "接入器启动完成", "行情数据接入器启动完成");
        Ok(())
    }

    /// 停止数据接入
    #[instrument(target = "MarketDataIngestor", skip(self), err)]
    pub async fn stop(&self) -> Result<()> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Ok(());
        }

        info!(target: "MarketDataIngestor", event_name = "接入器停止开始", "停止行情数据接入器");
        self.is_running.store(false, Ordering::Relaxed);

        // 停止WebSocket客户端
        if let Some(_client) = &*self.websocket_client.read().await {
            // WebSocket客户端会在连接断开时自动停止
            debug!(target: "MarketDataIngestor", event_name = "WebSocket客户端自动停止", "WebSocket客户端将自动停止");
        }

        info!(target: "MarketDataIngestor", event_name = "接入器停止完成", "行情数据接入器已停止");
        Ok(())
    }
    
    /// 启动统计输出任务
    async fn start_statistics_task(&self) {
        let is_running = self.is_running.clone();
        let message_count = self.message_count.clone();
        let error_count = self.error_count.clone();
        let connection_count = self.connection_count.clone();
        
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(30));
            let mut last_message_count = 0;
            let mut last_error_count = 0;
            
            while is_running.load(Ordering::Relaxed) {
                interval_timer.tick().await;
                
                let current_messages = message_count.load(Ordering::Relaxed);
                let current_errors = error_count.load(Ordering::Relaxed);
                let connections = connection_count.load(Ordering::Relaxed);

                let message_rate = current_messages - last_message_count;
                let error_rate = current_errors - last_error_count;

                info!(
                    target: "MarketDataIngestor",
                    event_name = "接入器统计报告",
                    connections = connections,
                    total_messages = current_messages,
                    message_rate = message_rate,
                    total_errors = current_errors,
                    error_rate = error_rate,
                    "行情数据统计报告"
                );
                
                last_message_count = current_messages;
                last_error_count = current_errors;
            }
        }.instrument(tracing::info_span!("ingestor_statistics_task")));
    }
    
    /// 获取连接数量
    pub async fn get_connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }
    
    /// 获取统计信息
    pub async fn get_statistics(&self) -> IngestorStatistics {
        IngestorStatistics {
            is_running: self.is_running.load(Ordering::Relaxed),
            connection_count: self.connection_count.load(Ordering::Relaxed),
            message_count: self.message_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
        }
    }
}





/// 接入器统计信息
#[derive(Debug, Clone)]
pub struct IngestorStatistics {
    /// 是否运行中
    pub is_running: bool,
    /// 连接数量
    pub connection_count: usize,
    /// 消息计数
    pub message_count: usize,
    /// 错误计数
    pub error_count: usize,
}
