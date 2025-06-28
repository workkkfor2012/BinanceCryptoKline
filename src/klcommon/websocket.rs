// WebSocket模块 - 提供通用的WebSocket连接管理功能 (使用 fastwebsockets 实现)
use crate::klcommon::{AppError, Database, KlineData, Result, PROXY_HOST, PROXY_PORT};
use tracing::{info, error, debug, warn, instrument, Instrument};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::future::Future;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;
use serde_json::{json, Value};
use futures_util::future::join_all;
use bytes::Bytes;
use fastwebsockets::{FragmentCollector, Frame, OpCode};
use hyper_util::rt::tokio::TokioIo;
use hyper::upgrade::Upgraded;
use http_body_util::Empty;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::Request;
use tokio_rustls::rustls::{ClientConfig, OwnedTrustAnchor, ServerName};
use tokio_rustls::TlsConnector;


//=============================================================================
// 常量和配置
//=============================================================================

/// 币安WebSocket URL
pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws";

/// WebSocket连接数量
/// 所有品种将平均分配到这些连接中
pub const WEBSOCKET_CONNECTION_COUNT: usize = 1;

//=============================================================================
// WebSocket配置
//=============================================================================

/// WebSocket配置接口
pub trait WebSocketConfig {
    /// 获取代理设置
    fn get_proxy_settings(&self) -> (bool, String, u16);
    /// 获取流列表
    fn get_streams(&self) -> Vec<String>;
}

/// 连续合约K线配置
#[derive(Clone)]
pub struct ContinuousKlineConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    /// 代理地址
    pub proxy_addr: String,
    /// 代理端口
    pub proxy_port: u16,
    /// 交易对列表
    pub symbols: Vec<String>,
    /// K线周期列表
    pub intervals: Vec<String>,
}

impl Default for ContinuousKlineConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_addr: PROXY_HOST.to_string(),
            proxy_port: PROXY_PORT,
            symbols: Vec::new(),
            intervals: Vec::new(),
        }
    }
}

impl WebSocketConfig for ContinuousKlineConfig {
    // #[instrument] 移除：简单的配置读取函数，追踪会产生噪音
    fn get_proxy_settings(&self) -> (bool, String, u16) {
        (self.use_proxy, self.proxy_addr.clone(), self.proxy_port)
    }

    // #[instrument] 移除：简单的流名称构建函数，追踪会产生噪音
    fn get_streams(&self) -> Vec<String> {
        let mut streams = Vec::new();
        for symbol in &self.symbols {
            for interval in &self.intervals {
                // 使用连续合约K线格式：<pair>_perpetual@continuousKline_<interval>
                let stream = format!("{}_perpetual@continuousKline_{}", symbol.to_lowercase(), interval);
                streams.push(stream);
            }
        }
        streams
    }
}

/// 归集交易配置
#[derive(Clone)]
pub struct AggTradeConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    /// 代理地址
    pub proxy_addr: String,
    /// 代理端口
    pub proxy_port: u16,
    /// 交易对列表
    pub symbols: Vec<String>,
}

impl Default for AggTradeConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_addr: PROXY_HOST.to_string(),
            proxy_port: PROXY_PORT,
            symbols: Vec::new(),
        }
    }
}

impl WebSocketConfig for AggTradeConfig {
    // #[instrument] 移除：简单的配置读取函数，追踪会产生噪音
    fn get_proxy_settings(&self) -> (bool, String, u16) {
        (self.use_proxy, self.proxy_addr.clone(), self.proxy_port)
    }

    // #[instrument] 移除：简单的流名称构建函数，追踪会产生噪音
    fn get_streams(&self) -> Vec<String> {
        self.symbols.iter()
            .map(|symbol| format!("{}@aggTrade", symbol.to_lowercase()))
            .collect()
    }
}

/// 创建订阅消息
#[instrument(target = "klcommon::websocket", skip_all)]
pub fn create_subscribe_message(streams: &[String]) -> String {
    json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    })
    .to_string()
}

//=============================================================================
// WebSocket客户端接口
//=============================================================================

/// WebSocket连接状态
#[derive(Debug, Clone)]
pub struct WebSocketConnection {
    pub id: usize,
    pub streams: Vec<String>,
    pub status: String,
    pub message_count: usize,
}

/// WebSocket客户端接口
pub trait WebSocketClient {
    /// 启动WebSocket客户端
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// 获取连接状态
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send;
}

//=============================================================================
// 数据结构
//=============================================================================

/// 币安原始归集交易数据
#[derive(Debug, Clone)]
pub struct BinanceRawAggTrade {
    pub event_type: String,
    pub event_time: u64,
    pub symbol: String,
    pub aggregate_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub trade_time: u64,
    pub is_buyer_maker: bool,
}

//=============================================================================
// 消息处理
//=============================================================================

/// 消息处理接口
pub trait MessageHandler {
    /// 处理WebSocket消息
    fn handle_message(&self, connection_id: usize, message: String) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// 临时的消息处理器，用于替代aggkline模块中的处理器
pub struct DummyMessageHandler {
    pub db: Arc<Database>,
}

impl MessageHandler for DummyMessageHandler {
    fn handle_message(&self, _connection_id: usize, _message: String) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // 临时实现，不做任何处理
            Ok(())
        }
    }
}

/// 归集交易消息处理器，用于K线聚合系统
pub struct AggTradeMessageHandler {
    pub message_count: Arc<std::sync::atomic::AtomicUsize>,
    pub error_count: Arc<std::sync::atomic::AtomicUsize>,
    pub trade_sender: Option<tokio::sync::mpsc::UnboundedSender<crate::klaggregate::AggTradeData>>,
}

impl AggTradeMessageHandler {
    #[instrument(target = "AggTradeMessageHandler", skip_all)]
    pub fn new(
        message_count: Arc<std::sync::atomic::AtomicUsize>,
        error_count: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            message_count,
            error_count,
            trade_sender: None,
        }
    }

    /// 创建带有交易数据发送器的消息处理器
    #[instrument(target = "AggTradeMessageHandler", skip_all)]
    pub fn with_trade_sender(
        message_count: Arc<std::sync::atomic::AtomicUsize>,
        error_count: Arc<std::sync::atomic::AtomicUsize>,
        trade_sender: tokio::sync::mpsc::UnboundedSender<crate::klaggregate::AggTradeData>,
    ) -> Self {
        Self {
            message_count,
            error_count,
            trade_sender: Some(trade_sender),
        }
    }
}

impl MessageHandler for AggTradeMessageHandler {
    fn handle_message(&self, connection_id: usize, message: String) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // 增加消息计数
            self.message_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // 添加详细的消息日志
            debug!(target: "MarketDataIngestor", "连接 {} 收到原始消息: {}", connection_id,
                if message.len() > 200 {
                    format!("{}...(长度:{})", &message[..200], message.len())
                } else {
                    message.clone()
                });

            // 解析归集交易消息
            match self.parse_agg_trade_message(&message).await {
                Ok(Some(agg_trade)) => {
                    info!(target: "MarketDataIngestor", "连接 {} 收到归集交易: {} {} @ {}",
                        connection_id, agg_trade.symbol, agg_trade.quantity, agg_trade.price);

                    // 将归集交易数据发送给TradeEventRouter
                    if let Some(ref sender) = self.trade_sender {
                        // 转换为AggTradeData格式
                        let trade_data = crate::klaggregate::AggTradeData {
                            symbol: agg_trade.symbol.clone(),
                            price: agg_trade.price.parse().unwrap_or(0.0),
                            quantity: agg_trade.quantity.parse().unwrap_or(0.0),
                            timestamp_ms: agg_trade.trade_time as i64,
                            is_buyer_maker: agg_trade.is_buyer_maker,
                            agg_trade_id: agg_trade.aggregate_trade_id as i64,
                            first_trade_id: agg_trade.first_trade_id as i64,
                            last_trade_id: agg_trade.last_trade_id as i64,
                        };

                        // 发送到交易事件路由器
                        if let Err(e) = sender.send(trade_data) {
                            error!(target: "MarketDataIngestor", "发送归集交易数据失败: {}", e);
                            self.error_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            debug!(target: "MarketDataIngestor", "成功发送归集交易数据到路由器");
                        }
                    } else {
                        warn!(target: "MarketDataIngestor", "没有配置交易数据发送器，跳过数据路由");
                    }

                    Ok(())
                }
                Ok(None) => {
                    // 非归集交易消息，可能是订阅确认等
                    info!(target: "MarketDataIngestor", "连接 {} 收到非归集交易消息，消息类型检查: {}",
                        connection_id,
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&message) {
                            if let Some(event_type) = json.get("e").and_then(|e| e.as_str()) {
                                format!("事件类型: {}", event_type)
                            } else if json.get("result").is_some() {
                                "订阅响应消息".to_string()
                            } else if json.get("id").is_some() {
                                "ID响应消息".to_string()
                            } else {
                                format!("未知消息格式: {}", json)
                            }
                        } else {
                            "非JSON消息".to_string()
                        });
                    Ok(())
                }
                Err(e) => {
                    self.error_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    error!(target: "MarketDataIngestor", "连接 {} 解析归集交易消息失败: {}, 原始消息: {}",
                        connection_id, e,
                        if message.len() > 100 {
                            format!("{}...", &message[..100])
                        } else {
                            message
                        });
                    Err(e)
                }
            }
        }
    }
}

impl AggTradeMessageHandler {
    /// 解析归集交易消息
    #[instrument(target = "AggTradeMessageHandler", skip_all, err)]
    async fn parse_agg_trade_message(&self, message: &str) -> Result<Option<BinanceRawAggTrade>> {
        // 解析JSON
        let json: serde_json::Value = serde_json::from_str(message)
            .map_err(|e| AppError::ParseError(format!("JSON解析失败: {}", e)))?;

        // 首先检查是否是包装在stream中的消息格式
        let data_json = if let Some(data) = json.get("data") {
            // 这是stream格式的消息，提取data部分
            debug!(target: "MarketDataIngestor", "检测到stream格式消息，提取data部分");
            data
        } else {
            // 这是直接格式的消息
            debug!(target: "MarketDataIngestor", "检测到直接格式消息");
            &json
        };

        // 检查是否是归集交易消息
        if let Some(event_type) = data_json.get("e").and_then(|e| e.as_str()) {
            if event_type == "aggTrade" {
                debug!(target: "MarketDataIngestor", "确认为归集交易消息，开始解析");

                // 解析归集交易数据
                let agg_trade = BinanceRawAggTrade {
                    event_type: event_type.to_string(),
                    event_time: data_json.get("E").and_then(|e| e.as_u64()).unwrap_or(0),
                    symbol: data_json.get("s").and_then(|s| s.as_str()).unwrap_or("").to_string(),
                    aggregate_trade_id: data_json.get("a").and_then(|a| a.as_u64()).unwrap_or(0),
                    price: data_json.get("p").and_then(|p| p.as_str()).unwrap_or("0").to_string(),
                    quantity: data_json.get("q").and_then(|q| q.as_str()).unwrap_or("0").to_string(),
                    first_trade_id: data_json.get("f").and_then(|f| f.as_u64()).unwrap_or(0),
                    last_trade_id: data_json.get("l").and_then(|l| l.as_u64()).unwrap_or(0),
                    trade_time: data_json.get("T").and_then(|t| t.as_u64()).unwrap_or(0),
                    is_buyer_maker: data_json.get("m").and_then(|m| m.as_bool()).unwrap_or(false),
                };

                debug!(target: "MarketDataIngestor", "归集交易解析成功: {} {} @ {}",
                    agg_trade.symbol, agg_trade.quantity, agg_trade.price);

                // 发出验证事件
                tracing::info!(
                    target: "MarketDataIngestor",
                    event_name = "trade_data_parsed",
                    symbol = %agg_trade.symbol,
                    price = agg_trade.price.parse::<f64>().unwrap_or(0.0),
                    quantity = agg_trade.quantity.parse::<f64>().unwrap_or(0.0),
                    timestamp_ms = agg_trade.trade_time as i64,
                    "交易数据解析完成"
                );

                return Ok(Some(agg_trade));
            } else {
                debug!(target: "MarketDataIngestor", "事件类型不是aggTrade: {}", event_type);
            }
        } else {
            debug!(target: "MarketDataIngestor", "消息中没有找到事件类型字段");
        }

        // 不是归集交易消息
        Ok(None)
    }
}

/// 处理WebSocket消息
#[instrument(target = "klcommon::websocket", skip_all)]
pub async fn process_messages<H: MessageHandler>(
    mut rx: mpsc::Receiver<(usize, String)>,
    handler: Arc<H>,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
) {
    info!(target: "MarketDataIngestor", log_type = "module", "🚀 启动WebSocket消息处理器");

    // 统计信息
    let mut _message_count = 0;
    let mut last_stats_time = Instant::now();
    let stats_interval = Duration::from_secs(30);

    // 处理消息
    while let Some((connection_id, text)) = rx.recv().await {
        _message_count += 1;

        // 每30秒输出一次统计信息
        let now = Instant::now();
        if now.duration_since(last_stats_time) >= stats_interval {
            //info!("WebSocket统计: 已处理 {} 条消息", message_count);

            // 输出每个连接的统计信息
            let connections_guard = connections.lock().await;
            for (_id, _conn) in connections_guard.iter() {
               //info!("连接 {}: {} 条消息, 状态: {}", id, conn.message_count, conn.status);
            }

            last_stats_time = now;
        }

        // 处理消息
        if let Err(e) = handler.handle_message(connection_id, text).await {
            error!(target: "MarketDataIngestor", "处理消息失败: {}", e);
        }
    }

    info!(target: "MarketDataIngestor", log_type = "module", "✅ WebSocket消息处理器已停止");
}

//=============================================================================
// 实现 hyper 的 Executor trait
//=============================================================================

/// 实现 hyper 的 Executor trait，用于 fastwebsockets 握手
struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

//=============================================================================
// 连接管理
//=============================================================================

/// WebSocket连接管理器
#[derive(Clone)]
pub struct ConnectionManager {
    /// 是否使用代理
    use_proxy: bool,
    /// 代理地址
    proxy_addr: String,
    /// 代理端口
    proxy_port: u16,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    // #[instrument] 移除：简单的构造函数，追踪会产生噪音
    pub fn new(use_proxy: bool, proxy_addr: String, proxy_port: u16) -> Self {
        Self {
            use_proxy,
            proxy_addr,
            proxy_port,
        }
    }

    /// 连接到WebSocket服务器
    #[instrument(target = "ConnectionManager", skip_all, err)]
    pub async fn connect(&self, streams: &[String]) -> Result<FragmentCollector<TokioIo<Upgraded>>> {
        // 设置主机和端口
        let host = "fstream.binance.com";
        let port = 443;
        let addr = format!("{}:{}", host, port);

        // 构建WebSocket URL
        let path = if streams.is_empty() {
            "/ws".to_string()
        } else if streams.len() == 1 {
            // 单个流使用直接连接格式
            format!("/ws/{}", streams[0])
        } else {
            // 多个流使用组合流订阅格式
            format!("/stream?streams={}", streams.join("/"))
        };

        info!(target: "MarketDataIngestor", log_type = "module", "🔗 连接到WebSocket: {}:{}{}", host, port, path);
        info!(target: "MarketDataIngestor", log_type = "module", "📡 订阅的流: {}", streams.join(", "));

        // 建立TCP连接（通过代理或直接）
        let tcp_stream = if self.use_proxy {
            info!(target: "MarketDataIngestor", log_type = "module", "🌐 通过代理 {}:{} 连接", self.proxy_addr, self.proxy_port);

            // 连接到代理
            let socks_stream = Socks5Stream::connect(
                (self.proxy_addr.as_str(), self.proxy_port),
                (host, port)
            )
            .await
            .map_err(|e| AppError::WebSocketError(format!("代理连接失败: {}", e)))?;

            // 获取TCP流
            socks_stream.into_inner()
        } else {
            // 直接连接
            TcpStream::connect(addr).await?
        };

        info!(target: "MarketDataIngestor", log_type = "module", "✅ TCP连接已建立");

        // 创建 TLS 连接
        let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
        root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(
            |ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            },
        ));

        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host)
            .map_err(|_| AppError::WebSocketError("无效的域名".to_string()))?;

        info!(target: "MarketDataIngestor", log_type = "module", "🔐 建立TLS连接...");
        let tls_stream = connector.connect(server_name, tcp_stream).await?;
        info!(target: "MarketDataIngestor", log_type = "module", "✅ TLS连接已建立");

        // 创建 HTTP 请求
        let req = Request::builder()
            .method("GET")
            .uri(format!("https://{}{}", host, path))
            .header("Host", host)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())
            .map_err(|e| AppError::WebSocketError(format!("创建HTTP请求失败: {}", e)))?;

        info!(target: "MarketDataIngestor", "执行WebSocket握手...");

        // 执行 WebSocket 握手
        let (ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await
            .map_err(|e| AppError::WebSocketError(format!("WebSocket握手失败: {}", e)))?;
        let mut ws_collector = FragmentCollector::new(ws);

        info!(target: "MarketDataIngestor", "WebSocket握手成功");

        // 如果使用的是组合流订阅格式（多个流），则需要发送订阅消息
        if path.contains("?streams=") && !streams.is_empty() {
            // 发送订阅消息
            let subscribe_msg = create_subscribe_message(streams);
            info!(target: "MarketDataIngestor", "发送订阅消息: {}", subscribe_msg);
            info!(target: "MarketDataIngestor", "订阅的流列表: {:?}", streams);

            ws_collector.write_frame(Frame::new(true, OpCode::Text, None, subscribe_msg.into_bytes().into())).await
                .map_err(|e| AppError::WebSocketError(format!("发送订阅消息失败: {}", e)))?;

            info!(target: "MarketDataIngestor", log_type = "module", "✅ 订阅消息发送成功，等待服务器响应");
        } else {
            info!(target: "MarketDataIngestor", "使用直接连接格式，无需发送额外订阅消息。路径: {}", path);
        }

        Ok(ws_collector)
    }

    /// 处理WebSocket消息
    #[instrument(target = "ConnectionManager", skip_all)]
    pub async fn handle_messages(
        &self,
        connection_id: usize,
        ws: &mut FragmentCollector<TokioIo<Upgraded>>,
        tx: mpsc::Sender<(usize, String)>,
        connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    ) {
        info!(target: "MarketDataIngestor", "开始处理连接 {} 的消息", connection_id);

        // 处理消息，添加超时处理
        loop {
            // 使用 tokio::time::timeout 添加超时处理
            match tokio::time::timeout(Duration::from_secs(30), ws.read_frame()).await {
                Ok(result) => {
                    match result {
                        Ok(frame) => {
                            match frame.opcode {
                                OpCode::Text => {
                                    // 将二进制数据转换为字符串
                                    let text = String::from_utf8(frame.payload.to_vec())
                                        .unwrap_or_else(|_| "无效的UTF-8数据".to_string());

                                    // 更新消息计数
                                    {
                                        let mut connections = connections.lock().await;
                                        if let Some(conn) = connections.get_mut(&connection_id) {
                                            conn.message_count += 1;
                                        }
                                    }

                                    // 发送消息到处理器
                                    if let Err(e) = tx.send((connection_id, text)).await {
                                        error!(target: "MarketDataIngestor", "发送消息到处理器失败: {}", e);
                                        break;
                                    }
                                },
                                OpCode::Binary => {
                                    debug!(target: "MarketDataIngestor", "收到二进制消息，长度: {}", frame.payload.len());
                                },
                                OpCode::Ping => {
                                    debug!(target: "MarketDataIngestor", "收到Ping，发送Pong");
                                    if let Err(e) = ws.write_frame(Frame::new(true, OpCode::Pong, None, frame.payload)).await {
                                        error!(target: "MarketDataIngestor", "发送Pong失败: {}", e);
                                        break;
                                    }
                                },
                                OpCode::Pong => {
                                    debug!(target: "MarketDataIngestor", "收到Pong");
                                },
                                OpCode::Close => {
                                    info!(target: "MarketDataIngestor", "收到关闭消息，连接将关闭");
                                    break;
                                },
                                _ => {
                                    debug!(target: "MarketDataIngestor", "收到其他类型的消息");
                                }
                            }
                        },
                        Err(e) => {
                            error!(target: "MarketDataIngestor", "WebSocket错误: {}", e);
                            break;
                        }
                    }
                },
                Err(_) => {
                    // 超时，发送ping以保持连接
                    debug!(target: "MarketDataIngestor", "WebSocket连接超时，发送Ping");
                    if let Err(e) = ws.write_frame(Frame::new(true, OpCode::Ping, None, vec![].into())).await {
                        error!(target: "MarketDataIngestor", "发送Ping失败: {}", e);
                        break;
                    }
                }
            }
        }

        // 更新连接状态
        {
            let mut connections = connections.lock().await;
            if let Some(conn) = connections.get_mut(&connection_id) {
                conn.status = "已断开".to_string();
            }
        }

        info!(target: "MarketDataIngestor", "连接 {} 已关闭", connection_id);
    }
}

//=============================================================================
// 连续合约K线客户端
//=============================================================================

/// 连续合约K线客户端
pub struct ContinuousKlineClient {
    config: ContinuousKlineConfig,
    db: Arc<Database>,
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
}

impl ContinuousKlineClient {
    /// 创建新的连续合约K线客户端
    #[instrument(target = "ContinuousKlineClient", skip_all)]
    pub fn new(config: ContinuousKlineConfig, db: Arc<Database>) -> Self {
        Self {
            config,
            db,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
        }
    }
}

impl WebSocketClient for ContinuousKlineClient {
    /// 启动客户端
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
        info!(target: "MarketDataIngestor", "启动连续合约K线客户端");
        info!(target: "MarketDataIngestor", "使用代理: {}", self.config.use_proxy);

        if self.config.use_proxy {
            info!(target: "MarketDataIngestor", "代理地址: {}:{}", self.config.proxy_addr, self.config.proxy_port);
        }

        // 确保日志目录存在
        let log_dir = Path::new("logs");
        if !log_dir.exists() {
            create_dir_all(log_dir)?;
        }

        // 创建连接管理器
        let (use_proxy, proxy_addr, proxy_port) = self.config.get_proxy_settings();
        let connection_manager = ConnectionManager::new(
            use_proxy,
            proxy_addr,
            proxy_port,
        );

        // 创建消息通道
        let (tx, rx) = mpsc::channel(1000);

        // 获取所有流
        let streams = self.config.get_streams();
        info!(target: "MarketDataIngestor", "总共 {} 个流需要订阅", streams.len());

        // 使用固定的连接数
        let connection_count = WEBSOCKET_CONNECTION_COUNT;
        info!(target: "MarketDataIngestor", "使用 {} 个WebSocket连接", connection_count);

        // 计算每个连接的流数量
        let streams_per_connection = (streams.len() + connection_count - 1) / connection_count;
        info!(target: "MarketDataIngestor", "每个连接平均处理 {} 个流", streams_per_connection);

        // 分配流到连接
        let mut connection_streams = Vec::new();

        for chunk in streams.chunks(streams_per_connection) {
            connection_streams.push(chunk.to_vec());
        }

        // 创建消息处理器
        let handler = Arc::new(ContinuousKlineMessageHandler {
            db: self.db.clone(),
        });
        let connections_clone = self.connections.clone();

        let message_handler = tokio::spawn(async move {
            process_messages(rx, handler, connections_clone).await;
        }.instrument(tracing::info_span!("continuous_kline_message_handler")));

        // 启动所有连接
        let mut connection_handles = Vec::new();

        for streams in connection_streams {
            let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);
            let tx_clone = tx.clone();
            let connection_manager_clone = connection_manager.clone();
            let connections_clone = self.connections.clone();

            // 更新连接状态
            {
                let mut connections = connections_clone.lock().await;
                connections.insert(connection_id, WebSocketConnection {
                    id: connection_id,
                    streams: streams.clone(),
                    status: "初始化".to_string(),
                    message_count: 0,
                });
            }

            // 启动连接
            let handle = tokio::spawn(async move {
                // 更新状态
                {
                    let mut connections = connections_clone.lock().await;
                    if let Some(conn) = connections.get_mut(&connection_id) {
                        conn.status = "连接中".to_string();
                    }
                }

                // 建立连接
                match connection_manager_clone.connect(&streams).await {
                    Ok(mut ws) => {
                        // 更新状态
                        {
                            let mut connections = connections_clone.lock().await;
                            if let Some(conn) = connections.get_mut(&connection_id) {
                                conn.status = "已连接".to_string();
                            }
                        }

                        info!(target: "MarketDataIngestor", "连接 {} 已建立，订阅 {} 个流", connection_id, streams.len());

                        // 处理消息
                        connection_manager_clone.handle_messages(connection_id, &mut ws, tx_clone, connections_clone).await;
                    }
                    Err(e) => {
                        // 更新状态
                        {
                            let mut connections = connections_clone.lock().await;
                            if let Some(conn) = connections.get_mut(&connection_id) {
                                conn.status = format!("连接失败: {}", e);
                            }
                        }

                        error!(target: "MarketDataIngestor", "连接 {} 失败: {}", connection_id, e);
                    }
                }
            }.instrument(tracing::info_span!("continuous_kline_connection", connection_id = connection_id)));

            connection_handles.push(handle);
        }

        // 等待所有连接完成
        join_all(connection_handles).await;

        // 等待消息处理器完成
        if let Err(e) = message_handler.await {
            error!(target: "MarketDataIngestor", "消息处理器错误: {}", e);
        }

        info!(target: "MarketDataIngestor", "连续合约K线客户端已停止");
        Ok(())
        }
    }

    /// 获取连接状态
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send {
        async move {
            let connections = self.connections.lock().await;
            connections.values().cloned().collect()
        }
    }
}

/// 连续合约K线消息处理器
struct ContinuousKlineMessageHandler {
    db: Arc<Database>,
}

impl MessageHandler for ContinuousKlineMessageHandler {
    fn handle_message(&self, _connection_id: usize, text: String) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
        // 解析消息
        match parse_message(&text) {
            Ok(Some((symbol, interval, kline_data))) => {
                // 处理K线数据
                process_kline_data(&symbol, &interval, &kline_data, &self.db).await;
            }
            Ok(None) => {
                // 非K线消息，忽略
            }
            Err(e) => {
                error!(target: "MarketDataIngestor", "解析消息失败: {}", e);
            }
        }

        Ok(())
        }
    }
}

/// 解析WebSocket消息
#[instrument(target = "klcommon::websocket", skip_all, err)]
fn parse_message(text: &str) -> Result<Option<(String, String, KlineData)>> {
    // 解析JSON
    let json: Value = serde_json::from_str(text)?;

    // 检查是否是连续合约K线消息
    if let Some(e_value) = json.get("e").and_then(|e| e.as_str()) {
        if e_value == "continuous_kline" {
            // 获取交易对
            let symbol = json.get("ps").and_then(|s| s.as_str()).unwrap_or("").to_uppercase();

            // 获取K线数据
            if let Some(k) = json.get("k") {
                // 获取K线周期
                let interval = k.get("i").and_then(|i| i.as_str()).unwrap_or("").to_string();

                // 获取K线数据
                let start_time = k.get("t").and_then(|t| t.as_i64()).unwrap_or(0);
                let end_time = k.get("T").and_then(|t| t.as_i64()).unwrap_or(0);
                let is_closed = k.get("x").and_then(|x| x.as_bool()).unwrap_or(false);
                let open = k.get("o").and_then(|o| o.as_str()).unwrap_or("0").to_string();
                let high = k.get("h").and_then(|h| h.as_str()).unwrap_or("0").to_string();
                let low = k.get("l").and_then(|l| l.as_str()).unwrap_or("0").to_string();
                let close = k.get("c").and_then(|c| c.as_str()).unwrap_or("0").to_string();
                let volume = k.get("v").and_then(|v| v.as_str()).unwrap_or("0").to_string();
                let quote_volume = k.get("q").and_then(|q| q.as_str()).unwrap_or("0").to_string();
                let number_of_trades = k.get("n").and_then(|n| n.as_i64()).unwrap_or(0);
                let taker_buy_volume = k.get("V").and_then(|v| v.as_str()).unwrap_or("0").to_string();
                let taker_buy_quote_volume = k.get("Q").and_then(|q| q.as_str()).unwrap_or("0").to_string();
                let ignore = k.get("B").and_then(|b| b.as_str()).unwrap_or("0").to_string();

                let kline_data = KlineData {
                    start_time,
                    end_time,
                    interval: interval.clone(),
                    first_trade_id: k.get("f").and_then(|f| f.as_i64()).unwrap_or(0),
                    last_trade_id: k.get("L").and_then(|l| l.as_i64()).unwrap_or(0),
                    is_closed,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quote_volume,
                    number_of_trades,
                    taker_buy_volume,
                    taker_buy_quote_volume,
                    ignore,
                };

                return Ok(Some((symbol, interval, kline_data)));
            }
        }
    }

    // 不是K线消息
    Ok(None)
}

/// 处理K线数据
#[instrument(target = "klcommon::websocket", skip_all)]
async fn process_kline_data(symbol: &str, interval: &str, kline_data: &KlineData, db: &Arc<Database>) {
    // 输出处理K线数据的详细信息
    info!(target: "MarketDataIngestor", "开始处理K线数据: symbol={}, interval={}, is_closed={}, start_time={}, end_time={}",
          symbol, interval, kline_data.is_closed, kline_data.start_time, kline_data.end_time);

    // 将KlineData转换为标准Kline格式
    let kline = kline_data.to_kline();

    // 根据is_closed决定是插入新记录还是更新现有记录
    if kline_data.is_closed {
        // K线已收盘，检查数据库中是否已存在
        match db.get_kline_by_time(symbol, interval, kline.open_time) {
            Ok(existing_kline) => {
                if existing_kline.is_some() {
                    // 更新现有K线
                    match db.update_kline(symbol, interval, &kline) {
                        Ok(_) => {
                            info!(target: "MarketDataIngestor", "更新K线成功: symbol={}, interval={}, open_time={}",
                                  symbol, interval, kline.open_time);
                        },
                        Err(e) => {
                            error!(target: "MarketDataIngestor", "更新K线失败: {}", e);
                        }
                    }
                } else {
                    // 插入新K线
                    match db.insert_kline(symbol, interval, &kline) {
                        Ok(_) => {
                            info!(target: "MarketDataIngestor", "插入K线成功: symbol={}, interval={}, open_time={}",
                                  symbol, interval, kline.open_time);
                        },
                        Err(e) => {
                            error!(target: "MarketDataIngestor", "插入K线失败: {}", e);
                        }
                    }
                }
            },
            Err(e) => {
                error!(target: "MarketDataIngestor", "查询K线失败: {}", e);
            }
        }
    } else {
        // K线未收盘，更新现有K线或插入新K线
        match db.get_kline_by_time(symbol, interval, kline.open_time) {
            Ok(existing_kline) => {
                if existing_kline.is_some() {
                    // 更新现有K线
                    match db.update_kline(symbol, interval, &kline) {
                        Ok(_) => {
                            info!(target: "MarketDataIngestor", "更新未收盘K线成功: symbol={}, interval={}, open_time={}",
                                  symbol, interval, kline.open_time);
                        },
                        Err(e) => {
                            error!(target: "MarketDataIngestor", "更新未收盘K线失败: {}", e);
                        }
                    }
                } else {
                    // 插入新K线
                    match db.insert_kline(symbol, interval, &kline) {
                        Ok(_) => {
                            info!(target: "MarketDataIngestor", "插入未收盘K线成功: symbol={}, interval={}, open_time={}",
                                  symbol, interval, kline.open_time);
                        },
                        Err(e) => {
                            error!(target: "MarketDataIngestor", "插入未收盘K线失败: {}", e);
                        }
                    }
                }
            },
            Err(e) => {
                error!(target: "MarketDataIngestor", "查询未收盘K线失败: {}", e);
            }
        }
    }
}



//=============================================================================
// 归集交易客户端
//=============================================================================

/// 归集交易客户端
pub struct AggTradeClient {
    config: AggTradeConfig,
    #[allow(dead_code)]
    db: Arc<Database>, // 数据库连接，预留用于未来功能
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    #[allow(dead_code)]
    intervals: Vec<String>, // 支持的时间周期列表
    /// 外部注入的消息处理器（可选）
    external_handler: Option<Arc<AggTradeMessageHandler>>,
}

impl AggTradeClient {
    /// 创建新的归集交易客户端
    #[instrument(target = "AggTradeClient", skip_all)]
    pub fn new(config: AggTradeConfig, db: Arc<Database>, intervals: Vec<String>) -> Self {
        Self {
            config,
            db,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
            intervals,
            external_handler: None,
        }
    }

    /// 创建带有外部消息处理器的归集交易客户端
    #[instrument(target = "AggTradeClient", skip_all)]
    pub fn new_with_handler(
        config: AggTradeConfig,
        db: Arc<Database>,
        intervals: Vec<String>,
        handler: Arc<AggTradeMessageHandler>
    ) -> Self {
        Self {
            config,
            db,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
            intervals,
            external_handler: Some(handler),
        }
    }
}

impl WebSocketClient for AggTradeClient {
    /// 启动客户端
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            info!(target: "MarketDataIngestor", "启动归集交易客户端");
            info!(target: "MarketDataIngestor", "使用代理: {}", self.config.use_proxy);

            if self.config.use_proxy {
                info!(target: "MarketDataIngestor", "代理地址: {}:{}", self.config.proxy_addr, self.config.proxy_port);
            }

            // 确保日志目录存在
            let log_dir = Path::new("logs");
            if !log_dir.exists() {
                create_dir_all(log_dir)?;
            }

            // 创建连接管理器
            let (use_proxy, proxy_addr, proxy_port) = self.config.get_proxy_settings();
            let connection_manager = ConnectionManager::new(
                use_proxy,
                proxy_addr,
                proxy_port,
            );

            // 创建消息通道
            let (tx, rx) = mpsc::channel(1000);

            // 获取所有流
            let streams = self.config.get_streams();
            info!(target: "MarketDataIngestor", "总共 {} 个流需要订阅", streams.len());
            info!(target: "MarketDataIngestor", "订阅的流详情: {:?}", streams);
            info!(target: "MarketDataIngestor", "配置的交易对: {:?}", self.config.symbols);

            // 使用固定的连接数
            let connection_count = WEBSOCKET_CONNECTION_COUNT;
            info!(target: "MarketDataIngestor", "使用 {} 个WebSocket连接", connection_count);

            // 计算每个连接的流数量
            let streams_per_connection = (streams.len() + connection_count - 1) / connection_count;
            info!(target: "MarketDataIngestor", "每个连接平均处理 {} 个流", streams_per_connection);

            // 分配流到连接
            let mut connection_streams = Vec::new();

            for (index, chunk) in streams.chunks(streams_per_connection).enumerate() {
                let chunk_vec = chunk.to_vec();
                info!(target: "MarketDataIngestor", "连接 {} 将处理 {} 个流: {:?}",
                    index + 1, chunk_vec.len(), chunk_vec);
                connection_streams.push(chunk_vec);
            }

            // 创建消息处理器
            // 优先使用外部传入的处理器，否则创建默认的
            let handler = if let Some(external_handler) = &self.external_handler {
                external_handler.clone()
            } else {
                Arc::new(AggTradeMessageHandler::new(
                    Arc::new(AtomicUsize::new(0)),
                    Arc::new(AtomicUsize::new(0)),
                ))
            };
            let connections_clone = self.connections.clone();

            let message_handler = tokio::spawn(async move {
                process_messages(rx, handler, connections_clone).await;
            }.instrument(tracing::info_span!("websocket_message_handler")));

            // 启动所有连接
            let mut connection_handles = Vec::new();

            for streams in connection_streams {
                let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);
                let tx_clone = tx.clone();
                let connection_manager_clone = connection_manager.clone();
                let connections_clone = self.connections.clone();

                // 更新连接状态
                {
                    let mut connections = connections_clone.lock().await;
                    connections.insert(connection_id, WebSocketConnection {
                        id: connection_id,
                        streams: streams.clone(),
                        status: "初始化".to_string(),
                        message_count: 0,
                    });
                }

                // 启动连接
                let handle = tokio::spawn(async move {
                    // 更新状态
                    {
                        let mut connections = connections_clone.lock().await;
                        if let Some(conn) = connections.get_mut(&connection_id) {
                            conn.status = "连接中".to_string();
                        }
                    }

                    // 建立连接
                    match connection_manager_clone.connect(&streams).await {
                        Ok(mut ws) => {
                            // 更新状态
                            {
                                let mut connections = connections_clone.lock().await;
                                if let Some(conn) = connections.get_mut(&connection_id) {
                                    conn.status = "已连接".to_string();
                                }
                            }

                            info!(target: "MarketDataIngestor", "连接 {} 已建立，订阅 {} 个流", connection_id, streams.len());

                            // 处理消息
                            connection_manager_clone.handle_messages(connection_id, &mut ws, tx_clone, connections_clone).await;
                        }
                        Err(e) => {
                            // 更新状态
                            {
                                let mut connections = connections_clone.lock().await;
                                if let Some(conn) = connections.get_mut(&connection_id) {
                                    conn.status = format!("连接失败: {}", e);
                                }
                            }

                            error!(target: "MarketDataIngestor", "连接 {} 失败: {}", connection_id, e);
                        }
                    }
                }.instrument(tracing::info_span!("websocket_connection", connection_id = connection_id)));

                connection_handles.push(handle);
            }

            // 等待所有连接完成
            join_all(connection_handles).await;

            // 等待消息处理器完成
            if let Err(e) = message_handler.await {
                error!(target: "MarketDataIngestor", "消息处理器错误: {}", e);
            }

            info!(target: "MarketDataIngestor", "归集交易客户端已停止");
            Ok(())
        }
    }

    /// 获取连接状态
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send {
        async move {
            let connections = self.connections.lock().await;
            connections.values().cloned().collect()
        }
    }
}
