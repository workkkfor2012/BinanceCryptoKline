use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant};
use tracing::{info, warn, error};
use fastwebsockets::{Frame, OpCode};

use kline_server::klcommon::{
    BinanceApi,
    websocket::{
        AggTradeMessageHandler, AggTradeData, ConnectionManager, MessageHandler
    },
    Result
};

/// 时间验证统计
#[derive(Debug, Default)]
struct TimeValidationStats {
    /// 总消息数
    total_messages: usize,
    /// 时间字段异常数量
    time_anomalies: usize,
    /// 事件时间与交易时间差异过大的数量
    large_time_diff: usize,
    /// 时间戳为0的数量
    zero_timestamps: usize,
    /// 未来时间戳数量
    future_timestamps: usize,
    /// 过旧时间戳数量（超过1小时前）
    old_timestamps: usize,
}

/// aggtrade时间字段测试器
struct AggTradeTimeValidator {
    api: BinanceApi,
    stats: Arc<std::sync::Mutex<TimeValidationStats>>,
    start_time: Instant,
    server_time_offset: i64, // 服务器时间与本地时间的偏移量（毫秒）
}

impl AggTradeTimeValidator {
    /// 创建新的时间验证器
    pub fn new() -> Self {
        Self {
            api: BinanceApi::new(),
            stats: Arc::new(std::sync::Mutex::new(TimeValidationStats::default())),
            start_time: Instant::now(),
            server_time_offset: 0,
        }
    }

    /// 同步服务器时间
    pub async fn sync_server_time(&mut self) -> Result<()> {
        info!("🕐 正在同步币安服务器时间...");
        
        let server_time = self.api.get_server_time().await?;
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        self.server_time_offset = server_time.server_time - local_time;
        
        info!(
            "✅ 服务器时间同步完成 - 服务器时间: {}, 本地时间: {}, 偏移: {}ms",
            server_time.server_time,
            local_time,
            self.server_time_offset
        );
        
        Ok(())
    }

    /// 获取所有U本位永续合约品种（限制数量用于测试）
    pub async fn get_test_symbols(&self, limit: usize) -> Result<Vec<String>> {
        info!("📡 获取U本位永续合约品种...");
        
        let (all_symbols, _delisted_symbols) = self.api.get_trading_usdt_perpetual_symbols().await?;
        let test_symbols: Vec<String> = all_symbols.into_iter().take(limit).collect();
        
        info!("✅ 获取到 {} 个测试品种: {:?}", test_symbols.len(), test_symbols);
        Ok(test_symbols)
    }

    /// 验证单个交易数据的时间字段
    fn validate_trade_time(&self, trade: &AggTradeData) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_messages += 1;

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        let server_adjusted_time = current_time + self.server_time_offset;

        // 检查时间戳是否为0
        if trade.timestamp_ms == 0 || trade.event_time_ms == 0 {
            stats.zero_timestamps += 1;
            warn!(
                "⚠️  时间戳为0 - 品种: {}, 交易时间: {}, 事件时间: {}",
                trade.symbol, trade.timestamp_ms, trade.event_time_ms
            );
        }

        // 检查是否为未来时间戳（允许5秒误差）
        let future_threshold = server_adjusted_time + 5000;
        if trade.timestamp_ms > future_threshold || trade.event_time_ms > future_threshold {
            stats.future_timestamps += 1;
            warn!(
                "⚠️  未来时间戳 - 品种: {}, 交易时间: {}, 事件时间: {}, 当前服务器时间: {}",
                trade.symbol, trade.timestamp_ms, trade.event_time_ms, server_adjusted_time
            );
        }

        // 检查是否为过旧时间戳（超过1小时前）
        let old_threshold = server_adjusted_time - 3600000; // 1小时前
        if trade.timestamp_ms < old_threshold || trade.event_time_ms < old_threshold {
            stats.old_timestamps += 1;
            warn!(
                "⚠️  过旧时间戳 - 品种: {}, 交易时间: {}, 事件时间: {}, 1小时前: {}",
                trade.symbol, trade.timestamp_ms, trade.event_time_ms, old_threshold
            );
        }

        // 检查事件时间与交易时间的差异
        let time_diff = (trade.event_time_ms - trade.timestamp_ms).abs();
        if time_diff > 1000 { // 差异超过1秒
            stats.large_time_diff += 1;
            warn!(
                "⚠️  时间差异过大 - 品种: {}, 交易时间: {}, 事件时间: {}, 差异: {}ms",
                trade.symbol, trade.timestamp_ms, trade.event_time_ms, time_diff
            );
        }

        // 每1000条消息输出一次正常的交易数据示例
        if stats.total_messages % 1000 == 0 {
            info!(
                "📊 正常交易数据示例 - 品种: {}, 价格: {}, 数量: {}, 交易时间: {}, 事件时间: {}, 时间差: {}ms",
                trade.symbol, trade.price, trade.quantity, 
                trade.timestamp_ms, trade.event_time_ms, time_diff
            );
        }
    }

    /// 打印统计信息
    fn print_stats(&self) {
        let stats = self.stats.lock().unwrap();
        let elapsed = self.start_time.elapsed();
        
        info!("📈 时间验证统计报告:");
        info!("  运行时间: {:.2}秒", elapsed.as_secs_f64());
        info!("  总消息数: {}", stats.total_messages);
        info!("  消息速率: {:.2} msg/s", stats.total_messages as f64 / elapsed.as_secs_f64());
        info!("  时间戳为0: {}", stats.zero_timestamps);
        info!("  未来时间戳: {}", stats.future_timestamps);
        info!("  过旧时间戳: {}", stats.old_timestamps);
        info!("  时间差异过大: {}", stats.large_time_diff);
        info!("  总异常数: {}", 
            stats.zero_timestamps + stats.future_timestamps + 
            stats.old_timestamps + stats.large_time_diff
        );
        
        if stats.total_messages > 0 {
            let anomaly_rate = (stats.zero_timestamps + stats.future_timestamps + 
                               stats.old_timestamps + stats.large_time_diff) as f64 
                               / stats.total_messages as f64 * 100.0;
            info!("  异常率: {:.4}%", anomaly_rate);
        }
    }

    /// 运行时间验证测试
    pub async fn run_validation_test(&mut self, test_duration_secs: u64) -> Result<()> {
        // 1. 同步服务器时间
        self.sync_server_time().await?;

        // 2. 获取测试品种（限制为10个品种以避免过多连接）
        let symbols = self.get_test_symbols(10).await?;
        
        // 3. 创建交易数据通道
        let (trade_sender, mut trade_receiver) = tokio::sync::mpsc::unbounded_channel();

        // 4. 创建消息处理器
        let message_handler = Arc::new(AggTradeMessageHandler::with_trade_sender(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            trade_sender,
        ));

        // 5. 启动数据验证任务
        let stats_clone = self.stats.clone();
        let validation_task = tokio::spawn(async move {
            while let Some(trade_data) = trade_receiver.recv().await {
                let mut stats = stats_clone.lock().unwrap();
                stats.total_messages += 1;

                // 检查时间戳是否为0
                if trade_data.timestamp_ms == 0 || trade_data.event_time_ms == 0 {
                    stats.zero_timestamps += 1;
                    drop(stats);
                    warn!("⚠️  时间戳为0 - 品种: {}", trade_data.symbol);
                    continue;
                }

                // 计算事件时间与交易时间的差异
                let time_diff = (trade_data.event_time_ms - trade_data.timestamp_ms).abs();

                if time_diff > 1000 { // 差异超过1秒
                    stats.large_time_diff += 1;
                    drop(stats);
                    warn!("⚠️  时间差异过大 - 品种: {}, 差异: {}ms", trade_data.symbol, time_diff);
                    continue;
                }

                // 每100条消息输出一次时间差异信息
                if stats.total_messages % 100 == 0 {
                    drop(stats);
                    info!("时间差异 #{}: {}ms ({})",
                        stats_clone.lock().unwrap().total_messages,
                        time_diff,
                        trade_data.symbol
                    );
                }
            }
        });

        // 8. 启动统计报告任务
        let stats_clone = self.stats.clone();
        let start_time = self.start_time;
        let stats_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let stats = stats_clone.lock().unwrap();
                let elapsed = start_time.elapsed();
                if stats.total_messages > 0 {
                    info!(
                        "📊 实时统计 - 消息数: {}, 速率: {:.1} msg/s, 异常: {}",
                        stats.total_messages,
                        stats.total_messages as f64 / elapsed.as_secs_f64(),
                        stats.zero_timestamps + stats.future_timestamps + 
                        stats.old_timestamps + stats.large_time_diff
                    );
                }
            }
        });

        // 7. 启动WebSocket连接任务
        info!("🚀 启动WebSocket连接，测试时长: {}秒", test_duration_secs);
        let websocket_task = tokio::spawn(async move {
            Self::run_websocket_connection(symbols, message_handler).await
        });

        // 8. 等待测试完成
        tokio::time::sleep(Duration::from_secs(test_duration_secs)).await;

        // 9. 停止任务并打印最终统计
        validation_task.abort();
        stats_task.abort();
        websocket_task.abort();

        self.print_stats();
        Ok(())
    }

    /// 运行WebSocket连接（基于klagg_sub_threads的模式）
    async fn run_websocket_connection(
        symbols: Vec<String>,
        handler: Arc<AggTradeMessageHandler>
    ) -> Result<()> {
        // 创建连接管理器
        let connection_manager = ConnectionManager::new(
            true,  // use_proxy
            "127.0.0.1".to_string(),
            1080,
        );

        loop {
            // 建立连接
            let mut ws = match connection_manager.connect().await {
                Ok(ws) => {
                    info!("✅ WebSocket连接成功");
                    ws
                },
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // 构造订阅消息
            let streams: Vec<String> = symbols.iter()
                .map(|s| format!("{}@aggTrade", s.to_lowercase()))
                .collect();

            let subscribe_msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": streams,
                "id": 1
            }).to_string();

            // 发送订阅消息
            let frame = Frame::text(subscribe_msg.into_bytes().into());
            if let Err(_) = ws.write_frame(frame).await {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            info!("📡 开始接收交易数据，监控时间差异...");

            // 消息循环
            loop {
                match ws.read_frame().await {
                    Ok(frame) => {
                        match frame.opcode {
                            OpCode::Text => {
                                let text = String::from_utf8_lossy(&frame.payload).to_string();

                                // 跳过订阅确认消息
                                if text.contains("\"result\":null") {
                                    continue;
                                }

                                // 处理交易数据
                                let _ = handler.handle_message(0, text).await;
                            },
                            OpCode::Close => {
                                break;
                            },
                            OpCode::Ping => {
                                let _ = ws.write_frame(Frame::pong(frame.payload)).await;
                            },
                            _ => {}
                        }
                    },
                    Err(_) => {
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志 - 只显示INFO级别，过滤掉WebSocket连接的详细日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_env_filter("test_aggtrade_time=info,kline_server::klcommon::websocket=warn")
        .init();

    info!("🧪 aggtrade时间差异监控测试开始");

    // 创建验证器并运行测试
    let mut validator = AggTradeTimeValidator::new();

    // 运行60秒的验证测试
    if let Err(e) = validator.run_validation_test(60).await {
        error!("测试运行失败: {}", e);
        return Err(e);
    }

    info!("✅ aggtrade时间差异监控测试完成");
    Ok(())
}
