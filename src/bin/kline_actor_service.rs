// K线Actor服务 - 基于Actor模型的K线合成系统
use kline_server::klcommon::{AppError, Result, BinanceApi, Database, PROXY_HOST, PROXY_PORT};
use kline_server::klcommon::aggkline::{
    AppAggTrade, KlineBar, KlineActor,
    run_trade_parser_task, run_app_trade_dispatcher_task,
    SqliteStorage, partition_symbols, run_websocket_connection_task,
    KLINE_PERIODS_MS, NUM_WEBSOCKET_CONNECTIONS, AGG_TRADE_STREAM_NAME
};
use kline_server::klcommon::websocket::{ConnectionManager, create_subscribe_message};

// 币安WebSocket URL
const BINANCE_WS_URL: &str = "wss://fstream.binance.com/stream";
use log::{info, error, debug};
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::sync::Arc;
use futures_util::StreamExt;
use fastwebsockets::OpCode;

// 代理设置已移至BinanceApi::new()中

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();

    info!("启动K线Actor服务");
    info!("使用 {} 个WebSocket连接", NUM_WEBSOCKET_CONNECTIONS);
    info!("支持的K线周期: {:?}", KLINE_PERIODS_MS);

    // 初始化数据库
    let _db = Arc::new(Database::new("data/klines.db")?);

    // 初始化API客户端
    let api = BinanceApi::new();

    // 获取所有U本位合约交易对
    info!("获取所有U本位合约交易对");
    let all_symbols = fetch_all_usdt_symbols(&api).await?;
    info!("获取到 {} 个交易对", all_symbols.len());

    // 初始化Channels
    info!("初始化通道");
    let (raw_frame_sender, raw_frame_receiver) =
        mpsc::channel::<String>(1024 * NUM_WEBSOCKET_CONNECTIONS); // 原始JSON帧

    let (app_trade_sender, app_trade_receiver) =
        mpsc::channel::<AppAggTrade>(2048); // 解析后的交易

    let (completed_kline_sender, completed_kline_receiver) =
        mpsc::channel::<KlineBar>(1024); // 完成的K线

    // 启动WebSocket连接管理任务
    info!("启动WebSocket连接管理任务");
    let symbol_partitions = partition_symbols(&all_symbols, NUM_WEBSOCKET_CONNECTIONS);

    let mut connection_handles = Vec::new();

    // 创建连接管理器
    let connection_manager = ConnectionManager::new(true, PROXY_HOST.to_string(), PROXY_PORT);
    info!("创建连接管理器，使用代理: {}:{}", PROXY_HOST, PROXY_PORT);

    for (i, partition) in symbol_partitions.into_iter().enumerate() {
        let stream_names: Vec<String> = partition
            .iter()
            .map(|s| format!("{}@{}", s.to_lowercase(), AGG_TRADE_STREAM_NAME))
            .collect();

        let raw_sender_clone = raw_frame_sender.clone();
        let task_id = format!("WS-Conn-{}", i + 1);
        let connection_manager_clone = connection_manager.clone();

        let handle = tokio::spawn(async move {
            info!("[{}] 开始连接WebSocket，订阅 {} 个流", task_id, stream_names.len());

            // 使用ConnectionManager连接WebSocket
            match connection_manager_clone.connect(&stream_names).await {
                Ok(mut ws) => {
                    info!("[{}] WebSocket连接成功", task_id);

                    // 处理消息
                    loop {
                        match ws.read_frame().await {
                            Ok(frame) => {
                                match frame.opcode {
                                    OpCode::Text => {
                                        // 将二进制数据转换为字符串
                                        let text = String::from_utf8(frame.payload.to_vec())
                                            .unwrap_or_else(|_| "无效的UTF-8数据".to_string());

                                        // 发送消息到处理器
                                        if let Err(e) = raw_sender_clone.send(text).await {
                                            error!("[{}] 发送消息到处理器失败: {}", task_id, e);
                                            break;
                                        }
                                    },
                                    OpCode::Ping => {
                                        debug!("[{}] 收到Ping，发送Pong", task_id);
                                        if let Err(e) = ws.write_frame(fastwebsockets::Frame::new(true, OpCode::Pong, None, frame.payload)).await {
                                            error!("[{}] 发送Pong失败: {}", task_id, e);
                                            break;
                                        }
                                    },
                                    OpCode::Close => {
                                        info!("[{}] 收到关闭消息，连接将关闭", task_id);
                                        break;
                                    },
                                    _ => {}
                                }
                            },
                            Err(e) => {
                                error!("[{}] WebSocket读取错误: {}", task_id, e);
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("[{}] WebSocket连接失败: {}", task_id, e);
                }
            }
        });

        connection_handles.push(handle);
    }

    // 不再需要原始发送器
    drop(raw_frame_sender);

    // 启动交易解析任务
    info!("启动交易解析任务");
    let _parser_handle = tokio::spawn(async move {
        if let Err(e) = run_trade_parser_task(raw_frame_receiver, app_trade_sender).await {
            error!("交易解析任务失败: {}", e);
        }
    });

    // 为每个交易对启动一个KlineActor
    info!("为每个交易对启动KlineActor");
    let mut actor_senders = HashMap::new();
    let mut actor_handles = Vec::new();

    for symbol in all_symbols.iter() {
        let (actor_tx, actor_rx) = mpsc::channel::<AppAggTrade>(256); // 每个Actor自己的队列
        actor_senders.insert(symbol.clone(), actor_tx);

        let kline_actor = KlineActor::new(
            symbol.clone(),
            KLINE_PERIODS_MS.to_vec(),
            actor_rx,
            completed_kline_sender.clone(),
        );

        let symbol_clone = symbol.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = kline_actor.run().await {
                error!("[KlineActor-{}] 任务失败: {}", symbol_clone, e);
            }
        });

        actor_handles.push(handle);
    }

    // 不再需要完成的K线发送器
    drop(completed_kline_sender);

    // 启动应用交易分发器任务
    info!("启动应用交易分发器任务");
    let _dispatcher_handle = tokio::spawn(async move {
        if let Err(e) = run_app_trade_dispatcher_task(
            app_trade_receiver,
            actor_senders,
        ).await {
            error!("应用交易分发器任务失败: {}", e);
        }
    });

    // 启动SQLite存储任务
    info!("启动SQLite存储任务");
    let sqlite_storage = SqliteStorage::new("data/klines.db").await?;
    sqlite_storage.init_tables().await?;

    let _storage_handle = tokio::spawn(async move {
        if let Err(e) = sqlite_storage.run_storage_task(completed_kline_receiver).await {
            error!("SQLite存储任务失败: {}", e);
        }
    });

    // 等待Ctrl+C信号
    info!("服务已启动，按Ctrl+C退出");
    tokio::signal::ctrl_c().await?;
    info!("收到Ctrl+C信号，正在关闭服务...");

    // 等待所有任务完成
    // 注意：这里我们不等待，因为任务可能会一直运行
    // 实际应用中，可能需要实现更优雅的关闭机制

    info!("服务已关闭");
    Ok(())
}

/// 获取所有U本位合约交易对
async fn fetch_all_usdt_symbols(api: &BinanceApi) -> Result<Vec<String>> {
    // 调用币安API获取所有正在交易的U本位永续合约交易对
    info!("正在获取所有正在交易的U本位永续合约交易对...");

    // 尝试使用API获取交易对
    match api.get_trading_usdt_perpetual_symbols().await {
        Ok(symbols) => {
            if symbols.is_empty() {
                return Err(AppError::ApiError("未找到任何U本位合约交易对".to_string()));
            }

            // 为了测试，只返回少量交易对
            let test_symbols = vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "BNBUSDT".to_string(),
                "SOLUSDT".to_string(),
                "ADAUSDT".to_string(),
            ];

            info!("测试模式：只使用 {} 个交易对", test_symbols.len());

            Ok(test_symbols)
        },
        Err(e) => {
            error!("获取U本位永续合约交易对失败: {}", e);

            // 如果API调用失败，使用硬编码的测试交易对
            info!("使用硬编码的测试交易对");
            let test_symbols = vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "BNBUSDT".to_string(),
                "SOLUSDT".to_string(),
                "ADAUSDT".to_string(),
            ];

            info!("测试模式：只使用 {} 个交易对", test_symbols.len());

            Ok(test_symbols)
        }
    }
}
