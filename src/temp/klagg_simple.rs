//! 启动“骨架级Actor模型”K线聚合服务。
//!
//! ... (文档注释保持不变) ...

use anyhow::Result;
use kline_macros::perf_profile;
use kline_server::klagg_simple::{KlineAggregator, HealthArray};
use kline_server::klcommon::{
    api::{self, BinanceApi},
    context::spawn_instrumented,
    db::Database,
    error::AppError,
    log::{shutdown_log_sender, init_ai_logging},
    server_time_sync::ServerTimeSyncManager,
    websocket::{AggTradeClient, AggTradeConfig, AggTradeMessageHandler, WebSocketClient, MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler},
    AggregateConfig,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Notify};
use tokio::time::sleep;
use tracing::{debug, error, info, info_span, trace, warn};

// 常量定义
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
const WEBSOCKET_CONNECTION_COUNT: usize = 2;
const CLOCK_SAFETY_MARGIN_MS: u64 = 10;
const MIN_SLEEP_MS: u64 = 10;

#[tokio::main]
async fn main() -> Result<()> {
    // ===================================================================
    // ✨ [新增] 在所有业务逻辑之前，首先初始化日志系统！
    // ===================================================================
    let _log_guard = init_ai_logging().await?;
    // 现在可以安全地使用日志宏了
    let _main_span = info_span!("main_lifecycle").entered();
    // ===================================================================

    info!(log_type = "low_freq", "初始化简易K线聚合服务...");

    let result = run_app().await;

    // ✨ [新增] 在程序结束前，确保日志被优雅地关闭
    shutdown_log_sender();

    // 返回业务逻辑的结果
    result
}

// ✨ [新增] 将原有的 main 函数逻辑封装到 run_app 中
async fn run_app() -> Result<()> {
    // 1. 初始化资源
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    info!(
        log_type = "low_freq",
        message = "配置文件已加载。",
        path = DEFAULT_CONFIG_PATH
    );

    // ✨ [新增] 从环境变量读取测试模式
    let enable_test_mode = std::env::var("KLINE_TEST_MODE")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    let api_client = Arc::new(BinanceApi::new());
    let db = Arc::new(Database::new(&config.database.database_path)?);
    info!(
        log_type = "low_freq",
        message = "数据库连接成功。",
        path = %config.database.database_path
    );
    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());

    // 2. 初始化通信和信令通道
    let (clock_tx, clock_rx) = watch::channel(0i64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let internal_shutdown_notify = Arc::new(Notify::new());
    let shutdown_complete_notify = Arc::new(Notify::new());

    // 3. 启动前置任务
    info!(log_type = "low_freq", "开始同步服务器时间...");
    time_sync_manager.sync_time_once().await?;
    info!(log_type = "low_freq", "服务器时间同步成功。");

    spawn_instrumented(run_clock_task(
        config.clone(),
        time_sync_manager.clone(),
        clock_tx,
        internal_shutdown_notify.clone(),
    ));

    let time_sync_manager_clone = time_sync_manager.clone();
    spawn_instrumented(async move {
        time_sync_manager_clone.start().await
    });

    // 4. 初始化品种索引
    info!(log_type = "low_freq", "开始获取并索引交易对...");
    let (all_symbols, symbol_to_index) = initialize_symbol_indexing(&api_client, &db, enable_test_mode).await?;
    if symbol_to_index.is_empty() {
        error!(log_type = "fatal_error", "No symbols to process. Exiting.");
        std::process::exit(1);
    }
    info!(
        log_type = "low_freq",
        message = "交易对索引完成。",
        indexed_count = symbol_to_index.len()
    );

    // [MODIFIED] 创建 index_to_symbol 映射，用于持久化任务
    let index_to_symbol: Arc<HashMap<usize, String>> =
        Arc::new(symbol_to_index.iter().map(|(s, &i)| (i, s.clone())).collect());

    // Watchdog 健康状态数组初始化
    let num_symbols = symbol_to_index.len();
    let health_array: HealthArray = Arc::new(
        (0..num_symbols)
            .map(|_| AtomicU64::new(chrono::Utc::now().timestamp_millis() as u64))
            .collect(),
    );

    // 启动 Watchdog 任务
    let watchdog_check_interval = Duration::from_secs(config.watchdog_check_interval_s.unwrap_or(15));
    let watchdog_timeout = Duration::from_secs(config.watchdog_actor_timeout_s.unwrap_or(60));

    spawn_instrumented(run_watchdog(
        health_array.clone(),
        watchdog_check_interval,
        watchdog_timeout,
    ));

    // 5. 创建并启动聚合器
    let aggregator = Arc::new(KlineAggregator::new(
        config.clone(),
        symbol_to_index.clone(),
        index_to_symbol.clone(), // [MODIFIED] 传入 index_to_symbol
        time_sync_manager.clone(),
        clock_rx,
        db.clone(),
        shutdown_rx.clone(),
        shutdown_complete_notify.clone(),
        health_array.clone(),
    ));

    // 启动聚合器
    let aggregator_clone = aggregator.clone();
    let aggregator_handle = spawn_instrumented(async move {
        aggregator_clone.run().await
    });

    // 等待分发器就绪 - 使用健壮的同步机制替代 sleep
    info!(log_type = "low_freq", "等待分发器初始化完成...");
    aggregator.wait_for_dispatcher_ready().await;

    let dispatcher = aggregator.get_dispatcher().await
        .ok_or_else(|| AppError::InitializationError("分发器未初始化".to_string()))?;

    info!(log_type = "low_freq", "分发器获取成功，开始启动WebSocket客户端");

    // 6. 启动WebSocket客户端
    info!(
        log_type = "low_freq",
        message = "正在初始化WebSocket客户端...",
        count = WEBSOCKET_CONNECTION_COUNT
    );
    let chunks: Vec<Vec<String>> = all_symbols
        .chunks((all_symbols.len() + WEBSOCKET_CONNECTION_COUNT - 1) / WEBSOCKET_CONNECTION_COUNT)
        .map(|chunk| chunk.to_vec())
        .collect();

    // [新增调试输出]
    debug!(log_type = "low_freq", "用于WebSocket连接的交易对分块: {:?}", chunks);

    for (i, symbol_chunk) in chunks.into_iter().enumerate() {
        let mut client_shutdown_rx = shutdown_rx.clone();
        let config_clone = config.clone();
        let dispatcher_clone = dispatcher.clone();
        let aggregator_clone = aggregator.clone(); // 在循环内克隆
        spawn_instrumented(async move {
            let agg_trade_config = AggTradeConfig {
                use_proxy: config_clone.websocket.use_proxy,
                proxy_addr: config_clone.websocket.proxy_host.clone(),
                proxy_port: config_clone.websocket.proxy_port,
                symbols: symbol_chunk,
            };
            let handler = Arc::new(create_adapted_message_handler(dispatcher_clone, aggregator_clone)); // 传入 aggregator

            let mut client =
                AggTradeClient::new_with_handler(agg_trade_config, handler);

            tokio::select! {
                res = client.start() => {
                    if let Err(e) = res {
                        error!(
                            log_type = "fatal_error",
                            client_id = i + 1,
                            error_details = %e,
                            error_chain = ?e,
                            "WebSocket client failed."
                        );
                    }
                },
                _ = client_shutdown_rx.changed() => {
                    if *client_shutdown_rx.borrow() {
                         info!(
                            log_type = "low_freq",
                            client_id = i + 1,
                            "WebSocket客户端收到关闭信号。"
                         );
                    }
                }
            }
        });
    }

    // [新增] 启动 SymbolManager 任务
    let symbol_manager_aggregator = aggregator.clone();
    let symbol_manager_config = config.clone();
    spawn_instrumented(async move {
        if let Err(e) = run_symbol_manager(symbol_manager_aggregator, symbol_manager_config).await {
            error!(log_type="fatal_error", component="SymbolManager", error=%e, "SymbolManager 任务失败。");
        }
    });

    // 7. 等待退出信号
    info!(log_type = "low_freq", "系统已启动，等待关闭信号。");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(log_type = "low_freq", reason = "ctrl_c", "收到关闭信号。");
        },
        _ = internal_shutdown_notify.notified() => {
            warn!(log_type = "low_freq", reason = "internal_failure", "因严重故障触发系统关闭。");
        }
    }

    // 8. 执行优雅停机
    info!(log_type = "low_freq", "向所有组件广播关闭信号...");
    if shutdown_tx.send(true).is_err() {
        warn!(log_type = "low_freq", "没有组件在监听关闭信号。");
    }

    info!(log_type = "low_freq", "等待聚合器完成数据最终持久化...");
    if let Err(e) = aggregator.shutdown().await {
        error!(
            log_type = "shutdown_error",
            component = "aggregator",
            error_details = %e,
            error_chain = ?e,
            "Aggregator shutdown failed."
        );
    } else {
        info!(log_type = "low_freq", component = "aggregator", "聚合器已成功关闭。");
    }

    // 等待聚合器主任务结束
    if let Err(e) = aggregator_handle.await {
       error!(
           log_type = "shutdown_error",
           component = "aggregator_run_task",
           error_details = %e,
           error_chain = ?e,
           "Aggregator run task failed on exit."
        );
    }

    info!(log_type = "low_freq", "服务已优雅地关闭。");
    Ok(())
}

// ... (run_clock_task, initialize_symbol_indexing 函数保持不变) ...
async fn run_clock_task(
    config: Arc<AggregateConfig>,
    time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    let shortest_interval_ms = config
        .supported_intervals
        .iter()
        .map(|i| api::interval_to_milliseconds(i))
        .min()
        .unwrap_or(60_000);

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            error!(
                log_type = "fatal_error",
                reason = "time_sync_invalid",
                "Time sync has been invalid for too long. Triggering system shutdown."
            );
            shutdown_notify.notify_one();
            break;
        }

        let now = time_sync_manager.get_calibrated_server_time();
        if now == 0 {
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let next_tick_point = (now / shortest_interval_ms + 1) * shortest_interval_ms;
        let wakeup_time = next_tick_point + CLOCK_SAFETY_MARGIN_MS as i64;

        let sleep_duration_ms = if wakeup_time > now {
            (wakeup_time - now) as u64
        } else {
            MIN_SLEEP_MS
        };

        sleep(Duration::from_millis(sleep_duration_ms.max(MIN_SLEEP_MS))).await;

        let final_time = time_sync_manager.get_calibrated_server_time();

        // 为本次广播创建一个高频事务 Span
        let tick_span = info_span!(
            "clock_tick_transaction",
            tx_id = %format!("clock_tick:{}", final_time),
            log_type = "high_freq",
            target_wakeup_time = wakeup_time,
            sleep_duration_ms,
            final_time,
        );
        let _enter = tick_span.enter();

        if clock_tx.send(final_time).is_err() {
            debug!("Clock channel closed, exiting clock task.");
            break;
        }
    }
}

#[perf_profile(fields(test_mode))]
async fn initialize_symbol_indexing(
    api: &BinanceApi,
    db: &Database,
    enable_test_mode: bool, // 接收参数
) -> Result<(Vec<String>, Arc<HashMap<String, usize>>)> {
    info!(
        log_type = "low_freq",
        message = "开始进行交易对索引。",
        test_mode = enable_test_mode
    );
    let symbols = if enable_test_mode {
        vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
            "DOGEUSDT".to_string(),
        ]
    } else {
        let (trading_symbols, _delisted_symbols) = api.get_trading_usdt_perpetual_symbols().await?;
        trading_symbols
    };

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;
    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .filter_map(|(symbol, time_opt)| time_opt.map(|time| (symbol, time)))
        .collect();

    if sorted_symbols_with_time.is_empty() {
        let err = AppError::DataError("No symbols with historical data found.".to_string());
        error!(
            log_type = "precondition_failed",
            error_details = %err,
            "Cannot continue without any processable symbols."
        );
        return Err(err.into());
    }

    sorted_symbols_with_time.sort_by_key(|&(_, time)| time);

    let all_sorted_symbols: Vec<String> = sorted_symbols_with_time
        .iter()
        .map(|(s, _)| s.clone())
        .collect();

    let symbol_to_index: HashMap<String, usize> = all_sorted_symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| (symbol.clone(), index))
        .collect();

    info!(
        log_type = "low_freq",
        message = "交易对索引成功完成。",
        indexed_count = symbol_to_index.len()
    );

    Ok((all_sorted_symbols, Arc::new(symbol_to_index)))
}

/// 创建适配的消息处理器，使用去中心化分发器
// [修改] 函数签名改变，接收 aggregator 以访问全局map
fn create_adapted_message_handler(
    dispatcher: kline_server::klagg_simple::Dispatcher,
    aggregator: Arc<kline_server::klagg_simple::KlineAggregator>, // 新增参数
) -> AggTradeMessageHandler {
    debug!(log_type = "low_freq", "正在创建带本地缓存的适配消息处理器。");
    let (klcommon_sender, mut klcommon_receiver) = mpsc::unbounded_channel::<kline_server::klcommon::websocket::AggTradeData>();

    // [修改] 将aggregator的引用移入异步块
    spawn_instrumented(async move {
        info!(log_type = "beacon", beacon_id = "ADP-01", "适配器转换任务已启动（带本地缓存）");

        // [新增] 本地缓存
        let mut local_cache: HashMap<String, usize> = HashMap::new();
        // [新增] 获取全局 map 的引用
        let global_symbol_map = aggregator.get_symbol_to_index_map();

        while let Some(klcommon_trade) = klcommon_receiver.recv().await {
            let symbol_str = &klcommon_trade.symbol;
            let symbol_index: usize;

            // 1. 优先查本地缓存
            if let Some(&index) = local_cache.get(symbol_str) {
                symbol_index = index;
            } else {
                // 2. 本地缓存未命中，查询全局Map (加读锁)
                let global_map_reader = global_symbol_map.read().await;
                if let Some(&index) = global_map_reader.get(symbol_str) {
                    // 3. 查到了，更新本地缓存
                    local_cache.insert(symbol_str.clone(), index);
                    symbol_index = index;
                    info!(log_type="low_freq", symbol=%symbol_str, index=symbol_index, "本地缓存已填充新品种。");
                } else {
                    // 4. 全局Map也没有，说明是新品种且尚未被SymbolManager完全处理，或数据错误
                    warn!(log_type="unknown_symbol", symbol=%symbol_str, "适配器收到未知品种交易，暂时丢弃。");
                    continue; // 跳过这笔交易
                }
            }

            // 进行类型转换
            let klagg_simple_trade = kline_server::klagg_simple::Trade {
                price: klcommon_trade.price,
                quantity: klcommon_trade.quantity,
                is_buyer_maker: klcommon_trade.is_buyer_maker,
                trade_time: klcommon_trade.timestamp_ms,
                agg_trade_id: 0, // 这些字段在 klagg_simple 中未使用
                first_trade_id: 0,
                last_trade_id: 0,
            };

            // [修改] 使用索引进行分发
            match dispatcher.dispatch(symbol_index, klagg_simple_trade).await {
                Ok(()) => {
                    info!(log_type = "beacon", beacon_id = "ADP-03", symbol_index = symbol_index, "交易数据已成功通过分发器发送至Actor");
                }
                Err(e) => {
                    warn!(
                        log_type = "beacon",
                        beacon_id = "ADP-ERR",
                        symbol_index = symbol_index,
                        error = %e,
                        "分发器发送失败: {}", e
                    );
                    // 注意：这里不break，因为单个品种的错误不应影响其他品种
                }
            }
        }
        info!(log_type = "beacon", beacon_id = "ADP-EXIT", "适配器转换任务退出");
    });

    // 返回配置了转换通道的 MessageHandler
    AggTradeMessageHandler::with_unbounded_sender(
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        klcommon_sender,
    )
}

/// 负责监控新品种并动态添加到系统的任务
async fn run_symbol_manager(
    aggregator: Arc<KlineAggregator>,
    config: Arc<AggregateConfig>,
) -> Result<()> {
    info!(log_type = "low_freq", "SymbolManager 已启动。");

    // 1. 初始化 MiniTicker WebSocket
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mini_ticker_handler = Arc::new(MiniTickerMessageHandler::new(tx));
    let mini_ticker_config = MiniTickerConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
    };
    let mut mini_ticker_client = MiniTickerClient::new(mini_ticker_config, mini_ticker_handler);

    spawn_instrumented(async move {
        // 在后台运行 MiniTicker 客户端
        if let Err(e) = mini_ticker_client.start().await {
            error!(log_type = "fatal_error", component = "MiniTickerClient", error = %e, "MiniTicker客户端错误退出");
        }
    });

    // 2. 初始化新品种专用的 WebSocket 管理状态
    let mut _new_symbols_client: Option<AggTradeClient> = None;
    let mut _new_symbols_to_subscribe: Vec<String> = Vec::new();

    // 3. 主循环，处理来自 MiniTicker 的数据
    while let Some(tickers) = rx.recv().await {
        let known_symbols = aggregator.get_symbol_to_index_map();
        let known_symbols_reader = known_symbols.read().await;

        for ticker in tickers {
            if !known_symbols_reader.contains_key(&ticker.symbol) {
                // 发现新品种！
                let new_symbol = ticker.symbol.clone();
                info!(log_type = "low_freq", component = "SymbolManager", symbol = %new_symbol, "发现新品种！");

                // TODO: 在这里实现完整的添加协议
                // 这是一个简化版本，实际需要更复杂的管理
                // 比如：聚合一段时间内发现的新品种，然后批量添加

                // a. 获取写锁 (这里简化，实际需要锁定多个)
                drop(known_symbols_reader); // 释放读锁，才能获取写锁

                // b. 分配新ID
                let new_index = aggregator.symbol_count.fetch_add(1, Ordering::SeqCst);

                // c. 创建新 Actor (简化，实际需要所有Arc)
                // spawn_instrumented(symbol_actor(...));

                // d. 更新元数据
                // aggregator.symbol_to_index.write().await.insert(...);
                // aggregator.index_to_symbol.write().await.insert(...);
                // aggregator.health_array.write().await.push(...);
                // aggregator.get_dispatcher().await.unwrap().add_sender(...).await;

                info!(log_type="low_freq", symbol=%new_symbol, index=new_index, "新品种处理协议完成（占位符）。");
                break; // 重新获取读锁
            }
        }
    }

    Ok(())
}

/// Watchdog 任务，周期性地、无锁地检查所有 Actor 的健康状况。
async fn run_watchdog(
    health_array: HealthArray,
    check_interval: Duration,
    timeout_threshold: Duration,
) {
    info!(
        log_type = "low_freq",
        component = "watchdog",
        check_interval_secs = check_interval.as_secs(),
        timeout_secs = timeout_threshold.as_secs(),
        "Watchdog服务已启动。"
    );

    let mut interval = tokio::time::interval(check_interval);
    let timeout_millis = timeout_threshold.as_millis() as u64;

    loop {
        interval.tick().await;
        trace!(log_type = "high_freq", component = "watchdog", "运行健康检查周期。");

        let now_millis = chrono::Utc::now().timestamp_millis() as u64;
        let mut dead_actors = Vec::new();

        for (actor_id, health_status) in health_array.iter().enumerate() {
            let last_heartbeat = health_status.load(Ordering::Relaxed);

            if now_millis.saturating_sub(last_heartbeat) > timeout_millis {
                dead_actors.push(actor_id);
            }
        }

        if !dead_actors.is_empty() {
            error!(
                log_type = "fatal_error",
                component = "watchdog",
                dead_actors = ?dead_actors,
                "检测到死亡或停滞的Actor！需要恢复操作。"
            );
            // TODO: 在这里实现 Actor 的重启恢复逻辑。
            // 例如：向一个管理服务发送消息，要求重启这些 actor_id 对应的 Actor。
        }
    }
}