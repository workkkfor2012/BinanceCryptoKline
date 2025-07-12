//! 启动“完全分区模型”K线聚合服务。
//!
//! ## 核心执行模型
//! - main函数手动创建一个多线程的 `io_runtime`，用于处理所有I/O密集型任务。
//! - 为每个计算Worker创建独立的、绑核的物理线程。
//! - 在每个绑核线程内创建单线程的 `computation_runtime`，专门运行K线聚合计算。
//! - 计算与I/O任务通过MPSC通道解耦。
//! - 实现基于JoinHandle的健壮关闭流程。

use anyhow::Result;
use chrono;
use kline_macros::perf_profile;
use kline_server::klagg_sub_threads::{self as klagg, WorkerCmd};
use kline_server::klcommon::{
    api::{self, BinanceApi},
    context::{spawn_instrumented, spawn_instrumented_on, Instrumented},
    db::Database,
    error::AppError,
    log::shutdown_log_sender,
    logging_setup::init_ai_logging,
    server_time_sync::ServerTimeSyncManager,
    websocket::{MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler, WebSocketClient},
    AggregateConfig,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, watch, Notify, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{error, info, info_span, trace, warn, Span};
use tracing_futures::Instrument;

// --- 常量定义 ---
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
const NUM_WORKERS: usize = 4;
const CLOCK_SAFETY_MARGIN_MS: u64 = 10;
const MIN_SLEEP_MS: u64 = 10;
const WATCHDOG_CHECK_INTERVAL_S: u64 = 15;
const WATCHDOG_TIMEOUT_S: u64 = 60;

fn main() -> Result<()> {
    // 1. 手动创建主 I/O 运行时
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4) // 可根据I/O密集程度调整
        .thread_name("io-worker")
        .build()?;

    // 2. 在 I/O 运行时上下文中执行应用启动和管理逻辑
    let result = io_runtime.block_on(async {
        let _log_guard = init_ai_logging().await?;
        let _main_span = info_span!("main_lifecycle").entered();
        info!(log_type = "low_freq", "正在初始化分区K线聚合服务...");

        run_app(&io_runtime).await
    });

    // 优雅地关闭I/O运行时和日志
    io_runtime.shutdown_timeout(Duration::from_secs(5));
    shutdown_log_sender();
    info!(log_type = "low_freq", "服务已优雅关闭");

    result
}

async fn run_app(io_runtime: &Runtime) -> Result<()> {
    // 1. ==================== 初始化全局资源 ====================
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    let enable_test_mode = std::env::var("KLINE_TEST_MODE").unwrap_or_default() == "true";
    let api_client = Arc::new(BinanceApi::new());
    let db = Arc::new(Database::new(&config.database.database_path)?);
    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());
    let periods = Arc::new(config.supported_intervals.clone());

    // 2. ==================== 初始化通信设施 ====================
    let (clock_tx, _) = watch::channel(0i64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let internal_shutdown_notify = Arc::new(Notify::new());
    let (w3_cmd_tx, w3_cmd_rx) = mpsc::channel::<Instrumented<WorkerCmd>>(128);

    // 3. ==================== 在 I/O 运行时启动核心后台服务 ====================
    time_sync_manager.sync_time_once().await?;
    spawn_instrumented_on(
        run_clock_task(
            config.clone(),
            time_sync_manager.clone(),
            clock_tx.clone(),
            internal_shutdown_notify.clone(),
        ),
        io_runtime,
    );

    // 4. ============ 获取并建立全局品种索引 (G_Index*) ============
    info!(log_type = "low_freq", "正在获取并索引交易品种...");
    let (all_symbols_sorted, symbol_to_index_map) =
        initialize_symbol_indexing(&api_client, &db, enable_test_mode).await?;
    let symbol_count = all_symbols_sorted.len();
    if symbol_count == 0 && !enable_test_mode {
        error!(log_type = "fatal_error", "没有可处理的交易品种，服务退出");
        return Err(AppError::InitializationError("No symbols found".into()).into());
    }
    let symbol_to_global_index = Arc::new(RwLock::new(symbol_to_index_map));
    let global_index_to_symbol = Arc::new(RwLock::new(all_symbols_sorted));
    let global_symbol_count = Arc::new(AtomicUsize::new(symbol_count));
    info!(log_type = "low_freq", indexed_count = symbol_count, "品种索引建立完成，共{}个品种", symbol_count);

    let health_array: Vec<Arc<AtomicI64>> =
        (0..NUM_WORKERS).map(|_| Arc::new(AtomicI64::new(0))).collect();

    // 5. ================ 创建并启动绑核的计算线程和独立的I/O任务 ================
    let chunks: Vec<_> = global_index_to_symbol
        .read()
        .await
        .chunks((symbol_count + NUM_WORKERS - 1) / NUM_WORKERS)
        .map(|s| s.to_vec())
        .collect();

    let mut worker_read_handles = Vec::with_capacity(NUM_WORKERS);
    let mut computation_thread_handles = Vec::new();
    let available_cores = core_affinity::get_core_ids().unwrap_or_default();

    if available_cores.len() < NUM_WORKERS {
        warn!(available_cores = ?available_cores, "Available CPU cores ({}) is less than NUM_WORKERS ({}). Binding may fail or overlap.", available_cores.len(), NUM_WORKERS);
    }

    let mut current_start_index = 0;
    let mut w3_cmd_rx_option = Some(w3_cmd_rx);

    for worker_id in 0..NUM_WORKERS {
        let assigned_symbols = chunks.get(worker_id).cloned().unwrap_or_default();
        let cmd_rx = if worker_id == NUM_WORKERS - 1 { w3_cmd_rx_option.take() } else { None };

        // [MODIFIED] Worker::new is now async and awaited.
        let (mut worker, ws_cmd_rx, trade_rx) = klagg::Worker::new(
            worker_id,
            current_start_index,
            &assigned_symbols,
            symbol_to_global_index.clone(),
            periods.clone(),
            cmd_rx,
            clock_tx.subscribe(),
        )
        .await?;

        worker_read_handles.push(worker.get_read_handle());
        let worker_health_probe = health_array[worker_id].clone();

        spawn_instrumented_on(
            klagg::run_io_loop(
                worker_id,
                assigned_symbols.clone(),
                config.clone(),
                shutdown_rx.clone(),
                ws_cmd_rx,
                worker.get_trade_sender(),
            ),
            io_runtime,
        );

        let core_to_bind = available_cores.get(worker_id).copied();
        let comp_shutdown_rx = shutdown_rx.clone();

        // 在创建线程前，捕获当前上下文的Span
        let parent_span = Span::current();

        let computation_handle = std::thread::Builder::new()
            .name(format!("computation-worker-{}", worker_id))
            .spawn(move || {
                // 在新线程中进入捕获的Span上下文
                parent_span.in_scope(|| {
                    if let Some(core_id) = core_to_bind {
                        if core_affinity::set_for_current(core_id) {
                            info!(log_type = "low_freq", worker_id, core_id = ?core_id, "计算线程成功绑定到CPU核心");
                        } else {
                            warn!(log_type = "low_freq", worker_id, core_id = ?core_id, "计算线程CPU亲和性设置失败");
                        }
                    }

                    let computation_runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();

                    computation_runtime.block_on(worker.run_computation_loop(
                        comp_shutdown_rx,
                        trade_rx,
                        worker_health_probe,
                    ));
                })
            })?;

        computation_thread_handles.push(computation_handle);
        current_start_index += assigned_symbols.len();
    }
    let worker_handles = Arc::new(worker_read_handles);
    info!(log_type = "low_freq", count = NUM_WORKERS, "所有Worker计算和I/O任务已启动完成");

    // 6. ================= 在 I/O 运行时启动依赖 Worker 的后台任务 ================
    let persistence_handle = spawn_instrumented_on(
        klagg::persistence_task(
            db.clone(),
            worker_handles.clone(),
            global_index_to_symbol.clone(),
            periods.clone(),
            config.clone(),
            shutdown_rx.clone(),
        ),
        io_runtime,
    );

    spawn_instrumented_on(
        run_symbol_manager(
            config.clone(),
            symbol_to_global_index,
            global_index_to_symbol,
            global_symbol_count,
            w3_cmd_tx,
        ),
        io_runtime,
    );

    spawn_instrumented_on(
        run_watchdog(
            health_array,
            Duration::from_secs(WATCHDOG_CHECK_INTERVAL_S),
            Duration::from_secs(WATCHDOG_TIMEOUT_S),
            internal_shutdown_notify.clone(),
        ),
        io_runtime,
    );

    // 7. ==================== 等待并处理关闭信号 ====================
    info!(log_type = "low_freq", "System up and running. Waiting for shutdown signal.");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => { info!(log_type = "low_freq", "Ctrl-C received."); },
        _ = internal_shutdown_notify.notified() => { warn!(log_type = "low_freq", "Internal failure triggered shutdown."); }
    }

    info!(log_type = "low_freq", "Broadcasting shutdown signal to all components...");
    let _ = shutdown_tx.send(true);

    info!("Waiting for computation threads to shut down...");
    for handle in computation_thread_handles {
        if let Err(e) = handle.join() {
            error!(error = ?e, "A computation thread panicked on exit.");
        }
    }
    info!("All computation threads have shut down.");

    info!("Waiting for persistence task to complete final write...");
    if let Err(e) = persistence_handle.await {
        error!(error = ?e, "Persistence task panicked on exit.");
    }
    info!("Persistence task has shut down.");

    Ok(())
}

/// 全局时钟任务
async fn run_clock_task(
    config: Arc<AggregateConfig>,
    time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    info!(log_type = "low_freq", "全局时钟任务启动");
    let shortest_interval_ms = config
        .supported_intervals
        .iter()
        .map(|i| api::interval_to_milliseconds(i))
        .min()
        .unwrap_or(60_000);

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            error!(log_type = "fatal_error", reason = "time_sync_invalid", "时间同步失效，触发系统关闭");
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
        let sleep_duration_ms = (wakeup_time - now).max(MIN_SLEEP_MS as i64) as u64;

        sleep(Duration::from_millis(sleep_duration_ms)).await;

        let final_time = time_sync_manager.get_calibrated_server_time();
        trace!(timestamp = final_time, "时钟滴答广播");
        if clock_tx.send(final_time).is_err() {
            info!(log_type = "low_freq", "时钟通道已关闭，退出任务");
            break;
        }
    }
    info!(log_type = "low_freq", "全局时钟任务结束");
}

/// 初始化品种索引
#[perf_profile(fields(enable_test_mode))]
async fn initialize_symbol_indexing(
    api: &BinanceApi,
    db: &Database,
    enable_test_mode: bool,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    let symbols = if enable_test_mode {
        vec!["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "BNBUSDT", "LTCUSDT"]
            .into_iter()
            .map(String::from)
            .collect()
    } else {
        api.get_trading_usdt_perpetual_symbols().await?
    };

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            // 如果没有历史数据，使用当前时间作为默认值
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    if sorted_symbols_with_time.is_empty() && !enable_test_mode {
        return Err(AppError::DataError("No symbols with historical data found.".to_string()).into());
    }

    sorted_symbols_with_time.sort_by_key(|&(_, time)| time);

    let all_sorted_symbols: Vec<String> =
        sorted_symbols_with_time.into_iter().map(|(s, _)| s).collect();

    let symbol_to_index: HashMap<String, usize> = all_sorted_symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| (symbol.clone(), index))
        .collect();

    Ok((all_sorted_symbols, symbol_to_index))
}

/// [MODIFIED] 负责发现新品种并以原子方式发送指令给 Worker 3 的任务
#[perf_profile]
async fn run_symbol_manager(
    config: Arc<AggregateConfig>,
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<Instrumented<WorkerCmd>>,
) -> Result<()> {
    info!(log_type = "low_freq", "品种管理器启动");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let handler = Arc::new(MiniTickerMessageHandler::new(tx));
    let mini_ticker_config = MiniTickerConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
    };
    let mut client = MiniTickerClient::new(mini_ticker_config, handler);
    spawn_instrumented(async move {
        if let Err(e) = client.start().await {
            error!(log_type = "fatal_error", component = "MiniTickerClient", error = %e, "MiniTicker客户端异常退出");
        }
    });

    while let Some(tickers) = rx.recv().await {
        let read_guard = symbol_to_global_index.read().await;
        let new_symbols: Vec<_> = tickers
            .into_iter()
            .filter(|t| !read_guard.contains_key(&t.symbol))
            .collect();
        drop(read_guard);

        if !new_symbols.is_empty() {
            for ticker in new_symbols {
                // 为每个新品种添加事务创建一个独立的事务Span
                let tx_id = format!("add_symbol:{}", ticker.symbol);
                let add_symbol_span = info_span!(
                    "add_symbol_transaction",
                    tx_id = %tx_id,
                    log_type = "high_freq",
                    symbol = %ticker.symbol
                );

                let result = async {
                    // [MODIFIED] 修复竞态条件的核心逻辑
                    // 1. 先生成新的全局索引
                    let new_global_index = global_symbol_count.fetch_add(1, Ordering::SeqCst);
                    info!(symbol = %ticker.symbol, new_global_index, "开始添加新品种");

                    // 2. 创建一个确认通道 (ack channel)
                    let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<(), String>>();
                    let cmd = WorkerCmd::AddSymbol {
                        symbol: ticker.symbol.clone(),
                        global_index: new_global_index,
                        ack: ack_tx,
                    };

                    // 3. 使用Instrumented包装命令以传播上下文
                    let instrumented_cmd = Instrumented::new(cmd);

                    // 4. 发送指令给 Worker 并等待其处理完成
                    info!(symbol = %ticker.symbol, "发送AddSymbol指令并等待确认...");
                    if cmd_tx.send(instrumented_cmd).await.is_err() {
                        error!(symbol = %ticker.symbol, "发送AddSymbol指令失败，通道已关闭，系统无法再添加新品种");
                        // 这是一个严重问题，因为特殊Worker可能已崩溃。我们停止SymbolManager。
                        global_symbol_count.fetch_sub(1, Ordering::SeqCst); // 回滚索引
                        return Err(());
                    }

                    Ok((new_global_index, ack_rx))
                }.instrument(add_symbol_span.clone()).await;

                let (new_global_index, ack_rx) = match result {
                    Ok(data) => data,
                    Err(_) => return Ok(()),
                };

                // 5. 在事务Span内等待 Worker 的确认信号
                let _final_result = async {
                    match ack_rx.await {
                        Ok(Ok(_)) => {
                            // Worker 成功处理
                            info!(symbol = %ticker.symbol, "收到Worker成功确认，更新全局索引");
                            let mut write_guard_map = symbol_to_global_index.write().await;
                            let mut write_guard_vec = global_index_to_symbol.write().await;

                            if !write_guard_map.contains_key(&ticker.symbol) {
                                write_guard_map.insert(ticker.symbol.clone(), new_global_index);
                                if new_global_index == write_guard_vec.len() {
                                    write_guard_vec.push(ticker.symbol.clone());
                                } else {
                                    // 这是一个非常严重的不一致性错误，理论上不应该发生
                                    error!(new_global_index, vec_len = write_guard_vec.len(), "严重错误：品种索引不一致！");
                                }
                                info!(symbol = %ticker.symbol, new_global_index, "全局索引更新成功");
                            } else {
                                 warn!(symbol=%ticker.symbol, "品种在最终更新时已存在于全局映射中，竞态条件已避免但需检查逻辑");
                            }
                        }
                        Ok(Err(e)) => {
                            // Worker 报告了处理错误
                            error!(symbol = %ticker.symbol, error = %e, "Worker报告处理失败，回滚索引");
                            global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                        }
                        Err(_) => {
                            // 通道关闭，意味着Worker可能已崩溃或请求方已超时
                            error!(symbol = %ticker.symbol, "未收到Worker确认(通道关闭)，回滚索引");
                            global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                }.instrument(add_symbol_span).await;
            }
        }
    }
    Ok(())
}


/// Watchdog 任务，周期性地检查所有Worker计算线程的健康状况。
async fn run_watchdog(
    health_array: Vec<Arc<AtomicI64>>,
    check_interval: Duration,
    timeout_threshold: Duration,
    shutdown_notify: Arc<Notify>,
) {
    info!(log_type = "low_freq", component = "watchdog", "看门狗服务启动");
    let mut interval = tokio::time::interval(check_interval);
    let timeout_millis = timeout_threshold.as_millis() as i64;

    loop {
        interval.tick().await;
        let now_millis = chrono::Utc::now().timestamp_millis();
        let mut dead_workers = Vec::new();

        for (worker_id, health_status) in health_array.iter().enumerate() {
            let last_heartbeat = health_status.load(Ordering::Relaxed);
            if last_heartbeat == 0 {
                continue;
            } //尚未开始心跳

            if now_millis.saturating_sub(last_heartbeat) > timeout_millis {
                dead_workers.push(worker_id);
            }
        }

        if !dead_workers.is_empty() {
            error!(
                log_type = "fatal_error",
                component = "watchdog",
                dead_workers = ?dead_workers,
                "检测到死亡或停滞的计算Worker！触发系统关闭"
            );
            shutdown_notify.notify_one();
            break;
        }
    }
}