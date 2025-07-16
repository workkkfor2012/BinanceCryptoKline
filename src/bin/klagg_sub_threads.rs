//! 启动“完全分区模型”K线聚合服务。
//!
//! ## 核心执行模型
//! - main函数手动创建一个多线程的 `io_runtime`，用于处理所有I/O密集型任务。
//! - 为每个计算Worker创建独立的、绑核的物理线程。
//! - 在每个绑核线程内创建单线程的 `computation_runtime`，专门运行K线聚合计算。
//! - 计算与I/O任务通过MPSC通道解耦。
//! - 实现基于JoinHandle的健壮关闭流程。

// ==================== 运行模式配置 ====================
// 修改这些常量来控制程序运行模式，无需设置环境变量

/// 可视化测试模式开关
/// - true: 启动Web服务器进行K线数据可视化验证，禁用数据库持久化
/// - false: 正常生产模式，启用数据库持久化，禁用Web服务器
const VISUAL_TEST_MODE: bool = false;

/// 测试模式开关（影响数据源）
/// - true: 使用少量测试品种（BTCUSDT等8个品种）
/// - false: 从币安API获取所有U本位永续合约品种
const TEST_MODE: bool = false;

use anyhow::Result;
use chrono;
use kline_server::klagg_sub_threads::{self as klagg, WorkerCmd};
use kline_server::klcommon::{
    api::{self, BinanceApi},
    db::Database,
    error::AppError,
    log::{self, init_ai_logging, shutdown_target_log_sender}, // 确保导入了 shutdown_target_log_sender
    server_time_sync::ServerTimeSyncManager,
    websocket::{MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler, WebSocketClient},
    WatchdogV2, // 引入 WatchdogV2
    AggregateConfig,
};
use kline_server::soft_assert;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, watch, Notify, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{error, info, instrument, span, warn, trace, Instrument, Level, Span};
// use uuid; // 移除未使用的导入

// --- 常量定义 ---
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
const NUM_WORKERS: usize = 4;
const CLOCK_SAFETY_MARGIN_MS: u64 = 10;
const MIN_SLEEP_MS: u64 = 10;
const HEALTH_CHECK_INTERVAL_S: u64 = 10; // 新的监控间隔

fn main() -> Result<()> {
    // 1. ==================== 日志系统必须最先初始化 ====================
    // 使用 block_on 是因为 init_ai_logging 是 async 的
    // guard 的生命周期将决定性能日志何时被刷新
    let _guard = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(init_ai_logging())?;

    // 设置一个 panic hook 来捕获未处理的 panic
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(target: "应用生命周期", panic_info = %panic_info, "程序发生未捕获的Panic，即将退出");
        original_hook(panic_info);
        std::process::exit(1);
    }));

    // 2. 手动创建主 I/O 运行时
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4) // 可根据I/O密集程度调整
        .thread_name("io-worker")
        .build()?;

    // 3. 在 I/O 运行时上下文中执行应用启动和管理逻辑
    // 使用 instrument 将 main_span 附加到整个应用生命周期
    let main_span = span!(target: "应用生命周期", Level::INFO, "klagg_app_lifecycle");
    let result = io_runtime.block_on(run_app(&io_runtime).instrument(main_span));

    if let Err(e) = &result {
        // 现在我们有日志系统了
        error!(target: "应用生命周期", error = ?e, "应用因顶层错误而异常退出");
    } else {
        info!(target: "应用生命周期", log_type = "low_freq", "应用程序正常关闭");
    }

    // 4. 优雅关闭
    info!(target: "应用生命周期", "主IO运行时开始关闭...");
    io_runtime.shutdown_timeout(Duration::from_secs(5));
    info!(target: "应用生命周期", "主IO运行时已关闭");

    // 5. [关键] 关闭日志系统，确保所有缓冲的日志都被处理
    shutdown_target_log_sender(); // [启用] 新的关闭函数
    // shutdown_log_sender();     // [禁用] 旧的关闭函数

    result
}

// 使用 `instrument` 宏自动创建和进入一个 Span
#[instrument(target = "应用生命周期", skip_all, name = "run_app")]
async fn run_app(io_runtime: &Runtime) -> Result<()> {
    info!(target: "应用生命周期",log_type = "low_freq", "K线聚合服务启动中...");
    // trace!(log_type = "low_freq", "K线聚合服务启动中...");
    // trace!("🔍 开始初始化全局资源...");

    // 1. ==================== 初始化全局资源 ====================
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    info!(
        target: "应用生命周期",
        log_type = "low_freq",
        path = DEFAULT_CONFIG_PATH,
        persistence_ms = config.persistence_interval_ms,
        "配置文件加载成功"
    );
    trace!(target: "应用生命周期", config_details = ?config, "📋 详细配置信息");

    // 使用编译时常量而不是环境变量
    let enable_test_mode = TEST_MODE;
    let visual_test_mode = VISUAL_TEST_MODE;
    info!(target: "应用生命周期", log_type = "low_freq", test_mode = enable_test_mode, visual_test_mode = visual_test_mode, "运行模式确定");
    trace!(target: "应用生命周期", test_mode = enable_test_mode, visual_test_mode = visual_test_mode, "🧪 测试模式详细信息");

    if visual_test_mode {
        warn!(target: "应用生命周期", log_type="low_freq", "警告：程序运行在可视化测试模式，数据库持久化已禁用！");
    }

    let api_client = Arc::new(BinanceApi::new());

    let db = Arc::new(Database::new(&config.database.database_path)?);
    info!(target: "应用生命周期", log_type = "low_freq", path = %config.database.database_path, "数据库连接成功");

    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());
    let periods = Arc::new(config.supported_intervals.clone());
    info!(target: "应用生命周期",log_type = "low_freq", ?periods, "支持的K线周期已加载");

    // 2. ==================== 初始化通信设施 ====================
    let (clock_tx, _) = watch::channel(0i64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let internal_shutdown_notify = Arc::new(Notify::new());
    let (w3_cmd_tx, w3_cmd_rx) = mpsc::channel::<WorkerCmd>(128);

    // 3. ==================== 在 I/O 运行时启动核心后台服务 ====================
    info!(target: "应用生命周期", "正在执行首次服务器时间同步...");
    time_sync_manager.sync_time_once().await?;
    info!(target: "应用生命周期",
        offset_ms = time_sync_manager.get_time_diff(),
        "首次服务器时间同步完成"
    );

    // [修改逻辑] 使用封装好的 spawn_instrumented_on 来确保上下文传播
    log::context::spawn_instrumented_on(
        run_clock_task(
            config.clone(),
            time_sync_manager.clone(),
            clock_tx.clone(),
            internal_shutdown_notify.clone(),
        ),
        io_runtime,
    );

    // 4. ============ 获取并建立全局品种索引 (G_Index*) ============
    let (all_symbols_sorted, symbol_to_index_map) =
        initialize_symbol_indexing(&api_client, &db, enable_test_mode).await?;
    let symbol_count = all_symbols_sorted.len();

    // [修改逻辑] 使用 soft_assert! 进行业务断言
    soft_assert!(
        symbol_count > 0 || enable_test_mode,
        message = "没有可处理的交易品种",
        actual_count = symbol_count,
        test_mode = enable_test_mode,
    );

    if symbol_count == 0 && !enable_test_mode {
        let err_msg = "没有可处理的交易品种，服务退出";
        // 使用 error! 记录致命错误
        error!(target: "应用生命周期", reason = err_msg);
        return Err(AppError::InitializationError(err_msg.into()).into());
    }
    info!(target: "应用生命周期", log_type = "low_freq", symbol_count, "全局品种索引初始化完成");

    let symbol_to_global_index = Arc::new(RwLock::new(symbol_to_index_map));
    let global_index_to_symbol = Arc::new(RwLock::new(all_symbols_sorted));
    let global_symbol_count = Arc::new(AtomicUsize::new(symbol_count));

    // ==================== 初始化健康监控中枢 ====================
    let watchdog = Arc::new(WatchdogV2::new());

    // 5. ================ 创建并启动绑核的计算线程和独立的I/O任务 ================
    let creation_span = span!(Level::INFO, "workers_creation");
    let _enter = creation_span.enter(); // 手动进入 Span，覆盖整个循环

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
        warn!(
            available = available_cores.len(),
            required = NUM_WORKERS,
            "CPU核心数不足，可能影响性能，将不会进行线程绑定"
        );
    }

    let mut current_start_index = 0;
    let mut w3_cmd_rx_option = Some(w3_cmd_rx);

    for worker_id in 0..NUM_WORKERS {
        let assigned_symbols = chunks.get(worker_id).cloned().unwrap_or_default();
        let cmd_rx = if worker_id == NUM_WORKERS - 1 { w3_cmd_rx_option.take() } else { None };

        info!(
            target: "应用生命周期",
            log_type = "low_freq",
            worker_id,
            assigned_symbols = assigned_symbols.len(),
            "计算Worker正在创建"
        );
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

        // [修改逻辑] 使用 spawn_instrumented_on
        log::context::spawn_instrumented_on(
            klagg::run_io_loop(
                worker_id,
                assigned_symbols.clone(),
                config.clone(),
                shutdown_rx.clone(),
                ws_cmd_rx,
                worker.get_trade_sender(),
                watchdog.clone(), // 传递 watchdog
            ),
            io_runtime,
        );

        let core_to_bind = available_cores.get(worker_id).copied();
        let comp_shutdown_rx = shutdown_rx.clone();
        let computation_watchdog = watchdog.clone(); // 为计算线程克隆

        let computation_handle = std::thread::Builder::new()
            .name(format!("computation-worker-{}", worker_id))
            .spawn({
                // [修改逻辑] 捕获当前 Span 以便在 OS 线程中恢复
                let parent_span = Span::current();
                move || {
                    // [修改逻辑] 在新线程中恢复 tracing 上下文
                    parent_span.in_scope(|| {
                        if let Some(core_id) = core_to_bind {
                            if core_affinity::set_for_current(core_id) {
                                info!(target: "计算核心", worker_id, core = ?core_id, "计算线程成功绑定到CPU核心");
                            } else {
                                warn!(target: "计算核心", worker_id, core = ?core_id, "计算线程绑定到CPU核心失败");
                            }
                        }

                        let computation_runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();

                        // [修改逻辑] 将 worker 的生命周期也 instrument
                        let worker_span = span!(target: "计算核心", Level::INFO, "computation_worker_runtime", worker_id);
                        computation_runtime.block_on(
                            worker.run_computation_loop(
                                comp_shutdown_rx,
                                trade_rx,
                                computation_watchdog, // [修改] 传递 watchdog
                            )
                            .instrument(worker_span),
                        );
                    })
                }
            })?;

        computation_thread_handles.push(computation_handle);
        current_start_index += assigned_symbols.len();
    }
    drop(_enter); // 退出 apen
    let worker_handles = Arc::new(worker_read_handles);

    // 6. ================= 在 I/O 运行时启动依赖 Worker 的后台任务 ================

    // [核心修改] 根据模式条件性地启动任务
    let persistence_handle = if visual_test_mode {
        info!(target: "应用生命周期", "启动可视化测试Web服务器...");
        // 【修改】调用时不再传递 config
        log::context::spawn_instrumented_on(
            klagg::web_server::run_visual_test_server(
                worker_handles.clone(),
                global_index_to_symbol.clone(),
                periods.clone(),
                shutdown_rx.clone(),
            ),
            io_runtime,
        );
        None
    } else {
        // 在生产模式下，启动持久化任务 (这部分逻辑保持不变)
        Some(log::context::spawn_instrumented_on(
            klagg::persistence_task(
                db.clone(),
                worker_handles.clone(),
                global_index_to_symbol.clone(),
                periods.clone(),
                config.clone(),
                shutdown_rx.clone(),
                watchdog.clone(), // 传递 watchdog
            ),
            io_runtime,
        ))
    };

    // [修改逻辑] 使用 spawn_instrumented_on
    log::context::spawn_instrumented_on(
        run_symbol_manager(
            config.clone(),
            symbol_to_global_index,
            global_index_to_symbol,
            global_symbol_count,
            w3_cmd_tx,
        ),
        io_runtime,
    );

    // [替换] 启动新的 WatchdogV2 监控中枢
    io_runtime.spawn(
        watchdog.run(
            Duration::from_secs(HEALTH_CHECK_INTERVAL_S),
            internal_shutdown_notify.clone(),
        )
    );

    // 7. ==================== 等待并处理关闭信号 ====================
    info!(target: "应用生命周期", "所有服务已启动，等待关闭信号 (Ctrl+C)...");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(target: "应用生命周期", log_type = "low_freq", reason = "received_ctrl_c", "接收到关闭信号，开始优雅关闭");
        },
        _ = internal_shutdown_notify.notified() => {
            info!(target: "应用生命周期", log_type = "low_freq", reason = "internal_shutdown", "接收到内部关闭通知，开始优雅关闭");
        }
    }

    let _ = shutdown_tx.send(true);

    for (worker_id, handle) in computation_thread_handles.into_iter().enumerate() {
        if let Err(e) = handle.join() {
            error!(target: "应用生命周期", worker_id, panic = ?e, "计算线程在退出时发生 panic");
        }
    }

    // [修改] 条件性地等待持久化任务
    if let Some(handle) = persistence_handle {
        if let Err(e) = handle.await {
            error!(target: "应用生命周期", task = "persistence", panic = ?e, "持久化任务在退出时发生 panic");
        }
    }

    Ok(())
}

/// 全局时钟任务
#[instrument(target = "全局时钟", skip_all, name="run_clock_task")]
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

    info!(target: "全局时钟", log_type="low_freq", shortest_interval_ms, "全局时钟任务已启动");

    // 时间同步重试计数器
    let mut time_sync_retry_count = 0;
    //const MAX_TIME_SYNC_RETRIES: u32 = 3;
    const MAX_TIME_SYNC_RETRIES: u32 = 99999999;

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            time_sync_retry_count += 1;
            warn!(target: "全局时钟", log_type="retry",
                  retry_count = time_sync_retry_count,
                  max_retries = MAX_TIME_SYNC_RETRIES,
                  "时间同步失效，尝试重试");

            if time_sync_retry_count >= MAX_TIME_SYNC_RETRIES {
                error!(target: "全局时钟", log_type="assertion", reason="time_sync_invalid",
                       retry_count = time_sync_retry_count,
                       "时间同步失效，已达到最大重试次数，服务将关闭");
                shutdown_notify.notify_one();
                break;
            }

            // 等待一段时间后重试
            sleep(Duration::from_millis(1000)).await;
            continue;
        } else {
            // 时间同步恢复正常，重置重试计数器
            if time_sync_retry_count > 0 {
                info!(target: "全局时钟", log_type="recovery",
                      previous_retry_count = time_sync_retry_count,
                      "时间同步已恢复正常");
                time_sync_retry_count = 0;
            }
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
        if clock_tx.send(final_time).is_err() {
            break;
        }
    }
    warn!(target: "全局时钟", "全局时钟任务已退出");
}

/// 初始化品种索引
#[instrument(target = "应用生命周期", skip_all, name = "initialize_symbol_indexing")]
async fn initialize_symbol_indexing(
    api: &BinanceApi,
    db: &Database,
    enable_test_mode: bool,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    info!(target: "应用生命周期",test_mode = enable_test_mode, "开始初始化品种索引");
    let symbols = if enable_test_mode {
        vec!["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "BNBUSDT", "LTCUSDT"]
            .into_iter()
            .map(String::from)
            .collect()
    } else {
        info!(target: "应用生命周期", "正在从币安API获取所有U本位永续合约品种...");
        api.get_trading_usdt_perpetual_symbols().await?
    };
    info!(target: "应用生命周期", count = symbols.len(), "品种列表获取成功");

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    soft_assert!(!sorted_symbols_with_time.is_empty() || !enable_test_mode,
        message = "未能找到任何带有历史数据的品种",
        enable_test_mode = enable_test_mode,
    );

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

    info!(target: "应用生命周期", count = all_sorted_symbols.len(), "品种索引构建完成，并按上市时间排序");
    Ok((all_sorted_symbols, symbol_to_index))
}

/// 负责发现新品种并以原子方式发送指令给 Worker 3 的任务
#[instrument(target = "品种管理器", skip_all, name = "run_symbol_manager")]
async fn run_symbol_manager(
    config: Arc<AggregateConfig>,
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    let enable_test_mode = TEST_MODE;

    if enable_test_mode {
        info!(target: "品种管理器", log_type = "low_freq", "品种管理器启动（测试模式）- 每60秒模拟添加一个新品种");
        run_test_symbol_manager(symbol_to_global_index, global_index_to_symbol, global_symbol_count, cmd_tx).await
    } else {
        info!(target: "品种管理器", log_type = "low_freq", "品种管理器启动（生产模式）- 基于MiniTicker实时发现新品种");
        run_production_symbol_manager(config, symbol_to_global_index, global_index_to_symbol, global_symbol_count, cmd_tx).await
    }
}

/// 测试模式的品种管理器 - 每60秒模拟添加一个新品种
#[instrument(target = "品种管理器", skip_all, name = "run_test_symbol_manager")]
async fn run_test_symbol_manager(
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    // +++ 新增: 简单的启动延迟 +++
    info!(target: "品种管理器", log_type = "low_freq", "测试模式启动，等待2秒以确保I/O核心初始化...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 测试用的额外品种列表（除了初始的8个品种）
    let test_symbols = vec![
        "AVAXUSDT", "MATICUSDT", "LINKUSDT", "UNIUSDT", "ATOMUSDT",
        "DOTUSDT", "FILUSDT", "TRXUSDT", "ETCUSDT", "XLMUSDT",
        "VETUSDT", "ICPUSDT", "FTMUSDT", "HBARUSDT", "NEARUSDT",
        "ALGOUSDT", "MANAUSDT", "SANDUSDT", "AXSUSDT", "THETAUSDT"
    ];

    let mut symbol_index = 0;
    let mut interval = tokio::time::interval(Duration::from_secs(60)); // 每60秒添加一个
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        if symbol_index >= test_symbols.len() {
            info!(target: "品种管理器", log_type = "low_freq", "测试模式：所有模拟品种已添加完毕，品种管理器进入等待状态");
            // 继续运行但不再添加新品种
            tokio::time::sleep(Duration::from_secs(3600)).await;
            continue;
        }

        let symbol = test_symbols[symbol_index].to_string();
        symbol_index += 1;

        // 检查品种是否已存在
        let read_guard = symbol_to_global_index.read().await;
        if read_guard.contains_key(&symbol) {
            drop(read_guard);
            continue;
        }
        drop(read_guard);

        info!(target: "品种管理器", log_type = "low_freq", symbol = %symbol, "测试模式：模拟发现新品种");

        let new_global_index = global_symbol_count.fetch_add(1, Ordering::SeqCst);
        let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<(), String>>();
        let cmd = WorkerCmd::AddSymbol {
            symbol: symbol.clone(),
            global_index: new_global_index,
            ack: ack_tx,
        };

        if cmd_tx.send(cmd).await.is_err() {
            warn!(target: "品种管理器", symbol=%symbol, "向Worker 3发送AddSymbol命令失败，通道可能已关闭");
            global_symbol_count.fetch_sub(1, Ordering::SeqCst);
            return Ok(());
        }

        match ack_rx.await {
            Ok(Ok(_)) => {
                let mut write_guard_map = symbol_to_global_index.write().await;
                let mut write_guard_vec = global_index_to_symbol.write().await;

                if !write_guard_map.contains_key(&symbol) {
                    write_guard_map.insert(symbol.clone(), new_global_index);
                    if new_global_index == write_guard_vec.len() {
                        write_guard_vec.push(symbol.clone());
                    } else {
                        error!(
                            log_type = "assertion",
                            symbol = %symbol,
                            new_global_index,
                            vec_len = write_guard_vec.len(),
                            "全局索引与向量长度不一致，发生严重逻辑错误！"
                        );
                    }
                    info!(target: "品种管理器", log_type = "low_freq", symbol = %symbol, new_global_index, "测试模式：成功添加模拟品种到全局索引和Worker {}", symbol);
                }
            }
            Ok(Err(e)) => {
                warn!(target: "品种管理器", symbol = %symbol, reason = %e, "添加新品种失败，Worker拒绝");
                global_symbol_count.fetch_sub(1, Ordering::SeqCst);
            }
            Err(_) => {
                warn!(target: "品种管理器", symbol = %symbol, reason = "ack_channel_closed", "添加新品种失败，与Worker的确认通道已关闭");
                global_symbol_count.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}

/// 生产模式的品种管理器 - 基于MiniTicker实时发现新品种
#[instrument(target = "品种管理器", skip_all, name = "run_production_symbol_manager")]
async fn run_production_symbol_manager(
    config: Arc<AggregateConfig>,
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let handler = Arc::new(MiniTickerMessageHandler::new(tx));
    let mini_ticker_config = MiniTickerConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
    };
    let mut client = MiniTickerClient::new(mini_ticker_config, handler);
    info!(target: "品种管理器", log_type = "low_freq", "正在连接MiniTicker WebSocket...");
    tokio::spawn(async move {
        if let Err(e) = client.start().await {
            warn!(target: "品种管理器", error = ?e, "MiniTicker WebSocket 客户端启动失败");
        }
    });

    while let Some(tickers) = rx.recv().await {
        let read_guard = symbol_to_global_index.read().await;
        let new_symbols: Vec<_> = tickers
            .into_iter()
            .filter(|t| t.symbol.ends_with("USDT"))
            .filter(|t| !read_guard.contains_key(&t.symbol))
            .collect();
        drop(read_guard);

        if !new_symbols.is_empty() {
            info!(target: "品种管理器", count = new_symbols.len(), "发现新品种，开始处理");
            for ticker in new_symbols {
                let new_global_index = global_symbol_count.fetch_add(1, Ordering::SeqCst);
                let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<(), String>>();
                let cmd = WorkerCmd::AddSymbol {
                    symbol: ticker.symbol.clone(),
                    global_index: new_global_index,
                    ack: ack_tx,
                };

                if cmd_tx.send(cmd).await.is_err() {
                    warn!(target: "品种管理器", symbol=%ticker.symbol, "向Worker 3发送AddSymbol命令失败，通道可能已关闭");
                    global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                    return Ok(());
                }

                match ack_rx.await {
                    Ok(Ok(_)) => {
                        let mut write_guard_map = symbol_to_global_index.write().await;
                        let mut write_guard_vec = global_index_to_symbol.write().await;

                        if !write_guard_map.contains_key(&ticker.symbol) {
                            write_guard_map.insert(ticker.symbol.clone(), new_global_index);
                            if new_global_index == write_guard_vec.len() {
                                write_guard_vec.push(ticker.symbol.clone());
                            } else {
                                error!(
                                    log_type = "assertion",
                                    symbol = %ticker.symbol,
                                    new_global_index,
                                    vec_len = write_guard_vec.len(),
                                    "全局索引与向量长度不一致，发生严重逻辑错误！"
                                );
                            }
                            info!(target: "品种管理器", symbol = %ticker.symbol, new_global_index, "成功添加新品种到全局索引和Worker");
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = %e, "添加新品种失败，Worker拒绝");
                        global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = "ack_channel_closed", "添加新品种失败，与Worker的确认通道已关闭");
                        global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            }
        }
    }
    warn!(target: "品种管理器", "品种管理器任务已退出");
    Ok(())
}

