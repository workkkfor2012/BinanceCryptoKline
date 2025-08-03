//! 启动"高保真自定义品种测试模式"K线聚合服务。
//!
//! ## 核心执行模型
//! - 与生产模式 (klagg_sub_threads.rs) 高度一致，但允许开发者自定义测试品种列表
//! - 完整保留生产环境下的数据库持久化任务，以模拟真实的I/O负载
//! - 移除动态品种管理器，使用固定的自定义品种列表
//! - 其他所有逻辑与生产模式完全相同，确保高保真测试环境



use anyhow::Result;
use kline_server::klagg_sub_threads::{
    self as klagg,
    DeltaBatch,
    WorkerCmd,
    run_clock_task,
    // run_symbol_manager, // [测试模式] 移除品种管理器
    // initialize_symbol_indexing, // [测试模式] 移除原有品种初始化
};
use kline_server::kldata::KlineBackfiller;
use kline_server::klcommon::{
    api::BinanceApi,
    db::Database,
    error::AppError,
    log::{self, init_ai_logging, shutdown_target_log_sender}, // 确保导入了 shutdown_target_log_sender
    server_time_sync::ServerTimeSyncManager,
    websocket::AggTradeMessageHandler,
    WatchdogV2, // 引入 WatchdogV2
    AggregateConfig,
};
use kline_server::soft_assert;

use std::collections::HashMap; // 确保在文件顶部引入
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch, Notify, RwLock};
use tokio::time::Duration;
use tracing::{error, info, instrument, span, trace, warn, Instrument, Level};
// use uuid; // 移除未使用的导入

// --- 常量定义 ---
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
const HEALTH_CHECK_INTERVAL_S: u64 = 10; // 新的监控间隔

// ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅
// ==                                                    ==
// ==        新增：基于自定义品种列表的初始化函数        ==
// ==                                                    ==
// ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅

#[instrument(target = "应用生命周期", skip_all, name = "initialize_custom_symbol_indexing")]
async fn initialize_custom_symbol_indexing(
    db: &Database,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    info!(target: "应用生命周期", "开始初始化自定义的品种索引 (高保真测试模式)");

    // ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅
    // ==                                                    ==
    // ==        在这里定义你需要测试的品种列表！            ==
    // ==                                                    ==
    // ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅
    let symbols = vec![
        "BTCUSDT",
        //"ETHUSDT",
        //"SOLUSDT",
        // 你可以按需注释或添加其他品种
        // "BNBUSDT",
        // "XRPUSDT",
    ];
    // ===================================================================

    let symbols: Vec<String> = symbols.into_iter().map(String::from).collect();
    info!(target: "应用生命周期", count = symbols.len(), ?symbols, "自定义测试品种列表加载成功");

    // 复用生产逻辑：从数据库获取这些品种的上线时间，以保证聚合器内部索引顺序的确定性。
    // 这对于复现问题和保证数据一致性很重要。
    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    if sorted_symbols_with_time.is_empty() {
        let err_msg = "自定义的测试品种在数据库中找不到任何历史数据，无法确定启动顺序。请确保数据库中有这些品种的1天K线数据，或检查数据补齐逻辑。".to_string();
        error!(target: "应用生命周期", reason = &err_msg);
        return Err(AppError::InitializationError(err_msg).into());
    }

    // 按上市时间和品种名排序，确保每次启动的内部索引都是一致的。
    sorted_symbols_with_time.sort_by(|(symbol_a, time_a), (symbol_b, time_b)| {
        (time_a, symbol_a).cmp(&(time_b, symbol_b))
    });

    let all_sorted_symbols: Vec<String> =
        sorted_symbols_with_time.into_iter().map(|(s, _)| s).collect();

    let symbol_to_index: HashMap<String, usize> = all_sorted_symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| (symbol.clone(), index))
        .collect();

    info!(target: "应用生命周期", count = all_sorted_symbols.len(), "自定义品种索引构建完成");
    Ok((all_sorted_symbols, symbol_to_index))
}

/// 启动器 main 函数
/// 它的唯一职责是创建一个具有更大栈空间的线程来运行我们的应用主逻辑。
fn main() -> Result<()> {
    // 检查一个环境变量，以防止在新线程中再次创建线程，从而导致无限递归。
    if std::env::var("KLINE_MAIN_THREAD").is_err() {
        // 设置环境变量作为标记。
        std::env::set_var("KLINE_MAIN_THREAD", "1");

        // 使用 Builder 创建一个新线程，并为其分配一个更大的栈。
        // 32MB 栈空间，确保足够的空间处理复杂的初始化逻辑
        let handle = std::thread::Builder::new()
            .name("main-with-large-stack".to_string())
            .stack_size(32 * 1024 * 1024) // 32MB 栈空间
            .spawn(move || {
                // 在这个拥有充裕栈空间的新线程中，执行我们真正的 main 逻辑。
                actual_main()
            })?;

        // 等待新线程执行结束，并返回其结果。
        // 这里的 unwrap 是安全的，因为如果子线程 panic，我们期望主程序也随之 panic 并退出。
        return handle.join().unwrap();
    }

    // 如果环境变量已设置，说明我们已经在正确的线程中，直接执行主逻辑。
    actual_main()
}

/// 应用程序的实际主函数 (由原来的 main 函数重命名而来)
fn actual_main() -> Result<()> {
    // [调试] 确认我们在正确的线程中
    eprintln!("🔍 [调试] actual_main 开始执行，线程名: {:?}", std::thread::current().name());

    // 1. ==================== 日志系统必须最先初始化 ====================
    // 使用 block_on 是因为 init_ai_logging 是 async 的
    // guard 的生命周期将决定性能日志何时被刷新
    let _guard = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(init_ai_logging())?;

    eprintln!("🔍 [调试] 日志系统初始化完成");

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
        .worker_threads(1) // [修改] IO线程数明确设置为1
        .thread_name("io-worker")
        .build()?;

    // 3. 在 I/O 运行时上下文中执行应用启动和管理逻辑
    // 使用 instrument 将 main_span 附加到整个应用生命周期
    let main_span = span!(target: "应用生命周期", Level::INFO, "klagg_custom_test_lifecycle");

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
async fn run_app(
    io_runtime: &Runtime,
) -> Result<()> {
    info!(target: "应用生命周期",log_type = "low_freq", "K线聚合服务启动中 (自定义品种测试模式)...");

    // 1. ==================== 初始化全局资源 ====================
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    info!(
        target: "应用生命周期",
        log_type = "low_freq",
        path = DEFAULT_CONFIG_PATH,
        gateway_pull_interval_ms = config.gateway.pull_interval_ms,
        "配置文件加载成功"
    );
    trace!(target: "应用生命周期", config_details = ?config, "📋 详细配置信息");

    // [新增] 显示审计功能状态
    #[cfg(feature = "full-audit")]
    {
        info!(
            target: "应用生命周期",
            log_type = "audit_status",
            audit_enabled = true,
            "🔍 审计功能已启用 - 包含生命周期事件校验和数据完整性审计"
        );
        info!(
            target: "应用生命周期",
            log_type = "audit_status",
            "✅ 生命周期校验器：已启用"
        );
    }
    #[cfg(not(feature = "full-audit"))]
    {
        info!(
            target: "应用生命周期",
            log_type = "audit_status",
            audit_enabled = false,
            "⚠️ 审计功能已禁用 - 使用零成本抽象模式，生产性能最优"
        );
    }

    let _api_client = Arc::new(BinanceApi); // [修改] BinanceApi现在是无状态的 (测试模式下不使用)

    let db = Arc::new(Database::new_with_config(&config.database.database_path, config.persistence.queue_size)?);
    info!(target: "应用生命周期", log_type = "low_freq", path = %config.database.database_path, "数据库连接成功");

    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());
    let periods = Arc::new(config.supported_intervals.clone());
    info!(target: "应用生命周期",log_type = "low_freq", ?periods, "支持的K线周期已加载");

    // ==================== 纯DB驱动的四步骤启动流程 ====================
    let startup_data_prep_start = std::time::Instant::now();
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程] 开始执行纯DB驱动的数据准备流程...");

    let backfiller = KlineBackfiller::new(db.clone(), periods.iter().cloned().collect());

    // --- 阶段一: 历史补齐 ---
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 1/4] 开始历史数据补齐...");
    let stage1_start = std::time::Instant::now();
    backfiller.run_once().await?;
    let stage1_duration = stage1_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage1_duration.as_millis(),
        duration_s = stage1_duration.as_secs_f64(),
        "✅ [启动流程 | 1/4] 历史数据补齐完成"
    );

    // --- 阶段二: 延迟追赶 ---
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 2/4] 开始延迟追赶补齐（高并发模式）...");
    let stage2_start = std::time::Instant::now();
    backfiller.run_once_with_round(2).await?;
    let stage2_duration = stage2_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage2_duration.as_millis(),
        duration_s = stage2_duration.as_secs_f64(),
        "✅ [启动流程 | 2/4] 延迟追赶补齐完成"
    );

    // ✨ [新增] 所有补齐轮次完成后，清理连接池
    backfiller.cleanup_after_all_backfill_rounds().await;
    let _stage3_duration = std::time::Duration::from_secs(0); // 占位符

    // --- 阶段三: 加载状态 ---（重新编号）
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 3/3] 开始从数据库加载最新K线状态...");
    let stage4_start = std::time::Instant::now();
    let initial_klines = backfiller.load_latest_klines_from_db().await?;
    let stage4_duration = stage4_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage4_duration.as_millis(),
        duration_s = stage4_duration.as_secs_f64(),
        klines_count = initial_klines.len(),
        "✅ [启动流程 | 3/3] 数据库状态加载完成"
    );
    if stage4_duration > std::time::Duration::from_secs(5) {
        warn!(target: "应用生命周期", log_type="performance_alert",
            duration_ms = stage4_duration.as_millis(),
            "⚠️ 性能警告：DB状态加载超过5秒"
        );
    }

    let total_startup_duration = startup_data_prep_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        总耗时_秒 = total_startup_duration.as_secs_f64(),
        总耗时_毫秒 = total_startup_duration.as_millis(),
        历史补齐_秒 = stage1_duration.as_secs_f64(),
        延迟追赶_秒 = stage2_duration.as_secs_f64(),
        加载状态_秒 = stage4_duration.as_secs_f64(),
        最终K线数量 = initial_klines.len(),
        "✅ [启动流程] 所有数据准备阶段完成 - 性能统计（跳过第三阶段）"
    );
    let initial_klines_arc = Arc::new(initial_klines);

    // 2. ==================== 初始化通信设施 ====================
    let (clock_tx, _) = watch::channel(0i64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let internal_shutdown_notify = Arc::new(Notify::new());
    let (_w3_cmd_tx, w3_cmd_rx) = mpsc::channel::<WorkerCmd>(128); // [测试模式] w3_cmd_tx不再用于品种管理器

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

    // 启动定时时间同步任务（每分钟30秒执行）
    let time_sync_manager_for_task = time_sync_manager.clone();
    log::context::spawn_instrumented_on(
        async move {
            if let Err(e) = time_sync_manager_for_task.start().await {
                error!(target: "应用生命周期", error = ?e, "时间同步任务异常终止");
            }
        },
        io_runtime,
    );

    // [新增] 启动周期性时间日志任务
    info!(target: "应用生命周期", "启动周期性时间日志任务...");
    log::context::spawn_instrumented_on(
        klagg::run_periodic_time_logger(
            time_sync_manager.clone(),
            shutdown_rx.clone(),
        ),
        io_runtime,
    );

    // 4. ============ [修改点 1: 替换品种来源] ============
    // 原有代码:
    // let (all_symbols_sorted, symbol_to_index_map) =
    //     initialize_symbol_indexing(&api_client, &db).await?;

    // 替换为:
    let (all_symbols_sorted, symbol_to_index_map) =
        initialize_custom_symbol_indexing(&db).await?;
    let symbol_count = all_symbols_sorted.len();

    // [修改逻辑] 使用 soft_assert! 进行业务断言
    soft_assert!(
        symbol_count > 0,
        message = "没有可处理的交易品种",
        actual_count = symbol_count,
    );

    if symbol_count == 0 {
        let err_msg = "没有可处理的交易品种，服务退出";
        // 使用 error! 记录致命错误
        error!(target: "应用生命周期", reason = err_msg);
        return Err(AppError::InitializationError(err_msg.into()).into());
    }
    info!(target: "应用生命周期", log_type = "low_freq", symbol_count, "全局品种索引初始化完成 (自定义测试模式)");

    let symbol_to_global_index = Arc::new(RwLock::new(symbol_to_index_map));
    let global_index_to_symbol = Arc::new(RwLock::new(all_symbols_sorted));
    let _global_symbol_count = Arc::new(AtomicUsize::new(symbol_count)); // [测试模式] 不再用于品种管理器

    // ==================== 初始化健康监控中枢 ====================
    let watchdog = Arc::new(WatchdogV2::new());

    // 5. ================ 创建并启动唯一的聚合器和I/O任务 ================
    info!(target: "应用生命周期", "启动中心化聚合器模型 (1 IO + 1 计算)...");

    // [修改] 直接获取所有品种
    let all_symbols: Vec<String> = global_index_to_symbol.read().await.clone();

    // [修改] 解构 new 函数的返回值
    let (mut aggregator, outputs) = klagg::KlineAggregator::new(
        &all_symbols,
        symbol_to_global_index.clone(),
        periods.clone(),
        Some(w3_cmd_rx), // 指令通道现在给唯一的聚合器
        clock_tx.subscribe(),
        initial_klines_arc.clone(),
        &config, // [修改] 传入config
    ).await?;
    let ws_cmd_rx = outputs.ws_cmd_rx;
    let trade_rx = outputs.trade_rx;
    let finalized_kline_rx = outputs.finalized_kline_rx;

    let aggregator_read_handle = aggregator.get_read_handle();
    let aggregator_trade_sender = aggregator.get_trade_sender();
    // [修改] 变量重命名，并保持Vec结构以兼容gateway_task的签名，这是一种务实的做法
    let aggregator_handles = Arc::new(vec![aggregator_read_handle.clone()]);

    // [核心修改] 创建 Handler 并注入依赖
    let agg_trade_handler = Arc::new(AggTradeMessageHandler::new(
        aggregator_trade_sender,      // 传入 Aggregator 的 trade sender
        symbol_to_global_index.clone(), // 注入全局索引
    ));

    // [修改] 启动 I/O 任务，传入 handler
    log::context::spawn_instrumented_on(
        klagg::run_io_loop(
            all_symbols,
            config.clone(),
            shutdown_rx.clone(),
            ws_cmd_rx,
            agg_trade_handler, // 传入创建好的 handler 实例
            watchdog.clone(),
        ),
        io_runtime,
    );

    // [修改] 启动唯一的计算线程，不进行核心绑定
    let comp_shutdown_rx = shutdown_rx.clone();
    let comp_watchdog = watchdog.clone();
    let computation_handle = std::thread::Builder::new()
        .name("computation-aggregator".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            // [修改] 调用新的方法名
            runtime.block_on(aggregator.run_aggregation_loop(
                comp_shutdown_rx,
                trade_rx,
                comp_watchdog,
            ));
        })?;

    // 6. ================= 在 I/O 运行时启动新的聚合与分发任务 ================

    // [修改] 初始化一个空的、默认的 DeltaBatch 实例，并用 Arc 包裹
    let initial_delta_batch = Arc::new(DeltaBatch::default());

    // [修改] watch 通道现在广播共享的增量批次
    let (klines_watch_tx, _klines_watch_rx) =
        watch::channel(initial_delta_batch.clone());

    // [V8 修改] 启动 gateway_task_for_web (仅负责Web推送)
    info!(target: "应用生命周期", "启动Gateway任务 (Web推送)...");
    log::context::spawn_instrumented_on(
        klagg::gateway_task_for_web(
            aggregator_handles.clone(),
            klines_watch_tx,
            config.clone(),
            shutdown_rx.clone(),
            watchdog.clone(),
        ),
        io_runtime,
    );

    // [V8 修改] 启动新的双持久化任务架构

    // 1. 启动 finalized_writer_task (高优先级)
    info!(target: "应用生命周期", "启动 Finalized-Writer 持久化任务 (高优先级)...");
    let finalized_persistence_handle = log::context::spawn_instrumented_on(
        klagg::finalized_writer_task(
            db.clone(),
            finalized_kline_rx,
            global_index_to_symbol.clone(),
            periods.clone(),
            config.clone(), // <-- 传入config
            shutdown_rx.clone(),
            watchdog.clone(),
        ),
        io_runtime,
    );

    // 2. 启动 snapshot_writer_task (低优先级)
    info!(target: "应用生命周期", "启动 Snapshot-Writer 持久化任务 (低优先级)...");
    let snapshot_clock_rx = clock_tx.subscribe();
    let snapshot_persistence_handle = log::context::spawn_instrumented_on(
        klagg::snapshot_writer_task(
            db.clone(),
            aggregator_handles[0].clone(),
            global_index_to_symbol.clone(),
            periods.clone(),
            snapshot_clock_rx,
            shutdown_rx.clone(),
            watchdog.clone(),
        ),
        io_runtime,
    );

    // --- [修改点 2: 移除动态品种管理器] ---
    // 找到启动品种管理器的代码，并将其整个移除或注释掉。

    /* // [测试模式下禁用] 不再需要动态扫描和管理全市场品种
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
    */
    info!(target: "应用生命周期", "品种管理器(Symbol Manager)任务已在测试模式下被禁用。系统将只处理自定义的固定品种列表。");

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

    // [修改] 简化线程等待逻辑
    info!(target: "应用生命周期", "正在等待计算线程退出...");
    if let Err(e) = computation_handle.join() {
        error!(target: "应用生命周期", panic = ?e, "计算线程在退出时发生 panic");
    }
    info!(target: "应用生命周期", "计算线程已成功退出。");

    // [V8 修改] 等待两个新持久化任务的 handle
    if let Err(e) = finalized_persistence_handle.await {
         error!(target: "应用生命周期", task = "finalized_persistence", panic = ?e, "高优先级持久化任务在退出时发生 panic");
    }
    if let Err(e) = snapshot_persistence_handle.await {
         error!(target: "应用生命周期", task = "snapshot_persistence", panic = ?e, "低优先级持久化任务在退出时发生 panic");
    }

    Ok(())
}
