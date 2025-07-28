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
use kline_server::klagg_sub_threads::{self as klagg, DeltaBatch, InitialKlineData, WorkerCmd};
use kline_server::kldata::KlineBackfiller;
use kline_server::klcommon::{
    api::BinanceApi,
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
use tracing::{error, info, instrument, span, warn, trace, Instrument, Level};
// use uuid; // 移除未使用的导入

// --- 常量定义 ---
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
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
        .worker_threads(1) // [修改] IO线程数明确设置为1
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
async fn run_app(
    io_runtime: &Runtime,
) -> Result<()> {
    info!(target: "应用生命周期",log_type = "low_freq", "K线聚合服务启动中...");
    // trace!(log_type = "low_freq", "K线聚合服务启动中...");
    // trace!("🔍 开始初始化全局资源...");

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



    let api_client = Arc::new(BinanceApi); // [修改] BinanceApi现在是无状态的

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

    // --- 阶段三: 最终追赶 ---（暂时注释，测试性能）
    // info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 3/4] 开始最终追赶补齐（最高并发模式）...");
    // let stage3_start = std::time::Instant::now();
    // // 使用轮次3，可以在backfill模块中为其配置更高的并发度
    // backfiller.run_once_with_round(3).await?;
    // let stage3_duration = stage3_start.elapsed();
    // info!(target: "应用生命周期", log_type="startup",
    //     duration_ms = stage3_duration.as_millis(),
    //     duration_s = stage3_duration.as_secs_f64(),
    //     "✅ [启动流程 | 3/4] 最终追赶补齐完成"
    // );

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

    // 4. ============ 获取并建立全局品种索引 (G_Index*) ============
    let (all_symbols_sorted, symbol_to_index_map) =
        initialize_symbol_indexing(&api_client, &db).await?;
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
    info!(target: "应用生命周期", log_type = "low_freq", symbol_count, "全局品种索引初始化完成");

    let symbol_to_global_index = Arc::new(RwLock::new(symbol_to_index_map));
    let global_index_to_symbol = Arc::new(RwLock::new(all_symbols_sorted));
    let global_symbol_count = Arc::new(AtomicUsize::new(symbol_count));

    // ==================== 初始化健康监控中枢 ====================
    let watchdog = Arc::new(WatchdogV2::new());

    // 5. ================ 创建并启动唯一的聚合器和I/O任务 ================
    info!(target: "应用生命周期", "启动中心化聚合器模型 (1 IO + 1 计算)...");

    // [修改] 直接获取所有品种
    let all_symbols: Vec<String> = global_index_to_symbol.read().await.clone();

    // [修改] 直接创建唯一的 KlineAggregator 实例
    let (mut aggregator, ws_cmd_rx, trade_rx) = klagg::KlineAggregator::new(
        &all_symbols,
        symbol_to_global_index.clone(),
        periods.clone(),
        Some(w3_cmd_rx), // 指令通道现在给唯一的聚合器
        clock_tx.subscribe(),
        initial_klines_arc.clone(),
        &config, // [修改] 传入config
    ).await?;

    let aggregator_read_handle = aggregator.get_read_handle();
    let aggregator_trade_sender = aggregator.get_trade_sender();
    // [修改] 变量重命名，并保持Vec结构以兼容gateway_task的签名，这是一种务实的做法
    let aggregator_handles = Arc::new(vec![aggregator_read_handle]);

    // [修改] 启动唯一的 I/O 任务 (run_io_loop的签名也将被修改)
    log::context::spawn_instrumented_on(
        klagg::run_io_loop(
            all_symbols,
            config.clone(),
            shutdown_rx.clone(),
            ws_cmd_rx,
            aggregator_trade_sender,
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

    // [修改] mpsc 通道也使用 Arc<DeltaBatch> 以支持零克隆转发
    let (db_queue_tx, db_queue_rx) =
        mpsc::channel::<Arc<DeltaBatch>>(config.persistence.queue_size);

    // 启动 Gateway 任务
    info!(target: "应用生命周期", "准备启动Gateway任务，聚合器数量: {}", aggregator_handles.len());
    log::context::spawn_instrumented_on(
        klagg::gateway_task(
            aggregator_handles, // [修改]
            klines_watch_tx,
            db_queue_tx,
            config.clone(),
            shutdown_rx.clone(),
            watchdog.clone(),
        ),
        io_runtime,
    );
    info!(target: "应用生命周期", "Gateway任务已提交到运行时");

    // 启动数据库写入任务
    info!(target: "应用生命周期", "启动数据库写入任务 (新架构)...");
    let persistence_handle = Some(log::context::spawn_instrumented_on(
        klagg::db_writer_task(
            db.clone(),
            db_queue_rx,
            global_index_to_symbol.clone(),
            periods.clone(),
            shutdown_rx.clone(),
            watchdog.clone(),
        ),
        io_runtime,
    ));

    // 启动品种管理器
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

    // [修改] 简化线程等待逻辑
    info!(target: "应用生命周期", "正在等待计算线程退出...");
    if let Err(e) = computation_handle.join() {
        error!(target: "应用生命周期", panic = ?e, "计算线程在退出时发生 panic");
    }
    info!(target: "应用生命周期", "计算线程已成功退出。");

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
    _config: Arc<AggregateConfig>, // config 不再需要，但保留参数以减少函数签名变动
    time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    // 【核心修改】时钟任务的目标是严格对齐到服务器时间的"整分钟"，不再依赖任何K线周期。
    const CLOCK_INTERVAL_MS: i64 = 60_000; // 60秒
    info!(target: "全局时钟", log_type="low_freq", interval_ms = CLOCK_INTERVAL_MS, "全局时钟任务已启动，将按整分钟对齐");

    // 时间同步重试计数器
    let mut time_sync_retry_count = 0;
    const MAX_TIME_SYNC_RETRIES: u32 = 10;

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            time_sync_retry_count += 1;
            warn!(target: "全局时钟", log_type="retry",
                  retry_count = time_sync_retry_count,
                  max_retries = MAX_TIME_SYNC_RETRIES,
                  "时间同步失效，正在主动重试同步...");

            // 主动尝试进行一次同步，并捕获具体错误
            match time_sync_manager.sync_time_once().await {
                Ok((diff, delay)) => {
                    // 如果意外成功了，就重置计数器并继续
                    info!(target: "全局时钟", log_type="recovery",
                          diff_ms = diff,
                          delay_ms = delay,
                          "在重试期间，时间同步成功恢复");
                    time_sync_retry_count = 0;
                    continue;
                }
                Err(e) => {
                    // 打印出底层的、具体的网络错误
                    error!(target: "全局时钟", log_type="retry_failure",
                           retry_count = time_sync_retry_count,
                           error = ?e, // <-- 这是关键的补充信息
                           "时间同步重试失败");
                }
            }

            if time_sync_retry_count >= MAX_TIME_SYNC_RETRIES {
                error!(target: "全局时钟", log_type="assertion", reason="time_sync_invalid",
                       retry_count = time_sync_retry_count,
                       "时间同步失效，已达到最大重试次数，服务将关闭");
                shutdown_notify.notify_one();
                break;
            }

            // 等待一段时间后再次尝试 (可以使用逐渐增长的等待时间)
            sleep(Duration::from_millis(1000 * time_sync_retry_count as u64)).await;
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

        // 【核心修改】计算下一个服务器时间整分钟点
        let next_tick_point = (now / CLOCK_INTERVAL_MS + 1) * CLOCK_INTERVAL_MS;
        let wakeup_time = next_tick_point + CLOCK_SAFETY_MARGIN_MS as i64;
        let sleep_duration_ms = (wakeup_time - now).max(MIN_SLEEP_MS as i64) as u64;

        trace!(target: "全局时钟",
            now,
            next_tick_point,
            wakeup_time,
            sleep_duration_ms,
            "计算下一次唤醒时间"
        );
        sleep(Duration::from_millis(sleep_duration_ms)).await;

        let final_time = time_sync_manager.get_calibrated_server_time();
        if clock_tx.send(final_time).is_err() {
            warn!(target: "全局时钟", "主时钟通道已关闭，任务退出");
            break;
        }
    }
    warn!(target: "全局时钟", "全局时钟任务已退出");
}

/// 初始化品种索引
#[instrument(target = "应用生命周期", skip_all, name = "initialize_symbol_indexing")]
async fn initialize_symbol_indexing(
    _api: &BinanceApi,
    db: &Database,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    info!(target: "应用生命周期", "开始初始化品种索引");

    info!(target: "应用生命周期", "正在从币安API获取所有U本位永续合约品种...");
    let temp_client = BinanceApi::create_new_client()?;
    let (trading_symbols, delisted_symbols) = BinanceApi::get_trading_usdt_perpetual_symbols(&temp_client).await?;

    // 处理已下架的品种
    if !delisted_symbols.is_empty() {
        info!(
            target: "应用生命周期",
            "发现已下架品种: {}，这些品种将不会被包含在索引中",
            delisted_symbols.join(", ")
        );
    }

    let symbols = trading_symbols;
    info!(target: "应用生命周期", count = symbols.len(), "品种列表获取成功");

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    soft_assert!(!sorted_symbols_with_time.is_empty(),
        message = "未能找到任何带有历史数据的品种",
        symbols_count = sorted_symbols_with_time.len(),
    );

    if sorted_symbols_with_time.is_empty() {
        return Err(AppError::DataError("No symbols with historical data found.".to_string()).into());
    }

    // 使用 sort_by 和元组比较来实现主次双重排序
    // 1. 主要按时间戳 (time) 升序排序
    // 2. 如果时间戳相同，则次要按品种名 (symbol) 的字母序升序排序
    // 这确保了每次启动时的排序结果都是确定和稳定的。
    sorted_symbols_with_time.sort_by(|(symbol_a, time_a), (symbol_b, time_b)| {
        (time_a, symbol_a).cmp(&(time_b, symbol_b))
    });

    // 打印排序后的序列，显示品种名称、时间戳和全局索引
    // 构建汇总的品种序列字符串（显示所有品种）
    let symbols_summary = sorted_symbols_with_time
        .iter()
        .enumerate()
        .map(|(index, (symbol, _))| format!("{}:{}", index, symbol))
        .collect::<Vec<_>>()
        .join(", ");

    info!(target: "应用生命周期",
        symbols_count = sorted_symbols_with_time.len(),
        symbols_summary = symbols_summary,
        "排序后的品种序列（所有品种）"
    );

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
    info!(target: "品种管理器", log_type = "low_freq", "品种管理器启动 - 基于MiniTicker实时发现新品种");

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

    // 辅助函数，用于安全地解析浮点数字符串
    let parse_or_zero = |s: &str, field_name: &str, symbol: &str| -> f64 {
        s.parse::<f64>().unwrap_or_else(|_| {
            warn!(target: "品种管理器", %symbol, field_name, value = %s, "无法解析新品种的初始数据，将使用0.0");
            0.0
        })
    };

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
                let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<usize, String>>();

                // 从 ticker 数据中解析完整的 OHLCV 数据
                let initial_data = InitialKlineData {
                    open: parse_or_zero(&ticker.open_price, "open", &ticker.symbol),
                    high: parse_or_zero(&ticker.high_price, "high", &ticker.symbol),
                    low: parse_or_zero(&ticker.low_price, "low", &ticker.symbol),
                    close: parse_or_zero(&ticker.close_price, "close", &ticker.symbol),
                    volume: parse_or_zero(&ticker.total_traded_volume, "volume", &ticker.symbol),
                    turnover: parse_or_zero(&ticker.total_traded_quote_volume, "turnover", &ticker.symbol),
                };

                // 从 ticker 中获取事件时间戳
                // ticker.event_time 是 u64 类型，需要转换为 i64
                let event_time = ticker.event_time as i64;

                let cmd = WorkerCmd::AddSymbol {
                    symbol: ticker.symbol.clone(),
                    initial_data,
                    // [修改] 传递从 miniTicker 事件中获取的精确时间戳
                    first_kline_open_time: event_time,
                    ack: ack_tx,
                };

                if cmd_tx.send(cmd).await.is_err() {
                    warn!(target: "品种管理器", symbol=%ticker.symbol, "向Worker 3发送AddSymbol命令失败，通道可能已关闭");
                    return Ok(());
                }

                match ack_rx.await {
                    Ok(Ok(new_global_index)) => {
                        let mut write_guard_map = symbol_to_global_index.write().await;
                        let mut write_guard_vec = global_index_to_symbol.write().await;

                        if !write_guard_map.contains_key(&ticker.symbol) {
                            // 核心同步逻辑：确保索引与向量长度一致
                            if new_global_index == write_guard_vec.len() {
                                write_guard_map.insert(ticker.symbol.clone(), new_global_index);
                                write_guard_vec.push(ticker.symbol.clone());
                                global_symbol_count.store(write_guard_vec.len(), Ordering::SeqCst);
                                info!(target: "品种管理器", symbol = %ticker.symbol, new_global_index, "成功添加新品种到全局索引和Worker");
                            } else {
                                error!(
                                    log_type = "assertion",
                                    symbol = %ticker.symbol,
                                    new_global_index,
                                    vec_len = write_guard_vec.len(),
                                    "全局索引与向量长度不一致，发生严重逻辑错误！"
                                );
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = %e, "添加新品种失败，Worker拒绝");
                    }
                    Err(_) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = "ack_channel_closed", "添加新品种失败，与Worker的确认通道已关闭");
                    }
                }
            }
        }
    }
    warn!(target: "品种管理器", "品种管理器任务已退出");
    Ok(())
}



