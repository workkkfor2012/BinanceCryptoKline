//! K线聚合服务可视化测试入口
//!
//! 这个入口专门用于测试和可视化，支持两种模式：
//!
//! ## 严格测试模式 (STRICT_TEST_MODE = true)
//! - 使用固定的10个主要品种进行测试
//! - 不进行任何动态添加/删除操作
//! - 专注于核心K线聚合功能的稳定性测试
//! - 启动Web服务器进行数据可视化
//! - 禁用数据库持久化
//!
//! ## 动态测试模式 (STRICT_TEST_MODE = false)
//! - 从单个品种开始，每60秒模拟添加新品种
//! - 测试动态品种管理功能
//! - 启动Web服务器进行数据可视化
//! - 禁用数据库持久化

use anyhow::Result;
use chrono;
use kline_server::klagg_sub_threads::{
    self as klagg,
    DeltaBatch,
    InitialKlineData,
    WorkerCmd,
};
use kline_server::kldata::KlineBackfiller;
use kline_server::klcommon::{
    api::BinanceApi,
    db::Database,
    error::AppError,
    log::{self, init_ai_logging, shutdown_target_log_sender},
    server_time_sync::ServerTimeSyncManager,
    WatchdogV2,
    AggregateConfig,
};
use kline_server::soft_assert;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, watch, Notify, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, instrument, span, warn, trace, Instrument, Level};

// --- 常量定义 ---
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
const CLOCK_SAFETY_MARGIN_MS: u64 = 10;
const MIN_SLEEP_MS: u64 = 10;
const HEALTH_CHECK_INTERVAL_S: u64 = 10;

// --- 【新增】严格测试模式标志位 ---
/// 严格测试模式：只测试指定品种，不进行任何动态添加/删除操作
const STRICT_TEST_MODE: bool = true;

fn main() -> Result<()> {
    // [修复栈溢出] 检查是否需要在新线程中运行以获得更大的栈空间
    if std::env::var("KLINE_VISUAL_TEST_MAIN_THREAD").is_err() {
        // 设置环境变量标记，避免无限递归
        std::env::set_var("KLINE_VISUAL_TEST_MAIN_THREAD", "1");

        // 在新线程中运行主逻辑，使用更大的栈大小 (16MB)
        let handle = std::thread::Builder::new()
            .name("visual-test-main-with-large-stack".to_string())
            .stack_size(16 * 1024 * 1024) // 16MB 栈大小
            .spawn(|| {
                actual_main()
            })?;

        return handle.join().unwrap();
    }

    actual_main()
}

fn actual_main() -> Result<()> {
    // 1. ==================== 日志系统必须最先初始化 ====================
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
        .worker_threads(1)
        .thread_name("io-worker")
        .build()?;

    // 3. 在 I/O 运行时上下文中执行应用启动和管理逻辑
    let main_span = span!(target: "应用生命周期", Level::INFO, "klagg_visual_test_lifecycle");

    let result = io_runtime.block_on(run_visual_test_app(&io_runtime).instrument(main_span));

    if let Err(e) = &result {
        error!(target: "应用生命周期", error = ?e, "可视化测试应用因顶层错误而异常退出");
    } else {
        info!(target: "应用生命周期", log_type = "low_freq", "可视化测试应用程序正常关闭");
    }

    // 4. 优雅关闭
    info!(target: "应用生命周期", "主IO运行时开始关闭...");
    io_runtime.shutdown_timeout(Duration::from_secs(5));
    info!(target: "应用生命周期", "主IO运行时已关闭");

    // 5. 关闭日志系统
    shutdown_target_log_sender();

    result
}

#[instrument(target = "应用生命周期", skip_all, name = "run_visual_test_app")]
async fn run_visual_test_app(
    io_runtime: &Runtime,
) -> Result<()> {
    info!(target: "应用生命周期", log_type = "low_freq", "K线聚合服务可视化测试模式启动中...");
    warn!(target: "应用生命周期", log_type="low_freq", "警告：程序运行在可视化测试模式，数据库持久化已禁用！");

    // 1. ==================== 初始化全局资源 ====================
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    info!(
        target: "应用生命周期",
        log_type = "low_freq",
        path = DEFAULT_CONFIG_PATH,
        gateway_pull_interval_ms = config.gateway.pull_interval_ms,
        "配置文件加载成功"
    );

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
        info!(
            target: "应用生命周期",
            log_type = "audit_status",
            "✅ 数据完整性审计器：已启用"
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

    let api_client = Arc::new(BinanceApi);
    let db = Arc::new(Database::new_with_config(&config.database.database_path, config.persistence.queue_size)?);
    info!(target: "应用生命周期", log_type = "low_freq", path = %config.database.database_path, "数据库连接成功");

    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());
    let periods = Arc::new(config.supported_intervals.clone());
    info!(target: "应用生命周期", log_type = "low_freq", ?periods, "支持的K线周期已加载");

    // ==================== 测试模式数据准备流程 ====================
    let startup_data_prep_start = std::time::Instant::now();
    info!(target: "应用生命周期", log_type="startup", "➡️ [测试启动流程] 开始执行测试模式数据准备流程...");

    let backfiller = KlineBackfiller::new(db.clone(), periods.iter().cloned().collect());

    // 简化的历史补齐（仅针对测试品种）
    info!(target: "应用生命周期", log_type="startup", "➡️ [测试启动流程 | 1/2] 开始测试品种历史数据补齐...");
    let stage1_start = std::time::Instant::now();
    backfiller.run_once().await?;
    let stage1_duration = stage1_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage1_duration.as_millis(),
        duration_s = stage1_duration.as_secs_f64(),
        "✅ [测试启动流程 | 1/2] 测试品种历史数据补齐完成"
    );

    // 加载状态
    info!(target: "应用生命周期", log_type="startup", "➡️ [测试启动流程 | 2/2] 开始从数据库加载最新K线状态...");
    let stage2_start = std::time::Instant::now();
    let initial_klines = backfiller.load_latest_klines_from_db().await?;
    let stage2_duration = stage2_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage2_duration.as_millis(),
        duration_s = stage2_duration.as_secs_f64(),
        klines_count = initial_klines.len(),
        "✅ [测试启动流程 | 2/2] 数据库状态加载完成"
    );

    let total_startup_duration = startup_data_prep_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        总耗时_秒 = total_startup_duration.as_secs_f64(),
        总耗时_毫秒 = total_startup_duration.as_millis(),
        历史补齐_秒 = stage1_duration.as_secs_f64(),
        加载状态_秒 = stage2_duration.as_secs_f64(),
        最终K线数量 = initial_klines.len(),
        "✅ [测试启动流程] 所有数据准备阶段完成 - 性能统计"
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

    log::context::spawn_instrumented_on(
        run_test_clock_task(
            config.clone(),
            time_sync_manager.clone(),
            clock_tx.clone(),
            internal_shutdown_notify.clone(),
        ),
        io_runtime,
    );

    // 启动定时时间同步任务
    let time_sync_manager_for_task = time_sync_manager.clone();
    log::context::spawn_instrumented_on(
        async move {
            if let Err(e) = time_sync_manager_for_task.start().await {
                error!(target: "应用生命周期", error = ?e, "时间同步任务异常终止");
            }
        },
        io_runtime,
    );

    // 4. ============ 获取并建立全局品种索引 (测试模式) ============
    let (all_symbols_sorted, symbol_to_index_map) =
        initialize_test_symbol_indexing(&api_client, &db).await?;
    let symbol_count = all_symbols_sorted.len();

    soft_assert!(
        symbol_count > 0,
        message = "没有可处理的测试品种",
        actual_count = symbol_count,
    );

    if symbol_count == 0 {
        let err_msg = "没有可处理的测试品种，服务退出";
        error!(target: "应用生命周期", reason = err_msg);
        return Err(AppError::InitializationError(err_msg.into()).into());
    }
    info!(target: "应用生命周期", log_type = "low_freq", symbol_count, "测试品种索引初始化完成");

    let symbol_to_global_index = Arc::new(RwLock::new(symbol_to_index_map));
    let global_index_to_symbol = Arc::new(RwLock::new(all_symbols_sorted));
    let global_symbol_count = Arc::new(AtomicUsize::new(symbol_count));

    // ==================== 初始化健康监控中枢 ====================
    let watchdog = Arc::new(WatchdogV2::new());

    // 5. ================ 创建并启动唯一的聚合器和I/O任务 ================
    info!(target: "应用生命周期", "启动中心化聚合器模型 (测试模式)...");

    let all_symbols: Vec<String> = global_index_to_symbol.read().await.clone();

    let (mut aggregator, outputs) = klagg::KlineAggregator::new(
        &all_symbols,
        symbol_to_global_index.clone(),
        periods.clone(),
        Some(w3_cmd_rx),
        clock_tx.subscribe(),
        initial_klines_arc.clone(),
        &config,
    ).await?;
    let ws_cmd_rx = outputs.ws_cmd_rx;
    let trade_rx = outputs.trade_rx;
    let mut finalized_kline_rx = outputs.finalized_kline_rx; // [修改] 变为 mut

    let aggregator_read_handle = aggregator.get_read_handle();
    let aggregator_trade_sender = aggregator.get_trade_sender();
    let aggregator_handles = Arc::new(vec![aggregator_read_handle]);

    // 创建 AggTradeMessageHandler
    let agg_trade_handler = Arc::new(kline_server::klcommon::websocket::AggTradeMessageHandler::new(
        aggregator_trade_sender,
        symbol_to_global_index.clone(),
    ));

    // 启动 I/O 任务
    log::context::spawn_instrumented_on(
        klagg::run_io_loop(
            all_symbols,
            config.clone(),
            shutdown_rx.clone(),
            ws_cmd_rx,
            agg_trade_handler,
            watchdog.clone(),
        ),
        io_runtime,
    );

    // 启动计算线程
    let comp_shutdown_rx = shutdown_rx.clone();
    let comp_watchdog = watchdog.clone();
    let computation_handle = std::thread::Builder::new()
        .name("computation-aggregator".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(aggregator.run_aggregation_loop(
                comp_shutdown_rx,
                trade_rx,
                comp_watchdog,
            ));
        })?;

    // 6. ================= 启动测试专用的聚合与分发任务 ================
    let initial_delta_batch = Arc::new(DeltaBatch::default());
    let (klines_watch_tx, klines_watch_rx) = watch::channel(initial_delta_batch.clone());
    let (_db_queue_tx, _db_queue_rx) = mpsc::channel::<Arc<DeltaBatch>>(config.persistence.queue_size);

    // 启动 Gateway 任务
    info!(target: "应用生命周期", "准备启动Gateway任务（测试模式）...");
    log::context::spawn_instrumented_on(
        klagg::gateway_task_for_web(
            aggregator_handles,
            klines_watch_tx,
            config.clone(),
            shutdown_rx.clone(),
            watchdog.clone(),
        ),
        io_runtime,
    );

    // 启动可视化测试Web服务器（替代数据库写入任务）
    info!(target: "应用生命周期", "启动可视化测试Web服务器...");
    log::context::spawn_instrumented_on(
        klagg::web_server::run_visual_test_server(
            klines_watch_rx,
            global_index_to_symbol.clone(),
            periods.clone(),
            shutdown_rx.clone(),
        ),
        io_runtime,
    );

    // 启动测试品种管理器
    log::context::spawn_instrumented_on(
        run_test_symbol_manager(
            symbol_to_global_index,
            global_index_to_symbol,
            global_symbol_count,
            w3_cmd_tx,
        ),
        io_runtime,
    );

    // [增强] 新增一个简单的任务来排空 finalized_kline_rx 通道。
    // 这可以防止通道被填满，从而确保测试环境的行为与生产环境更一致。
    tokio::spawn(async move {
        info!("启动模拟的 finalized_kline 消费者，防止通道阻塞");
        while let Some(_) = finalized_kline_rx.recv().await {
            // 什么也不做，只是消费消息
        }
        warn!("finalized_kline 通道已关闭，消费者退出");
    });

    // [修改] 只有在 full-audit 模式下，才处理审计相关的通道和任务
    #[cfg(feature = "full-audit")]
    {
        info!(
            target: "应用生命周期",
            log_type = "audit_startup",
            "🔍 可视化测试模式：启用 full-audit，正在启动审计任务..."
        );

        let lifecycle_event_rx_for_validator = outputs.lifecycle_event_tx.subscribe();

        info!(
            target: "应用生命周期",
            log_type = "audit_startup",
            "🚀 启动生命周期校验器任务..."
        );
        log::context::spawn_instrumented_on(
            klagg::run_lifecycle_validator_task(lifecycle_event_rx_for_validator, shutdown_rx.clone()),
            io_runtime,
        );

        info!(
            target: "应用生命周期",
            log_type = "audit_startup",
            "✅ 所有审计任务启动完成 - 系统现在具备完整的审计能力"
        );
    }
    #[cfg(not(feature = "full-audit"))]
    {
        info!(
            target: "应用生命周期",
            log_type = "audit_startup",
            "⚠️ 可视化测试模式：审计功能已禁用，使用零成本抽象模式"
        );
    }

    // 启动监控中枢
    io_runtime.spawn(
        watchdog.run(
            Duration::from_secs(HEALTH_CHECK_INTERVAL_S),
            internal_shutdown_notify.clone(),
        )
    );

    // 7. ==================== 等待并处理关闭信号 ====================
    info!(target: "应用生命周期", "所有测试服务已启动，等待关闭信号 (Ctrl+C)...");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(target: "应用生命周期", log_type = "low_freq", reason = "received_ctrl_c", "接收到关闭信号，开始优雅关闭");
        },
        _ = internal_shutdown_notify.notified() => {
            info!(target: "应用生命周期", log_type = "low_freq", reason = "internal_shutdown", "接收到内部关闭通知，开始优雅关闭");
        }
    }

    let _ = shutdown_tx.send(true);

    // 等待计算线程退出
    info!(target: "应用生命周期", "正在等待计算线程退出...");
    if let Err(e) = computation_handle.join() {
        error!(target: "应用生命周期", panic = ?e, "计算线程在退出时发生 panic");
    }
    info!(target: "应用生命周期", "计算线程已成功退出。");

    Ok(())
}

/// 测试专用全局时钟任务（5秒间隔）
#[instrument(target = "全局时钟", skip_all, name="run_test_clock_task")]
async fn run_test_clock_task(
    _config: Arc<AggregateConfig>,
    time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    const CLOCK_INTERVAL_MS: i64 = 5_000; // 5秒
    info!(target: "全局时钟", log_type="low_freq", interval_ms = CLOCK_INTERVAL_MS, "全局时钟任务已启动，将每5秒输出校准时间");

    let mut time_sync_retry_count = 0;
    const MAX_TIME_SYNC_RETRIES: u32 = 10;

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            time_sync_retry_count += 1;
            warn!(target: "全局时钟", log_type="retry",
                  retry_count = time_sync_retry_count,
                  max_retries = MAX_TIME_SYNC_RETRIES,
                  "时间同步失效，正在主动重试同步...");

            match time_sync_manager.sync_time_once().await {
                Ok((diff, delay)) => {
                    info!(target: "全局时钟", log_type="recovery",
                          diff_ms = diff,
                          delay_ms = delay,
                          "在重试期间，时间同步成功恢复");
                    time_sync_retry_count = 0;
                    continue;
                }
                Err(e) => {
                    error!(target: "全局时钟", log_type="retry_failure",
                           retry_count = time_sync_retry_count,
                           error = ?e,
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

            sleep(Duration::from_millis(1000 * time_sync_retry_count as u64)).await;
            continue;
        } else {
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
        // [日志增强] 在发送时钟信号前，记录关键决策信息
        debug!(
            target: "全局时钟",
            sent_time = final_time,
            "发送时钟滴答信号"
        );

        // 输出校准后的服务器时间
        let datetime = chrono::DateTime::from_timestamp_millis(final_time)
            .unwrap_or_else(|| chrono::Utc::now());
        let formatted_time = datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string();
        info!(target: "全局时钟",
              log_type="checkpoint",
              server_time_ms = final_time,
              offset_ms = time_sync_manager.get_time_diff(),
              "校准后的服务器时间: {}", formatted_time);

        if clock_tx.send(final_time).is_err() {
            warn!(target: "全局时钟", "主时钟通道已关闭，任务退出");
            break;
        }
    }
    warn!(target: "全局时钟", "全局时钟任务已退出");
}

/// 初始化测试品种索引
#[instrument(target = "应用生命周期", skip_all, name = "initialize_test_symbol_indexing")]
async fn initialize_test_symbol_indexing(
    _api: &BinanceApi,
    db: &Database,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    info!(target: "应用生命周期", "开始初始化测试品种索引");

    // 根据测试模式选择品种列表
    let symbols = if STRICT_TEST_MODE {
        // 严格测试模式：使用固定的多个主要品种，不进行动态添加/删除
        vec![
             "BTCUSDT"
           // "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
            //"ADAUSDT", "BNBUSDT", "LTCUSDT", "AVAXUSDT", "DOTUSDT"
        ]
        .into_iter()
        .map(String::from)
        .collect::<Vec<String>>()
    } else {
        // 动态测试模式：只使用一个品种，后续会动态添加
        vec!["BTCUSDT"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<String>>()
    };

    info!(target: "应用生命周期", count = symbols.len(), "测试品种列表获取成功");

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    soft_assert!(!sorted_symbols_with_time.is_empty(),
        message = "未能找到任何带有历史数据的测试品种",
        symbols_count = sorted_symbols_with_time.len(),
    );

    if sorted_symbols_with_time.is_empty() {
        return Err(AppError::DataError("No test symbols with historical data found.".to_string()).into());
    }

    // 按时间戳和品种名排序
    sorted_symbols_with_time.sort_by(|(symbol_a, time_a), (symbol_b, time_b)| {
        (time_a, symbol_a).cmp(&(time_b, symbol_b))
    });

    // 构建汇总的品种序列字符串
    let symbols_summary = sorted_symbols_with_time
        .iter()
        .enumerate()
        .map(|(index, (symbol, _))| format!("{}:{}", index, symbol))
        .collect::<Vec<_>>()
        .join(", ");

    info!(target: "应用生命周期",
        symbols_count = sorted_symbols_with_time.len(),
        symbols_summary = symbols_summary,
        "排序后的测试品种序列"
    );

    let all_sorted_symbols: Vec<String> =
        sorted_symbols_with_time.into_iter().map(|(s, _)| s).collect();

    let symbol_to_index: HashMap<String, usize> = all_sorted_symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| (symbol.clone(), index))
        .collect();

    info!(target: "应用生命周期", count = all_sorted_symbols.len(), "测试品种索引构建完成，并按上市时间排序");
    Ok((all_sorted_symbols, symbol_to_index))
}

/// 测试模式的品种管理器 - 动态添加和删除品种
#[instrument(target = "品种管理器", skip_all, name = "run_test_symbol_manager")]
async fn run_test_symbol_manager(
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    // 【新增】严格测试模式检查
    if STRICT_TEST_MODE {
        info!(target: "品种管理器", log_type = "low_freq", "严格测试模式已启用，跳过所有动态品种管理操作");
        // 在严格测试模式下，只是保持任务运行但不执行任何操作
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    info!(target: "品种管理器", log_type = "low_freq", "动态测试模式启动，等待2秒以确保I/O核心初始化...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 立即添加 ETHUSDT 进行测试
    info!(target: "品种管理器", log_type = "low_freq", "开始执行ETHUSDT添加/删除测试...");

    // 添加 ETHUSDT
    let ethusdt_symbol = "ETHUSDT".to_string();
    let add_result = add_symbol_to_system(
        &ethusdt_symbol,
        &symbol_to_global_index,
        &global_index_to_symbol,
        &global_symbol_count,
        &cmd_tx,
    ).await;

    match add_result {
        Ok(()) => {
            info!(target: "品种管理器", log_type = "low_freq", symbol = %ethusdt_symbol, "ETHUSDT添加成功，2秒后将删除");

            // 等待2秒
            tokio::time::sleep(Duration::from_secs(2)).await;

            // 删除 ETHUSDT
            let remove_result = remove_symbol_from_system(
                &ethusdt_symbol,
                &symbol_to_global_index,
                &global_index_to_symbol,
                &global_symbol_count,
                &cmd_tx,
            ).await;

            match remove_result {
                Ok(()) => {
                    info!(target: "品种管理器", log_type = "low_freq", symbol = %ethusdt_symbol, "ETHUSDT删除成功，测试完成");
                }
                Err(e) => {
                    warn!(target: "品种管理器", symbol = %ethusdt_symbol, error = %e, "ETHUSDT删除失败");
                }
            }
        }
        Err(e) => {
            warn!(target: "品种管理器", symbol = %ethusdt_symbol, error = %e, "ETHUSDT添加失败");
        }
    }

    // 继续原有的定期添加/删除逻辑
    let test_symbols = vec![
        "AVAXUSDT", "MATICUSDT", "LINKUSDT", "UNIUSDT", "ATOMUSDT",
        "DOTUSDT", "FILUSDT", "TRXUSDT", "ETCUSDT", "XLMUSDT",
        "VETUSDT", "ICPUSDT", "FTMUSDT", "HBARUSDT", "NEARUSDT",
        "ALGOUSDT", "MANAUSDT", "SANDUSDT", "AXSUSDT", "THETAUSDT"
    ];

    let mut symbol_index = 0;
    let mut added_symbols: Vec<String> = Vec::new();
    let mut add_interval = tokio::time::interval(Duration::from_secs(30));
    let mut remove_interval = tokio::time::interval(Duration::from_secs(45));
    add_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    remove_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // --- 添加逻辑 ---
            _ = add_interval.tick() => {
                if symbol_index >= test_symbols.len() {
                    continue;
                }

                let symbol_to_add = test_symbols[symbol_index].to_string();
                symbol_index += 1;

                // 检查品种是否已存在
                let read_guard = symbol_to_global_index.read().await;
                if read_guard.contains_key(&symbol_to_add) {
                    drop(read_guard);
                    continue;
                }
                drop(read_guard);

                info!(target: "品种管理器", log_type = "low_freq", symbol = %symbol_to_add, "测试模式：模拟发现新品种");

                match add_symbol_to_system(
                    &symbol_to_add,
                    &symbol_to_global_index,
                    &global_index_to_symbol,
                    &global_symbol_count,
                    &cmd_tx,
                ).await {
                    Ok(()) => {
                        added_symbols.push(symbol_to_add.clone());
                        info!(target: "品种管理器", symbol = %symbol_to_add, "品种添加成功");
                    }
                    Err(e) => {
                        warn!(target: "品种管理器", symbol = %symbol_to_add, error = %e, "品种添加失败");
                    }
                }
            },

            // --- 删除逻辑 ---
            _ = remove_interval.tick() => {
                if let Some(symbol_to_remove) = added_symbols.pop() {
                    info!(target: "品种管理器", log_type = "low_freq", symbol = %symbol_to_remove, "测试模式：模拟品种下架");

                    match remove_symbol_from_system(
                        &symbol_to_remove,
                        &symbol_to_global_index,
                        &global_index_to_symbol,
                        &global_symbol_count,
                        &cmd_tx,
                    ).await {
                        Ok(()) => {
                            info!(target: "品种管理器", symbol = %symbol_to_remove, "品种删除成功");
                        }
                        Err(e) => {
                            warn!(target: "品种管理器", symbol = %symbol_to_remove, error = %e, "品种删除失败");
                        }
                    }
                }
            }
        }
    }
}

/// 添加品种到系统的辅助函数
async fn add_symbol_to_system(
    symbol: &str,
    symbol_to_global_index: &Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: &Arc<RwLock<Vec<String>>>,
    global_symbol_count: &Arc<AtomicUsize>,
    cmd_tx: &mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<usize, String>>();

    // 创建一个模拟的 InitialKlineData
    let initial_data = InitialKlineData {
        open: 100.0,
        high: 101.0,
        low: 99.0,
        close: 100.5,
        volume: 10.0,
        turnover: 1005.0,
    };

    let cmd = WorkerCmd::AddSymbol {
        symbol: symbol.to_string(),
        initial_data,
        first_kline_open_time: chrono::Utc::now().timestamp_millis(),
        ack: ack_tx,
    };

    if cmd_tx.send(cmd).await.is_err() {
        return Err(anyhow::anyhow!("向Worker发送AddSymbol命令失败，通道可能已关闭"));
    }

    match ack_rx.await {
        Ok(Ok(new_index)) => {
            let mut write_map = symbol_to_global_index.write().await;
            let mut write_vec = global_index_to_symbol.write().await;

            // 核心同步逻辑
            if new_index == write_vec.len() {
                write_map.insert(symbol.to_string(), new_index);
                write_vec.push(symbol.to_string());
                global_symbol_count.store(write_vec.len(), Ordering::SeqCst);
                info!(target: "品种管理器", symbol = %symbol, new_index, "全局视图已更新");
                Ok(())
            } else {
                error!(
                    log_type = "assertion",
                    symbol = %symbol,
                    new_index,
                    vec_len = write_vec.len(),
                    "全局索引与向量长度不一致，发生严重逻辑错误！"
                );
                Err(anyhow::anyhow!("全局索引与向量长度不一致"))
            }
        }
        Ok(Err(e)) => {
            Err(anyhow::anyhow!("Worker拒绝添加品种: {}", e))
        }
        Err(_) => {
            Err(anyhow::anyhow!("与Worker的确认通道已关闭"))
        }
    }
}

/// 从系统中删除品种的辅助函数
async fn remove_symbol_from_system(
    symbol: &str,
    symbol_to_global_index: &Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: &Arc<RwLock<Vec<String>>>,
    global_symbol_count: &Arc<AtomicUsize>,
    cmd_tx: &mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<(usize, Vec<(String, usize)>), String>>();
    let cmd = WorkerCmd::RemoveSymbol {
        symbol: symbol.to_string(),
        ack: ack_tx
    };

    if cmd_tx.send(cmd).await.is_err() {
        return Err(anyhow::anyhow!("向Worker发送RemoveSymbol命令失败，通道可能已关闭"));
    }

    match ack_rx.await {
        Ok(Ok((removed_index, index_changes))) => {
            let mut write_map = symbol_to_global_index.write().await;
            let mut write_vec = global_index_to_symbol.write().await;

            // 1. 从 Vec 和 Map 中移除
            write_map.remove(symbol);
            if removed_index < write_vec.len() {
                write_vec.remove(removed_index);
            }

            // 2. 应用变更集
            for (symbol, new_index) in index_changes {
                if let Some(entry) = write_map.get_mut(&symbol) {
                    *entry = new_index;
                }
            }

            // 3. 更新全局计数
            global_symbol_count.store(write_vec.len(), Ordering::SeqCst);
            info!(target: "品种管理器", symbol = %symbol, "全局索引同步完成");
            Ok(())
        }
        Ok(Err(e)) => {
            Err(anyhow::anyhow!("Worker拒绝删除品种: {}", e))
        }
        Err(_) => {
            Err(anyhow::anyhow!("与Worker的确认通道已关闭"))
        }
    }
}
