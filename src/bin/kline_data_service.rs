// K线数据服务主程序 - 专注于K线补齐功能
use kline_server::klcommon::{Database, Result, AppError, AggregateConfig};
use kline_server::kldata::KlineBackfiller;

use std::sync::Arc;
use std::path::Path;
use std::time::Duration;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{info, error, instrument, info_span, Instrument};

// 导入轨迹提炼器组件
use kline_server::klcommon::log::{
    TraceDistillerStore,
    TraceDistillerLayer,
    distill_all_completed_traces_to_text,
    TransactionLayer,
    TransactionLogManager
};

/// 默认配置文件路径
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";

// ========================================
// 🔧 测试开关配置
// ========================================
/// 是否启用测试模式（限制为只处理 BTCUSDT）
const TEST_MODE: bool = false;

/// 测试模式下使用的交易对
const TEST_SYMBOLS: &[&str] = &["BTCUSDT"];

/// 程序运行期间的快照计数器，用于生成有序的文件名
static SNAPSHOT_COUNTER: AtomicU32 = AtomicU32::new(1);
// ========================================

#[tokio::main]
#[instrument(name = "main", ret, err)]
async fn main() -> Result<()> {
    // ✨ [修改] 接收 TransactionLogManager 实例
    let (distiller_store, transaction_manager) = init_logging_with_distiller();

    // 创建应用程序的根Span，代表整个应用生命周期
    let root_span = info_span!(
        "kline_service_app",
        service = "kline_data_service",
        version = env!("CARGO_PKG_VERSION"),
        test_mode = TEST_MODE
    );

    tracing::info!(
        message = "应用程序启动",
        service = "kline_data_service",
        version = env!("CARGO_PKG_VERSION"),
        test_mode = TEST_MODE
    );

    // 在根Span的上下文中运行整个应用
    let result = run_app().instrument(root_span).await;

    // 记录应用程序退出状态
    match &result {
        Ok(_) => {
            tracing::info!(message = "应用程序正常退出");
        },
        Err(e) => {
            tracing::error!(
                message = "应用程序异常退出",
                error.summary = e.get_error_type_summary(),
                error.details = %e
            );
        }
    }

    // 程序退出时生成最终快照
    generate_final_snapshot(&distiller_store).await;

    // ✨ [新增] 在程序完全退出前，优雅地关闭业务追踪日志
    if let Some(mut manager) = transaction_manager {
        info!(target: "kline_data_service", log_type = "module", "正在关闭业务追踪日志，确保所有日志已写入...");
        manager.shutdown().await;
        info!(target: "kline_data_service", log_type = "module", "业务追踪日志已关闭。");
    }

    result
}

/// 应用程序的核心业务逻辑
#[instrument(name = "run_app", ret, err)]
async fn run_app() -> Result<()> {
    // K线周期配置
    let intervals = "1m,5m,30m,1h,4h,1d,1w".to_string();
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    tracing::info!(
        message = "应用程序核心业务流程开始",
        intervals = %intervals,
        interval_count = interval_list.len()
    );

    info!(target: "kline_data_service", log_type = "module", "启动K线数据补齐服务");
    info!(target: "kline_data_service", log_type = "module", "使用周期: {}", intervals);

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    tracing::debug!(
        message = "初始化数据库连接",
        db_path = %db_path.display()
    );

    let db = match Database::new(&db_path) {
        Ok(database) => {
            tracing::info!(message = "数据库连接成功", db_path = %db_path.display());
            Arc::new(database)
        },
        Err(e) => {
            tracing::error!(
                message = "数据库连接失败",
                error.summary = e.get_error_type_summary(),
                error.details = %e,
                db_path = %db_path.display()
            );
            return Err(e);
        }
    };

    // 执行K线数据补齐
    info!(target: "kline_data_service", log_type = "module", "开始补齐K线数据...");

    // 创建补齐器实例 - 决策点：测试模式vs生产模式
    let backfiller = if TEST_MODE {
        info!(target: "kline_data_service", log_type = "module", "🔧 启用测试模式，限制交易对为: {:?}", TEST_SYMBOLS);
        tracing::debug!(
            decision = "backfiller_mode",
            mode = "test",
            symbols = ?TEST_SYMBOLS,
            symbol_count = TEST_SYMBOLS.len(),
            "创建测试模式补齐器"
        );
        KlineBackfiller::new_test_mode(
            db.clone(),
            interval_list,
            TEST_SYMBOLS.iter().map(|s| s.to_string()).collect()
        )
    } else {
        info!(target: "kline_data_service", log_type = "module", "📡 生产模式，将获取所有交易对");
        tracing::debug!(
            decision = "backfiller_mode",
            mode = "production",
            "创建生产模式补齐器"
        );
        KlineBackfiller::new(db.clone(), interval_list)
    };

    tracing::info!(message = "补齐器实例创建完成，开始执行补齐流程");

    // 运行一次性补齐流程 - 决策点：补齐成功vs失败
    match backfiller.run_once().await {
        Ok(_) => {
            info!(target: "kline_data_service", log_type = "module", "历史K线补齐完成");
            tracing::debug!(
                decision = "backfill_result",
                result = "success",
                "K线补齐流程成功完成"
            );
        },
        Err(e) => {
            error!(target: "kline_data_service", log_type = "module", "历史K线补齐失败: {}", e);
            tracing::error!(
                message = "K线补齐流程失败",
                error.summary = e.get_error_type_summary(),
                error.details = %e
            );
            return Err(e);
        }
    }

    info!(target: "kline_data_service", log_type = "module", "K线数据补齐服务完成");
    tracing::info!(message = "应用程序核心业务流程完成");

    Ok(())
}

/// ✨ [修改] 让日志初始化函数返回 TransactionLogManager 实例
#[instrument(name = "init_logging_with_distiller", skip_all)]
fn init_logging_with_distiller() -> (TraceDistillerStore, Option<TransactionLogManager>) {
    use kline_server::klcommon::log::{
        ModuleLayer,
        NamedPipeLogManager,
        TraceVisualizationLayer,
    };
    use std::sync::Arc;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

    // 确保日志目录存在
    if let Err(e) = std::fs::create_dir_all("logs") {
        eprintln!("警告：无法创建日志目录: {}", e);
    }

    // 获取日志配置 - 优先从配置文件读取，回退到环境变量
    let (log_level, log_transport, pipe_name, enable_full_tracing) = match load_logging_config() {
        Ok(config) => {
            // 注意：此时日志系统还未完全初始化，使用println!而非tracing宏
            println!("✅ 从配置文件加载日志设置成功");
            config
        },
        Err(e) => {
            // 配置文件读取失败，回退到环境变量
            println!("⚠️ 配置文件读取失败，使用环境变量: {}", e);
            let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
            let log_transport = std::env::var("LOG_TRANSPORT").unwrap_or_else(|_| "file".to_string());
            let pipe_name = std::env::var("PIPE_NAME").unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());
            let enable_full_tracing = std::env::var("ENABLE_FULL_TRACING").unwrap_or_else(|_| "true".to_string()).parse().unwrap_or(true);
            (log_level, log_transport, pipe_name, enable_full_tracing)
        }
    };

    // 初始化追踪配置
    use kline_server::klcommon::context::init_tracing_config;
    init_tracing_config(enable_full_tracing);

    println!("追踪配置初始化完成，完全追踪: {}", enable_full_tracing);

    // 创建TraceDistillerStore用于轨迹提炼
    let distiller_store = TraceDistillerStore::default();
    let distiller_layer = TraceDistillerLayer::new(distiller_store.clone());

    // 根据传输方式初始化日志 - 决策点：日志传输模式选择
    match log_transport.as_str() {
        "named_pipe" => {
            println!("选择命名管道日志传输模式: {}", pipe_name);
            // 命名管道模式 - 使用三层架构
            let log_manager = Arc::new(NamedPipeLogManager::new(pipe_name.clone()));
            // 注意：NamedPipeLogManager::new() 现在会自动启动后台任务

            let module_layer = ModuleLayer::new(log_manager.clone());
            let trace_layer = TraceVisualizationLayer::new(log_manager.clone());

            // ✨ [修改] 创建 TransactionLayer 并准备返回
            match TransactionLayer::new() {
                Ok((transaction_layer, transaction_manager)) => {
                    Registry::default()
                        .with(module_layer)         // 处理 log_type="module"
                        .with(transaction_layer)    // 处理 log_type="transaction"
                        .with(trace_layer)          // 处理 span 日志用于路径可视化
                        .with(distiller_layer)      // 处理轨迹提炼用于调试快照
                        .with(create_env_filter(&log_level))
                        .init();

                    info!(target: "kline_data_service", log_type = "module", "🎯 四重日志系统已初始化（命名管道模式 + 轨迹提炼）");
                    info!(target: "kline_data_service", log_type = "module", "📊 模块日志: 只处理顶层日志，log_type=module");
                    info!(target: "kline_data_service", log_type = "module", "🔖 业务追踪: 保存到 logs/transaction_log，log_type=transaction");
                    info!(target: "kline_data_service", log_type = "module", "🔍 Trace可视化: 只处理Span内日志，log_type=trace");
                    info!(target: "kline_data_service", log_type = "module", "🔬 轨迹提炼: 构建调用树用于调试快照");
                    info!(target: "kline_data_service", log_type = "module", "🔗 共享管道: {}", pipe_name);

                    if enable_full_tracing {
                        info!(target: "kline_data_service", log_type = "module", "🔍 完全追踪功能已启用");
                    } else {
                        info!(target: "kline_data_service", log_type = "module", "⚡ 完全追踪功能已禁用（高性能模式）");
                    }

                    // ✨ [修改] 返回 distiller_store 和 transaction_manager
                    return (distiller_store, Some(transaction_manager));
                },
                Err(e) => {
                    eprintln!("严重错误：无法创建业务追踪日志文件层: {}", e);
                    // 如果失败，只初始化其他层
                    Registry::default()
                        .with(module_layer)         // 处理 log_type="module"
                        .with(trace_layer)          // 处理 span 日志用于路径可视化
                        .with(distiller_layer)      // 处理轨迹提炼用于调试快照
                        .with(create_env_filter(&log_level))
                        .init();

                    info!(target: "kline_data_service", log_type = "module", "🎯 三重日志系统已初始化（命名管道模式 + 轨迹提炼，业务追踪层创建失败）");
                    info!(target: "kline_data_service", log_type = "module", "📊 模块日志: log_type=module");
                    info!(target: "kline_data_service", log_type = "module", "🔍 Trace可视化: log_type=trace");

                    return (distiller_store, None);
                }
            }
        }
        "websocket" => {
            println!("WebSocket模式已不再支持，回退到文件模式");
            // WebSocket模式已不再支持，回退到文件模式 + 轨迹提炼
            Registry::default()
                .with(tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_level(true)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339()))
                .with(distiller_layer)      // 添加轨迹提炼层
                .with(create_env_filter(&log_level))
                .init();

            info!(target: "kline_data_service", log_type = "module", "⚠️  WebSocket模式已不再支持，已回退到文件模式 + 轨迹提炼");
            info!(target: "kline_data_service", log_type = "module", "💡 请使用 LOG_TRANSPORT=named_pipe 启用日志传输");
        }
        _ => {
            println!("选择文件日志传输模式（默认）");
            // 文件模式（默认）+ 轨迹提炼
            Registry::default()
                .with(tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_level(true)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339()))
                .with(distiller_layer)      // 添加轨迹提炼层
                .with(create_env_filter(&log_level))
                .init();

            info!(target: "kline_data_service", log_type = "module", "日志系统已初始化（文件模式 + 轨迹提炼）");
        }
    }

    info!(target: "kline_data_service", log_type = "module", "日志级别: {}", log_level);

    // 记录日志系统初始化完成
    tracing::info!(
        message = "日志系统初始化完成",
        log_level = %log_level,
        log_transport = %log_transport,
        enable_full_tracing = enable_full_tracing
    );

    // ✨ [修改] 其他日志模式返回 None，因为没有 TransactionLayer
    (distiller_store, None)
}

/// 创建环境过滤器，始终过滤掉第三方库的调试日志
fn create_env_filter(log_level: &str) -> tracing_subscriber::EnvFilter {
    // 无论应用日志级别如何，都过滤掉第三方库的噪音日志
    let filter_str = format!(
        "{},hyper=warn,reqwest=warn,tokio_tungstenite=warn,tungstenite=warn,rustls=warn,h2=warn,sqlx=warn,rusqlite=warn",
        log_level
    );

    tracing_subscriber::EnvFilter::new(filter_str)
}

/// 加载日志配置
#[instrument(name = "load_logging_config", ret, err)]
fn load_logging_config() -> Result<(String, String, String, bool)> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    tracing::debug!(
        message = "尝试加载日志配置",
        config_path = %config_path
    );

    if Path::new(&config_path).exists() {
        tracing::debug!(message = "配置文件存在，开始解析");

        match AggregateConfig::from_file(&config_path) {
            Ok(config) => {
                // 确保管道名称格式正确（Windows命名管道需要完整路径）
                let pipe_name = if config.logging.pipe_name.starts_with(r"\\.\pipe\") {
                    config.logging.pipe_name
                } else {
                    format!(r"\\.\pipe\{}", config.logging.pipe_name)
                };

                tracing::debug!(
                    decision = "config_load_result",
                    result = "success",
                    log_level = %config.logging.log_level,
                    log_transport = %config.logging.log_transport,
                    pipe_name = %pipe_name,
                    enable_full_tracing = config.logging.enable_full_tracing,
                    "配置文件解析成功"
                );

                Ok((
                    config.logging.log_level,
                    config.logging.log_transport,
                    pipe_name,
                    config.logging.enable_full_tracing,
                ))
            },
            Err(e) => {
                tracing::error!(
                    message = "配置文件解析失败",
                    error.summary = e.get_error_type_summary(),
                    error.details = %e,
                    config_path = %config_path
                );
                Err(e)
            }
        }
    } else {
        let error = AppError::ConfigError(format!("配置文件不存在: {}，回退到环境变量", config_path));
        tracing::debug!(
            decision = "config_load_result",
            result = "file_not_found",
            config_path = %config_path,
            "配置文件不存在，将回退到环境变量"
        );
        Err(error)
    }
}

/// 生成程序退出时的最终快照
#[instrument(name = "generate_final_snapshot", skip_all)]
async fn generate_final_snapshot(store: &TraceDistillerStore) {
    info!(target: "kline_data_service", log_type = "module", "🔬 程序退出，生成最终Trace快照...");

    tracing::info!(message = "开始生成最终快照");

    // 等待一小段时间，确保所有正在进行的span都能完成
    tokio::time::sleep(Duration::from_millis(100)).await;

    let log_dir = "logs/debug_snapshots";

    // 确保目录存在
    if let Err(e) = tokio::fs::create_dir_all(log_dir).await {
        error!(target: "kline_data_service", log_type = "module", "无法创建调试快照目录: {}", e);
        tracing::error!(
            message = "创建调试快照目录失败",
            error.summary = "io_operation_failed",
            error.details = %e,
            log_dir = log_dir
        );
        return;
    }

    // 生成并写入快照
    tracing::debug!(message = "开始提炼trace数据");
    let report_text = distill_all_completed_traces_to_text(store);
    let report_size = report_text.len();

    tracing::debug!(
        message = "trace数据提炼完成",
        report_size_bytes = report_size,
        report_size_kb = report_size / 1024
    );

    // 获取下一个序号（程序运行期间递增）
    let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let filename = format!("{}/final_snapshot_{}_{}.log", log_dir, sequence, timestamp);

    tracing::debug!(
        message = "准备写入快照文件",
        filename = %filename,
        sequence = sequence
    );

    match tokio::fs::File::create(&filename).await {
        Ok(mut file) => {
            use tokio::io::AsyncWriteExt;
            match file.write_all(report_text.as_bytes()).await {
                Ok(_) => {
                    info!(target: "kline_data_service", log_type = "module", "✅ 已生成最终Trace快照: {}", filename);
                    tracing::info!(
                        message = "快照文件写入成功",
                        filename = %filename,
                        size_bytes = report_size
                    );
                },
                Err(e) => {
                    error!(target: "kline_data_service", log_type = "module", "写入快照文件 {} 失败", filename);
                    tracing::error!(
                        message = "快照文件写入失败",
                        error.summary = "io_operation_failed",
                        error.details = %e,
                        filename = %filename
                    );
                }
            }
        },
        Err(e) => {
            error!(target: "kline_data_service", log_type = "module", "创建快照文件 {} 失败: {}", filename, e);
            tracing::error!(
                message = "快照文件创建失败",
                error.summary = "io_operation_failed",
                error.details = %e,
                filename = %filename
            );
        }
    }

    info!(target: "kline_data_service", log_type = "module", "✅ 最终快照生成完成");
    tracing::info!(message = "最终快照生成流程完成");
}
