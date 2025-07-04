// K线数据服务主程序 - 专注于K线补齐功能
use kline_server::klcommon::{Database, Result, AppError, AggregateConfig};
use kline_server::kldata::KlineBackfiller;

use std::sync::Arc;
use std::path::Path;
use tracing::{info, error, instrument, info_span, Instrument};

// 导入新的AI日志组件
use kline_server::klcommon::log::{
    init_log_sender,
    McpLayer,
    init_problem_summary_log,
    ProblemSummaryLayer,
    init_module_log,
    ModuleLayer,
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


// ========================================

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化AI日志系统
    init_ai_logging().await?;

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
                error_summary = e.get_error_type_summary(),
                error_details = %e
            );
        }
    }

    // 在程序结束前等待，确保日志发送完成
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

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

    info!(log_type = "module", "启动K线数据补齐服务");
    info!(log_type = "module", "使用周期: {}", intervals);

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
    info!(log_type = "module", "开始补齐K线数据...");

    // 创建补齐器实例 - 决策点：测试模式vs生产模式
    let backfiller = if TEST_MODE {
        info!(log_type = "module", "🔧 启用测试模式，限制交易对为: {:?}", TEST_SYMBOLS);
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
        info!(log_type = "module", "📡 生产模式，将获取所有交易对");
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
            info!(log_type = "module", "历史K线补齐完成");
            tracing::debug!(
                decision = "backfill_result",
                result = "success",
                "K线补齐流程成功完成"
            );
        },
        Err(e) => {
            error!(log_type = "module", "历史K线补齐失败: {}", e);
            tracing::error!(
                message = "K线补齐流程失败",
                error.summary = e.get_error_type_summary(),
                error.details = %e
            );
            return Err(e);
        }
    }

    info!(log_type = "module", "K线数据补齐服务完成");
    tracing::info!(message = "应用程序核心业务流程完成");

    Ok(())
}

/// 初始化AI日志系统 - 异步批量处理架构
async fn init_ai_logging() -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

    // 确保日志目录存在
    if let Err(e) = std::fs::create_dir_all("logs") {
        eprintln!("警告：无法创建日志目录: {}", e);
    }

    // 获取日志配置 - 优先从配置文件读取，回退到环境变量
    let (log_level, _log_transport, pipe_name, enable_full_tracing) = match load_logging_config() {
        Ok(config) => {
            println!("✅ 从配置文件加载日志设置成功");
            config
        },
        Err(e) => {
            println!("⚠️ 配置文件读取失败，使用环境变量: {}", e);
            let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
            let pipe_name = std::env::var("PIPE_NAME").unwrap_or_else(|_| "kline_mcp_log_pipe".to_string());
            let enable_full_tracing = std::env::var("ENABLE_FULL_TRACING").unwrap_or_else(|_| "true".to_string()).parse().unwrap_or(true);
            (log_level, "named_pipe".to_string(), pipe_name, enable_full_tracing)
        }
    };

    // 初始化追踪配置
    use kline_server::klcommon::context::init_tracing_config;
    init_tracing_config(enable_full_tracing);

    println!("追踪配置初始化完成，完全追踪: {}", enable_full_tracing);

    // 1. 初始化异步批量日志发送器（所有复杂逻辑都在ai_log模块内部处理）
    println!("初始化AI日志发送器 (异步批量模式)，管道名称: {}", pipe_name);
    init_log_sender(&pipe_name);

    // 2. 初始化本地的问题摘要日志文件
    if let Err(e) = init_problem_summary_log("logs/problem_summary.log") {
        eprintln!("[Init] Failed to create problem summary log: {}", e);
    }

    // 3. 初始化模块日志文件
    if let Err(e) = init_module_log("logs/module.log") {
        eprintln!("[Init] Failed to create module log: {}", e);
    }

    // 4. 设置包含三个核心Layer的tracing订阅者
    Registry::default()
        .with(McpLayer)                    // Layer 1: 异步批量发送所有详细日志到daemon
        .with(ProblemSummaryLayer)         // Layer 2: 将WARN/ERROR写入本地问题文件
        .with(ModuleLayer::new())          // Layer 3: 将模块日志写入本地文件
        .with(create_env_filter(&log_level)) // 使用配置的日志级别
        .init();

    println!("AI Log System Initialized (Async Batch Mode).");
    println!("-> Detailed logs: 异步批量保存到 logs/ai_detailed.log + 发送到管道: {}", pipe_name);
    println!("-> Problem summary: saved to logs/problem_summary.log");
    println!("-> Module logs: saved to logs/module.log");

    if enable_full_tracing {
        println!("-> Full tracing enabled");
    } else {
        println!("-> Full tracing disabled (high performance mode)");
    }

    Ok(())
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


