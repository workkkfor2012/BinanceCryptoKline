// K线数据服务主程序 - 专注于K线补齐功能
use kline_server::klcommon::{Database, Result, AppError, AggregateConfig};
use kline_server::kldata::KlineBackfiller;
use kline_macros::perf_profile;

use std::sync::Arc;
use std::path::Path;

// 导入新的AI日志组件
use kline_server::klcommon::log::{
    init_log_sender,
    shutdown_log_sender, // 导入关闭函数
    McpLayer,
    init_problem_summary_log,
    ProblemSummaryLayer,
    init_low_freq_log,
    LowFreqLogLayer,
};

// 导入tracing宏
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry, EnvFilter, Layer};
use tracing_flame::FlameLayer;

/// 默认配置文件路径
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";

// ========================================
// 🔧 测试开关配置
// ========================================
/// 是否启用测试模式（限制为只处理 BTCUSDT）
const TEST_MODE: bool = false;

/// 测试模式下使用的交易对
const TEST_SYMBOLS: &[&str] = &["BTCUSDT"];

/// 空的Guard实现，用于性能日志未启用时的占位符
struct DummyGuard;

impl Drop for DummyGuard {
    fn drop(&mut self) {
        // 什么都不做
    }
}


// ========================================

#[tokio::main]
async fn main() -> Result<()> {
    // 持有 guard，直到 main 函数结束，确保文件被正确写入
    let _log_guard = init_ai_logging().await?;

    let result = run_app().await;

    // ✨ [修改] 使用确定性的关闭逻辑替换 sleep
    shutdown_log_sender();

    result
}

/// 应用程序的核心业务逻辑
#[perf_profile]
async fn run_app() -> Result<()> {
    // 在 run_app 开始时增加低频日志，标记核心业务逻辑的开始
    info!(log_type = "low_freq", message = "核心应用逻辑开始执行");

    let intervals = "1m,5m,30m,1h,4h,1d,1w".to_string();
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    let backfiller = if TEST_MODE {
        KlineBackfiller::new_test_mode(
            db.clone(),
            interval_list.clone(),
            TEST_SYMBOLS.iter().map(|s| s.to_string()).collect()
        )
    } else {
        KlineBackfiller::new(db.clone(), interval_list.clone())
    };

    backfiller.run_once().await?;

    // ✨ [新增] 低频日志：标记核心业务逻辑的成功结束
    info!(log_type = "low_freq", message = "核心应用逻辑成功完成");

    Ok(())
}

/// 初始化统一日志与性能分析系统
async fn init_ai_logging() -> Result<Box<dyn Drop + Send + Sync>> {
    // 确保日志目录存在
    if let Err(e) = std::fs::create_dir_all("logs") {
        eprintln!("警告：无法创建日志目录: {}", e);
    }

    // 获取日志配置 - 优先从配置文件读取，回退到环境变量
    let (log_level, _log_transport, pipe_name, enable_full_tracing) = match load_logging_config() {
        Ok(config) => {
            config
        },
        Err(e) => {
            eprintln!("配置文件读取失败，回退到环境变量: {}", e);
            let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
            let pipe_name = std::env::var("PIPE_NAME").unwrap_or_else(|_| "kline_mcp_log_pipe".to_string());
            let enable_full_tracing = std::env::var("ENABLE_FULL_TRACING").unwrap_or_else(|_| "true".to_string()).parse().unwrap_or(true);
            (log_level, "named_pipe".to_string(), pipe_name, enable_full_tracing)
        }
    };

    // 初始化追踪配置
    use kline_server::klcommon::context::init_tracing_config;
    init_tracing_config(enable_full_tracing);

    // [恢复] 初始化所有自定义日志组件
    init_log_sender(&pipe_name);
    init_problem_summary_log("logs/problem_summary.log").ok();
    init_low_freq_log("logs/low_freq.log").ok();

    // --- 日志订阅者设置核心逻辑 ---

    // 1. [恢复] 创建分离的业务日志过滤器字符串
    let business_filter_str = format!(
        "{},perf=off,hyper=warn,reqwest=warn,sqlx=warn,rusqlite=warn",
        log_level
    );

    // 2. 初始化订阅者注册表，并为业务层应用业务过滤器
    let registry = Registry::default()
        .with(McpLayer.with_filter(EnvFilter::new(&business_filter_str)))
        .with(ProblemSummaryLayer.with_filter(EnvFilter::new(&business_filter_str)))
        .with(LowFreqLogLayer::new().with_filter(EnvFilter::new(&business_filter_str)));

    // 3. 条件性地准备性能分析层和其 guard
    let enable_perf_log = std::env::var("ENABLE_PERF_LOG").is_ok();

    // [最终方案] 使用 Option<Layer> 的惯用模式来处理条件层
    let (perf_layer, final_guard): (Option<_>, Box<dyn Drop + Send + Sync>) = if enable_perf_log {
        // [恢复] 创建只关心 `target = "perf"` 的性能过滤器
        let perf_filter = EnvFilter::new("perf=trace");

        // 创建 FlameLayer，并获得它的 guard
        let (flame_layer, flame_guard) = FlameLayer::with_file("logs/performance.folded")
            .map_err(|e| AppError::ConfigError(format!("Failed to create flamegraph file: {}", e)))?;

        eprintln!("性能日志系统已激活，日志将写入 logs/performance.folded");

        // 将 flame_layer 和真实的 guard 打包
        (Some(flame_layer.with_filter(perf_filter)), Box::new(flame_guard))
    } else {
        // 如果未启用，则层为 None，guard 为空实现
        (None, Box::new(DummyGuard))
    };

    // 4. 组装最终的订阅者并 **只初始化一次**
    //    .with(perf_layer) 在 perf_layer 为 None 时是无操作的
    registry
        .with(perf_layer)
        .init();

    eprintln!("统一日志系统初始化完成");

    // 5. 返回 guard，它的生命周期将由 main 函数管理
    Ok(final_guard)
}

// 旧的create_env_filter函数已被集成到init_ai_logging中的双通道过滤器系统
// 现在使用business_filter和perf_filter分别处理业务日志和性能日志

/// 加载日志配置
fn load_logging_config() -> Result<(String, String, String, bool)> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    if Path::new(&config_path).exists() {
        match AggregateConfig::from_file(&config_path) {
            Ok(config) => {
                // 确保管道名称格式正确（Windows命名管道需要完整路径）
                let pipe_name = if config.logging.pipe_name.starts_with(r"\\.\pipe\") {
                    config.logging.pipe_name
                } else {
                    format!(r"\\.\pipe\{}", config.logging.pipe_name)
                };

                Ok((
                    config.logging.log_level,
                    config.logging.log_transport,
                    pipe_name,
                    config.logging.enable_full_tracing,
                ))
            },
            Err(e) => {
                Err(e)
            }
        }
    } else {
        let error = AppError::ConfigError(format!("配置文件不存在: {}，回退到环境变量", config_path));
        Err(error)
    }
}


