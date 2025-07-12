//! 统一日志系统初始化模块
//!
//! 提供跨二进制文件的日志系统初始化功能，避免代码重复
//! 支持AI日志、问题摘要、低频日志和性能分析等多层日志架构

use crate::klcommon::{
    log::{
        init_log_sender,
        McpLayer,
        init_problem_summary_log,
        ProblemSummaryLayer,
        init_low_freq_log,
        LowFreqLogLayer,
        init_beacon_log,
        BeaconLogLayer,
    },
    AppError,
    Result,
    AggregateConfig,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry, EnvFilter, Layer};
use tracing_flame::FlameLayer;
use std::path::Path;

/// 自定义Guard trait，避免dyn Drop警告
pub trait LogGuard: Send + Sync {}

/// 空的Guard实现，用于性能日志未启用时的占位符
pub struct DummyGuard;

impl Drop for DummyGuard {
    fn drop(&mut self) {
        // 什么都不做
    }
}

impl LogGuard for DummyGuard {}

/// 为 tracing_flame::FlushGuard 实现 LogGuard trait
impl<W: std::io::Write + Send + Sync + 'static> LogGuard for tracing_flame::FlushGuard<W> {}

/// 初始化统一日志与性能分析系统
pub async fn init_ai_logging() -> Result<Box<dyn LogGuard>> {
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
    use crate::klcommon::context::init_tracing_config;
    init_tracing_config(enable_full_tracing);

    // [恢复] 初始化所有自定义日志组件
    init_log_sender(&pipe_name);
    init_problem_summary_log("logs/problem_summary.log").ok();
    init_low_freq_log("logs/low_freq.log").ok();
    init_beacon_log("logs/beacon.log").ok();

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
        .with(LowFreqLogLayer::new().with_filter(EnvFilter::new(&business_filter_str)))
        .with(BeaconLogLayer::new().with_filter(EnvFilter::new(&business_filter_str)));

    // 3. 条件性地准备性能分析层和其 guard
    let enable_perf_log = std::env::var("ENABLE_PERF_LOG").is_ok();

    // [最终方案] 使用 Option<Layer> 的惯用模式来处理条件层
    let (perf_layer, final_guard): (Option<_>, Box<dyn LogGuard>) = if enable_perf_log {
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

/// 加载日志配置
pub fn load_logging_config() -> Result<(String, String, String, bool)> {
    const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
    
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
    
    if Path::new(&config_path).exists() {
        match AggregateConfig::from_file(&config_path) {
            Ok(config) => {
                // 确保管道名称格式正确
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
            Err(e) => Err(AppError::ConfigError(format!("Failed to parse config: {}", e))),
        }
    } else {
        Err(AppError::ConfigError(format!("配置文件不存在: {}，回退到环境变量", config_path)))
    }
}
