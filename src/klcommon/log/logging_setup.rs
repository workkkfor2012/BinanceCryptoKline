//! 统一日志系统初始化模块
//!
//! 提供跨二进制文件的日志系统初始化功能，避免代码重复
//! 支持AI日志、问题摘要、低频日志和性能分析等多层日志架构

use crate::klcommon::{
    log::{
        // init_log_sender,     // [注释掉] 未使用的导入
        // McpLayer,            // [注释掉] 未使用的导入
        init_problem_summary_log,
        ProblemSummaryLayer,
        init_low_freq_log,
        LowFreqLogLayer,
        init_beacon_log,
        BeaconLogLayer,
        // [新增] 导入新组件
        init_target_log_sender,
        TargetLogLayer,
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

    // 获取日志配置 - 只从配置文件读取
    let (log_enabled, log_level, log_transport, pipe_name, enable_full_tracing, enable_console_output) = load_logging_config()
        .map_err(|e| {
            eprintln!("配置文件读取失败: {}", e);
            eprintln!("请确保 config/BinanceKlineConfig.toml 文件存在且格式正确");
            e
        })?;

    // 检查日志开关
    if !log_enabled {
        eprintln!("📋 日志系统已禁用");
        eprintln!("  如需启用日志，请在配置文件中设置 enabled = true");
        // 返回一个空的guard，不初始化任何日志系统
        return Ok(Box::new(DummyGuard));
    }

    // 在控制台显示读取到的日志配置
    eprintln!("📋 日志配置信息:");
    eprintln!("  日志开关: {}", if log_enabled { "启用" } else { "禁用" });
    eprintln!("  日志级别: {}", log_level);
    eprintln!("  传输方式: {}", log_transport);
    eprintln!("  管道名称: {}", pipe_name);
    eprintln!("  完全追踪: {}", if enable_full_tracing { "启用" } else { "禁用" });

    // 初始化追踪配置
    use crate::klcommon::log::context::init_tracing_config;
    init_tracing_config(enable_full_tracing);

    // [修改] 使用配置文件中的 `weblog_pipe_name` 来初始化新的日志发送器
    // 从配置中获取weblog管道名称，如果没有则使用默认值
    let weblog_pipe_name = load_weblog_pipe_name().unwrap_or_else(|_| "weblog_pipe".to_string());
    init_target_log_sender(&weblog_pipe_name);

    // [注释掉] 旧的初始化
    // init_log_sender(&pipe_name);

    init_problem_summary_log("logs/problem_summary.log").ok();
    init_low_freq_log("logs/low_freq.log").ok();
    init_beacon_log("logs/beacon.log").ok();

    // --- 日志订阅者设置核心逻辑 ---

    // 1. [恢复] 创建分离的业务日志过滤器字符串
    let business_filter_str = format!(
        "{},perf=off,hyper=warn,reqwest=warn,sqlx=warn,rusqlite=warn",
        log_level
    );

    // 显示过滤器字符串用于调试
    eprintln!("🔍 日志过滤器: {}", business_filter_str);

    // [修改] 这是切换日志层的关键点 - 根据配置决定是否添加控制台输出层
    let mut registry = Registry::default()
        .with(TargetLogLayer::new().with_filter(EnvFilter::new(&business_filter_str))) // [启用] 简单日志层
        // .with(McpLayer.with_filter(EnvFilter::new(&business_filter_str)))          // [禁用] AI富日志层
        .with(ProblemSummaryLayer.with_filter(EnvFilter::new(&business_filter_str))) // 可按需保留
        .with(LowFreqLogLayer::new().with_filter(EnvFilter::new(&business_filter_str)))   // 可按需保留
        .with(BeaconLogLayer::new().with_filter(EnvFilter::new(&business_filter_str)));    // 可按需保留

    // 检查性能日志开关
    let enable_perf_log = std::env::var("ENABLE_PERF_LOG").is_ok();

    // [条件性] 根据配置决定是否添加控制台输出层
    if enable_console_output {
        eprintln!("🖥️  控制台日志输出: 已启用");

        // 继续处理性能分析层
        let (perf_layer, final_guard): (Option<_>, Box<dyn LogGuard>) = if enable_perf_log {
            let perf_filter = EnvFilter::new("perf=trace");
            let (flame_layer, flame_guard) = FlameLayer::with_file("logs/performance.folded")
                .map_err(|e| AppError::ConfigError(format!("Failed to create flamegraph file: {}", e)))?;
            eprintln!("性能日志系统已激活，日志将写入 logs/performance.folded");
            (Some(flame_layer.with_filter(perf_filter)), Box::new(flame_guard))
        } else {
            (None, Box::new(DummyGuard))
        };

        let final_registry = registry
            .with(tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(false)
                .with_level(true)
                .with_filter(EnvFilter::new(&business_filter_str)));

        if let Some(perf_layer) = perf_layer {
            final_registry.with(perf_layer).init();
        } else {
            final_registry.init();
        }

        return Ok(final_guard);
    } else {
        eprintln!("🖥️  控制台日志输出: 已禁用");
    }

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

    // 使用刚初始化的日志系统记录配置信息
    use tracing::info;
    info!(
        log_type = "low_freq",
        event_type = "logging_config_loaded",
        log_enabled = log_enabled,
        log_level = %log_level,
        log_transport = %log_transport,
        pipe_name = %pipe_name,
        enable_full_tracing = enable_full_tracing,
        "📋 日志配置已加载并应用"
    );

    // [新增] 发送会话开始标记，通知weblog端清空所有历史数据
    send_session_start_marker();

    // 5. 返回 guard，它的生命周期将由 main 函数管理
    Ok(final_guard)
}

/// 加载日志配置
pub fn load_logging_config() -> Result<(bool, String, String, String, bool, bool)> {
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
                    config.logging.enabled,
                    config.logging.log_level,
                    config.logging.log_transport,
                    pipe_name,
                    config.logging.enable_full_tracing,
                    config.logging.enable_console_output,
                ))
            },
            Err(e) => Err(AppError::ConfigError(format!("Failed to parse config: {}", e))),
        }
    } else {
        Err(AppError::ConfigError(format!("配置文件不存在: {}，回退到环境变量", config_path)))
    }
}

/// 加载WebLog管道名称配置
pub fn load_weblog_pipe_name() -> Result<String> {
    const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";

    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    if Path::new(&config_path).exists() {
        match AggregateConfig::from_file(&config_path) {
            Ok(config) => {
                // 获取weblog管道名称，如果没有则使用默认值
                let weblog_pipe_name = config.logging.weblog_pipe_name
                    .unwrap_or_else(|| "weblog_pipe".to_string());

                // 确保管道名称格式正确
                let pipe_name = if weblog_pipe_name.starts_with(r"\\.\pipe\") {
                    weblog_pipe_name
                } else {
                    format!(r"\\.\pipe\{}", weblog_pipe_name)
                };

                Ok(pipe_name)
            },
            Err(e) => Err(AppError::ConfigError(format!("Failed to parse config: {}", e))),
        }
    } else {
        // 如果配置文件不存在，使用默认值
        Ok(r"\\.\pipe\weblog_pipe".to_string())
    }
}

/// 发送会话开始标记，通知weblog端清空所有历史数据
fn send_session_start_marker() {
    use tracing::info;

    // 创建会话开始标记日志
    // weblog端会识别这个特殊消息并清空所有数据
    info!(
        session_start = true,
        event_type = "session_start",
        program_name = env!("CARGO_PKG_NAME"),
        program_version = env!("CARGO_PKG_VERSION"),
        timestamp = chrono::Utc::now().to_rfc3339(),
        "session_start"
    );

    eprintln!("[Session] 会话开始标记已发送，weblog端将清空所有历史数据");
}
