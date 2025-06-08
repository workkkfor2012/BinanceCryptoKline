//! K线聚合服务启动文件
//!
//! 启动完整的K线聚合系统，包括数据接入、聚合、存储和持久化。

use kline_server::klaggregate::{KlineAggregateSystem, AggregateConfig};
use kline_server::klaggregate::observability::WebSocketLogForwardingLayer;
use kline_server::klcommon::{Result, AppError};
use std::path::Path;
use tokio::signal;
use tokio::time::{Duration};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use chrono;

/// 默认配置文件路径
const DEFAULT_CONFIG_PATH: &str = "config/aggregate_config.toml";

#[tokio::main]
async fn main() -> Result<()> {
    // 处理命令行参数
    if !handle_args() {
        return Ok(());
    }

    // 初始化可观察性系统
    init_observability_system()?;

    tracing::info!(target = "KlineAggregateService", "启动K线聚合服务...");

    // 加载配置
    let config = load_config().await?;
    tracing::info!(target = "KlineAggregateService", "配置加载完成");

    // 创建K线聚合系统
    let system = match KlineAggregateSystem::new(config).await {
        Ok(system) => {
            tracing::info!(target = "KlineAggregateService", "K线聚合系统创建成功");
            system
        }
        Err(e) => {
            tracing::error!(target = "KlineAggregateService", "创建K线聚合系统失败: {}", e);
            return Err(e);
        }
    };

    // 启动系统
    if let Err(e) = system.start().await {
        tracing::error!(target = "KlineAggregateService", "启动K线聚合系统失败: {}", e);
        return Err(e);
    }

    tracing::info!(target = "KlineAggregateService", "K线聚合服务启动完成");

    // 启动状态监控任务
    start_status_monitor(system.clone()).await;

    // 启动测试日志任务
    start_test_logging().await;

    // 等待关闭信号
    wait_for_shutdown_signal().await;

    // 优雅关闭
    tracing::info!(target = "KlineAggregateService", "收到关闭信号，开始优雅关闭...");
    if let Err(e) = system.stop().await {
        tracing::error!(target = "KlineAggregateService", "关闭K线聚合系统失败: {}", e);
    } else {
        tracing::info!(target = "KlineAggregateService", "K线聚合服务已优雅关闭");
    }

    Ok(())
}

/// 初始化可观察性系统
fn init_observability_system() -> Result<()> {
    use std::sync::{Once, Mutex};

    // 使用更安全的方式存储初始化结果
    static OBSERVABILITY_INIT: Once = Once::new();
    static INIT_RESULT: Mutex<Option<bool>> = Mutex::new(None);

    let mut init_success = false;

    OBSERVABILITY_INIT.call_once(|| {
        match init_observability_system_inner() {
            Ok(_) => {
                init_success = true;
                if let Ok(mut result) = INIT_RESULT.lock() {
                    *result = Some(true);
                }
            }
            Err(e) => {
                eprintln!("可观察性系统初始化失败: {}", e);
                if let Ok(mut result) = INIT_RESULT.lock() {
                    *result = Some(false);
                }
            }
        }
    });

    // 检查初始化结果
    if let Ok(result) = INIT_RESULT.lock() {
        match *result {
            Some(true) => Ok(()),
            Some(false) => Err(AppError::ConfigError("可观察性系统初始化失败".to_string())),
            None => {
                // 如果是第一次调用且在call_once中成功了
                if init_success {
                    Ok(())
                } else {
                    Err(AppError::ConfigError("可观察性系统初始化状态未知".to_string()))
                }
            }
        }
    } else {
        Err(AppError::ConfigError("无法获取初始化状态".to_string()))
    }
}

/// 内部初始化函数，只会被调用一次
fn init_observability_system_inner() -> Result<()> {
    // 设置日志级别
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    // 检查传输方式配置
    let log_transport = std::env::var("LOG_TRANSPORT").unwrap_or_else(|_| "named_pipe".to_string());

    let log_forwarding_layer = match log_transport.as_str() {
        "named_pipe" => {
            let pipe_name = std::env::var("PIPE_NAME")
                .unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());
            WebSocketLogForwardingLayer::new_named_pipe(pipe_name)
        }
        "websocket" => {
            let web_port = std::env::var("WEB_PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse::<u16>()
                .unwrap_or(3000);
            WebSocketLogForwardingLayer::new_websocket(web_port)
        }
        _ => {
            let pipe_name = r"\\.\pipe\kline_log_pipe".to_string();
            WebSocketLogForwardingLayer::new_named_pipe(pipe_name)
        }
    };

    // 设置tracing订阅器，遵循WebLog日志规范
    let init_result = match log_transport.as_str() {
        "named_pipe" => {
            // 命名管道模式：只发送JSON格式到WebLog，不使用控制台输出层
            Registry::default()
                .with(log_forwarding_layer) // 只有JSON格式发送到WebLog
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log_level))
                )
                .try_init()
        }
        _ => {
            // 其他模式：保持原有行为
            Registry::default()
                .with(log_forwarding_layer)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                ) // 添加控制台输出层（文本格式）
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log_level))
                )
                .try_init()
        }
    };

    // 检查tracing订阅器初始化结果并决定是否初始化LogTracer
    let tracing_init_success = match init_result {
        Ok(_) => {
            // tracing订阅器初始化成功，我们是第一个初始化的
            true
        }
        Err(e) => {
            // 如果已经初始化过，这是正常情况，不需要报错
            eprintln!("注意: tracing订阅器已存在: {}", e);
            false
        }
    };

    // 设置log到tracing的桥接，捕获第三方库的log日志
    // 只有当我们成功初始化了tracing订阅器时，才初始化LogTracer
    if tracing_init_success {
        // 我们是第一个初始化tracing的，所以也需要初始化LogTracer
        match tracing_log::LogTracer::init() {
            Ok(_) => {
                // 初始化成功
                tracing::debug!(target = "KlineAggregateService", "log桥接器初始化成功");
            }
            Err(e) => {
                // 这种情况很少见，但也是可能的
                tracing::debug!(target = "KlineAggregateService", "log桥接器初始化失败: {}", e);
            }
        }
    } else {
        // tracing订阅器已存在，说明日志系统已经完整初始化，不需要再初始化LogTracer
        tracing::debug!(target = "KlineAggregateService", "检测到现有日志系统，跳过log桥接器初始化");
    }

    // 等待一小段时间确保tracing系统完全初始化
    std::thread::sleep(std::time::Duration::from_millis(10));

    tracing::info!(target = "KlineAggregateService", "🔍 可观察性系统初始化完成，级别: {}", log_level);
    tracing::info!(target = "KlineAggregateService", "📊 规格验证层已禁用，减少日志输出");
    tracing::info!(target = "KlineAggregateService", "📡 日志传输方式: {}", log_transport);

    // 显示传输配置信息
    match log_transport.as_str() {
        "named_pipe" => {
            let pipe_name = std::env::var("PIPE_NAME")
                .unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());
            tracing::info!(target = "KlineAggregateService", "📡 使用命名管道传输日志: {}", pipe_name);
        }
        "websocket" => {
            let web_port = std::env::var("WEB_PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse::<u16>()
                .unwrap_or(3000);
            tracing::info!(target = "KlineAggregateService", "🌐 使用WebSocket传输日志，端口: {}", web_port);
        }
        _ => {
            tracing::info!(target = "KlineAggregateService", "⚠️ 未知传输方式 '{}', 使用默认命名管道", log_transport);
        }
    }

    // 发送测试日志确保传输工作
    tracing::info!(target = "KlineAggregateService", "🧪 测试日志1: 可观察性系统测试");
    tracing::warn!(target = "KlineAggregateService", "🧪 测试日志2: 警告级别测试");
    tracing::error!(target = "KlineAggregateService", "🧪 测试日志3: 错误级别测试");

    Ok(())
}

/// 加载配置
async fn load_config() -> Result<AggregateConfig> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
    
    if Path::new(&config_path).exists() {
        tracing::info!(target = "KlineAggregateService", "从文件加载配置: {}", config_path);
        AggregateConfig::from_file(&config_path)
    } else {
        tracing::warn!(target = "KlineAggregateService", "配置文件不存在: {}，使用默认配置", config_path);

        // 创建默认配置
        let config = AggregateConfig::default();

        // 尝试创建配置目录
        if let Some(parent) = Path::new(&config_path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| AppError::IoError(e))?;
            }
        }

        // 保存默认配置到文件
        if let Err(e) = config.save_to_file(&config_path) {
            tracing::warn!(target = "KlineAggregateService", "保存默认配置失败: {}", e);
        } else {
            tracing::info!(target = "KlineAggregateService", "默认配置已保存到: {}", config_path);
        }
        
        Ok(config)
    }
}

/// 启动状态监控任务
async fn start_status_monitor(system: KlineAggregateSystem) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let status = system.get_status().await;
            tracing::info!(
                target = "KlineAggregateService",
                "系统状态 - 品种数: {}, 活跃连接: {}, 缓冲切换: {}, 持久化: {}",
                status.total_symbols,
                status.active_connections,
                status.buffer_swap_count,
                status.persistence_status
            );
        }
    });
}

/// 启动测试日志任务（每10秒发送一次测试日志）
async fn start_test_logging() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut counter = 0;

        loop {
            interval.tick().await;
            counter += 1;

            tracing::info!(
                target = "KlineAggregateService",
                "🧪 定期测试日志 #{}: 系统运行正常，时间戳: {}",
                counter,
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
            );

            if counter % 3 == 0 {
                tracing::warn!(target = "KlineAggregateService", "🧪 警告测试日志 #{}: 这是一个测试警告", counter);
            }
        }
    });
}

/// 等待关闭信号
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("安装Ctrl+C处理器失败");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("安装SIGTERM处理器失败")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!(target = "KlineAggregateService", "收到Ctrl+C信号");
        },
        _ = terminate => {
            tracing::info!(target = "KlineAggregateService", "收到SIGTERM信号");
        },
    }
}

/// 显示帮助信息
fn show_help() {
    println!("K线聚合服务");
    println!();
    println!("用法:");
    println!("  kline_aggregate_service [选项]");
    println!();
    println!("选项:");
    println!("  -h, --help     显示此帮助信息");
    println!("  -v, --version  显示版本信息");
    println!();
    println!("环境变量:");
    println!("  CONFIG_PATH    配置文件路径 (默认: {})", DEFAULT_CONFIG_PATH);
    println!("  RUST_LOG       日志级别 (默认: info)");
    println!();
    println!("示例:");
    println!("  # 使用默认配置启动");
    println!("  kline_aggregate_service");
    println!();
    println!("  # 使用自定义配置文件");
    println!("  CONFIG_PATH=my_config.toml kline_aggregate_service");
    println!();
    println!("  # 启用调试日志");
    println!("  RUST_LOG=debug kline_aggregate_service");
}

/// 显示版本信息
fn show_version() {
    println!("K线聚合服务 v{}", env!("CARGO_PKG_VERSION"));
}

/// 处理命令行参数
fn handle_args() -> bool {
    let args: Vec<String> = std::env::args().collect();

    for arg in &args[1..] {
        match arg.as_str() {
            "-h" | "--help" => {
                show_help();
                return false;
            }
            "-v" | "--version" => {
                show_version();
                return false;
            }
            _ => {
                eprintln!("未知参数: {}", arg);
                eprintln!("使用 --help 查看帮助信息");
                return false;
            }
        }
    }

    true
}
