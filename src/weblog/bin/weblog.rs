//! WebLog - 通用Web日志显示系统
//!
//! 完全独立的日志可视化工具，支持任何基于tracing规范的Rust应用程序
//! 设计理念：解耦、通用、可重用

use clap::Parser;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, warn, error};
use weblog::{WebLogConfig, AppState, create_app, LogTransport};
use serde::Deserialize;

#[derive(Parser)]
#[command(name = "weblog")]
#[command(about = "WebLog日志显示系统 - 专用于命名管道JSON日志")]
#[command(version = "0.1.0")]
struct Cli {
    /// Web服务端口
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// 命名管道名称（必需）
    #[arg(long, required = true)]
    pipe_name: String,

    // 移除日志级别命令行参数，改为在代码中直接设置

    /// 最大保留的日志条目数量
    #[arg(long, default_value = "10000")]
    max_logs: usize,

    /// 最大保留的Trace数量
    #[arg(long, default_value = "1000")]
    max_traces: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // 初始化日志 - 从统一配置文件读取日志级别
    let log_level = load_weblog_log_level();
    tracing_subscriber::fmt()
        .with_env_filter(&log_level)
        .init();

    info!("🚀 启动WebLog - 命名管道JSON日志显示系统");

    // 创建配置
    let config = WebLogConfig {
        web_port: cli.port,
        log_transport: LogTransport::NamedPipe(cli.pipe_name.clone()),
        pipe_name: Some(cli.pipe_name.clone()),
        max_log_entries: cli.max_logs,
    };

    info!("📋 配置: Web端口={}, 最大日志={}, 命名管道={}",
          config.web_port, config.max_log_entries, cli.pipe_name);

    // 创建应用状态
    let (state, _log_receiver) = AppState::new();
    let state = Arc::new(state);

    // 启动命名管道日志处理任务
    let log_state = state.clone();
    let pipe_name = cli.pipe_name.clone();
    tokio::spawn(async move {
        if let Err(e) = process_named_pipe_logs(log_state, pipe_name).await {
            error!("命名管道日志处理任务失败: {}", e);
        }
    });

    // 创建Web应用
    let app = create_app(state.clone());

    // 启动Web服务器
    let bind_addr = format!("0.0.0.0:{}", config.web_port);
    info!("🌐 Web服务器启动在: http://localhost:{}", config.web_port);

    print_startup_info(&config);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// 打印启动信息
fn print_startup_info(config: &WebLogConfig) {
    println!();
    println!("🌐 WebLog - 通用Web日志显示系统");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 主仪表板: http://localhost:{}", config.web_port);
    println!("🔍 Trace可视化: http://localhost:{}/trace", config.web_port);
    println!("📋 模块监控: http://localhost:{}/modules", config.web_port);
    println!("🔗 WebSocket: ws://localhost:{}/ws", config.web_port);
    println!("📡 日志API: POST http://localhost:{}/api/log", config.web_port);
    println!();

    match &config.log_transport {
        LogTransport::NamedPipe(pipe_name) => {
            println!("📡 日志传输方式: 命名管道");
            println!("🔧 管道名称: {}", pipe_name);
            println!("💡 只接受JSON格式的tracing日志");
        }
    }

    println!();
    println!("💡 支持的日志格式:");
    println!("  - JSON格式的tracing日志");
    println!("  - 文本格式的tracing日志");
    println!("  - 结构化字段和Span追踪");
    println!();
    println!("🔧 使用示例:");
    println!("  # 从标准输入读取");
    println!("  echo '{{\"timestamp\":\"2024-01-01T12:00:00Z\",\"level\":\"INFO\",\"target\":\"app\",\"message\":\"Hello\"}}' | weblog");
    println!("  # 从文件读取");
    println!("  weblog file --path app.log --follow");
    println!("  # 监听TCP端口");
    println!("  weblog tcp --addr 0.0.0.0:9999");
    println!("  # 监听命名管道");
    println!("  weblog named-pipe --name \\\\.\\pipe\\my_app_logs");
    println!();
    println!("🎯 设计理念: 解耦、通用、可重用");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}



/// 处理命名管道日志
async fn process_named_pipe_logs(
    state: Arc<AppState>,
    pipe_name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};

    loop {
        info!("📡 创建命名管道服务器: {}", pipe_name);

        match create_named_pipe_server(&pipe_name).await {
            Ok(pipe_server) => {
                info!("✅ 命名管道服务器已创建，等待客户端连接");

                // 等待客户端连接
                pipe_server.connect().await?;
                info!("🔗 客户端已连接到命名管道");

                let mut reader = AsyncBufReader::new(pipe_server);
                let mut line_count = 0;
                let mut line = String::new();

                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => {
                            // EOF reached
                            info!("📡 命名管道连接断开");
                            break;
                        }
                        Ok(_) => {
                            line_count += 1;
                            process_log_line(&state, line.trim()).await;

                            if line_count % 100 == 0 {
                                info!("📊 已处理 {} 行日志", line_count);
                            }
                        }
                        Err(e) => {
                            error!("读取日志行失败: {}", e);
                            break;
                        }
                    }
                }

                info!("📡 命名管道连接断开，总共处理了 {} 行日志", line_count);
            }
            Err(e) => {
                warn!("创建命名管道服务器失败: {}, 5秒后重试", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// 创建命名管道服务器（Windows）
#[cfg(windows)]
async fn create_named_pipe_server(pipe_name: &str) -> Result<tokio::net::windows::named_pipe::NamedPipeServer, std::io::Error> {
    use tokio::net::windows::named_pipe::ServerOptions;

    // 尝试不同的管道名称格式
    let pipe_formats = vec![
        pipe_name.to_string(),
        format!(r"\\.\pipe\{}", pipe_name.trim_start_matches(r"\\.\pipe\").trim_start_matches(r"\\\\.\\pipe\\")),
        format!(r"\\.\pipe\kline_log_pipe"),
    ];

    for (i, format_name) in pipe_formats.iter().enumerate() {
        info!("尝试管道格式 {}: {}", i + 1, format_name);

        match ServerOptions::new()
            .first_pipe_instance(true)
            .create(format_name) {
            Ok(server) => {
                info!("✅ 成功创建命名管道: {}", format_name);
                return Ok(server);
            }
            Err(e) => {
                warn!("❌ 管道格式 {} 失败: {} - 错误: {}", i + 1, format_name, e);
            }
        }
    }

    // 如果所有格式都失败，返回最后一个错误
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("所有管道名称格式都失败: {}", pipe_name)
    ))
}

/// 非Windows平台的占位实现
#[cfg(not(windows))]
async fn create_named_pipe_server(_pipe_name: &str) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "命名管道仅在Windows平台支持"
    ))
}



/// 处理单行日志 - 极简版本：解析 → 缓存 → 广播
async fn process_log_line(state: &Arc<AppState>, line: &str) {
    use weblog::{parse_tracing_log_line, validate_log_entry};

    // 尝试解析JSON格式的tracing日志
    if let Some(log_entry) = parse_tracing_log_line(line) {
        if validate_log_entry(&log_entry) {
            // 使用AppState的统一处理方法：缓存 + 广播
            state.process_log_entry(log_entry);
        } else {
            warn!("日志条目验证失败: {}", line);
        }
    } else {
        // 不是有效的JSON格式tracing日志，记录错误
        error!("无法解析JSON格式日志: {}", line);
    }
}



/// WebLog配置结构
#[derive(Deserialize)]
struct WebLogLoggingConfig {
    weblog: WebLogServiceConfig,
}

#[derive(Deserialize)]
struct WebLogServiceConfig {
    log_level: String,
}

/// 读取WebLog日志级别配置 - 从WebLog自己的配置文件读取
fn load_weblog_log_level() -> String {
    // 首先检查环境变量，这样可以被外部脚本覆盖
    if let Ok(env_log_level) = std::env::var("RUST_LOG") {
        eprintln!("从环境变量读取日志级别: {}", env_log_level);
        return env_log_level;
    }

    // 获取当前可执行文件的目录
    let exe_path = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let exe_dir = exe_path.parent().unwrap_or_else(|| std::path::Path::new("."));

    // 尝试多个可能的配置文件路径
    let possible_paths = vec![
        std::path::PathBuf::from("config/logging_config.toml"),  // 相对于当前工作目录
        exe_dir.join("config/logging_config.toml"),  // 相对于可执行文件目录
        exe_dir.join("../config/logging_config.toml"),  // 上级目录的config
        exe_dir.join("../../config/logging_config.toml"),  // 再上级目录的config
    ];

    for config_path in possible_paths {
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            match toml::from_str::<WebLogLoggingConfig>(&content) {
                Ok(config) => {
                    eprintln!("从配置文件读取日志级别: {} (路径: {:?})", config.weblog.log_level, config_path);
                    return config.weblog.log_level;
                }
                Err(e) => {
                    eprintln!("解析配置文件失败: {} (路径: {:?})", e, config_path);
                }
            }
        }
    }

    // 所有路径都失败，使用默认值
    eprintln!("未找到有效的配置文件，使用默认日志级别 info");
    "info".to_string()
}