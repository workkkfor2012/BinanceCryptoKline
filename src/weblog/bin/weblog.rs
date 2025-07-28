//! WebLog - 通用Web日志显示系统
//!
//! 完全独立的日志可视化工具，支持任何基于tracing规范的Rust应用程序
//! 设计理念：解耦、通用、可重用

use clap::Parser;
use std::sync::Arc;
use tokio::time::{sleep, Duration, interval, Instant};
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
    let (state, _log_receiver, _websocket_receiver) = AppState::new();
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
                let mut line_count = 0u64;
                let mut line = String::new();

                // 创建10秒定时器用于统计输出
                let mut stats_timer = interval(Duration::from_secs(10));
                let mut last_reported_count = 0u64;
                let start_time = Instant::now();

                loop {
                    tokio::select! {
                        // 处理日志行
                        read_result = reader.read_line(&mut line) => {
                            match read_result {
                                Ok(0) => {
                                    // EOF reached
                                    info!("📡 命名管道连接断开");
                                    break;
                                }
                                Ok(_) => {
                                    line_count += 1;
                                    process_log_line(&state, line.trim()).await;
                                    line.clear();
                                }
                                Err(e) => {
                                    error!("读取日志行失败: {}", e);
                                    break;
                                }
                            }
                        }

                        // 每10秒输出统计信息
                        _ = stats_timer.tick() => {
                            let new_logs = line_count - last_reported_count;
                            let elapsed = start_time.elapsed().as_secs();
                            if new_logs > 0 {
                                info!("📊 过去10秒收到 {} 条日志，总计 {} 条，运行时间 {}秒",
                                      new_logs, line_count, elapsed);
                            }
                            last_reported_count = line_count;
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

    // 定义一个更大的缓冲区大小，例如 1MB
    const PIPE_BUFFER_SIZE: u32 = 1024 * 1024 * 1024;

    // 尝试不同的管道名称格式
    let pipe_formats = vec![
        pipe_name.to_string(),
        format!(r"\\.\pipe\{}", pipe_name.trim_start_matches(r"\\.\pipe\").trim_start_matches(r"\\\\.\\pipe\\")),
        format!(r"\\.\pipe\weblog_pipe"),
    ];

    for (i, format_name) in pipe_formats.iter().enumerate() {
        info!("尝试管道格式 {}: {}", i + 1, format_name);

        match ServerOptions::new()
            .first_pipe_instance(true)
            .in_buffer_size(PIPE_BUFFER_SIZE)  // 设置输入缓冲区大小为 1MB
            .out_buffer_size(PIPE_BUFFER_SIZE) // 设置输出缓冲区大小为 1MB
            .create(format_name) {
            Ok(server) => {
                info!("✅ 成功创建命名管道: {} (缓冲区: {} KB)", format_name, PIPE_BUFFER_SIZE / 1024);
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
            // 检查是否是会话开始标记
            if is_session_start_marker(&log_entry) {
                info!("🆕 检测到会话开始标记，开始新会话");
                info!("📋 会话开始标记详情:");
                info!("   - 消息内容: '{}'", log_entry.message);
                info!("   - 时间戳: {}", log_entry.timestamp);
                info!("   - 目标模块: {}", log_entry.target);

                // 显示关键字段
                if let Some(session_start) = log_entry.fields.get("session_start") {
                    info!("   - session_start字段: {}", session_start);
                }
                if let Some(event_type) = log_entry.fields.get("event_type") {
                    info!("   - event_type字段: {}", event_type);
                }
                if let Some(program_name) = log_entry.fields.get("program_name") {
                    info!("   - program_name字段: {}", program_name);
                }

                let new_session_id = state.start_new_session();
                info!("✅ 新会话已开始: {}", new_session_id);
                info!("🧹 历史数据已清空，准备接收新会话的日志");
                return; // 不处理会话开始标记本身
            }

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

/// 检查是否是会话开始标记
fn is_session_start_marker(log_entry: &weblog::LogEntry) -> bool {
    // 检查消息是否为 session_start
    if log_entry.message == "session_start" {
        info!("✅ 会话开始标记匹配 - 消息匹配: message == 'session_start'");
        return true;
    }

    // 检查fields中是否有session_start标记
    if let Some(session_start) = log_entry.fields.get("session_start") {
        if let Some(is_start) = session_start.as_bool() {
            if is_start {
                info!("✅ 会话开始标记匹配 - 字段匹配: fields.session_start == true");
                return true;
            }
        }
    }

    false
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

/// 读取WebLog日志级别配置 - 从统一配置文件读取
fn load_weblog_log_level() -> String {
    // 尝试从统一配置文件读取
    let possible_paths = vec![
        std::path::PathBuf::from("config/BinanceKlineConfig.toml"),  // 相对于当前工作目录
        std::path::PathBuf::from("../config/BinanceKlineConfig.toml"),  // 上级目录的config
        std::path::PathBuf::from("../../config/BinanceKlineConfig.toml"),  // 再上级目录的config
    ];

    for config_path in possible_paths {
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            // 首先尝试读取 [logging.services] 部分的 weblog 配置
            let lines: Vec<&str> = content.lines().collect();
            let mut in_services_section = false;

            for line in lines.iter() {
                let trimmed = line.trim();
                if trimmed == "[logging.services]" {
                    in_services_section = true;
                } else if trimmed.starts_with('[') && trimmed != "[logging.services]" {
                    in_services_section = false;
                } else if in_services_section && trimmed.starts_with("weblog") {
                    if let Some(value) = trimmed.split('=').nth(1) {
                        let log_level = value.trim().trim_matches('"').trim_matches('\'');
                        eprintln!("从统一配置文件读取WebLog日志级别: {} (路径: {:?})", log_level, config_path);
                        return log_level.to_string();
                    }
                }
            }

            // 如果没有找到 weblog 特定配置，回退到 [logging] 部分的 log_level
            let mut in_logging_section = false;
            for line in lines {
                let trimmed = line.trim();
                if trimmed == "[logging]" {
                    in_logging_section = true;
                } else if trimmed.starts_with('[') && trimmed != "[logging]" {
                    in_logging_section = false;
                } else if in_logging_section && trimmed.starts_with("log_level") {
                    if let Some(value) = trimmed.split('=').nth(1) {
                        let log_level = value.trim().trim_matches('"').trim_matches('\'');
                        eprintln!("从统一配置文件读取默认日志级别: {} (路径: {:?})", log_level, config_path);
                        return log_level.to_string();
                    }
                }
            }
        }
    }

    // 所有路径都失败，使用默认值
    eprintln!("未找到统一配置文件，使用默认日志级别 info");
    "info".to_string()
}