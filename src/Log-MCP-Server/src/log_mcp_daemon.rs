//! bin/log_mcp_daemon.rs (Mutex Version for Absolute Sequential Workflow)
//!
//! 这个版本假定用户的工作流严格保证了写入和读取不会同时发生。
//! Mutex在这里的作用，主要是为了满足Rust编译器的所有权和借用规则，
//! 而不是为了解决实际的锁竞争问题。

use axum::{extract::State, http::StatusCode, response::Json, routing::post, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::io::{self, Write};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex; // 使用tokio的异步Mutex

// [关键修改 1] 导入Windows命名管道相关的模块
#[cfg(windows)]
use tokio::net::windows::named_pipe::{ServerOptions, NamedPipeServer};

// 非Windows平台回退到TCP
#[cfg(not(windows))]
use tokio::net::TcpListener;

// --- 配置读取 ---
#[derive(Debug, Deserialize)]
struct ServerConfig {
    mcp_port: u16,
}

#[derive(Debug, Deserialize)]
struct LoggingConfig {
    pipe_name: String,
    enable_debug_output: bool,
    auto_clear_on_new_session: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    server: ServerConfig,
    logging: LoggingConfig,
}

fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let config_path = "config.toml";
    let config_content = std::fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_content)?;
    Ok(config)
}

// --- 1. 数据模型 ---
// 简化AttributeValue，直接使用serde_json::Value
pub type AttributeValue = serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanModel {
    pub id: String,
    pub trace_id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub target: String,
    pub level: String,
    pub timestamp: String,
    pub duration_ms: Option<f64>,
    pub attributes: std::collections::HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp: String,
    pub name: String,
    pub attributes: std::collections::HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StructuredLog {
    Span(SpanModel),
    Event {
        timestamp: String,
        level: String,
        target: String,
        message: String,
        attributes: std::collections::HashMap<String, AttributeValue>,
    },
}

// --- 2. 内存数据库 (普通struct) ---
#[derive(Default, Debug)]
pub struct InMemoryDb {
    spans: HashMap<String, SpanModel>,
    trace_to_spans: HashMap<String, Vec<String>>,
}

impl InMemoryDb {
    fn insert_span(&mut self, span: SpanModel) {
        let span_id = span.id.clone();
        let trace_id = span.trace_id.clone();

        // 存储span
        self.spans.insert(span_id.clone(), span);

        // 维护trace到spans的映射
        self.trace_to_spans
            .entry(trace_id)
            .or_insert_with(Vec::new)
            .push(span_id);
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    /// [关键改进 1] 使用迭代器风格重写query方法
    fn query(&self, spec: &McpQuerySpec) -> Vec<SpanModel> {
        self.spans
            .values()
            .filter(|span| {
                let trace_id_match = spec.trace_id.as_ref().map_or(true, |id| &span.trace_id == id);
                let target_match = spec.target.as_ref().map_or(true, |t| span.target.contains(t));
                let level_match = spec.level.as_ref().map_or(true, |l| &span.level == l);
                let start_time_match = spec.start_time.as_ref().map_or(true, |st| &span.timestamp >= st);
                let end_time_match = spec.end_time.as_ref().map_or(true, |et| &span.timestamp <= et);

                trace_id_match && target_match && level_match && start_time_match && end_time_match
            })
            .cloned()
            .take(spec.limit.unwrap_or(usize::MAX))
            .collect()
    }
}

// --- 3. Axum State 和 MCP 定义 ---
// AppState现在包裹一个Mutex保护的DB
type AppState = Arc<Mutex<InMemoryDb>>;

#[derive(Debug, Deserialize)]
pub struct McpQuerySpec {
    pub trace_id: Option<String>,
    pub target: Option<String>,
    pub level: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub limit: Option<usize>,
}

// --- 4. HTTP 处理函数 ---
async fn handle_mcp_query(
    State(db_mutex): State<AppState>,
    Json(payload): Json<McpQuerySpec>,
) -> Json<Vec<SpanModel>> {
    // 获取锁。因为没有竞争，这个操作会立即成功。
    let db = db_mutex.lock().await; 
    let results = db.query(&payload);
    Json(results)
    // 锁在db离开作用域时自动释放
}

// 注意：手动clear的接口我们依然保留，以备不时之需
async fn handle_mcp_clear(State(db_mutex): State<AppState>) -> StatusCode {
    let mut db = db_mutex.lock().await;
    db.clear();
    println!("[Daemon] InMemoryDb has been cleared by manual MCP request.");
    StatusCode::OK
}

// --- 5. 日志接收器任务 (Windows命名管道版本) ---
#[cfg(windows)]
async fn log_receiver_task(pipe_name: &str, db_mutex: Arc<Mutex<InMemoryDb>>, enable_debug: bool) {
    println!("[Log Receiver] Ready to accept new log sessions on named pipe: {}", pipe_name);
    let mut last_waiting_log = Instant::now();
    let waiting_log_interval = Duration::from_secs(30); // 每30秒输出一次等待日志

    loop {
        // 创建一个新的命名管道服务器实例，并等待客户端连接
        let server = match ServerOptions::new().create(pipe_name) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[Log Receiver] Failed to create named pipe server: {}. Retrying in 5s.", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        if enable_debug && last_waiting_log.elapsed() >= waiting_log_interval {
            println!("[Log Receiver] Waiting for a client to connect...");
            last_waiting_log = Instant::now();
        }

        // 等待K线程序连接
        if let Err(e) = server.connect().await {
            eprintln!("[Log Receiver] Client failed to connect: {}. Restarting pipe server.", e);
            continue; // 如果连接失败，重新创建server
        }

        println!("\n[Log Receiver] Log source connected, starting a new session.");

        // [自动清理逻辑]
        {
            let mut db = db_mutex.lock().await;
            db.clear();
            println!("[Log Receiver] Previous session data cleared.");
        }

        let db_clone = db_mutex.clone();
        // 为这个会话创建一个处理任务
        tokio::spawn(async move {
            // server 现在就像一个 TcpStream
            let mut reader = BufReader::new(server);
            let mut line = String::new();
            let mut last_debug_time = Instant::now();
            let debug_interval = Duration::from_secs(5); // 每5秒最多输出一次调试信息
            let mut received_count = 0u64;
            let mut span_count = 0u64;

            while let Ok(bytes_read) = reader.read_line(&mut line).await {
                if bytes_read == 0 { break; } // 连接断开
                received_count += 1;

                let should_debug = enable_debug && last_debug_time.elapsed() >= debug_interval;
                if should_debug {
                    println!("[Log Receiver] 已接收 {} 条日志记录，其中 {} 条Span", received_count, span_count);
                    last_debug_time = Instant::now();
                }

                match serde_json::from_str::<StructuredLog>(&line) {
                    Ok(StructuredLog::Span(span)) => {
                        span_count += 1;
                        let mut db = db_clone.lock().await;
                        db.insert_span(span);
                    }
                    Ok(_) => {
                        // 非span日志条目，静默处理
                    }
                    Err(_) => {
                        // JSON解析错误，静默处理
                    }
                }
                line.clear();
            }
            println!("[Log Receiver] Log session ended. 总计处理: {} 条记录, {} 条Span", received_count, span_count);
        });
    }
}

// --- 5. 日志接收器任务 (非Windows TCP版本) ---
#[cfg(not(windows))]
async fn log_receiver_task(tcp_addr: &str, db_mutex: Arc<Mutex<InMemoryDb>>, enable_debug: bool) {
    let listener = match TcpListener::bind(tcp_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[Log Receiver] Failed to bind TCP listener to {}: {}", tcp_addr, e);
            return;
        }
    };

    println!("[Log Receiver] Ready to accept new log sessions on TCP: {}", tcp_addr);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("\n[Log Receiver] New connection from {}, starting a new session.", addr);

                // [自动清理逻辑]
                {
                    let mut db = db_mutex.lock().await;
                    db.clear();
                    println!("[Log Receiver] Previous session data cleared.");
                }

                let db_clone = db_mutex.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    let mut last_debug_time = Instant::now();
                    let debug_interval = Duration::from_secs(5); // 每5秒最多输出一次调试信息
                    let mut received_count = 0u64;
                    let mut span_count = 0u64;

                    while let Ok(bytes_read) = reader.read_line(&mut line).await {
                        if bytes_read == 0 { break; }
                        received_count += 1;

                        let should_debug = enable_debug && last_debug_time.elapsed() >= debug_interval;
                        if should_debug {
                            println!("[Log Receiver] 已接收 {} 条日志记录，其中 {} 条Span", received_count, span_count);
                            last_debug_time = Instant::now();
                        }

                        match serde_json::from_str::<StructuredLog>(&line) {
                            Ok(StructuredLog::Span(span)) => {
                                span_count += 1;
                                let mut db = db_clone.lock().await;
                                db.insert_span(span);
                            }
                            Ok(_) => {
                                // 非span日志条目，静默处理
                            }
                            Err(_) => {
                                // JSON解析错误，静默处理
                            }
                        }
                        line.clear();
                    }
                    println!("[Log Receiver] Log session from {} ended. 总计处理: {} 条记录, {} 条Span", addr, received_count, span_count);
                });
            }
            Err(e) => {
                eprintln!("[Log Receiver] Failed to accept connection: {}", e);
                break;
            }
        }
    }
}

// --- 6. 主函数 ---
#[tokio::main]
async fn main() {
    println!("[Daemon] Starting Log MCP Daemon...");
    io::stdout().flush().unwrap();

    // 1. 加载配置
    let config = match load_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[Daemon] Failed to load config: {}", e);
            return;
        }
    };

    println!("[Daemon] Configuration loaded:");
    println!("[Daemon] -> Pipe name: {}", config.logging.pipe_name);
    println!("[Daemon] -> MCP port: {}", config.server.mcp_port);
    println!("[Daemon] -> Debug output: {}", config.logging.enable_debug_output);
    io::stdout().flush().unwrap();

    // 2. 创建被Mutex保护的数据库实例
    let db = Arc::new(Mutex::new(InMemoryDb::default()));
    println!("[Daemon] Database initialized.");
    io::stdout().flush().unwrap();

    // 3. 启动日志接收服务器
    #[cfg(windows)]
    let log_task_handle = {
        let pipe_name = format!(r"\\.\pipe\{}", config.logging.pipe_name);
        tokio::spawn(log_receiver_task(
            pipe_name.leak(), // 转换为 &'static str
            db.clone(),
            config.logging.enable_debug_output,
        ))
    };

    #[cfg(not(windows))]
    let log_task_handle = {
        let tcp_addr = "127.0.0.1:9000";
        tokio::spawn(log_receiver_task(
            tcp_addr,
            db.clone(),
            config.logging.enable_debug_output,
        ))
    };

    println!("[Daemon] Log receiver task started.");

    // 4. 启动MCP查询服务器
    let app = Router::new()
        .route("/query", post(handle_mcp_query))
        .route("/clear", post(handle_mcp_clear))
        .with_state(db.clone());

    let mcp_addr = format!("127.0.0.1:{}", config.server.mcp_port);
    let mcp_listener = match tokio::net::TcpListener::bind(&mcp_addr).await {
        Ok(listener) => {
            println!("[Daemon] MCP listener bound to {}", mcp_addr);
            listener
        }
        Err(e) => {
            eprintln!("[Daemon] Failed to bind MCP listener to {}: {}", mcp_addr, e);
            return;
        }
    };

    println!("[Daemon] Log MCP Daemon is running (Named Pipe Auto-Clear Session Mode).");
    #[cfg(windows)]
    println!("[Daemon] -> Listening for logs on named pipe: {}", format!(r"\\.\pipe\{}", config.logging.pipe_name));
    #[cfg(not(windows))]
    println!("[Daemon] -> Listening for logs on TCP: 127.0.0.1:9000");
    println!("[Daemon] -> Listening for MCP queries on http://{}", mcp_addr);
    println!("[Daemon] -> Each new log connection will start a fresh session");

    let mcp_task_handle = tokio::spawn(async move {
        axum::serve(mcp_listener, app).await.unwrap();
    });

    // 启动状态监控任务 - 每5秒输出一条状态日志
    let status_db = db.clone();
    let status_task_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let db_guard = status_db.lock().await;
            let span_count = db_guard.spans.len();
            let trace_count = db_guard.trace_to_spans.len();
            drop(db_guard); // 释放锁

            println!("[Status] Log MCP Daemon运行中 - 存储Spans: {}, Traces: {}", span_count, trace_count);
            io::stdout().flush().unwrap();
        }
    });

    // 等待Ctrl+C
    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c signal");
    println!("\nCtrl+C received. Shutting down.");

    log_task_handle.abort();
    mcp_task_handle.abort();
    status_task_handle.abort();
}