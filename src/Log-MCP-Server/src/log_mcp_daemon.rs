//! bin/log_mcp_daemon.rs
//!
//! 最终版本，提供两个核心的GET查询接口:
//! 1. GET /api/v1/trace/{trace_id} - 用于精确查询
//! 2. GET /api/v1/spans?tx_id=... - 用于按业务ID查询

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;

// [关键修改 1] 导入Windows命名管道相关的模块
#[cfg(windows)]
use tokio::net::windows::named_pipe::NamedPipeServer;

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
    pub duration_ms: f64,
    pub attributes: std::collections::HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp: String,
    pub name: String,
    pub level: String,
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

    /// [核心方法 1] 根据 trace_id 高效获取整个调用链
    pub fn get_trace_by_id(&self, trace_id: &str) -> Vec<SpanModel> {
        self.trace_to_spans
            .get(trace_id)
            .map(|span_ids| {
                span_ids
                    .iter()
                    .filter_map(|id| self.spans.get(id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_else(Vec::new)
    }

    /// [核心方法 2] 根据 tx_id 属性扫描所有 Spans
    pub fn find_spans_by_tx_id(&self, spec: &TxIdQuerySpec) -> Vec<SpanModel> {
        self.spans
            .values()
            .filter(|span| {
                // 在 attributes 中查找 tx_id
                if let Some(attr_val) = span.attributes.get("tx_id") {
                    if let Some(tx_id_str) = attr_val.as_str() {
                        return tx_id_str == spec.tx_id;
                    }
                }
                false
            })
            .cloned()
            .take(spec.limit.unwrap_or(1000)) // 设置一个合理的默认上限
            .collect()
    }
}

// --- 3. Axum State 和 MCP 定义 ---
// AppState现在包裹一个Mutex保护的DB
type AppState = Arc<Mutex<InMemoryDb>>;

// [修改] 简化查询结构体，只为 tx_id 查询服务，可用于 GET 的 Query Param
#[derive(Debug, Deserialize)]
pub struct TxIdQuerySpec {
    pub tx_id: String,
    pub limit: Option<usize>,
}

// --- 4. HTTP 处理函数 ---

/// 处理 GET /api/v1/trace/{trace_id}
async fn handle_get_trace_by_id(
    State(db_mutex): State<AppState>,
    Path(trace_id): Path<String>,
) -> Json<Vec<SpanModel>> {
    let db = db_mutex.lock().await;
    Json(db.get_trace_by_id(&trace_id))
}

/// 处理 GET /api/v1/spans?tx_id=...
async fn handle_find_spans_by_tx_id(
    State(db_mutex): State<AppState>,
    Query(payload): Query<TxIdQuerySpec>,
) -> Json<Vec<SpanModel>> {
    let db = db_mutex.lock().await;
    Json(db.find_spans_by_tx_id(&payload))
}

// 手动clear的接口我们依然保留，以备不时之需
async fn handle_mcp_clear(State(db_mutex): State<AppState>) -> StatusCode {
    let mut db = db_mutex.lock().await;
    db.clear();
    println!("[Daemon] InMemoryDb has been cleared by manual MCP request.");
    StatusCode::OK
}

// --- 5. 日志接收器任务 (Windows命名管道版本) ---
#[cfg(windows)]
async fn log_receiver_task(pipe_name: &str, db_mutex: Arc<Mutex<InMemoryDb>>, enable_debug: bool) {
    println!(
        "[Log Receiver] Ready to accept new log sessions on named pipe: {}",
        pipe_name
    );
    let mut last_waiting_log = Instant::now();
    let waiting_log_interval = Duration::from_secs(30);

    loop {
        let server = match tokio::net::windows::named_pipe::ServerOptions::new().create(pipe_name) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "[Log Receiver] Failed to create named pipe server: {}. Retrying in 5s.",
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        if enable_debug && last_waiting_log.elapsed() >= waiting_log_interval {
            println!("[Log Receiver] Waiting for a client to connect...");
            last_waiting_log = Instant::now();
        }

        if let Err(e) = server.connect().await {
            eprintln!(
                "[Log Receiver] Client failed to connect: {}. Restarting pipe server.",
                e
            );
            continue;
        }

        println!("\n[Log Receiver] Log source connected, starting a new session.");

        {
            let mut db = db_mutex.lock().await;
            db.clear();
            println!("[Log Receiver] Previous session data cleared.");
        }

        let db_clone = db_mutex.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(server);
            let mut line = String::new();
            let mut last_debug_time = Instant::now();
            let debug_interval = Duration::from_secs(5);
            let mut received_count = 0u64;
            let mut span_count = 0u64;

            while let Ok(bytes_read) = reader.read_line(&mut line).await {
                if bytes_read == 0 {
                    break;
                }
                received_count += 1;

                let should_debug = enable_debug && last_debug_time.elapsed() >= debug_interval;
                if should_debug {
                    println!(
                        "[Log Receiver] 已接收 {} 条日志记录，其中 {} 条Span",
                        received_count, span_count
                    );
                    last_debug_time = Instant::now();
                }

                match serde_json::from_str::<StructuredLog>(&line) {
                    Ok(StructuredLog::Span(span)) => {
                        span_count += 1;
                        let mut db = db_clone.lock().await;
                        db.insert_span(span);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if enable_debug {
                            eprintln!(
                                "[Log Receiver] WARN: Failed to parse log line. Error: {}. Line: '{}'",
                                e,
                                line.trim()
                            );
                        }
                    }
                }
                line.clear();
            }
            println!(
                "[Log Receiver] Log session ended. 总计处理: {} 条记录, {} 条Span",
                received_count, span_count
            );
        });
    }
}

// --- 5. 日志接收器任务 (非Windows TCP版本) ---
#[cfg(not(windows))]
async fn log_receiver_task(tcp_addr: &str, db_mutex: Arc<Mutex<InMemoryDb>>, enable_debug: bool) {
    let listener = match TcpListener::bind(tcp_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "[Log Receiver] Failed to bind TCP listener to {}: {}",
                tcp_addr, e
            );
            return;
        }
    };

    println!(
        "[Log Receiver] Ready to accept new log sessions on TCP: {}",
        tcp_addr
    );

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!(
                    "\n[Log Receiver] New connection from {}, starting a new session.",
                    addr
                );

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
                    let debug_interval = Duration::from_secs(5);
                    let mut received_count = 0u64;
                    let mut span_count = 0u64;

                    while let Ok(bytes_read) = reader.read_line(&mut line).await {
                        if bytes_read == 0 {
                            break;
                        }
                        received_count += 1;

                        let should_debug = enable_debug && last_debug_time.elapsed() >= debug_interval;
                        if should_debug {
                            println!(
                                "[Log Receiver] 已接收 {} 条日志记录，其中 {} 条Span",
                                received_count, span_count
                            );
                            last_debug_time = Instant::now();
                        }

                        match serde_json::from_str::<StructuredLog>(&line) {
                            Ok(StructuredLog::Span(span)) => {
                                span_count += 1;
                                let mut db = db_clone.lock().await;
                                db.insert_span(span);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                if enable_debug {
                                    eprintln!(
                                        "[Log Receiver] WARN: Failed to parse log line. Error: {}. Line: '{}'",
                                        e,
                                        line.trim()
                                    );
                                }
                            }
                        }
                        line.clear();
                    }
                    println!(
                        "[Log Receiver] Log session from {} ended. 总计处理: {} 条记录, {} 条Span",
                        addr, received_count, span_count
                    );
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
    println!(
        "[Daemon] -> Debug output: {}",
        config.logging.enable_debug_output
    );
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
        .route("/api/v1/trace/:trace_id", get(handle_get_trace_by_id))
        .route("/api/v1/spans", get(handle_find_spans_by_tx_id))
        .route("/clear", post(handle_mcp_clear))
        .with_state(db.clone());

    let mcp_addr = format!("127.0.0.1:{}", config.server.mcp_port);
    let mcp_listener = match tokio::net::TcpListener::bind(&mcp_addr).await {
        Ok(listener) => {
            println!("[Daemon] MCP listener bound to {}", mcp_addr);
            listener
        }
        Err(e) => {
            eprintln!(
                "[Daemon] Failed to bind MCP listener to {}: {}",
                mcp_addr, e
            );
            return;
        }
    };

    println!("[Daemon] Log MCP Daemon is running (Named Pipe Auto-Clear Session Mode).");
    #[cfg(windows)]
    println!(
        "[Daemon] -> Listening for logs on named pipe: {}",
        format!(r"\\.\pipe\{}", config.logging.pipe_name)
    );
    #[cfg(not(windows))]
    println!("[Daemon] -> Listening for logs on TCP: 127.0.0.1:9000");
    println!(
        "[Daemon] -> Listening for MCP queries on http://{}",
        mcp_addr
    );
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

            println!(
                "[Status] Log MCP Daemon运行中 - 存储Spans: {}, Traces: {}",
                span_count, trace_count
            );
            io::stdout().flush().unwrap();
        }
    });

    // 等待Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c signal");
    println!("\nCtrl+C received. Shutting down.");

    log_task_handle.abort();
    mcp_task_handle.abort();
    status_task_handle.abort();
}