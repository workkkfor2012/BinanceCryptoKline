//! src/klcommon/log/ai_log.rs
//!
//! AI日志核心模块 (真相之源)
//! 负责捕获所有tracing事件，转换为结构化日志，并通过异步批量处理发送到log_mcp_daemon。
//!
//! 架构设计：
//! - 主线程：快速将日志发送到内部通道，无阻塞
//! - 后台工作线程：负责批量处理、序列化、文件写入和管道发送
//! - 使用crossbeam_channel实现高性能的线程间通信

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::thread;
use tracing::{Id, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};
use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};
use once_cell::sync::Lazy;

// --- 1. 数据模型 (与 daemon 端完全一致) ---
//    这是日志生产者和消费者之间的"契约"。

/// 通用属性值，使用serde_json::Value以获得最大灵活性
pub type AttributeValue = serde_json::Value;

/// 发送到daemon的Span的完整模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanModel {
    pub id: String,
    pub trace_id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub target: String,
    pub level: String,
    pub timestamp: String, // ISO 8601 format
    pub duration_ms: f64,
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub status: String, // "SUCCESS" or "FAILURE"
}

/// Span内部发生的事件的模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp: String, // ISO 8601 format
    pub name: String,
    pub level: String,
    pub attributes: HashMap<String, AttributeValue>,
}

/// 发送到daemon的统一日志结构
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StructuredLog {
    Span(SpanModel),
    // 可以扩展以支持独立的Event，但当前模型中Event都附属于Span
}

// --- 2. 异步批量日志发送器 ---

use std::fs::OpenOptions;
use std::io::BufWriter;

// 批量处理配置
const BATCH_SIZE: usize = 10000;
const BATCH_TIMEOUT: Duration = Duration::from_secs(3);

// 在Windows上使用命名管道
#[cfg(windows)]
type LogStream = BufWriter<std::fs::File>;

// 在非Windows上回退到TCP
#[cfg(not(windows))]
use std::net::TcpStream;
#[cfg(not(windows))]
type LogStream = TcpStream;

// 全局通道发送端，传递未序列化的StructuredLog结构体
static LOG_CHANNEL_SENDER: Lazy<Mutex<Option<Sender<StructuredLog>>>> =
    Lazy::new(|| Mutex::new(None));

/// 初始化异步日志发送器，启动后台工作线程处理批量日志。
/// 在`main`函数开始时调用一次。
pub fn init_log_sender(pipe_name: &str) {
    // 1. 创建无界通道用于线程间通信
    let (tx, rx) = unbounded::<StructuredLog>();

    // 2. 将发送端保存到全局变量
    *LOG_CHANNEL_SENDER.lock().unwrap() = Some(tx);

    // 3. 复制管道名称用于工作线程
    let pipe_name_clone = pipe_name.to_string();

    // 4. 启动专用的OS线程来处理日志I/O
    thread::Builder::new()
        .name("ai_log_worker".into())
        .spawn(move || {
            log_worker_loop(rx, &pipe_name_clone);
        })
        .expect("无法启动AI日志工作线程");

    println!("[Log Sender] AI日志异步批量处理系统已启动，管道: {}", pipe_name);
}

/// 后台工作线程的主循环，负责批量处理日志
/// 严格按照"1000条或5秒"的规则进行批量传输
fn log_worker_loop(rx: Receiver<StructuredLog>, pipe_name: &str) {
    // 初始化连接和文件句柄
    let mut daemon_stream = init_daemon_connection(pipe_name);
    let mut local_writer = init_local_log_file();

    // 批量缓冲区
    let mut batch_buffer = Vec::with_capacity(BATCH_SIZE);

    println!("[Log Worker] 后台日志工作线程已启动 (批量规则: {}条或{}秒)", BATCH_SIZE, BATCH_TIMEOUT.as_secs());

    loop {
        // 如果缓冲区为空，等待第一条日志（可能超时）
        if batch_buffer.is_empty() {
            match rx.recv_timeout(BATCH_TIMEOUT) {
                Ok(log) => {
                    batch_buffer.push(log);
                    // 继续收集更多日志，但不超过BATCH_SIZE
                    collect_more_logs(&rx, &mut batch_buffer);
                    // 如果达到BATCH_SIZE，立即处理
                    if batch_buffer.len() >= BATCH_SIZE {
                        process_batch(&mut batch_buffer, &mut daemon_stream, &mut local_writer);
                        continue;
                    }
                }
                Err(_) => {
                    // 超时但缓冲区为空，继续等待
                    continue;
                }
            }
        }

        // 缓冲区不为空，等待更多日志或超时
        match rx.recv_timeout(BATCH_TIMEOUT) {
            Ok(log) => {
                batch_buffer.push(log);
                // 继续收集更多日志
                collect_more_logs(&rx, &mut batch_buffer);
                // 检查是否达到批量大小
                if batch_buffer.len() >= BATCH_SIZE {
                    process_batch(&mut batch_buffer, &mut daemon_stream, &mut local_writer);
                }
            }
            Err(_) => {
                // 超时，处理当前批次（如果有的话）
                if !batch_buffer.is_empty() {
                    process_batch(&mut batch_buffer, &mut daemon_stream, &mut local_writer);
                }
            }
        }

        // 检查通道是否已关闭
        if rx.is_empty() && rx.len() == 0 {
            // 尝试一次非阻塞接收来检查通道状态
            match rx.try_recv() {
                Err(TryRecvError::Disconnected) => {
                    // 通道关闭，处理剩余日志后退出
                    if !batch_buffer.is_empty() {
                        process_batch(&mut batch_buffer, &mut daemon_stream, &mut local_writer);
                    }
                    println!("[Log Worker] 通道已关闭，工作线程退出");
                    return;
                }
                Ok(log) => {
                    // 还有日志，加入缓冲区
                    batch_buffer.push(log);
                }
                Err(TryRecvError::Empty) => {
                    // 通道为空但未关闭，继续循环
                }
            }
        }
    }
}

/// 收集更多日志到缓冲区，但不超过BATCH_SIZE
fn collect_more_logs(rx: &Receiver<StructuredLog>, batch_buffer: &mut Vec<StructuredLog>) {
    while batch_buffer.len() < BATCH_SIZE {
        match rx.try_recv() {
            Ok(log) => batch_buffer.push(log),
            Err(TryRecvError::Empty) => break, // 没有更多日志
            Err(TryRecvError::Disconnected) => break, // 通道关闭
        }
    }
}

/// 初始化到daemon的连接
fn init_daemon_connection(pipe_name: &str) -> Option<LogStream> {
    #[cfg(windows)]
    {
        // 检查管道名称是否已经包含完整路径前缀
        let pipe_path = if pipe_name.starts_with(r"\\.\pipe\") {
            pipe_name.to_string()
        } else {
            format!(r"\\.\pipe\{}", pipe_name)
        };
        match OpenOptions::new().write(true).open(&pipe_path) {
            Ok(file) => {
                println!("[Log Worker] 已连接到daemon管道: {}", pipe_path);
                Some(BufWriter::new(file))
            }
            Err(e) => {
                eprintln!("[Log Worker] 连接管道失败 '{}': {}", pipe_path, e);
                eprintln!("[Log Worker] 请确保log_mcp_daemon正在运行");
                eprintln!("[Log Worker] 日志将仅保存到本地文件");
                None
            }
        }
    }

    #[cfg(not(windows))]
    {
        match TcpStream::connect(pipe_name) {
            Ok(stream) => {
                println!("[Log Worker] 已连接到daemon: {}", pipe_name);
                Some(stream)
            }
            Err(e) => {
                eprintln!("[Log Worker] 连接daemon失败 '{}': {}", pipe_name, e);
                eprintln!("[Log Worker] 请确保log_mcp_daemon正在运行");
                eprintln!("[Log Worker] 日志将仅保存到本地文件");
                None
            }
        }
    }
}

/// 初始化本地日志文件
fn init_local_log_file() -> Option<BufWriter<std::fs::File>> {
    // 确保logs目录存在
    if let Err(e) = std::fs::create_dir_all("logs") {
        eprintln!("[Log Worker] 创建logs目录失败: {}", e);
        return None;
    }

    // 创建本地详细日志文件
    let log_path = "logs/ai_detailed.log";
    match OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        Ok(file) => {
            println!("[Log Worker] 本地日志文件已初始化: {}", log_path);
            Some(BufWriter::new(file))
        }
        Err(e) => {
            eprintln!("[Log Worker] 创建本地日志文件失败 '{}': {}", log_path, e);
            None
        }
    }
}

/// 处理一个批次的日志
fn process_batch(
    batch: &mut Vec<StructuredLog>,
    daemon_stream: &mut Option<LogStream>,
    local_writer: &mut Option<BufWriter<std::fs::File>>
) {
    if batch.is_empty() {
        return;
    }

    let batch_size = batch.len();
    let start_time = Instant::now();

    // 序列化整个批次
    let mut serialized_logs = Vec::with_capacity(batch_size);
    let mut serialization_errors = 0;

    for log in batch.iter() {
        match serde_json::to_string(log) {
            Ok(mut json) => {
                json.push('\n');
                serialized_logs.push(json);
            }
            Err(e) => {
                serialization_errors += 1;
                eprintln!("[Log Worker] 序列化失败: {}", e);
            }
        }
    }

    let serialization_time = start_time.elapsed();
    let mut local_write_success = false;
    let mut daemon_send_success = false;

    // 批量写入本地文件
    if let Some(writer) = local_writer {
        let mut write_errors = 0;
        for json_line in &serialized_logs {
            if let Err(e) = writer.write_all(json_line.as_bytes()) {
                write_errors += 1;
                if write_errors == 1 { // 只打印第一个错误
                    eprintln!("[Log Worker] 本地文件写入失败: {}", e);
                }
                break;
            }
        }
        if write_errors == 0 {
            let _ = writer.flush();
            local_write_success = true;
        }
    }

    // 批量发送到daemon
    if let Some(stream) = daemon_stream {
        let mut send_errors = 0;
        for json_line in &serialized_logs {
            if let Err(e) = stream.write_all(json_line.as_bytes()) {
                send_errors += 1;
                if send_errors == 1 { // 只打印第一个错误
                    eprintln!("[Log Worker] daemon发送失败: {}", e);
                }
                break;
            }
        }
        if send_errors == 0 {
            let _ = stream.flush();
            daemon_send_success = true;
        }
    }

    let total_time = start_time.elapsed();

    // 详细的批量处理统计
    println!(
        "[Log Worker] 批量处理完成: {} 条日志, 序列化: {:.2}ms, 总耗时: {:.2}ms, 本地: {}, daemon: {}",
        batch_size,
        serialization_time.as_secs_f64() * 1000.0,
        total_time.as_secs_f64() * 1000.0,
        if local_write_success { "✓" } else { "✗" },
        if daemon_send_success { "✓" } else { "✗" }
    );

    if serialization_errors > 0 {
        eprintln!("[Log Worker] 序列化错误: {} 条", serialization_errors);
    }

    batch.clear();
}

/// 发送一个结构化的日志事件（轻量级，非阻塞）
/// 这是提供给`McpLayer`调用的核心函数。
fn send_log(log: StructuredLog) {
    if let Some(sender) = &*LOG_CHANNEL_SENDER.lock().unwrap() {
        // 直接发送结构体到后台线程，让后台线程处理序列化和I/O
        if let Err(_) = sender.send(log) {
            // 通道已关闭，静默忽略（避免在关闭时产生噪音）
        }
    }
}

// --- 3. 核心转换层 (McpLayer) ---

// 辅助工具：用于从tracing字段中提取serde_json::Value
pub struct JsonVisitor<'a>(pub &'a mut HashMap<String, AttributeValue>);
impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    // 实现所有类型，确保不会漏掉字段
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) { 
        self.0.insert(field.name().to_string(), serde_json::json!(value)); 
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) { 
        self.0.insert(field.name().to_string(), serde_json::json!(value)); 
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) { 
        self.0.insert(field.name().to_string(), serde_json::json!(value)); 
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) { 
        self.0.insert(field.name().to_string(), serde_json::json!(value)); 
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) { 
        self.0.insert(field.name().to_string(), serde_json::json!(value)); 
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_string(), serde_json::json!(format!("{:?}", value)));
    }
}

/// 用于在Span的生命周期中临时存储数据的结构
#[derive(Default, Clone)]
pub struct SpanContext {
    pub trace_id: String,
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub has_error: bool,
}

pub struct McpLayer;

impl<S> Layer<S> for McpLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found");
        let mut extensions = span.extensions_mut();

        // 1. 确定Trace ID
        let trace_id = if let Some(parent) = span.parent() {
            // 如果有父Span，从父Span继承Trace ID
            parent.extensions()
                  .get::<SpanContext>()
                  .map(|d| d.trace_id.clone())
                  .unwrap_or_else(|| parent.id().into_u64().to_string()) // Fallback
        } else {
            // 如果是根Span，用自己的ID作为Trace ID
            id.into_u64().to_string()
        };

        // 2. 收集字段
        let mut attributes = HashMap::new();
        let mut visitor = JsonVisitor(&mut attributes);
        attrs.record(&mut visitor);

        // 3. 将Trace ID、字段和开始时间存入Span的extensions中
        extensions.insert(Instant::now());
        extensions.insert(SpanContext {
            trace_id,
            attributes,
            events: Vec::new(),
            has_error: false,
        });
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        // 找到当前事件所属的Span
        if let Some(span) = ctx.lookup_current() {
            let mut extensions = span.extensions_mut();
            if let Some(data) = extensions.get_mut::<SpanContext>() {

                // 标记Span包含错误
                if *event.metadata().level() <= tracing::Level::ERROR {
                    data.has_error = true;
                }

                // 收集事件的字段
                let mut attributes = HashMap::new();
                let mut visitor = JsonVisitor(&mut attributes);
                event.record(&mut visitor);

                // 将 `message` 字段作为事件的 `name`
                let name = if let Some(serde_json::Value::String(msg)) = attributes.remove("message") {
                    msg
                } else {
                    event.metadata().name().to_string()
                };

                data.events.push(SpanEvent {
                    timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    name,
                    level: event.metadata().level().to_string(),
                    attributes,
                });
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("Span not found");
        let metadata = span.metadata();
        let extensions = span.extensions();

        // 从extensions中恢复我们之前存储的所有数据
        let start_time_instant = extensions.get::<Instant>().cloned().unwrap_or_else(Instant::now);
        let span_context = extensions.get::<SpanContext>().cloned().unwrap_or_default();

        // `instrument(err)`会自动在span上记录一个error事件，我们的on_event会捕获它并设置has_error
        let status = if span_context.has_error { "FAILURE" } else { "SUCCESS" }.to_string();

        let model = SpanModel {
            id: id.into_u64().to_string(),
            trace_id: span_context.trace_id,
            parent_id: span.parent().map(|p| p.id().into_u64().to_string()),
            name: metadata.name().to_string(),
            target: metadata.target().to_string(),
            level: metadata.level().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            duration_ms: start_time_instant.elapsed().as_secs_f64() * 1000.0,
            attributes: span_context.attributes,
            events: span_context.events,
            status,
        };

        send_log(StructuredLog::Span(model));
    }
}
