//! src/klcommon/log/target_log.rs
//!
//! 一个简单的、只关注模块化日志的层。
//! 它将日志事件格式化为扁平的JSON，并通过一个专用的命名管道发送。

use serde::Serialize;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crossbeam_channel::{unbounded, Receiver, Sender};
use once_cell::sync::Lazy;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

// --- 数据模型 ---
// 这个结构将直接序列化为JSON发送给weblog。它的字段应与weblog端期望的LogEntry一致。
#[derive(Debug, Serialize)]
struct SimpleLog {
    timestamp: String,
    level: String,
    target: String,
    message: String,
    fields: HashMap<String, serde_json::Value>,
    span_path: Option<String>, 
}

// --- 后台发送任务 ---
static TARGET_LOG_SENDER: Lazy<Mutex<Option<Sender<String>>>> = Lazy::new(|| Mutex::new(None));
static TARGET_LOG_WORKER_HANDLE: Lazy<Mutex<Option<JoinHandle<()>>>> = Lazy::new(|| Mutex::new(None));

/// 初始化简单日志的后台发送任务
pub fn init_target_log_sender(pipe_name: &str) {
    if TARGET_LOG_SENDER.lock().unwrap().is_some() { return; }

    let (tx, rx) = unbounded::<String>();
    *TARGET_LOG_SENDER.lock().unwrap() = Some(tx);

    let pipe_name_clone = pipe_name.to_string();
    let handle = thread::Builder::new()
        .name("target_log_worker".into())
        .spawn(move || target_log_worker_loop(rx, &pipe_name_clone))
        .expect("无法启动TargetLog工作线程");

    *TARGET_LOG_WORKER_HANDLE.lock().unwrap() = Some(handle);
    println!("[Target Log] 简单日志管道已启动，准备连接: {}", pipe_name);
}

/// 安全地关闭简单日志的后台发送任务
pub fn shutdown_target_log_sender() {
    if let Some(sender) = TARGET_LOG_SENDER.lock().unwrap().take() { drop(sender); }
    if let Some(handle) = TARGET_LOG_WORKER_HANDLE.lock().unwrap().take() {
        let _ = handle.join();
        println!("[Target Log] 简单日志管道已成功关闭。");
    }
}

// 后台工作线程主循环
fn target_log_worker_loop(rx: Receiver<String>, pipe_name: &str) {
    let pipe_path = if pipe_name.starts_with(r"\\.\pipe\") {
        pipe_name.to_string()
    } else {
        format!(r"\\.\pipe\{}", pipe_name)
    };

    loop {
        if let Ok(file) = OpenOptions::new().write(true).open(&pipe_path) {
            println!("[Target Log] 已连接到管道: {}", &pipe_path);
            let mut writer = BufWriter::new(file);
            loop {
                match rx.recv() {
                    Ok(log_json) => {
                        if writeln!(writer, "{}", log_json).is_err() || writer.flush().is_err() {
                            eprintln!("[Target Log] 写入或刷新管道失败，准备重连...");
                            break;
                        }
                    }
                    Err(_) => {
                        // 通道已关闭，退出循环
                        println!("[Target Log] 发送端已关闭，工作线程优雅退出。");
                        return;
                    }
                }
            }
        } else {
            thread::sleep(Duration::from_secs(1));
        }
    }
}

// --- TargetLogLayer 实现 ---
pub struct TargetLogLayer;

impl TargetLogLayer {
    pub fn new() -> Self { Self }
}

impl<S> Layer<S> for TargetLogLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        use crate::klcommon::log::JsonVisitor;

        let mut attributes = HashMap::new();
        let mut visitor = JsonVisitor(&mut attributes);
        event.record(&mut visitor);

        // --- 新增逻辑：优先从字段中提取 target ---
        let target = attributes.remove("target")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| event.metadata().target().to_string());
        // --- 结束新增逻辑 ---

        let message = attributes.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| event.metadata().name().to_string());

        let span_path = ctx.lookup_current().map(|span| {
            let mut path = vec![];
            let mut current = Some(span);
            while let Some(s) = current {
                path.push(s.name());
                current = s.parent();
            }
            path.reverse();
            path.join(" > ")
        });

        let log = SimpleLog {
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            level: event.metadata().level().to_string(),
            target, // <--- 使用我们新逻辑处理过的 target
            message,
            fields: attributes,
            span_path,
        };

        if let Ok(json) = serde_json::to_string(&log) {
            if let Some(sender) = &*TARGET_LOG_SENDER.lock().unwrap() {
                let _ = sender.try_send(json);
            }
        }
    }
}