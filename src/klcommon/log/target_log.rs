//! src/klcommon/log/target_log.rs
//!
//! 一个简单的、只关注模块化日志的层。
//! 它将日志事件格式化为扁平的JSON，并通过一个专用的命名管道发送。

// 【修改】移除不再需要的导入
// use serde::Serialize;
// use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crossbeam_channel::{unbounded, Receiver, Sender};
use once_cell::sync::Lazy;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

// 【修改】删除或注释掉不再需要的 SimpleLog 结构体
// use serde::Serialize;
// #[derive(Debug, Serialize)]
// struct SimpleLog { ... }

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
        use serde_json::{json, Value};

        // 0. 屏蔽底层第三方库的trace级别日志
        if event.metadata().level() == &tracing::Level::TRACE && event.metadata().target() == "log" {
            return;
        }

        // 1. 将所有业务字段收集到一个独立的 'fields_map' 中。
        //    例如 `总耗时_秒`, `延迟追赶_秒` 等都会在这里。
        let mut fields_map = serde_json::Map::new();
        let mut visitor = JsonVisitor(&mut fields_map);
        event.record(&mut visitor);

        // 2. 从业务字段中提取 'message'，如果不存在则使用元数据中的名称。
        //    这使得 info!(message="...", ...) 和 info!(..., "...") 两种写法都健壮。
        //    提取后，它将成为顶层 message，不再出现在 "fields" 对象中。
        let message = fields_map.remove("message")
            .and_then(|v| if let Value::String(s) = v { Some(s) } else { Some(v.to_string()) })
            .unwrap_or_else(|| event.metadata().name().to_string());

        // 3. 构建一个全新的、结构清晰的根JSON对象，只包含元数据。
        let mut root_map = serde_json::Map::new();
        root_map.insert("timestamp".to_string(), json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)));
        root_map.insert("level".to_string(), json!(event.metadata().level().to_string()));
        root_map.insert("target".to_string(), json!(event.metadata().target()));
        root_map.insert("message".to_string(), json!(message));

        // 4. 获取并插入 span_path 到元数据中
        if let Some(span) = ctx.lookup_current() {
            let mut path = vec![];
            let mut current = Some(span);
            while let Some(s) = current {
                path.push(s.name());
                current = s.parent();
            }
            path.reverse();
            root_map.insert("span_path".to_string(), json!(path.join(" > ")));
        }

        // 5. 将剩余的所有业务字段作为一个嵌套对象放入 'fields' 键下。
        //    如果业务字段为空，则不添加 'fields' 键，保持日志整洁。
        if !fields_map.is_empty() {
            root_map.insert("fields".to_string(), Value::Object(fields_map));

            // [修复] 移除这行导致无限递归的 trace! 调用。
            // 日志层内部绝不能使用 tracing! 宏来记录自身状态。
            // trace!(target: "target_log", fields_count, "业务字段已添加到日志");
        }

        // 6. 将最终的结构化对象序列化为字符串并发送
        let final_json = Value::Object(root_map);
        if let Ok(json_string) = serde_json::to_string(&final_json) {
            if let Some(sender) = &*TARGET_LOG_SENDER.lock().unwrap() {
                // 使用 try_send 避免在日志高峰期阻塞业务线程
                let _ = sender.try_send(json_string);
            }
        }
    }
}