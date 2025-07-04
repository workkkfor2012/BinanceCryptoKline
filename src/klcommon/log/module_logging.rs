//! 模块日志层
//!
//! 专门用于处理模块级别的人类可读日志，保存到本地文件。
//! 只处理明确标记为 log_type = "module" 的事件。

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};
use once_cell::sync::Lazy;

// 全局模块日志文件写入器
static MODULE_WRITER: Lazy<Mutex<Option<BufWriter<std::fs::File>>>> =
    Lazy::new(|| Mutex::new(None));

/// 初始化模块日志文件
pub fn init_module_log<P: AsRef<Path>>(log_path: P) -> std::io::Result<()> {
    // 确保日志目录存在
    if let Some(parent) = log_path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;

    *MODULE_WRITER.lock().unwrap() = Some(BufWriter::new(file));
    println!("[Module Log] Initialized module log");
    Ok(())
}

/// 模块日志层 - 只处理模块级别的日志
pub struct ModuleLayer;

impl ModuleLayer {
    /// 创建一个新的模块日志层
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for ModuleLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // ✨ [关键修复]: 只处理明确标记为模块级别的日志
        // 通过检查事件中是否存在 log.type = "module" 字段来精确过滤
        let mut fields = std::collections::HashMap::new();
        let mut visitor = super::ai_log::JsonVisitor(&mut fields);
        event.record(&mut visitor);

        // 只处理明确标记为 log_type = "module" 的事件
        if fields.get("log_type").and_then(|v| v.as_str()) != Some("module") {
            return;
        }

        let metadata = event.metadata();
        let message = fields.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(String::new);

        // 写入模块日志到本地文件
        write_module_log(metadata, &message, &fields);
    }
}

/// 写入模块日志到文件
fn write_module_log(
    metadata: &tracing::Metadata,
    message: &str,
    fields: &std::collections::HashMap<String, serde_json::Value>
) {
    if let Some(writer) = &mut *MODULE_WRITER.lock().unwrap() {
        let log_entry = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": metadata.level().to_string(),
            "target": metadata.target(),
            "message": message,
            "fields": fields,
        });

        if let Ok(json) = serde_json::to_string(&log_entry) {
            if let Err(e) = writeln!(writer, "{}", json) {
                eprintln!("[Module Log] Failed to write log: {}", e);
            } else {
                let _ = writer.flush(); // 确保立即写入
            }
        }
    }
}


