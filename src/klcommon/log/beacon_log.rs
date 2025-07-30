//! 信标日志层
//!
//! 专门用于处理周期性的"心跳"或状态信标日志。
//! 只处理明确标记为 log_type = "beacon" 的事件。

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};
use once_cell::sync::Lazy;

// 全局信标日志文件写入器
static BEACON_WRITER: Lazy<Mutex<Option<BufWriter<std::fs::File>>>> =
    Lazy::new(|| Mutex::new(None));

/// 初始化信标日志文件
pub fn init_beacon_log<P: AsRef<Path>>(log_path: P) -> std::io::Result<()> {
    if let Some(parent) = log_path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;

    *BEACON_WRITER.lock().unwrap() = Some(BufWriter::new(file));
    println!("[Beacon Log] Initialized beacon log");
    Ok(())
}

/// 信标日志层
pub struct BeaconLogLayer;

impl BeaconLogLayer {
    /// 创建一个新的信标日志层
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for BeaconLogLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // 【修改】JsonVisitor 现在直接操作 serde_json::Map
        let mut fields = serde_json::Map::new();
        let mut visitor = super::ai_log::JsonVisitor(&mut fields);
        event.record(&mut visitor);

        // 只处理明确标记为 log_type = "beacon" 的事件
        if fields.get("log_type").and_then(|v| v.as_str()) != Some("beacon") {
            return;
        }

        let metadata = event.metadata();
        let message = fields.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| event.metadata().name().to_string());

        write_beacon_log(metadata, &message, fields); // 【修改】传递 Map 而不是 HashMap
    }
}

/// 写入信标日志到文件
fn write_beacon_log(
    metadata: &tracing::Metadata,
    message: &str,
    mut fields: serde_json::Map<String, serde_json::Value> // 【修改】接收 Map
) {
    if let Some(writer) = &mut *BEACON_WRITER.lock().unwrap() {
        // 【修改】将元数据直接插入到 Map 中，实现扁平化
        fields.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().to_rfc3339()));
        fields.insert("level".to_string(), serde_json::json!(metadata.level().to_string()));
        fields.insert("target".to_string(), serde_json::json!(metadata.target()));
        fields.insert("message".to_string(), serde_json::json!(message));

        let log_entry = serde_json::Value::Object(fields);

        if let Ok(json) = serde_json::to_string(&log_entry) {
            if let Err(e) = writeln!(writer, "{}", json) {
                eprintln!("[Beacon Log] Failed to write log: {}", e);
            } else {
                let _ = writer.flush();
            }
        }
    }
}
