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
        let mut fields = std::collections::HashMap::new();
        let mut visitor = super::ai_log::JsonVisitor(&mut fields);
        event.record(&mut visitor);

        // 只处理明确标记为 log_type = "beacon" 的事件
        if fields.get("log_type").and_then(|v| v.as_str()) != Some("beacon") {
            return;
        }

        let metadata = event.metadata();
        let message = fields.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(String::new);

        write_beacon_log(metadata, &message, &fields);
    }
}

/// 写入信标日志到文件
fn write_beacon_log(
    metadata: &tracing::Metadata,
    message: &str,
    fields: &std::collections::HashMap<String, serde_json::Value>
) {
    if let Some(writer) = &mut *BEACON_WRITER.lock().unwrap() {
        let log_entry = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": metadata.level().to_string(),
            "target": metadata.target(),
            "message": message,
            "fields": fields,
        });

        if let Ok(json) = serde_json::to_string(&log_entry) {
            if let Err(e) = writeln!(writer, "{}", json) {
                eprintln!("[Beacon Log] Failed to write log: {}", e);
            } else {
                let _ = writer.flush();
            }
        }
    }
}
