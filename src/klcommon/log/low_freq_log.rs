//! 低频日志层
//!
//! 专门用于处理程序生命周期中的关键检查点，保存到本地文件。
//! 只处理明确标记为 log_type = "low_freq" 的事件。

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};
use once_cell::sync::Lazy;

// 全局低频日志文件写入器
static LOW_FREQ_WRITER: Lazy<Mutex<Option<BufWriter<std::fs::File>>>> =
    Lazy::new(|| Mutex::new(None));

/// 初始化低频日志文件
pub fn init_low_freq_log<P: AsRef<Path>>(log_path: P) -> std::io::Result<()> {
    // 确保日志目录存在
    if let Some(parent) = log_path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;

    *LOW_FREQ_WRITER.lock().unwrap() = Some(BufWriter::new(file));
    println!("[Low Freq Log] Initialized low frequency log");
    Ok(())
}

/// 低频日志层
pub struct LowFreqLogLayer;

impl LowFreqLogLayer {
    /// 创建一个新的低频日志层
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for LowFreqLogLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // ✨ [关键修复]: 只处理明确标记为低频级别的日志
        // 通过检查事件中是否存在 log.type = "low_freq" 字段来精确过滤
        // 【修改】JsonVisitor 现在直接操作 serde_json::Map
        let mut fields = serde_json::Map::new();
        let mut visitor = super::ai_log::JsonVisitor(&mut fields);
        event.record(&mut visitor);

        // 只处理明确标记为 log_type = "low_freq" 的事件
        if fields.get("log_type").and_then(|v| v.as_str()) != Some("low_freq") {
            return;
        }

        let metadata = event.metadata();
        let message = fields.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| event.metadata().name().to_string());

        // 写入低频日志到本地文件
        write_low_freq_log(metadata, &message, fields); // 【修改】传递 Map 而不是 HashMap
    }
}

/// 写入低频日志到文件
fn write_low_freq_log(
    metadata: &tracing::Metadata,
    message: &str,
    mut fields: serde_json::Map<String, serde_json::Value> // 【修改】接收 Map
) {
    if let Some(writer) = &mut *LOW_FREQ_WRITER.lock().unwrap() {
        // 【修改】将元数据直接插入到 Map 中，实现扁平化
        fields.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().to_rfc3339()));
        fields.insert("level".to_string(), serde_json::json!(metadata.level().to_string()));
        fields.insert("target".to_string(), serde_json::json!(metadata.target()));
        fields.insert("message".to_string(), serde_json::json!(message));

        let log_entry = serde_json::Value::Object(fields);

        if let Ok(json) = serde_json::to_string(&log_entry) {
            if let Err(e) = writeln!(writer, "{}", json) {
                eprintln!("[Low Freq Log] Failed to write log: {}", e);
            } else {
                let _ = writer.flush(); // 确保立即写入
            }
        }
    }
}
