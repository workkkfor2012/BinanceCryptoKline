//! src/klcommon/log/ai_problem_summary.rs
//!
//! AI问题摘要模块 - 为AI提供问题导航地图
//!
//! 包含用于业务逻辑软断言的`soft_assert!`宏，
//! 以及用于捕获这些断言的`ProblemSummaryLayer`。
//! 捕获WARN/ERROR级别事件以及特定log_type的INFO级别事件，生成多维度问题摘要文件

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};
use once_cell::sync::Lazy;

// ==============================================================
// == 1. 软断言宏 (The Generator)
// ==============================================================

/// 业务逻辑软断言。
///
/// 如果条件为false, 会记录一个WARN级别的事件，这个事件将被
/// `ProblemSummaryLayer`捕获并写入到问题摘要日志中。
/// 现在自动设置log_type="assertion"用于分类。
///
/// # Examples
///
/// ```
/// // Basic usage with message
/// soft_assert!(symbol_count > 0, message = "Symbol count should be positive");
///
/// // Usage with context attributes
/// soft_assert!(
///     symbol_count >= 400,
///     message = "获取到的交易对数量远低于预期",
///     expected_min = 400,
///     actual_count = symbol_count,
/// );
/// ```
#[macro_export]
macro_rules! soft_assert {
    // 带有message和其他属性的版本 - 支持字符串字面量和表达式
    ($condition:expr, message = $msg:expr, $($key:ident = $value:expr),+ $(,)?) => {
        if !$condition {
            tracing::warn!(
                log_type = "assertion",
                condition = stringify!($condition),
                message = $msg,
                $($key = $value),+
            );
        }
    };
    // 只有message的版本
    ($condition:expr, message = $msg:expr) => {
        if !$condition {
            tracing::warn!(
                log_type = "assertion",
                condition = stringify!($condition),
                message = $msg,
            );
        }
    };
}

// ==============================================================
// == 2. 数据模型与写入器 (The Data & The Writer)
// ==============================================================

/// 问题摘要条目 - AI分析的入口点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProblemSummary {
    pub timestamp: String,
    pub level: String,
    pub trace_id: String,
    pub span_id: String,
    pub target: String,
    pub message: String,
    pub attributes: HashMap<String, serde_json::Value>,
    pub context: Option<String>, // 可选的上下文信息
}

// 全局问题摘要文件写入器
static PROBLEM_WRITER: Lazy<Mutex<Option<BufWriter<std::fs::File>>>> = 
    Lazy::new(|| Mutex::new(None));

/// 初始化问题摘要日志文件
pub fn init_problem_summary_log<P: AsRef<Path>>(log_path: P) -> std::io::Result<()> {
    // 确保日志目录存在
    if let Some(parent) = log_path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    
    *PROBLEM_WRITER.lock().unwrap() = Some(BufWriter::new(file));
    println!("[Problem Summary] Initialized problem summary log");
    Ok(())
}

/// 写入问题摘要到文件
fn write_problem_summary(summary: ProblemSummary) {
    if let Some(writer) = &mut *PROBLEM_WRITER.lock().unwrap() {
        if let Ok(json) = serde_json::to_string(&summary) {
            if let Err(e) = writeln!(writer, "{}", json) {
                eprintln!("[Problem Summary] Failed to write summary: {}", e);
            } else {
                let _ = writer.flush(); // 确保立即写入
            }
        }
    }
}

/// AI问题摘要层 - 只捕获问题和警告
pub struct ProblemSummaryLayer;

impl<S> Layer<S> for ProblemSummaryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let level = metadata.level();

        // 收集事件的属性以检查log_type
        let mut attributes = HashMap::new();
        let mut visitor = crate::klcommon::log::ai_log::JsonVisitor(&mut attributes);
        event.record(&mut visitor);

        // ✨ [新的过滤逻辑] ✨
        let is_problem = *level <= tracing::Level::WARN;

        let log_type = attributes.get("log_type").and_then(|v| v.as_str());
        let is_summary_event = match log_type {
            Some("checkpoint") | Some("snapshot") | Some("assertion") => true,
            _ => false,
        };

        // 如果既不是问题，也不是我们定义的摘要事件，就跳过
        if !is_problem && !is_summary_event {
            return;
        }

        // 获取当前Span的信息
        let (trace_id, span_id) = if let Some(span) = ctx.lookup_current() {
            // 尝试从Span的extensions中获取trace_id
            let trace_id = span.extensions()
                .get::<crate::klcommon::log::ai_log::SpanContext>()
                .map(|ctx| ctx.trace_id.clone())
                .unwrap_or_else(|| {
                    // 如果没有找到，使用span的ID作为trace_id
                    span.id().into_u64().to_string()
                });

            (trace_id, span.id().into_u64().to_string())
        } else {
            // 如果没有当前Span，生成一个临时ID
            let temp_id = chrono::Utc::now().timestamp_millis().to_string();
            (temp_id.clone(), temp_id)
        };

        // attributes已经在上面收集过了，这里不需要重复收集

        // 提取消息
        let message = attributes.remove("message")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| event.metadata().name().to_string());

        // 构建问题摘要
        let summary = ProblemSummary {
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            level: level.to_string(),
            trace_id,
            span_id,
            target: event.metadata().target().to_string(),
            message,
            attributes,
            context: None, // 可以在未来扩展以包含更多上下文
        };

        write_problem_summary(summary);
    }
}

/// 便利函数：创建默认的问题摘要层
pub fn create_problem_summary_layer() -> ProblemSummaryLayer {
    ProblemSummaryLayer
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tracing::{error, warn, info};

    #[test]
    fn test_problem_summary_initialization() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test_problems.log");
        
        assert!(init_problem_summary_log(&log_path).is_ok());
        assert!(log_path.exists());
    }

    #[tokio::test]
    async fn test_problem_summary_layer() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("test_problems.log");
        
        // 初始化问题摘要日志
        init_problem_summary_log(&log_path).unwrap();
        
        // 设置tracing订阅者
        let subscriber = tracing_subscriber::registry()
            .with(create_problem_summary_layer());
        
        tracing::subscriber::with_default(subscriber, || {
            info!("This should not be captured");
            warn!("This is a warning");
            error!("This is an error");
        });

        // 读取日志文件并验证内容
        let content = std::fs::read_to_string(&log_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        
        // 应该只有2行（warn和error）
        assert_eq!(lines.len(), 2);
        
        // 验证第一行是warning
        let warning: ProblemSummary = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(warning.level, "WARN");
        assert_eq!(warning.message, "This is a warning");
        
        // 验证第二行是error
        let error: ProblemSummary = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(error.level, "ERROR");
        assert_eq!(error.message, "This is an error");
    }
}
