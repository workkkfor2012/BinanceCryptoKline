//! 模块日志层
//!
//! 专门用于处理模块级别的人类可读日志，生成扁平化的日志流。
//! 它只关心那些 target 以 "kline_server" 开头，并且不在任何子 Span 内的事件。

use std::sync::Arc;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

// 重新导入需要的类型
use super::observability::NamedPipeLogManager;

/// 模块日志层 - 只处理顶层模块日志
pub struct ModuleLayer {
    manager: Arc<NamedPipeLogManager>,
}

impl ModuleLayer {
    /// 创建一个新的模块日志层
    /// 它需要一个已经创建好的、可共享的 NamedPipeLogManager 实例
    pub fn new(manager: Arc<NamedPipeLogManager>) -> Self {
        Self { manager }
    }
}

impl<S> Layer<S> for ModuleLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // ✨ [关键修复]: 只处理明确标记为模块级别的日志
        // 通过检查事件中是否存在 log.type = "module" 字段来精确过滤
        let mut fields = serde_json::Map::new();
        let mut visitor = super::trace_visualization::JsonVisitor::new(&mut fields);
        event.record(&mut visitor);

        // 只处理明确标记为 log_type = "module" 的事件
        if fields.get("log_type").and_then(|v| v.as_str()) != Some("module") {
            return;
        }

        let metadata = event.metadata();
        let message = fields.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(String::new);

        // ✨ [关键修复 2]: 简化日志对象，不再需要复杂的上下文追溯
        let module_log_obj = serde_json::json!({
            "log_type": "module",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": metadata.level().to_string(),
            "target": metadata.target(),
            "message": message,
            "fields": fields,
        });

        if let Ok(log_line) = serde_json::to_string(&module_log_obj) {
            self.manager.send_log(log_line);
        }
    }
}


