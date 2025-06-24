//! 模块日志层
//!
//! 专门用于处理模块级别的人类可读日志，生成扁平化的日志流

use std::sync::Arc;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

// 重新导入需要的类型
use super::observability::NamedPipeLogManager;

/// 模块日志层 - 将所有不在任何 Span 内部的日志事件作为模块级日志进行转发
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
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // 【关键修改】移除过滤条件，现在处理所有日志事件
        // 这样 ModuleLayer 就能产生完整的、扁平化的人类可读日志流

        let metadata = event.metadata();

        // 避免处理由 TraceVisualizationLayer 自身产生的日志，防止循环
        if metadata.target() == "TraceVisualization" {
            return;
        }

        let mut fields = serde_json::Map::new();
        // 使用 trace_visualization 中的 JsonVisitor
        let mut visitor = super::trace_visualization::JsonVisitor::new(&mut fields);
        event.record(&mut visitor);

        let message = fields.remove("message")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(String::new);

        // 【增强】如果事件在 Span 内，添加 span 上下文信息，让模块日志更丰富
        let span_info = if let Some(span) = ctx.lookup_current() {
            // 构建 trace_id
            fn find_root_id<S: for<'a> tracing_subscriber::registry::LookupSpan<'a>>(span: &tracing_subscriber::registry::SpanRef<S>) -> tracing::Id {
                span.parent().map_or_else(|| span.id(), |p| find_root_id(&p))
            }
            let trace_id = format!("trace_{}", find_root_id(&span).into_u64());

            serde_json::json!({
                "span_name": span.name(),
                "span_id": format!("span_{}", span.id().into_u64()),
                "trace_id": trace_id
            })
        } else {
            serde_json::Value::Null
        };

        let module_log_obj = serde_json::json!({
            "log_type": "module", // 标识这是给人类看的模块日志
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": metadata.level().to_string(),
            "target": metadata.target(),
            "message": message,
            "fields": fields,
            "span": span_info, // 附加 span 上下文信息，便于调试
        });

        if let Ok(log_line) = serde_json::to_string(&module_log_obj) {
            // **👇 核心修改 👇**
            // 不再需要 tokio::spawn，也不再是 async/await 调用。
            // 这是一个快速的、非阻塞的发送操作。
            self.manager.send_log(log_line);
        }
    }
}

/// 命名管道日志转发层 - 向后兼容的包装器
pub struct NamedPipeLogForwardingLayer {
    inner: ModuleLayer,
}

impl NamedPipeLogForwardingLayer {
    /// 创建命名管道日志转发层（向后兼容）
    pub fn new(pipe_name: String) -> Self {
        // manager 在创建时就已经启动了后台任务
        let manager = Arc::new(NamedPipeLogManager::new(pipe_name));
        Self {
            inner: ModuleLayer::new(manager)
        }
    }
}

impl<S> Layer<S> for NamedPipeLogForwardingLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        self.inner.on_event(event, ctx)
    }
}
