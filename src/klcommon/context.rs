//! src/klcommon/context.rs
//! 追踪上下文的抽象层

use tracing::Span;
use std::sync::OnceLock;

/// 全局配置：是否启用完全追踪
static ENABLE_FULL_TRACING: OnceLock<bool> = OnceLock::new();

/// 初始化追踪配置
pub fn init_tracing_config(enable_full_tracing: bool) {
    ENABLE_FULL_TRACING.set(enable_full_tracing).ok();
}

/// 检查是否启用完全追踪
pub fn is_full_tracing_enabled() -> bool {
    *ENABLE_FULL_TRACING.get().unwrap_or(&true)
}

/// 定义一个可被克隆的、可跨线程传递的上下文对象 Trait
pub trait TraceContext: Clone + Send + Sync + 'static {
    /// 在当前上下文中创建一个新的上下文实例
    fn new() -> Self where Self: Sized;

    /// 使用此上下文来包裹一个 Future
    async fn instrument<F: std::future::Future>(&self, future: F) -> F::Output;
}

//--- 运行时配置的实现 ---//

#[derive(Clone, Debug)]
pub struct AppTraceContext {
    span: Option<Span>,
}

impl TraceContext for AppTraceContext {
    fn new() -> Self {
        if is_full_tracing_enabled() {
            Self { span: Some(Span::current()) }
        } else {
            Self { span: None }
        }
    }

    async fn instrument<F: std::future::Future>(&self, future: F) -> F::Output {
        if let Some(ref span) = self.span {
            // 启用完全追踪时，使用tracing-futures
            use tracing_futures::Instrument;
            future.instrument(span.clone()).await
        } else {
            // 禁用追踪时，直接执行future
            future.await
        }
    }
}