//! src/klcommon/context.rs
//! 追踪上下文的零成本抽象

use tracing::Span;
use std::sync::OnceLock;
use tracing_futures::Instrument;
use std::future::Future;

/// 全局配置：是否启用完全追踪
static ENABLE_FULL_TRACING: OnceLock<bool> = OnceLock::new();

/// 初始化追踪配置
pub fn init_tracing_config(enable_full_tracing: bool) {
    ENABLE_FULL_TRACING.set(enable_full_tracing).ok();
}

/// 检查是否启用完全追踪（内部使用，默认禁用更安全）
fn is_full_tracing_enabled() -> bool {
    *ENABLE_FULL_TRACING.get().unwrap_or(&false)
}

/// 根据运行时配置，条件化地包裹一个 Future。
///
/// 这是推荐的方式，因为它比自定义 Trait 和 Struct 更直接、开销更低，
/// 并且通过 `futures::future::Either` 实现了真正的零成本抽象。
/// 当追踪被禁用时，此函数在编译后几乎没有额外开销。
pub fn instrument_if_enabled<F>(future: F, span: Span) -> impl Future<Output = F::Output>
where
    F: Future,
{
    if is_full_tracing_enabled() {
        futures::future::Either::Left(future.instrument(span))
    } else {
        futures::future::Either::Right(future)
    }
}