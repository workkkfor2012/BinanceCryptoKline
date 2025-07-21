//! src/klcommon/context.rs
//! 追踪上下文的零成本抽象

use tracing::Span;
use std::sync::OnceLock;
use tracing_futures::Instrument;
use std::future::Future;
use tokio::task::JoinHandle;

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

/// 派生带上下文的后台任务
///
/// 这个函数封装了正确的模式来创建一个新的后台任务，
/// 并且让这个新任务能完全继承当前任务的日志上下文（Span和trace_id）。
///
/// 使用这个函数而不是手动调用 `future.instrument(Span::current())` 后再 `tokio::spawn`。
pub fn spawn_instrumented<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let current_span = Span::current();
    tokio::spawn(future.instrument(current_span))
}

/// 在指定运行时上派生带上下文的任务
///
/// 这个函数封装了正确的模式来在指定的运行时上创建一个新的任务，
/// 并且让这个新任务能完全继承当前任务的日志上下文。
pub fn spawn_instrumented_on<F>(future: F, runtime: &tokio::runtime::Runtime) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let current_span = Span::current();
    runtime.spawn(future.instrument(current_span))
}

/// 派生带上下文的阻塞任务
///
/// 这个函数封装了正确的模式来创建一个新的阻塞任务，
/// 并且让这个新任务能完全继承当前任务的日志上下文。
///
/// 在阻塞任务中，需要使用 `parent_span.in_scope()` 来恢复上下文，
/// 因为上下文是线程局部的。
pub fn spawn_blocking_instrumented<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let parent_span = Span::current();
    tokio::task::spawn_blocking(move || {
        parent_span.in_scope(|| f())
    })
}