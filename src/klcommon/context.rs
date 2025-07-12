//! src/klcommon/context.rs
//!
//! # 追踪上下文的统一解决方案
//!
//! 本模块提供了在复杂异步系统中无损传播 `tracing` 上下文的统一、标准化工具箱。
//! 它旨在将上下文传递的复杂性与业务逻辑完全解耦，并为两种主要的异步边界提供零成本或低成本的抽象：
//!
//! 1.  **任务派生边界 (Task Spawning)**: 当通过 `tokio::spawn` 或类似方式创建新任务时，确保子任务继承父任务的`Span`。
//! 2.  **数据通道边界 (MPSC Channels)**: 当通过通道在两个独立运行的任务间传递消息时，确保消息的接收方可以进入消息创建时的`Span`上下文。
//!
//! **核心原则**: 在实现任何涉及异步边界的上下文传递逻辑之前，**必须**优先使用本模块中提供的函数。
//! 如果现有函数不能满足需求，你的任务是**扩充本模块**，而不是在业务代码中创建一次性的临时解决方案。

use std::future::Future;
use std::sync::OnceLock;
use tokio::task::JoinHandle;
use tracing::Span;
use tracing_futures::Instrument;

// --- I. 全局追踪配置 ---

/// 全局配置：是否启用完全追踪。
/// 用于实现可配置的、在生产环境中对性能影响最小的日志系统。
static ENABLE_FULL_TRACING: OnceLock<bool> = OnceLock::new();

/// 初始化追踪配置。应在程序启动时由配置模块调用。
pub fn init_tracing_config(enable_full_tracing: bool) {
    ENABLE_FULL_TRACING.set(enable_full_tracing).ok();
}

/// 检查是否启用完全追踪（内部使用，默认禁用更安全）。
fn is_full_tracing_enabled() -> bool {
    *ENABLE_FULL_TRACING.get().unwrap_or(&false)
}

// --- II. 任务派生边界的上下文管理 ---

/// 根据运行时配置，条件化地为 Future 附加 `Span`。
///
/// 这是实现“零成本抽象”的推荐方式。当追踪被禁用时，
/// 此函数通过 `futures::future::Either` 在编译后几乎没有额外开销。
///
/// # Example
/// ```ignore
/// // 在热路径上，我们可以创建一个 trace 级别的 Span
/// let hot_path_span = tracing::trace_span!("hot_path");
/// // 只有在配置开启时，这个 Span 才会真正附加到 future 上
/// context::instrument_if_enabled(async { /* ... */ }, hot_path_span).await;
/// ```
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

/// 派生一个继承了当前上下文的后台任务。
///
/// 封装了 `tokio::spawn` 的标准模式，确保新任务能继承当前任务的 `Span`。
/// **严禁**在业务代码中手动调用 `future.instrument(Span::current())`。
pub fn spawn_instrumented<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let current_span = Span::current();
    tokio::spawn(future.instrument(current_span))
}

/// 在指定的 `tokio` 运行时上派生一个继承了当前上下文的任务。
pub fn spawn_instrumented_on<F>(future: F, runtime: &tokio::runtime::Runtime) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let current_span = Span::current();
    runtime.spawn(future.instrument(current_span))
}

/// 派生一个继承了当前上下文的阻塞任务。
///
/// **注意**: 阻塞任务在不同的线程上运行，因此必须使用 `in_scope` 来恢复上下文。
pub fn spawn_blocking_instrumented<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let parent_span = Span::current();
    tokio::task::spawn_blocking(move || parent_span.in_scope(|| f()))
}


// --- III. 数据通道边界的上下文管理 ---

/// 一个通用的包装器，用于将数据 (`payload`) 与其创建时的 `tracing::Span` 绑定。
///
/// 这使得 `Span` 上下文可以作为数据的一部分，安全地通过通道、消息队列等边界进行传递，
/// 解决了两个独立任务间（非父子派生关系）的上下文传播问题。
#[derive(Debug)]
pub struct Instrumented<T> {
    pub payload: T,
    span: Span,
}

impl<T> Instrumented<T> {
    /// 使用当前的 `Span` 包装一个 `payload`。
    ///
    /// 在将数据发送到通道之前调用此方法。
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            span: Span::current(),
        }
    }
    
    /// 获取内部的 `payload`。
    pub fn into_inner(self) -> T {
        self.payload
    }

    /// 解构为 payload 和 span。
    pub fn into_parts(self) -> (T, Span) {
        (self.payload, self.span)
    }

    /// 进入此数据所携带的 `Span` 上下文，并执行一个**同步**闭包。
    ///
    /// # Example
    /// ```ignore
    /// if let Some(instrumented_cmd) = rx.recv().await {
    ///     instrumented_cmd.in_span(|| {
    ///         // 这里的代码都在 instrumented_cmd.span 的上下文中运行
    ///         process_sync(instrumented_cmd.payload);
    ///     });
    /// }
    /// ```
    pub fn in_span<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.span.in_scope(f)
    }

    /// 为一个 Future 附加此数据所携带的 `Span` 上下文，使其可在**异步**块中被追踪。
    ///
    /// # Example
    /// ```ignore
    /// if let Some(instrumented_cmd) = rx.recv().await {
    ///     let future = async {
    ///         process_async(instrumented_cmd.payload).await;
    ///     };
    ///     // 异步执行，同时保持追踪上下文
    ///     instrumented_cmd.instrument(future).await;
    /// }
    /// ```
    pub fn instrument<Fut>(&self, future: Fut) -> impl Future<Output = Fut::Output>
    where
        Fut: Future,
    {
        future.instrument(self.span.clone())
    }
}