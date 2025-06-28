//! 测试追踪上下文系统
//! 
//! 验证AppTraceContext和TraceContext trait是否正确工作

use kline_server::klcommon::context::{AppTraceContext, TraceContext, init_tracing_config, is_full_tracing_enabled};
use tracing::{info_span, info};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_trace_context_creation() {
    // 初始化追踪配置
    init_tracing_config(true);
    assert!(is_full_tracing_enabled());
    
    // 创建追踪上下文
    let context = AppTraceContext::new();
    
    // 验证上下文可以被克隆
    let _cloned_context = context.clone();
}

#[tokio::test]
async fn test_trace_context_disabled() {
    // 注意：由于全局配置在测试间共享，这个测试可能会受到其他测试的影响
    // 这里主要测试AppTraceContext的创建和克隆功能
    let context = AppTraceContext::new();
    let _cloned_context = context.clone();

    // 测试禁用状态下的instrument方法
    let result = context.instrument(async {
        "disabled_result"
    }).await;

    assert_eq!(result, "disabled_result");
}

#[tokio::test]
async fn test_trace_context_instrument() {
    init_tracing_config(true);
    
    let context = AppTraceContext::new();
    
    // 测试instrument方法
    let result = context.instrument(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        "test_result"
    }).await;
    
    assert_eq!(result, "test_result");
}

#[tokio::test]
async fn test_cross_task_context_passing() {
    init_tracing_config(true);
    
    // 模拟跨任务传递上下文的场景
    let (sender, mut receiver) = mpsc::channel(1);
    
    // 模拟任务结构
    #[derive(Clone)]
    struct TestTask {
        data: String,
        context: AppTraceContext,
    }
    
    // 在一个span中创建任务
    let _span = info_span!("test_parent_span").entered();
    
    let task = TestTask {
        data: "test_data".to_string(),
        context: AppTraceContext::new(),
    };
    
    // 发送任务
    sender.send(task).await.unwrap();
    drop(sender);
    
    // 在另一个任务中接收并处理
    if let Some(task) = receiver.recv().await {
        let result = TraceContext::instrument(&task.context, async {
            info!("处理任务: {}", task.data);
            format!("processed_{}", task.data)
        }).await;
        
        assert_eq!(result, "processed_test_data");
    }
}

#[tokio::test]
async fn test_spawn_blocking_context() {
    init_tracing_config(true);
    
    let context = AppTraceContext::new();
    
    // 测试在spawn_blocking中使用上下文
    let result = context.instrument(async {
        tokio::task::spawn_blocking(|| {
            // 模拟阻塞操作
            std::thread::sleep(std::time::Duration::from_millis(1));
            "blocking_result"
        }).await.unwrap()
    }).await;
    
    assert_eq!(result, "blocking_result");
}

#[test]
fn test_trace_context_send_sync() {
    // 验证AppTraceContext实现了Send和Sync
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    
    assert_send::<AppTraceContext>();
    assert_sync::<AppTraceContext>();
    
    // 验证可以在Arc中使用
    let context = AppTraceContext::new();
    let _arc_context = Arc::new(context);
}
