//! 测试MPSC Channel重构后的日志系统
//! 
//! 这个测试验证重构后的NamedPipeLogManager、ModuleLayer和TraceVisualizationLayer
//! 是否能正常工作，特别是验证：
//! 1. NamedPipeLogManager的MPSC Channel架构
//! 2. ModuleLayer的简化实现
//! 3. TraceVisualizationLayer的简化实现
//! 4. 所有日志都通过同一个高性能通道传输

use std::sync::Arc;
use std::time::Duration;
use tokio;
use tracing::{info, warn, error, instrument};
use tracing_subscriber::{Registry, layer::SubscriberExt};

// 导入重构后的日志组件
use kline_server::klcommon::log::{
    NamedPipeLogManager,
    ModuleLayer, 
    TraceVisualizationLayer
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 开始测试MPSC Channel重构后的日志系统");

    // 1. 创建共享的NamedPipeLogManager
    // 注意：这会自动启动后台任务
    let pipe_name = r"\\.\pipe\test_mpsc_refactor".to_string();
    let log_manager = Arc::new(NamedPipeLogManager::new(pipe_name));
    println!("✅ NamedPipeLogManager创建成功，后台任务已自动启动");

    // 2. 创建ModuleLayer和TraceVisualizationLayer，共享同一个管理器
    let module_layer = ModuleLayer::new(log_manager.clone());
    let trace_layer = TraceVisualizationLayer::new(log_manager.clone());
    println!("✅ ModuleLayer和TraceVisualizationLayer创建成功");

    // 3. 初始化tracing订阅器
    let subscriber = Registry::default()
        .with(module_layer)
        .with(trace_layer)
        .with(tracing_subscriber::EnvFilter::new("debug"));

    tracing::subscriber::set_global_default(subscriber)?;
    println!("✅ Tracing订阅器初始化成功");

    // 4. 等待一小段时间确保后台任务启动
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 5. 测试基本日志功能
    println!("\n📝 测试基本日志功能...");
    info!("这是一个INFO级别的测试日志");
    warn!("这是一个WARN级别的测试日志");
    error!("这是一个ERROR级别的测试日志");

    // 6. 测试带字段的日志
    println!("\n📊 测试带字段的日志...");
    info!(
        test_id = 1,
        test_type = "performance",
        duration_ms = 123.45,
        "性能测试日志"
    );

    // 7. 测试函数调用链追踪
    println!("\n🔗 测试函数调用链追踪...");
    test_function_tracing().await;

    // 8. 测试高频日志发送（验证MPSC Channel性能）
    println!("\n⚡ 测试高频日志发送...");
    for i in 0..100 {
        info!(batch_id = i, "高频测试日志 #{}", i);
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    // 9. 等待一段时间确保所有日志都被处理
    println!("\n⏳ 等待日志处理完成...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\n🎉 测试完成！请检查weblog前端是否收到了所有日志。");
    println!("预期结果：");
    println!("- 所有日志都应该通过单一的MPSC Channel传输");
    println!("- ModuleLayer生成的日志应该有log_type: 'module'");
    println!("- TraceVisualizationLayer生成的日志应该有log_type: 'trace'");
    println!("- 函数调用链应该正确显示span_start和span_end事件");
    println!("- 高频日志应该没有丢失，证明MPSC Channel的高性能");

    Ok(())
}

/// 测试函数调用链追踪
#[instrument]
async fn test_function_tracing() {
    info!("进入test_function_tracing函数");
    
    // 调用子函数
    sub_function_1().await;
    sub_function_2().await;
    
    info!("退出test_function_tracing函数");
}

#[instrument]
async fn sub_function_1() {
    info!("执行sub_function_1");
    tokio::time::sleep(Duration::from_millis(10)).await;
    info!("sub_function_1执行完成");
}

#[instrument]
async fn sub_function_2() {
    info!("执行sub_function_2");
    
    // 嵌套调用
    nested_function().await;
    
    info!("sub_function_2执行完成");
}

#[instrument]
async fn nested_function() {
    info!("执行nested_function");
    tokio::time::sleep(Duration::from_millis(5)).await;
    warn!("nested_function中的警告日志");
    info!("nested_function执行完成");
}
