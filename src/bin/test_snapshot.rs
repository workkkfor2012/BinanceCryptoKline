//! 测试快照功能
//! 
//! 这个测试验证TraceDistillerLayer是否能正确生成快照文件

use std::time::Duration;
use tracing::{info, warn, error, instrument};
use tracing_subscriber::{Registry, layer::SubscriberExt};

// 导入轨迹提炼器组件
use kline_server::klcommon::log::{
    TraceDistillerStore, 
    TraceDistillerLayer,
    distill_all_completed_traces_to_text
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 开始测试快照功能");

    // 1. 创建TraceDistillerStore和Layer
    let distiller_store = TraceDistillerStore::default();
    let distiller_layer = TraceDistillerLayer::new(distiller_store.clone());
    println!("✅ TraceDistillerStore和TraceDistillerLayer创建成功");

    // 2. 初始化tracing订阅器（只使用distiller layer）
    let subscriber = Registry::default()
        .with(distiller_layer)
        .with(tracing_subscriber::EnvFilter::new("debug"));

    tracing::subscriber::set_global_default(subscriber)?;
    println!("✅ Tracing订阅器初始化成功");

    // 3. 执行一些带有函数调用链的操作
    println!("\n🔗 执行测试函数调用链...");
    test_complex_operation().await;

    // 4. 等待一段时间确保所有span都已关闭
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 5. 生成快照
    println!("\n📊 生成快照文件...");
    
    // 确保目录存在
    tokio::fs::create_dir_all("logs/debug_snapshots").await?;
    
    let report_text = distill_all_completed_traces_to_text(&distiller_store);
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let filename = format!("logs/debug_snapshots/test_snapshot_{}.log", timestamp);
    
    tokio::fs::write(&filename, report_text.as_bytes()).await?;
    println!("✅ 快照文件已生成: {}", filename);

    // 6. 显示快照内容
    println!("\n📄 快照内容:");
    println!("{}", report_text);

    println!("\n🎉 测试完成！");

    Ok(())
}

/// 测试复杂的函数调用链
#[instrument]
async fn test_complex_operation() {
    info!("开始复杂操作");
    
    // 并行执行多个子任务
    let task1 = async_task_1();
    let task2 = async_task_2();
    let task3 = async_task_3();
    
    // 等待所有任务完成
    tokio::join!(task1, task2, task3);
    
    // 执行一个可能出错的操作
    error_prone_operation().await;
    
    info!("复杂操作完成");
}

#[instrument]
async fn async_task_1() {
    info!("执行异步任务1");
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // 调用子函数
    sub_operation_a().await;
    
    info!("异步任务1完成");
}

#[instrument]
async fn async_task_2() {
    info!("执行异步任务2");
    tokio::time::sleep(Duration::from_millis(30)).await;
    
    // 调用子函数
    sub_operation_b().await;
    
    info!("异步任务2完成");
}

#[instrument]
async fn async_task_3() {
    info!("执行异步任务3");
    tokio::time::sleep(Duration::from_millis(80)).await; // 这个最耗时，应该被标记为关键路径
    
    // 嵌套调用
    nested_operation().await;
    
    info!("异步任务3完成");
}

#[instrument]
async fn sub_operation_a() {
    info!("执行子操作A");
    tokio::time::sleep(Duration::from_millis(10)).await;
    info!("子操作A完成");
}

#[instrument]
async fn sub_operation_b() {
    info!("执行子操作B");
    tokio::time::sleep(Duration::from_millis(15)).await;
    info!("子操作B完成");
}

#[instrument]
async fn nested_operation() {
    info!("执行嵌套操作");
    tokio::time::sleep(Duration::from_millis(20)).await;
    
    // 更深层的嵌套
    deep_nested_operation().await;
    
    info!("嵌套操作完成");
}

#[instrument]
async fn deep_nested_operation() {
    info!("执行深层嵌套操作");
    tokio::time::sleep(Duration::from_millis(5)).await;
    info!("深层嵌套操作完成");
}

#[instrument]
async fn error_prone_operation() {
    info!("执行可能出错的操作");
    
    // 模拟一个警告
    warn!("这是一个警告信息：某些条件不理想");
    
    // 模拟一个错误
    error!("这是一个错误信息：操作失败");
    
    tokio::time::sleep(Duration::from_millis(5)).await;
    info!("错误操作处理完成");
}
