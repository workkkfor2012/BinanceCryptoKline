//! 测试轨迹提炼器系统
//!
//! 这个测试验证TraceDistillerLayer是否能正确构建内存中的调用树，
//! 并生成对大模型友好的文本摘要。

use std::time::Duration;
use tracing::{info, instrument, Instrument};
use kline_server::klcommon::log::trace_distiller::{TraceDistillerStore, TraceDistillerLayer, distill_all_completed_traces_to_text};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 开始测试轨迹提炼器系统");

    // 初始化tracing系统
    let distiller_store = TraceDistillerStore::default();
    let distiller_layer = TraceDistillerLayer::new(distiller_store.clone());

    Registry::default()
        .with(tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_level(true))
        .with(distiller_layer)
        .with(tracing_subscriber::EnvFilter::new("debug"))
        .init();

    println!("📊 开始执行测试函数调用链...");

    // 在根span中执行测试
    let root_span = tracing::info_span!("test_root_function");
    test_main_function().instrument(root_span).await;

    // 等待一下确保所有span都完成
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 生成并打印报告
    let report = distill_all_completed_traces_to_text(&distiller_store);
    println!("\n🔬 生成的Trace分析报告:");
    println!("{}", report);

    println!("✅ 测试完成");
    Ok(())
}

#[instrument(name = "test_main_function")]
async fn test_main_function() {
    info!("开始主函数测试");

    // 调用子函数
    test_database_operation().await;
    test_api_call().await;
    test_data_processing().await;

    info!("主函数测试完成");
}

#[instrument(name = "test_database_operation")]
async fn test_database_operation() {
    info!("执行数据库操作");

    // 模拟数据库查询
    test_query_symbols().await;
    test_insert_klines().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    info!("数据库操作完成");
}

#[instrument(name = "test_query_symbols")]
async fn test_query_symbols() {
    info!("查询交易对列表");
    tokio::time::sleep(Duration::from_millis(20)).await;
    info!("查询到 437 个交易对");
}

#[instrument(name = "test_insert_klines")]
async fn test_insert_klines() {
    info!("插入K线数据");
    tokio::time::sleep(Duration::from_millis(30)).await;
    info!("插入了 100 条K线记录");
}

#[instrument(name = "test_api_call")]
async fn test_api_call() {
    info!("调用币安API");

    // 模拟API调用
    test_get_exchange_info().await;
    test_get_klines().await;

    tokio::time::sleep(Duration::from_millis(40)).await;
    info!("API调用完成");
}

#[instrument(name = "test_get_exchange_info")]
async fn test_get_exchange_info() {
    info!("获取交易所信息");
    tokio::time::sleep(Duration::from_millis(60)).await;
    info!("获取交易所信息成功");
}

#[instrument(name = "test_get_klines")]
async fn test_get_klines() {
    info!("获取K线数据");
    tokio::time::sleep(Duration::from_millis(80)).await;
    info!("获取到 1000 条K线数据");
}

#[instrument(name = "test_data_processing")]
async fn test_data_processing() {
    info!("开始数据处理");

    // 模拟数据处理
    test_validate_data().await;
    test_transform_data().await;

    tokio::time::sleep(Duration::from_millis(30)).await;
    info!("数据处理完成");
}

#[instrument(name = "test_validate_data")]
async fn test_validate_data() {
    info!("验证数据格式");
    tokio::time::sleep(Duration::from_millis(15)).await;
    info!("数据验证通过");
}

#[instrument(name = "test_transform_data")]
async fn test_transform_data() {
    info!("转换数据格式");
    tokio::time::sleep(Duration::from_millis(20)).await;
    info!("数据转换完成");
}


