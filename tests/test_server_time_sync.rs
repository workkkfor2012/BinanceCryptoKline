use kline_server::klcommon::{Database, ServerTimeSyncManager};
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use std::sync::Arc;
use std::error::Error;

fn init_logger() {
    // 设置RUST_BACKTRACE为1，以便更好地报告错误
    std::env::set_var("RUST_BACKTRACE", "1");

    // 初始化tracing日志
    Registry::default()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                .json() // 使用JSON格式符合WebLog规范
        )
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 初始化日志
    init_logger();

    info!("开始测试服务器时间同步功能");

    // 创建临时数据库
    let temp_db_path = std::path::PathBuf::from("./temp_test.db");
    let db = Arc::new(Database::new(&temp_db_path)?);

    // 测试结束后删除临时数据库
    let _cleanup = scopeguard::guard((), |_| {
        if temp_db_path.exists() {
            let _ = std::fs::remove_file(&temp_db_path);
        }
    });

    // 创建服务器时间同步管理器
    let intervals = vec!["1m".to_string()];
    let symbols = vec!["BTCUSDT".to_string()];

    let time_sync_manager = ServerTimeSyncManager::new(db, intervals, symbols);

    // 获取服务器时间并计算时间差
    info!("正在与币安服务器进行时间同步...");
    match time_sync_manager.start().await {
        Ok(_) => info!("服务器时间同步成功"),
        Err(e) => error!("服务器时间同步失败: {}", e),
    }

    info!("测试完成");
    Ok(())
}
