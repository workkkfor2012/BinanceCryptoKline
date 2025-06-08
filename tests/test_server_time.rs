use kline_server::klcommon::BinanceApi;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use std::error::Error;

// 初始化tracing日志
fn init_logger() {
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

    info!("开始测试获取币安服务器时间功能");

    // 创建API客户端
    let api = BinanceApi::new();

    // 获取服务器时间
    info!("正在获取币安服务器时间...");
    let server_time = api.get_server_time().await?;

    // 打印服务器时间
    info!("币安服务器时间: {}", server_time.server_time);

    // 转换为本地时间并打印
    let datetime = chrono::DateTime::from_timestamp_millis(server_time.server_time)
        .expect("无效的时间戳");
    info!("格式化时间: {}", datetime.format("%Y-%m-%d %H:%M:%S%.3f"));

    info!("测试完成");
    Ok(())
}
