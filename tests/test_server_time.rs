use kline_server::klcommon::BinanceApi;
use log::{info, LevelFilter};
use env_logger::Builder;
use std::error::Error;
use std::io::Write;

// 初始化日志
fn init_logger() {
    Builder::new()
        .filter_level(LevelFilter::Info)
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.target(),
                record.args()
            )
        })
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
