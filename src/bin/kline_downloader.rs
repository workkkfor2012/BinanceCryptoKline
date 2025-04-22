// K线下载器主程序
use kline_server::kldownload::config::Config;
use kline_server::kldownload::downloader::Downloader;
use kline_server::kldownload::error::Result;
use log::{info, error};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    init_logging(true);

    info!("启动币安U本位永续合约K线下载器");
    info!("此程序仅用于下载历史K线数据，不会启动服务器");

    // 解析命令行参数
    let args: Vec<String> = env::args().collect();
    let mut intervals = "1m,5m,30m,4h,1d,1w".to_string();
    let mut concurrency = 15;

    // 简单的参数解析
    for i in 1..args.len() {
        if args[i] == "--intervals" && i + 1 < args.len() {
            intervals = args[i + 1].clone();
        } else if args[i] == "--concurrency" && i + 1 < args.len() {
            if let Ok(value) = args[i + 1].parse::<usize>() {
                concurrency = value;
            }
        }
    }

    info!("开始下载历史K线数据...");
    info!("使用周期: {}", intervals);
    info!("并发数: {}", concurrency);

    // 初始化下载器配置
    let config = Config::new(
        Some("./data".to_string()),
        Some(concurrency),                // 并发数
        Some(intervals.split(',').map(|s| s.trim().to_string()).collect()), // K线周期
        None,                             // 起始时间（自动计算）
        None,                             // 结束时间（当前时间）
        None,                             // 交易对（下载所有U本位永续合约）
        Some(true),                       // 使用SQLite文件存储
        Some(0),                          // 不限制K线数量
        Some(false),                      // 不使用更新模式
    );

    // 创建下载器实例
    let downloader = Downloader::new(config)?;

    // 运行下载流程
    match downloader.run().await {
        Ok(_) => {
            info!("历史K线下载完成，数据已保存到数据库");
            Ok(())
        },
        Err(e) => {
            error!("历史K线下载失败: {}", e);
            Err(e.into())
        }
    }
}

fn init_logging(verbose: bool) {
    let env = env_logger::Env::default()
        .filter_or("LOG_LEVEL", if verbose { "debug" } else { "info" });

    env_logger::init_from_env(env);
}
