mod api;
mod models;
mod config;
mod downloader;
mod storage;
mod error;
mod utils;
mod db;

use log::{info, error};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 硬编码参数，不再使用命令行参数
    let verbose = false; // 设置为false，只显示INFO级别的日志
    let output_dir = "./data".to_string();
    let concurrency = 15;
    let intervals = Some("1m,5m,30m,4h,1d,1w".to_string());
    let start_time = None; // 不再使用固定的起始时间，而是根据周期计算
    let end_time = None; // 使用当前时间作为结束时间
    let symbols = None; // 下载所有U本位永续合约
    let use_sqlite = true; // 使用SQLite存储
    let max_klines = Some(0); // 不限制K线数量
    let update_only = false; // 不使用更新模式

    // Initialize logging
    init_logging(verbose);

    info!("Starting Binance U-margined perpetual futures kline downloader");

    // Load configuration
    let config = config::Config::new(
        output_dir,
        concurrency,
        intervals,
        start_time,
        end_time,
        symbols,
        use_sqlite,
        max_klines,
        update_only,
    );

    // Initialize the downloader
    let downloader = match downloader::Downloader::new(config.clone()) {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to initialize downloader: {}", e);
            return Err(e.into());
        }
    };

    // Run the download process
    match downloader.run().await {
        Ok(_) => {
            info!("Download completed successfully");
            Ok(())
        },
        Err(e) => {
            error!("Download failed: {}", e);
            Err(e.into())
        }
    }
}

fn init_logging(verbose: bool) {
    let env = env_logger::Env::default()
        .filter_or("LOG_LEVEL", if verbose { "debug" } else { "info" });

    env_logger::init_from_env(env);
}
