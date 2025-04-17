mod api;
mod models;
mod config;
mod downloader;
mod storage;
mod error;
mod utils;
mod db;
mod startup_check;

use log::{info, error};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 硬编码参数，不再使用命令行参数
    let verbose = false; // 设置为false，只显示INFO级别的日志

    // Initialize logging
    init_logging(verbose);

    info!("Starting Binance U-margined perpetual futures kline downloader");

    // 执行启动检查流程
    info!("执行启动检查流程...");
    let download_executed = match startup_check::StartupCheck::run().await {
        Ok(executed) => executed,
        Err(e) => {
            error!("启动检查失败: {}", e);
            return Err(e.into());
        }
    };

    // 如果执行了下载，等待一秒确保数据已写入数据库
    if download_executed {
        info!("启动检查已执行下载流程，数据已保存到数据库");

        // 等待一秒，确保数据已写入数据库
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        return Ok(());
    }

    // 如果没有执行下载，则继续正常流程
    info!("启动检查完成，继续执行正常流程");

    // 这里可以添加程序的其他功能
    // ...

    Ok(())
}

fn init_logging(verbose: bool) {
    let env = env_logger::Env::default()
        .filter_or("LOG_LEVEL", if verbose { "debug" } else { "info" });

    env_logger::init_from_env(env);
}
