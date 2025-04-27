// K线服务器主程序
use log::{info, error};
use anyhow::Result;
use std::sync::Arc;

// 使用库中的模块
use kline_server::klcommon::Database;
use kline_server::klserver::web;

// 硬编码参数
const VERBOSE: bool = true;
const SKIP_CHECK: bool = false;

#[tokio::main]
async fn main() -> Result<()> {
    // 使用硬编码参数
    let verbose = VERBOSE;

    // Initialize logging
    init_logging(verbose);

    info!("Starting Binance U-margined perpetual futures kline server");

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db: Arc<Database> = match Database::new(&db_path) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            error!("创建数据库连接失败: {}", e);
            return Err(e.into());
        }
    };

    // 检查数据库是否存在
    if !db_path.exists() {
        error!("数据库文件不存在，请先运行 kline_downloader 下载历史数据");
        return Err(anyhow::anyhow!("数据库文件不存在").into());
    }

    // 如果没有跳过检查，则检查BTC 1分钟K线数量
    if !SKIP_CHECK {
        info!("检查数据库中的K线数据...");
        let btc_1m_count = match db.get_kline_count("BTCUSDT", "1m") {
            Ok(count) => count,
            Err(e) => {
                error!("检查BTC 1分钟K线数量失败: {}", e);
                return Err(e.into());
            }
        };

        info!("BTC 1分钟K线数量: {}", btc_1m_count);

        // 判断是否有足够的数据
        if btc_1m_count < 100 {
            error!("数据库中的BTC 1分钟K线数量不足，请先运行 kline_data_service 下载历史数据");
            return Err(anyhow::anyhow!("数据库中的K线数量不足").into());
        }
    } else {
        info!("跳过数据库检查，直接启动服务器");
    }

    // 以正常模式启动服务器
    info!("以正常模式启动服务器");

    // 启动Web服务器
    info!("启动Web服务器...");

    // 启动Web服务器
    match web::start_web_server(db.clone()).await {
        Ok(_) => info!("启动Web服务器成功"),
        Err(e) => error!("启动Web服务器失败: {}", e),
    }

    Ok(())
}



/// 初始化日志
fn init_logging(verbose: bool) {
    // 设置RUST_BACKTRACE为1，以便更好地报告错误
    std::env::set_var("RUST_BACKTRACE", "1");

    // 确保日志目录存在
    let log_dir = "logs";
    std::fs::create_dir_all(log_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create logs directory: {}", e);
    });

    let dispatch = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"), // 添加毫秒
                record.level(),
                record.target(),
                message
            ))
        })
        .level(if verbose { log::LevelFilter::Debug } else { log::LevelFilter::Info })
        // 过滤掉过于频繁的第三方库日志
        .level_for("hyper", log::LevelFilter::Warn)
        .level_for("reqwest", log::LevelFilter::Warn)
        .chain(std::io::stdout()) // 输出到控制台
        .chain(fern::log_file(format!("{}/kline_server.log", log_dir)).expect("Failed to open log file")); // 输出到日志文件

    match dispatch.apply() {
        Ok(_) => log::info!("日志系统 (fern) 已初始化"),
        Err(e) => eprintln!("日志系统初始化失败: {}", e),
    }

    // 输出一条日志，确认日志系统已初始化
    log::info!("日志系统已初始化，UTF-8编码测试：中文、日文、韩文");
}
