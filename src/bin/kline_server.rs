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
    let env = env_logger::Env::default()
        .filter_or("LOG_LEVEL", if verbose { "debug" } else { "info" });

    env_logger::init_from_env(env);
}
