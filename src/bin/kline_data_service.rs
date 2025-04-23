// K线数据服务主程序
use kline_server::klcommon::{Database, Result};
use kline_server::kldata::streamer::{ContinuousKlineClient, ContinuousKlineConfig};
use kline_server::kldata::aggregator::KlineAggregator;
use kline_server::kldata::backfill::KlineBackfiller;
use log::{info, error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // 硬编码参数
    let intervals = "1m,5m,30m,4h,1d,1w".to_string();
    let _concurrency = 15; // 不再使用并发数，因为补齐器内部已经设置了并发数
    let download_only = false;
    let stream_only = false;

    // 初始化日志
    init_logging(true);

    info!("启动币安U本位永续合约K线数据服务");

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    // 如果不是仅流模式，则补齐历史数据
    if !stream_only {
        info!("开始补齐K线数据...");
        info!("使用周期: {}", intervals);

        // 将周期字符串转换为列表
        let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

        // 创建补齐器实例
        let backfiller = KlineBackfiller::new(db.clone(), interval_list);

        // 运行一次性补齐流程
        match backfiller.run_once().await {
            Ok(_) => {
                info!("历史K线补齐完成");
            },
            Err(e) => {
                error!("历史K线补齐失败: {}", e);
                return Err(e);
            }
        }
    }

    // 如果是仅下载模式，则退出
    if download_only {
        info!("仅下载模式，程序退出");
        return Ok(());
    }

    // 创建K线聚合器
    let aggregator = KlineAggregator::new(db.clone());
    info!("K线聚合器已创建");

    // 启动K线聚合器
    let aggregator_clone = aggregator.clone();
    tokio::spawn(async move {
        if let Err(e) = aggregator_clone.start().await {
            error!("K线聚合器启动失败: {}", e);
        }
    });

    // 只使用BTCUSDT交易对
    let symbols = vec!["BTCUSDT".to_string()];
    info!("只使用BTCUSDT交易对进行测试");

    // 创建连续合约K线配置（硬编码代理设置）
    let continuous_config = ContinuousKlineConfig {
        use_proxy: true,
        proxy_addr: "127.0.0.1".to_string(),
        proxy_port: 1080,
        symbols, // 使用从数据库获取的所有交易对
        intervals: vec!["1m".to_string()] // 只订阅一分钟周期，其他周期通过本地合成
    };

    // 设置HTTP代理环境变量（为了兼容可能使用环境变量的其他组件）
    std::env::set_var("https_proxy", "http://127.0.0.1:1080");
    std::env::set_var("http_proxy", "http://127.0.0.1:1080");
    std::env::set_var("HTTPS_PROXY", "http://127.0.0.1:1080");
    std::env::set_var("HTTP_PROXY", "http://127.0.0.1:1080");

    info!("只订阅一分钟周期，其他周期通过本地合成");

    // 创建连续合约K线客户端
    let mut continuous_client = ContinuousKlineClient::new(continuous_config, db.clone());

    // 启动连续合约K线客户端
    match continuous_client.start().await {
        Ok(_) => info!("连续合约K线实时更新完成"),
        Err(e) => error!("连续合约K线实时更新失败: {}", e),
    }

    Ok(())
}

fn init_logging(verbose: bool) {
    // 直接设置日志级别，不使用环境变量
    let log_level = if verbose { "debug" } else { "info" };

    // 设置RUST_BACKTRACE为1，以便更好地报告错误
    std::env::set_var("RUST_BACKTRACE", "1");

    // 设置日志级别
    std::env::set_var("RUST_LOG", log_level);

    // 初始化日志
    env_logger::init();
}


