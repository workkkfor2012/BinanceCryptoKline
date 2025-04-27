// K线数据服务主程序
use kline_server::klcommon::{Database, Result};
use kline_server::kldata::streamer::{ContinuousKlineClient, ContinuousKlineConfig};
use kline_server::kldata::backfill::KlineBackfiller;
use kline_server::kldata::downloader; // 导入 downloader 模块
use log::{info, error}; // 移除 debug, warn, 由 downloader 内部处理
use std::sync::Arc;
// 移除 std::time::Duration, kline_server::kldata::downloader::{BinanceApi, DownloadTask}, tokio::time::{interval, Instant}

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
        let backfiller = KlineBackfiller::new(db.clone(), interval_list.clone());

        // 运行一次性补齐流程
        match backfiller.run_once().await {
            Ok(_) => {
                info!("历史K线补齐完成");

                // // 启动定时更新任务
                // let timer_db = db.clone();
                // let timer_intervals = interval_list.clone();
                // tokio::spawn(async move {
                //     // 调用 downloader 中的函数来处理定时任务
                //     downloader::start_periodic_update(timer_db, timer_intervals).await;
                // });

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

    // 注意：K线聚合器已移除，现在使用新的K线合成算法
    info!("使用新的K线合成算法，每次接收WebSocket推送的1分钟K线数据时触发合成");

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
    // 设置RUST_BACKTRACE为1，以便更好地报告错误
    std::env::set_var("RUST_BACKTRACE", "1");

    // 设置控制台代码页为UTF-8 (如果需要)
    // std::process::Command::new("cmd").args(&["/c", "chcp", "65001"]).output().expect("Failed to execute chcp command");

    // 确保日志目录存在
    let log_dir = "logs";
    std::fs::create_dir_all(log_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create logs directory: {}", e);
    });

    let base_config = fern::Dispatch::new()
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
        .level_for("tokio_tungstenite", log::LevelFilter::Warn)
        .level_for("tungstenite", log::LevelFilter::Warn);

    // 通用日志文件
    let general_log_path = format!("{}/kline_data_service.log", log_dir);
    let file_dispatch = fern::Dispatch::new()
        .chain(fern::log_file(&general_log_path).expect("Failed to open general log file"));

    // 各周期聚合日志文件
    let mut dispatch = base_config;

    let intervals = ["5m", "30m", "4h", "1d", "1w"];
    for interval in intervals {
        let target_name = format!("aggregator_{}", interval);
        let log_path = format!("{}/aggregate_{}.log", log_dir, interval);
        let interval_dispatch = fern::Dispatch::new()
            .filter(move |metadata| metadata.target() == target_name)
            .chain(fern::log_file(&log_path).expect(&format!("Failed to open log file for {}", interval)));
        dispatch = dispatch.chain(interval_dispatch);
    }

    // 将不匹配任何周期 target 的日志（包括默认日志）发送到通用文件和控制台
    let default_dispatch = fern::Dispatch::new()
        .filter(move |metadata| !intervals.iter().any(|i| metadata.target() == format!("aggregator_{}", i)))
        .chain(std::io::stdout()) // 输出到控制台
        .chain(file_dispatch); // 输出到通用日志文件

    dispatch = dispatch.chain(default_dispatch);


    match dispatch.apply() {
        Ok(_) => log::info!("日志系统 (fern) 已初始化"),
        Err(e) => eprintln!("日志系统初始化失败: {}", e),
    }

    // 输出一条日志，确认日志系统已初始化
    log::info!("日志系统已初始化，UTF-8编码测试：中文、日文、韩文");
}


