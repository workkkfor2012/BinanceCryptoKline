// K线服务器主程序
use log::{info, error};
use anyhow::Result;
use std::sync::Arc;

// 使用库中的模块
use kline_server::klserver::db::Database as ServerDatabase;
use kline_server::kldownload::db::Database as DownloadDatabase;
use kline_server::kldownload::websocket::{ContinuousKlineClient, ContinuousKlineConfig};
use kline_server::klserver::test_continuous_kline_client::ContinuousKlineClient as TestContinuousKlineClient;
use kline_server::kldownload::aggregator::KlineAggregator;
use kline_server::klserver::web;

use std::env;

// 命令行参数结构体
struct AppArgs {
    /// 是否为测试模式
    is_test_mode: bool,
    /// 是否显示详细日志
    verbose: bool,
    /// 是否跳过启动检查
    skip_check: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 解析命令行参数
    let args = parse_args();

    // 设置日志级别
    let verbose = args.verbose;

    // Initialize logging
    init_logging(verbose);

    info!("Starting Binance U-margined perpetual futures kline server");

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let server_db: Arc<ServerDatabase> = match ServerDatabase::new(&db_path) {
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
    if !args.skip_check {
        info!("检查数据库中的K线数据...");
        let btc_1m_count = match server_db.get_kline_count("BTCUSDT", "1m") {
            Ok(count) => count,
            Err(e) => {
                error!("检查BTC 1分钟K线数量失败: {}", e);
                return Err(e.into());
            }
        };

        info!("BTC 1分钟K线数量: {}", btc_1m_count);

        // 判断是否有足够的数据
        if btc_1m_count < 100 {
            error!("数据库中的BTC 1分钟K线数量不足，请先运行 kline_downloader 下载历史数据");
            return Err(anyhow::anyhow!("数据库中的K线数量不足").into());
        }
    } else {
        info!("跳过数据库检查，直接启动服务器");
    }

    // 根据命令行参数决定运行模式
    if args.is_test_mode {
        info!("以测试模式启动服务器");

        // 创建测试客户端配置
        let test_config = TestContinuousKlineClient::default_config();

        // 创建测试客户端
        let mut test_client = TestContinuousKlineClient::new(test_config, server_db.clone());

        // 运行测试
        match test_client.start().await {
            Ok(_) => info!("测试完成"),
            Err(e) => error!("测试失败: {}", e),
        }
    } else {
        info!("以正常模式启动服务器");

        // 只使用BTCUSDT交易对
        let symbols = vec!["BTCUSDT".to_string()];

        info!("只使用BTCUSDT交易对进行测试");

        // 创建连续合约K线配置
        let continuous_config = ContinuousKlineConfig {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols, // 使用从数据库获取的所有交易对
            intervals: vec!["1m".to_string()] // 只订阅一分钟周期，其他周期通过本地合成
        };

        info!("只订阅一分钟周期，其他周期通过本地合成");

        // 启动Web服务器
        info!("启动Web服务器...");

        // 使用tokio::spawn在新的任务中启动Web服务器
        let web_db = server_db.clone();
        let _web_handle = tokio::spawn(async move {
            info!("在新的任务中启动Web服务器");
            match web::start_web_server(web_db).await {
                Ok(_) => info!("启动Web服务器成功"),
                Err(e) => error!("启动Web服务器失败: {}", e),
            }
        });

        // 等待一下，确保Web服务器有时间启动
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!("请访问 http://localhost:3000 查看K线数据");

        // 创建K线合成器
        // 创建下载器数据库连接
        let download_db: Arc<DownloadDatabase> = match DownloadDatabase::new(&db_path) {
            Ok(db) => Arc::new(db),
            Err(e) => {
                error!("创建下载器数据库连接失败: {}", e);
                return Err(e.into());
            }
        };
        let aggregator = KlineAggregator::new(download_db.clone());

        // 打印日志，确认K线合成器实例已创建
        info!("创建K线合成器实例成功");

        // 检查全局实例是否已设置
        if let Some(_) = KlineAggregator::get_instance() {
            info!("全局K线合成器实例已设置");
        } else {
            error!("全局K线合成器实例设置失败");
        }

        // 启动K线合成器
        match aggregator.start().await {
            Ok(_) => info!("启动K线合成器成功"),
            Err(e) => error!("启动K线合成器失败: {}", e),
        }

        // 创建连续合约K线客户端
        let mut continuous_client = ContinuousKlineClient::new(continuous_config, download_db.clone());

        // 启动连续合约K线客户端
        match continuous_client.start().await {
            Ok(_) => info!("连续合约K线实时更新完成"),
            Err(e) => error!("连续合约K线实时更新失败: {}", e),
        }
    }

    Ok(())
}

/// 解析命令行参数
fn parse_args() -> AppArgs {
    let args: Vec<String> = env::args().collect();
    let mut is_test_mode = false;
    let mut verbose = false;
    let mut skip_check = false;

    // 简单的参数解析
    for arg in args.iter() {
        match arg.as_str() {
            "test" => is_test_mode = true,
            "--verbose" | "-v" => verbose = true,
            "--skip-check" => skip_check = true,
            _ => {}
        }
    }

    AppArgs {
        is_test_mode,
        verbose,
        skip_check,
    }
}

/// 初始化日志
fn init_logging(verbose: bool) {
    let env = env_logger::Env::default()
        .filter_or("LOG_LEVEL", if verbose { "debug" } else { "info" });

    env_logger::init_from_env(env);
}
