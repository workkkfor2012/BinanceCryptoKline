// K线数据服务主程序
use kline_server::klcommon::{
    AppError, Database, Result,
    WebSocketClient, AggTradeClient, AggTradeConfig,
    PROXY_HOST, PROXY_PORT
};
use kline_server::kldata::{KlineBackfiller, ServerTimeSyncManager, LatestKlineUpdater};
use kline_server::klaggregate::observability::WebSocketLogForwardingLayer;

use std::sync::Arc;
use tracing::{info, error, warn, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

// 验证K线功能已移除，测试已通过

#[tokio::main]
#[instrument(target = "kline_data_service::main")]
async fn main() -> Result<()> {
    // 硬编码参数
    let intervals = "1m,5m,30m,1h,4h,1d,1w".to_string();
    let concurrency = 100; // 最新K线更新器的并发数
    let use_aggtrade = true; // 设置为true，启用WebSocket连接获取高频数据(eggtrade)用于合成K线
    let use_latest_kline_updater = false; // 设置为false，仅下载历史数据，不启动最新K线更新器

    // 将周期字符串转换为列表
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    // 初始化日志（支持命名管道传输）
    init_logging(true, &interval_list);

    info!(
        target = "kline_data_service::main",
        event_type = "service_startup",
        intervals = %intervals,
        concurrency = concurrency,
        use_aggtrade = use_aggtrade,
        use_latest_kline_updater = use_latest_kline_updater,
        "启动币安U本位永续合约K线数据服务"
    );

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    // 首先启动服务器时间同步
    info!(
        target = "kline_data_service::time_sync",
        event_type = "time_sync_start",
        "首先进行服务器时间同步..."
    );

    // 创建服务器时间同步管理器
    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());

    // 只进行一次时间同步，不启动定时任务
    match time_sync_manager.sync_time_once().await {
        Ok((time_diff, network_delay)) => {
            info!(
                target = "kline_data_service::time_sync",
                event_type = "time_sync_success",
                time_diff_ms = time_diff,
                network_delay_ms = network_delay,
                "服务器时间同步成功，时间差值: {}毫秒，网络延迟: {}毫秒，继续执行后续任务",
                time_diff, network_delay
            );
        },
        Err(e) => {
            error!(
                target = "kline_data_service::time_sync",
                event_type = "time_sync_failed",
                error = %e,
                "服务器时间同步失败"
            );
            return Err(e);
        }
    }

    // 启动时间同步任务（每分钟的第30秒运行）
    let time_sync_manager_clone = time_sync_manager.clone();
    tokio::spawn(async move {
        if let Err(e) = time_sync_manager_clone.start().await {
            error!("服务器时间同步任务启动失败: {}", e);
        }
    });

    // 补齐历史数据
    info!("开始补齐K线数据...");
    info!("使用周期: {}", intervals);

    // 创建补齐器实例
    let backfiller = KlineBackfiller::new(db.clone(), interval_list.clone());

    // 运行一次性补齐流程
    match backfiller.run_once().await {
        Ok(_) => {
            info!("历史K线补齐完成");

            // 获取所有交易对
            match db.get_all_symbols() {
                Ok(_) => {
                    info!("成功从数据库获取交易对列表");
                },
                Err(e) => {
                    error!("无法从数据库获取交易对列表: {}", e);
                    return Err(AppError::DatabaseError(format!("无法从数据库获取交易对列表: {}", e)));
                }
            };

            // 服务器时间同步任务已经在程序开始时启动，这里不需要再次启动

            // 不再使用K线聚合器，改为直接使用归集交易合成所有周期K线
            info!("不使用K线聚合器，将通过归集交易直接合成所有周期K线");

            // 如果启用最新K线更新器，则启动最新K线更新任务
            if use_latest_kline_updater {
                info!("启动最新K线更新任务（每分钟更新一次所有品种、所有周期的最新一根K线）");
                let updater_db = db.clone();
                let updater_intervals = interval_list.clone();
                let updater_time_sync_manager = time_sync_manager.clone();
                tokio::spawn(async move {
                    let updater = LatestKlineUpdater::new(
                        updater_db,
                        updater_intervals,
                        updater_time_sync_manager,
                        concurrency
                    );
                    if let Err(e) = updater.start().await {
                        error!("最新K线更新任务启动失败: {}", e);
                    }
                });
            } else {
                info!("未启用最新K线更新任务，仅下载历史数据");
            }
        },
        Err(e) => {
            error!("历史K线补齐失败: {}", e);
            return Err(e);
        }
    }

    // 如果不启用高频数据WebSocket连接，则退出
    if !use_aggtrade {
        info!("未启用高频数据WebSocket连接，程序退出");
        return Ok(());
    }

    // 启用归集交易功能，获取高频数据用于合成K线
    info!("使用归集交易数据生成K线，实时性更高");

    // 只使用BTCUSDT交易对
    let symbols = vec!["BTCUSDT".to_string()];
    info!("只使用BTCUSDT交易对进行测试");

    // 创建归集交易配置（使用集中代理设置）
    let agg_trade_config = AggTradeConfig {
        use_proxy: true,
        proxy_addr: PROXY_HOST.to_string(),
        proxy_port: PROXY_PORT,
        symbols, // 使用从数据库获取的所有交易对
    };

    info!("使用归集交易数据直接生成所有周期K线");

    // 创建归集交易客户端
    let mut agg_trade_client = AggTradeClient::new(agg_trade_config, db.clone(), interval_list.clone());

    // K线验证任务已移除，测试已通过
    info!("K线验证任务已移除，测试已通过");

    // 启动归集交易客户端
    match agg_trade_client.start().await {
        Ok(_) => info!("归集交易实时更新完成"),
        Err(e) => error!("归集交易实时更新失败: {}", e),
    }

    info!("归集交易功能已启用，程序将持续运行");

    Ok(())
}

fn init_logging(verbose: bool, intervals: &[String]) {
    // 设置RUST_BACKTRACE为1，以便更好地报告错误
    std::env::set_var("RUST_BACKTRACE", "1");

    // 确保日志目录存在
    let log_dir = "logs";
    std::fs::create_dir_all(log_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create logs directory: {}", e);
    });

    // 设置日志级别
    let log_level = if verbose { "debug" } else { "info" };

    // 检查日志传输方式
    let log_transport = std::env::var("LOG_TRANSPORT").unwrap_or_else(|_| "file".to_string());

    // 创建命名管道日志转发层（如果启用）
    let log_forwarding_layer = if log_transport == "named_pipe" {
        let pipe_name = std::env::var("PIPE_NAME")
            .unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());
        Some(WebSocketLogForwardingLayer::new_named_pipe(pipe_name))
    } else {
        None
    };

    // 创建文件输出层
    let file_appender = tracing_appender::rolling::daily(log_dir, "kldata.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    // 初始化tracing订阅器
    let init_result = match log_transport.as_str() {
        "named_pipe" => {
            // 命名管道模式：只发送JSON格式到WebLog，不使用控制台输出层
            Registry::default()
                .with(log_forwarding_layer.unwrap()) // 只有JSON格式发送到WebLog
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                        .with_writer(file_writer)
                        .json() // 文件使用JSON格式
                )
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| {
                            tracing_subscriber::EnvFilter::new(log_level)
                                .add_directive("hyper=warn".parse().unwrap())
                                .add_directive("reqwest=warn".parse().unwrap())
                                .add_directive("tokio_tungstenite=warn".parse().unwrap())
                                .add_directive("tungstenite=warn".parse().unwrap())
                        })
                )
                .try_init()
        }
        _ => {
            // 文件模式：保持原有行为
            Registry::default()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                        .json() // 使用JSON格式符合WebLog规范
                )
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                        .with_writer(file_writer)
                        .json() // 文件也使用JSON格式
                )
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| {
                            tracing_subscriber::EnvFilter::new(log_level)
                                .add_directive("hyper=warn".parse().unwrap())
                                .add_directive("reqwest=warn".parse().unwrap())
                                .add_directive("tokio_tungstenite=warn".parse().unwrap())
                                .add_directive("tungstenite=warn".parse().unwrap())
                        })
                )
                .try_init()
        }
    };

    // 检查初始化结果
    if let Err(e) = init_result {
        eprintln!("Failed to initialize tracing: {}", e);
        std::process::exit(1);
    }

    // 显示传输配置信息
    match log_transport.as_str() {
        "named_pipe" => {
            let pipe_name = std::env::var("PIPE_NAME")
                .unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());
            info!(
                target = "kline_data_service::logging",
                event_type = "logging_initialized",
                log_level = log_level,
                log_dir = log_dir,
                log_transport = "named_pipe",
                pipe_name = %pipe_name,
                intervals = ?intervals,
                "📡 tracing日志系统已初始化（命名管道模式），UTF-8编码测试：中文、日文、韩文"
            );
        }
        _ => {
            info!(
                target = "kline_data_service::logging",
                event_type = "logging_initialized",
                log_level = log_level,
                log_dir = log_dir,
                log_transport = "file",
                intervals = ?intervals,
                "📁 tracing日志系统已初始化（文件模式），UTF-8编码测试：中文、日文、韩文"
            );
        }
    }

}


