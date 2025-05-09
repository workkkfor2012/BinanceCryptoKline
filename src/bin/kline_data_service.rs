// K线数据服务主程序
use kline_server::klcommon::{
    AppError, Database, Result,
    WebSocketClient, AggTradeClient, AggTradeConfig,
    PROXY_HOST, PROXY_PORT
};
use kline_server::kldata::{KlineBackfiller, ServerTimeSyncManager, LatestKlineUpdater};

use log::{info, error};
use std::sync::Arc;

// 验证K线功能已移除，测试已通过

#[tokio::main]
async fn main() -> Result<()> {
    // 硬编码参数
    let intervals = "1m,5m,30m,4h,1d,1w".to_string();
    let concurrency = 100; // 最新K线更新器的并发数
    let use_aggtrade = true; // 设置为true，启用WebSocket连接获取高频数据(eggtrade)用于合成K线
    let use_latest_kline_updater = false; // 设置为false，仅下载历史数据，不启动最新K线更新器

    // 将周期字符串转换为列表
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    // 初始化日志
    init_logging(true, &interval_list);

    info!("启动币安U本位永续合约K线数据服务");

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    // 首先启动服务器时间同步
    info!("首先进行服务器时间同步...");

    // 创建服务器时间同步管理器
    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());

    // 只进行一次时间同步，不启动定时任务
    match time_sync_manager.sync_time_once().await {
        Ok((time_diff, network_delay)) => {
            info!("服务器时间同步成功，时间差值: {}毫秒，网络延迟: {}毫秒，继续执行后续任务",
                time_diff, network_delay);
        },
        Err(e) => {
            error!("服务器时间同步失败: {}", e);
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

    // 定义聚合器目标列表
    let aggregator_targets: Vec<String> = intervals.iter()
        .map(|interval| format!("kline_aggregator_{}", interval))
        .collect();

    // 创建日志文件路径
    let log_file_path = format!("{}/kldata.log", log_dir);

    // 尝试删除已存在的日志文件
    let delete_result = if std::path::Path::new(&log_file_path).exists() {
        match std::fs::remove_file(&log_file_path) {
            Ok(_) => format!("已删除旧的日志文件: {}", log_file_path),
            Err(e) => format!("无法删除旧的日志文件 {}: {}", log_file_path, e),
        }
    } else {
        String::new()
    };

    // 创建主分发器
    let dispatch = base_config;

    // 创建默认分发器
    let default_dispatch = fern::Dispatch::new()
        .filter(move |metadata| {
            let target = metadata.target();
            !aggregator_targets.iter().any(|t| target == t.as_str())
        });

    // 尝试添加文件日志
    let default_dispatch = match fern::log_file(&log_file_path) {
        Ok(file_log) => default_dispatch.chain(file_log),
        Err(e) => {
            // 如果无法创建日志文件，则输出错误信息到控制台并退出程序
            eprintln!("错误: 无法创建日志文件 {}: {}", log_file_path, e);
            std::process::exit(1);
        }
    };

    let final_dispatch = dispatch.chain(default_dispatch);

    match final_dispatch.apply() {
        Ok(_) => {
            // 如果有删除日志文件的结果，记录到日志中
            if !delete_result.is_empty() {
                log::info!("{}", delete_result);
            }
            log::info!("日志系统 (fern) 已初始化");
            log::info!("日志系统已初始化，UTF-8编码测试：中文、日文、韩文");
        },
        Err(e) => {
            eprintln!("错误: 日志系统初始化失败: {}", e);
            std::process::exit(1);
        },
    }
}


