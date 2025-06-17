// K线数据服务主程序 - 专注于K线补齐功能
use kline_server::klcommon::{Database, Result};
use kline_server::kldata::KlineBackfiller;

use std::sync::Arc;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // K线周期配置
    let intervals = "1m,5m,30m,1h,4h,1d,1w".to_string();
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    // 初始化简单日志
    init_simple_logging();

    info!("启动K线数据补齐服务");
    info!("使用周期: {}", intervals);

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    // 执行K线数据补齐
    info!("开始补齐K线数据...");

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

    info!("K线数据补齐服务完成");
    Ok(())
}

/// 初始化日志系统（支持命名管道）
fn init_simple_logging() {
    use kline_server::klaggregate::observability::WebSocketLogForwardingLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

    // 确保日志目录存在
    std::fs::create_dir_all("logs").unwrap_or_else(|_| {
        // 日志目录创建失败，忽略错误
    });

    // 获取日志配置
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let log_transport = std::env::var("LOG_TRANSPORT").unwrap_or_else(|_| "file".to_string());
    let pipe_name = std::env::var("PIPE_NAME").unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());

    // 根据传输方式初始化日志
    match log_transport.as_str() {
        "named_pipe" => {
            // 命名管道模式
            let log_forwarding_layer = WebSocketLogForwardingLayer::new_named_pipe(pipe_name.clone());

            Registry::default()
                .with(log_forwarding_layer)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                        .json()
                )
                .with(create_env_filter(&log_level))
                .init();

            info!("日志系统已初始化（命名管道模式）");
            info!("管道名称: {}", pipe_name);
        }
        _ => {
            // 文件模式（默认）
            tracing_subscriber::fmt()
                .with_env_filter(create_env_filter(&log_level))
                .with_target(true)
                .with_level(true)
                .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                .init();

            info!("日志系统已初始化（文件模式）");
        }
    }

    info!("日志级别: {}", log_level);
}

/// 创建环境过滤器，始终过滤掉第三方库的调试日志
fn create_env_filter(log_level: &str) -> tracing_subscriber::EnvFilter {
    // 无论应用日志级别如何，都过滤掉第三方库的噪音日志
    let filter_str = format!(
        "{},hyper=warn,reqwest=warn,tokio_tungstenite=warn,tungstenite=warn,rustls=warn,h2=warn,sqlx=warn,rusqlite=warn",
        log_level
    );

    tracing_subscriber::EnvFilter::new(filter_str)
}
