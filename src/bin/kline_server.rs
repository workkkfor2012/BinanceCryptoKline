// K线服务器主程序
use anyhow::Result;
use std::sync::Arc;
use tracing::{info, error, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

// 使用库中的模块
use kline_server::klcommon::Database;
use kline_server::klserver::web;

// 硬编码参数
const VERBOSE: bool = true;
const SKIP_CHECK: bool = false;

#[tokio::main]
#[instrument(target = "kline_server::main")]
async fn main() -> Result<()> {
    // 使用硬编码参数
    let verbose = VERBOSE;

    // Initialize logging
    init_logging(verbose);

    info!(
        target = "kline_server::main",
        event_type = "server_startup",
        "Starting Binance U-margined perpetual futures kline server"
    );

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db: Arc<Database> = match Database::new(&db_path) {
        Ok(db) => {
            info!(
                target = "kline_server::database",
                event_type = "database_connection_success",
                db_path = %db_path.display(),
                "数据库连接创建成功"
            );
            Arc::new(db)
        },
        Err(e) => {
            error!(
                target = "kline_server::database",
                event_type = "database_connection_failed",
                db_path = %db_path.display(),
                error = %e,
                "创建数据库连接失败"
            );
            return Err(e.into());
        }
    };

    // 检查数据库是否存在
    if !db_path.exists() {
        error!(
            target = "kline_server::database",
            event_type = "database_file_missing",
            db_path = %db_path.display(),
            "数据库文件不存在，请先运行 kline_downloader 下载历史数据"
        );
        return Err(anyhow::anyhow!("数据库文件不存在").into());
    }

    // 如果没有跳过检查，则检查BTC 1分钟K线数量
    if !SKIP_CHECK {
        info!(
            target = "kline_server::startup_check",
            event_type = "database_check_start",
            "检查数据库中的K线数据..."
        );
        let btc_1m_count = match db.get_kline_count("BTCUSDT", "1m") {
            Ok(count) => count,
            Err(e) => {
                error!(
                    target = "kline_server::startup_check",
                    event_type = "kline_count_check_failed",
                    symbol = "BTCUSDT",
                    interval = "1m",
                    error = %e,
                    "检查BTC 1分钟K线数量失败"
                );
                return Err(e.into());
            }
        };

        info!(
            target = "kline_server::startup_check",
            event_type = "kline_count_result",
            symbol = "BTCUSDT",
            interval = "1m",
            count = btc_1m_count,
            "BTC 1分钟K线数量"
        );

        // 判断是否有足够的数据
        if btc_1m_count < 100 {
            error!(
                target = "kline_server::startup_check",
                event_type = "insufficient_data",
                symbol = "BTCUSDT",
                interval = "1m",
                count = btc_1m_count,
                min_required = 100,
                "数据库中的BTC 1分钟K线数量不足，请先运行 kline_data_service 下载历史数据"
            );
            return Err(anyhow::anyhow!("数据库中的K线数量不足").into());
        }
    } else {
        info!(
            target = "kline_server::startup_check",
            event_type = "database_check_skipped",
            "跳过数据库检查，直接启动服务器"
        );
    }

    // 以正常模式启动服务器
    info!(
        target = "kline_server::main",
        event_type = "server_mode_normal",
        "以正常模式启动服务器"
    );

    // 启动Web服务器
    info!(
        target = "kline_server::web_server",
        event_type = "web_server_starting",
        "启动Web服务器..."
    );

    // 启动Web服务器
    match web::start_web_server(db.clone()).await {
        Ok(_) => info!(
            target = "kline_server::web_server",
            event_type = "web_server_started",
            "启动Web服务器成功"
        ),
        Err(e) => error!(
            target = "kline_server::web_server",
            event_type = "web_server_failed",
            error = %e,
            "启动Web服务器失败"
        ),
    }

    Ok(())
}



/// 初始化tracing日志系统
/// 加载日志配置
fn load_logging_config() -> Result<String> {
    use kline_server::klaggregate::config::AggregateConfig;

    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config/aggregate_config.toml".to_string());

    if std::path::Path::new(&config_path).exists() {
        let config = AggregateConfig::from_file(&config_path)?;
        Ok(config.logging.log_level)
    } else {
        // 配置文件不存在，使用环境变量或默认值
        Ok(std::env::var("RUST_LOG").unwrap_or_else(|_| "trace".to_string()))
    }
}

fn init_logging(verbose: bool) {
    // 设置RUST_BACKTRACE为1，以便更好地报告错误
    std::env::set_var("RUST_BACKTRACE", "1");

    // 确保日志目录存在
    let log_dir = "logs";
    std::fs::create_dir_all(log_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create logs directory: {}", e);
    });

    // 从配置文件读取日志级别
    let log_level = load_logging_config()
        .unwrap_or_else(|_| if verbose { "trace".to_string() } else { "trace".to_string() });

    // 创建文件输出层
    let file_appender = tracing_appender::rolling::daily(log_dir, "kline_server.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    // 初始化tracing订阅器
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
                    tracing_subscriber::EnvFilter::new(&log_level)
                        .add_directive("hyper=warn".parse().unwrap())
                        .add_directive("reqwest=warn".parse().unwrap())
                })
        )
        .init();

    info!(
        target = "kline_server::logging",
        event_type = "logging_initialized",
        log_level = log_level,
        log_dir = log_dir,
        "tracing日志系统已初始化，UTF-8编码测试：中文、日文、韩文"
    );
}
