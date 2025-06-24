// K线数据服务主程序 - 专注于K线补齐功能
use kline_server::klcommon::{Database, Result, AppError};
use kline_server::kldata::KlineBackfiller;
use kline_server::klaggregate::config::AggregateConfig;

use std::sync::Arc;
use std::path::Path;
use std::time::Duration;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{info, error, instrument, info_span, Instrument};

// 导入轨迹提炼器组件
use kline_server::klcommon::log::{
    TraceDistillerStore,
    TraceDistillerLayer,
    distill_all_completed_traces_to_text
};

/// 默认配置文件路径
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";

// ========================================
// 🔧 测试开关配置
// ========================================
/// 是否启用测试模式（限制为只处理 BTCUSDT）
const TEST_MODE: bool = false;

/// 测试模式下使用的交易对
const TEST_SYMBOLS: &[&str] = &["BTCUSDT"];

/// 程序运行期间的快照计数器，用于生成有序的文件名
static SNAPSHOT_COUNTER: AtomicU32 = AtomicU32::new(1);
// ========================================

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志系统并获取TraceDistillerStore
    let distiller_store = init_logging_with_distiller();

    // 创建应用程序的根Span，代表整个应用生命周期
    let root_span = info_span!(
        "kline_service_app",
        service = "kline_data_service",
        version = env!("CARGO_PKG_VERSION")
    );

    // 在根Span的上下文中运行整个应用
    let result = run_app().instrument(root_span).await;

    // 程序退出时生成最终快照
    generate_final_snapshot(&distiller_store).await;

    result
}

/// 应用程序的核心业务逻辑
#[instrument(name = "run_app", skip_all)]
async fn run_app() -> Result<()> {
    // K线周期配置
    let intervals = "1m,5m,30m,1h,4h,1d,1w".to_string();
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    info!("启动K线数据补齐服务");
    info!("使用周期: {}", intervals);

    // 创建数据库连接
    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    // 执行K线数据补齐
    info!("开始补齐K线数据...");

    // 创建补齐器实例
    let backfiller = if TEST_MODE {
        info!("🔧 启用测试模式，限制交易对为: {:?}", TEST_SYMBOLS);
        KlineBackfiller::new_test_mode(
            db.clone(),
            interval_list,
            TEST_SYMBOLS.iter().map(|s| s.to_string()).collect()
        )
    } else {
        info!("📡 生产模式，将获取所有交易对");
        KlineBackfiller::new(db.clone(), interval_list)
    };

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

/// 初始化日志系统（同时支持模块日志、trace可视化和轨迹提炼）
fn init_logging_with_distiller() -> TraceDistillerStore {
    use kline_server::klcommon::log::{
        ModuleLayer,
        NamedPipeLogManager,
        TraceVisualizationLayer,
    };
    use std::sync::Arc;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

    // 确保日志目录存在
    std::fs::create_dir_all("logs").unwrap_or_else(|_| {
        // 日志目录创建失败，忽略错误
    });

    // 获取日志配置 - 优先从配置文件读取，回退到环境变量
    let (log_level, log_transport, pipe_name) = match load_logging_config() {
        Ok(config) => config,
        Err(_) => {
            // 配置文件读取失败，回退到环境变量
            let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
            let log_transport = std::env::var("LOG_TRANSPORT").unwrap_or_else(|_| "file".to_string());
            let pipe_name = std::env::var("PIPE_NAME").unwrap_or_else(|_| r"\\.\pipe\kline_log_pipe".to_string());
            (log_level, log_transport, pipe_name)
        }
    };

    // 创建TraceDistillerStore用于轨迹提炼
    let distiller_store = TraceDistillerStore::default();
    let distiller_layer = TraceDistillerLayer::new(distiller_store.clone());

    // 根据传输方式初始化日志
    match log_transport.as_str() {
        "named_pipe" => {
            // 命名管道模式 - 使用三层架构
            let log_manager = Arc::new(NamedPipeLogManager::new(pipe_name.clone()));
            // 注意：NamedPipeLogManager::new() 现在会自动启动后台任务

            let module_layer = ModuleLayer::new(log_manager.clone());
            let trace_layer = TraceVisualizationLayer::new(log_manager.clone());

            Registry::default()
                .with(module_layer)         // 处理顶层模块日志
                .with(trace_layer)          // 处理 span 日志用于路径可视化
                .with(distiller_layer)      // 处理轨迹提炼用于调试快照
                .with(create_env_filter(&log_level))
                .init();

            info!("🎯 三重日志系统已初始化（命名管道模式 + 轨迹提炼）");
            info!("📊 模块日志: 只处理顶层日志，log_type=module");
            info!("🔍 Trace可视化: 只处理Span内日志，log_type=trace");
            info!("🔬 轨迹提炼: 构建调用树用于调试快照");
            info!("🔗 共享管道: {}", pipe_name);
        }
        "websocket" => {
            // WebSocket模式已不再支持，回退到文件模式 + 轨迹提炼
            Registry::default()
                .with(tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_level(true)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339()))
                .with(distiller_layer)      // 添加轨迹提炼层
                .with(create_env_filter(&log_level))
                .init();

            info!("⚠️  WebSocket模式已不再支持，已回退到文件模式 + 轨迹提炼");
            info!("💡 请使用 LOG_TRANSPORT=named_pipe 启用日志传输");
        }
        _ => {
            // 文件模式（默认）+ 轨迹提炼
            Registry::default()
                .with(tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_level(true)
                    .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339()))
                .with(distiller_layer)      // 添加轨迹提炼层
                .with(create_env_filter(&log_level))
                .init();

            info!("日志系统已初始化（文件模式 + 轨迹提炼）");
        }
    }

    info!("日志级别: {}", log_level);

    // 返回distiller_store供主程序使用
    distiller_store
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

/// 加载日志配置
fn load_logging_config() -> Result<(String, String, String)> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    if Path::new(&config_path).exists() {
        let config = AggregateConfig::from_file(&config_path)?;

        // 确保管道名称格式正确（Windows命名管道需要完整路径）
        let pipe_name = if config.logging.pipe_name.starts_with(r"\\.\pipe\") {
            config.logging.pipe_name
        } else {
            format!(r"\\.\pipe\{}", config.logging.pipe_name)
        };

        Ok((
            config.logging.log_level,
            config.logging.log_transport,
            pipe_name,
        ))
    } else {
        Err(AppError::ConfigError(format!("配置文件不存在: {}，回退到环境变量", config_path)))
    }
}

/// 生成程序退出时的最终快照
async fn generate_final_snapshot(store: &TraceDistillerStore) {
    info!("🔬 程序退出，生成最终Trace快照...");

    // 等待一小段时间，确保所有正在进行的span都能完成
    tokio::time::sleep(Duration::from_millis(100)).await;

    let log_dir = "logs/debug_snapshots";

    // 确保目录存在
    if let Err(e) = tokio::fs::create_dir_all(log_dir).await {
        error!("无法创建调试快照目录: {}", e);
        return;
    }

    // 生成并写入快照
    let report_text = distill_all_completed_traces_to_text(store);

    // 获取下一个序号（程序运行期间递增）
    let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let filename = format!("{}/final_snapshot_{}_{}.log", log_dir, sequence, timestamp);

    match tokio::fs::File::create(&filename).await {
        Ok(mut file) => {
            use tokio::io::AsyncWriteExt;
            if file.write_all(report_text.as_bytes()).await.is_err() {
                error!("写入快照文件 {} 失败", filename);
            } else {
                info!("✅ 已生成最终Trace快照: {}", filename);
            }
        },
        Err(e) => {
            error!("创建快照文件 {} 失败: {}", filename, e);
        }
    }

    info!("✅ 最终快照生成完成");
}
