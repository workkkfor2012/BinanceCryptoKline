// src/bin/klineaggnew.rs

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};
use tokio::signal;

// 使用库中的模块
use kline_server::klaggnew::{RealtimeAggregator, PersistenceService};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 初始化可观察性系统 (Tracing/Logging)
    // 这是AI调试的眼睛
    init_observability_system().context("Failed to initialize observability system")?;

    let main_span = tracing::info_span!(
        "kline_aggregate_app",
        version = env!("CARGO_PKG_VERSION")
    );
    let _enter = main_span.enter();

    info!(event_name = "ApplicationStarting", "Kline Aggregate Service is starting up.");

    // 2. 加载配置 (为了简化，这里硬编码，实际项目中应从文件加载)
    let db_path = "kline_data_ai_friendly.db";
    let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    let periods = vec!["1m".to_string(), "5m".to_string()];
    let max_symbols = 10;
    let websocket_url = "wss://fstream.binance.com/stream".to_string();

    info!(
        event_name = "ConfigurationLoaded",
        db_path,
        ?symbols,
        ?periods,
        "Application configuration loaded."
    );

    // 3. 初始化核心模块
    // a. 创建实时聚合器模块
    let aggregator = Arc::new(
        RealtimeAggregator::new(symbols, periods, max_symbols, websocket_url)
            .context("Failed to create RealtimeAggregator")?
    );
    info!(event_name = "ModuleCreated", module = "RealtimeAggregator");

    // b. 创建持久化服务模块，并注入聚合器
    let persistor = PersistenceService::new(aggregator.clone(), db_path)
        .await
        .context("Failed to create PersistenceService")?;
    info!(event_name = "ModuleCreated", module = "PersistenceService");


    // 4. 启动服务
    // 模块各自管理自己的生命周期和后台任务
    aggregator.start().await;
    persistor.start().await;
    info!(event_name = "ServicesStarted", "All services have been started.");

    // 5. 等待关闭信号
    wait_for_shutdown_signal().await;
    info!(event_name = "ShutdownSignalReceived", "Graceful shutdown initiated.");

    // 6. 优雅关闭服务 (按依赖逆序)
    persistor.stop().await;
    aggregator.stop().await;
    info!(event_name = "ServicesStopped", "All services have been stopped gracefully.");

    info!(event_name = "ApplicationShutdownComplete");
    Ok(())
}

/// 初始化日志/追踪系统
fn init_observability_system() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,sqlx=warn")); // 默认日志级别

    Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_thread_ids(true))
        .try_init()
        .context("Failed to initialize tracing subscriber")?;
    Ok(())
}

/// 等待 Ctrl+C 或 SIGTERM 信号
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}