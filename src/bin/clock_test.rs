//! 时钟测试程序 - 测试每5秒输出校准时间功能

use anyhow::Result;
use chrono;
use kline_server::klcommon::{
    log::{self, init_ai_logging, shutdown_target_log_sender},
    server_time_sync::ServerTimeSyncManager,
    AggregateConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::time::sleep;
use tracing::{info, warn, error, trace, instrument};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志系统
    let config = AggregateConfig::load_from_file("config/BinanceKlineConfig.toml")?;
    let _log_guard = init_ai_logging(&config.logging)?;
    
    info!("时钟测试程序启动");
    
    // 创建模拟的时间同步管理器（不需要网络连接）
    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());
    
    // 创建时钟通道
    let (clock_tx, _clock_rx) = watch::channel(0i64);
    
    // 创建关闭通知
    let shutdown_notify = Arc::new(Notify::new());
    
    // 启动时钟任务
    let clock_task = run_clock_task(
        config.clone(),
        time_sync_manager.clone(),
        clock_tx,
        shutdown_notify.clone(),
    );
    
    // 运行时钟任务10次（50秒）然后退出
    tokio::select! {
        _ = clock_task => {
            info!("时钟任务已完成");
        }
        _ = tokio::time::sleep(Duration::from_secs(50)) => {
            info!("测试时间到，准备关闭");
            shutdown_notify.notify_one();
        }
    }
    
    // 关闭日志系统
    shutdown_target_log_sender().await;
    
    Ok(())
}

/// 全局时钟任务（简化版，不依赖网络时间同步）
#[instrument(target = "全局时钟", skip_all, name="run_clock_task")]
async fn run_clock_task(
    _config: Arc<AggregateConfig>,
    _time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    const CLOCK_INTERVAL_MS: i64 = 5_000; // 5秒
    info!(target: "全局时钟", log_type="low_freq", interval_ms = CLOCK_INTERVAL_MS, "全局时钟任务已启动，将每5秒输出校准时间");
    
    let mut counter = 0;
    
    loop {
        // 使用本地时间模拟校准时间
        let now = chrono::Utc::now();
        let final_time = now.timestamp_millis();
        
        // 输出校准后的服务器时间
        let formatted_time = now.format("%Y-%m-%d %H:%M:%S UTC").to_string();
        info!(target: "全局时钟", 
              log_type="checkpoint",
              server_time_ms = final_time,
              offset_ms = 0, // 模拟无偏移
              "校准后的服务器时间: {}", formatted_time);
        
        if clock_tx.send(final_time).is_err() {
            warn!(target: "全局时钟", "主时钟通道已关闭，任务退出");
            break;
        }
        
        counter += 1;
        if counter >= 10 {
            info!(target: "全局时钟", "已输出10次时间，测试完成");
            break;
        }
        
        // 等待5秒
        tokio::select! {
            _ = sleep(Duration::from_millis(CLOCK_INTERVAL_MS as u64)) => {}
            _ = shutdown_notify.notified() => {
                info!(target: "全局时钟", "收到关闭信号，任务退出");
                break;
            }
        }
    }
    
    warn!(target: "全局时钟", "全局时钟任务已退出");
}
