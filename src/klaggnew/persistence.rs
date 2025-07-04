// src/persistence.rs

use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, instrument, span, warn, Level, Instrument};

// 使用现有的数据库系统
use crate::klcommon::db::Database;
use crate::klcommon::models::Kline;

// Import the necessary types from the aggregator module
use super::aggregator::RealtimeAggregator;

// Helper function for period duration
fn get_period_duration_ms(period: &str) -> i64 {
    let last_char = period.chars().last().unwrap_or('m');
    let value: i64 = period[..period.len() - 1].parse().unwrap_or(1);

    match last_char {
        'm' => value * 60 * 1000,        // 分钟
        'h' => value * 60 * 60 * 1000,   // 小时
        'd' => value * 24 * 60 * 60 * 1000, // 天
        'w' => value * 7 * 24 * 60 * 60 * 1000, // 周
        _ => value * 60 * 1000,  // 默认为分钟
    }
}

// --- Module 2: PersistenceService ---
pub struct PersistenceService {
    is_running: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,

    // Dependencies
    aggregator: Arc<RealtimeAggregator>,
    database: Arc<Database>,

    // Cloned metadata for convenience
    index_to_symbol: HashMap<u32, String>,
    index_to_period: HashMap<u32, String>,
}

impl PersistenceService {
    #[instrument(name="persistence_init", skip_all, err)]
    pub async fn new(aggregator: Arc<RealtimeAggregator>, db_path: &str) -> Result<Self> {
        info!(event_name = "PersistenceServiceInitStarted");
        let database = Arc::new(Database::new(db_path)?);
        
        // Get metadata from aggregator instead of managing it itself
        let (index_to_symbol, index_to_period) = aggregator.get_metadata();

        Ok(Self {
            is_running: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            aggregator,
            database,
            index_to_symbol,
            index_to_period,
        })
    }

    #[instrument(name="persistence_start", skip(self))]
    pub async fn start(&self) {
        if self.is_running.swap(true, Ordering::SeqCst) {
            warn!(event_name = "PersistenceServiceAlreadyRunning");
            return;
        }
        info!(event_name = "PersistenceServiceStarting");
        
        let self_clone = self.clone_for_task();
        tokio::spawn(async move {
            self_clone.run_persistence_loop().await;
        }.instrument(span!(Level::INFO, "persistence_task")));
    }

    #[instrument(name="persistence_stop", skip(self))]
    pub async fn stop(&self) {
        if !self.is_running.swap(false, Ordering::SeqCst) { return; }
        info!(event_name = "PersistenceServiceStopping");
        self.shutdown_notify.notify_waiters();
        // A small delay to allow the final persistence run to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    
    fn clone_for_task(&self) -> Arc<Self> {
        Arc::new(Self {
            is_running: self.is_running.clone(),
            shutdown_notify: self.shutdown_notify.clone(),
            aggregator: self.aggregator.clone(),
            database: self.database.clone(),
            index_to_symbol: self.index_to_symbol.clone(),
            index_to_period: self.index_to_period.clone(),
        })
    }
    
    async fn run_persistence_loop(self: Arc<Self>) {
        info!(event_name = "PersistenceLoopStarted");
        // Instead of a fixed interval, we wait for the aggregator's signal
        loop {
            tokio::select! {
                _ = self.aggregator.wait_for_new_snapshot() => {
                    if let Err(e) = self.persist_snapshot().await {
                         error!(event_name="PersistenceCycleFailed", error=%e);
                    }
                },
                _ = self.shutdown_notify.notified() => {
                    info!(event_name="FinalPersistenceRun");
                    let _ = self.persist_snapshot().await;
                    break;
                }
            }
        }
        info!(event_name = "PersistenceLoopStopped");
    }

    #[instrument(name="persist_snapshot", skip(self), fields(persisted_count=0))]
    async fn persist_snapshot(self: &Arc<Self>) -> Result<()> {
        let snapshot = self.aggregator.get_kline_snapshot().await;
        info!(event_name="SnapshotTakenForPersistence", kline_count=snapshot.len());

        let mut persisted_count = 0;
        for kline in snapshot.iter().filter(|k| !k.is_empty()) {
            let symbol = self.index_to_symbol.get(&kline.symbol_index).unwrap();
            let period = self.index_to_period.get(&kline.period_index).unwrap();

            // 转换为现有数据库系统的Kline格式
            let close_time = kline.open_time + get_period_duration_ms(period) - 1;
            let db_kline = Kline {
                open_time: kline.open_time,
                open: kline.open.to_string(),
                high: kline.high.to_string(),
                low: kline.low.to_string(),
                close: kline.close.to_string(),
                volume: kline.volume.to_string(),
                close_time,
                quote_asset_volume: "0".to_string(), // 简化处理
                number_of_trades: 0,
                taker_buy_base_asset_volume: "0".to_string(),
                taker_buy_quote_asset_volume: "0".to_string(),
                ignore: "0".to_string(),
            };

            // 使用现有数据库API保存K线
            if let Err(e) = self.database.save_kline(symbol, period, &db_kline) {
                error!("Failed to persist kline: {}", e);
            } else {
                persisted_count += 1;
            }
        }
        
        tracing::Span::current().record("persisted_count", persisted_count);
        info!(event_name="PersistenceSnapshotCompleted", persisted_count);
        Ok(())
    }
}