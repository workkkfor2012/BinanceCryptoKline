// src/persistence.rs

use anyhow::Result;
use sqlx::{sqlite::{SqlitePool, SqlitePoolOptions}, Row};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, instrument, span, warn, Level, Instrument};

// Import the necessary types from the aggregator module
use super::aggregator::{KlineData, RealtimeAggregator};

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
    db_pool: SqlitePool,

    // Cloned metadata for convenience
    index_to_symbol: HashMap<u32, String>,
    index_to_period: HashMap<u32, String>,
}

impl PersistenceService {
    #[instrument(name="persistence_init", skip_all, err)]
    pub async fn new(aggregator: Arc<RealtimeAggregator>, db_path: &str) -> Result<Self> {
        info!(event_name = "PersistenceServiceInitStarted");
        let db_pool = SqlitePoolOptions::new().connect(db_path).await?;
        
        // Get metadata from aggregator instead of managing it itself
        let (index_to_symbol, index_to_period) = aggregator.get_metadata();

        Ok(Self {
            is_running: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            aggregator,
            db_pool,
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
            db_pool: self.db_pool.clone(),
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
            let table_name = format!("kline_{}_{}", symbol.to_lowercase(), period);

            // 构建UPSERT查询
            let query = format!(
                r#"
                INSERT INTO {} (open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore_field)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(open_time) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    close_time = excluded.close_time,
                    quote_asset_volume = excluded.quote_asset_volume,
                    number_of_trades = excluded.number_of_trades,
                    taker_buy_base_asset_volume = excluded.taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume = excluded.taker_buy_quote_asset_volume,
                    ignore_field = excluded.ignore_field
                "#,
                table_name
            );

            // 执行查询（这里简化处理，实际应该批量执行）
            let close_time = kline.open_time + get_period_duration_ms(period) - 1;
            let result = sqlx::query(&query)
                .bind(kline.open_time)
                .bind(kline.open.to_string())
                .bind(kline.high.to_string())
                .bind(kline.low.to_string())
                .bind(kline.close.to_string())
                .bind(kline.volume.to_string())
                .bind(close_time)
                .bind("0") // quote_asset_volume
                .bind(0i64) // number_of_trades
                .bind("0") // taker_buy_base_asset_volume
                .bind("0") // taker_buy_quote_asset_volume
                .bind("0") // ignore_field
                .execute(&self.db_pool)
                .await;

            if let Err(e) = result {
                error!("Failed to persist kline: {}", e);
            }
            persisted_count += 1;
        }
        
        tracing::Span::current().record("persisted_count", persisted_count);
        info!(event_name="PersistenceSnapshotCompleted", persisted_count);
        Ok(())
    }
}