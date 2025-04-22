// WebSocket statistics module
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;
use crate::kldownload::websocket::config::STATS_INTERVAL;
use crate::kldownload::websocket::message_handler::{LATEST_KLINES, SYMBOL_INTERVAL_UPDATES};

/// Statistics manager
pub struct StatsManager {
    total_updates: Arc<AtomicUsize>,
    connections_count: usize,
    connection_updates: Vec<Arc<AtomicUsize>>,
    symbol_updates: Arc<TokioMutex<HashMap<String, Arc<AtomicUsize>>>>,
    interval_updates: HashMap<String, Arc<AtomicUsize>>,
    log_tx: Sender<String>,
}

impl StatsManager {
    /// Create a new statistics manager
    pub fn new(
        total_updates: Arc<AtomicUsize>,
        connections_count: usize,
        connection_updates: Vec<Arc<AtomicUsize>>,
        symbol_updates: Arc<TokioMutex<HashMap<String, Arc<AtomicUsize>>>>,
        interval_updates: HashMap<String, Arc<AtomicUsize>>,
        log_tx: Sender<String>,
    ) -> Self {
        Self {
            total_updates,
            connections_count,
            connection_updates,
            symbol_updates,
            interval_updates,
            log_tx,
        }
    }

    /// Start statistics task
    pub async fn start_stats_task(self) {
        // Clone to avoid ownership issues with self
        let total_updates = self.total_updates.clone();
        let connections_count = self.connections_count;
        let connection_updates = self.connection_updates.clone();
        let symbol_updates = self.symbol_updates.clone();
        let interval_updates = self.interval_updates.clone();
        let log_tx = self.log_tx.clone();

        tokio::spawn(async move {
            // Create periodic counters for each connection (reset every 30 seconds)
            let mut connection_period_counts = vec![0; connections_count];
            // Create periodic counters for each symbol
            let mut symbol_period_counts = HashMap::new();
            // Create periodic counters for each interval
            let mut interval_period_counts = HashMap::new();

            loop {
                // Wait for statistics interval
                sleep(Duration::from_secs(STATS_INTERVAL)).await;
                let update_count = total_updates.load(Ordering::Relaxed);
                let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

                // Output basic statistics
                info!("[{}] WebSocket statistics: Total connections: {}, Total updates: {}, Average updates per connection: {}",
                      timestamp,
                      connections_count,
                      update_count,
                      if connections_count > 0 { update_count / connections_count } else { 0 });

                // Output updates for each connection
                let mut connection_stats = String::new();
                for (i, counter) in connection_updates.iter().enumerate() {
                    let total_count = counter.load(Ordering::Relaxed);
                    let period_count = total_count - connection_period_counts[i];
                    connection_period_counts[i] = total_count; // Update last count
                    connection_stats.push_str(&format!("Connection{}: {}(+{}), ",
                        i + 1,
                        total_count,
                        period_count));
                }
                if !connection_stats.is_empty() {
                    connection_stats.truncate(connection_stats.len() - 2); // Remove trailing comma and space
                    info!("[{}] Connection update statistics: {}", timestamp, connection_stats);
                }

                // Output updates for each interval
                let mut interval_stats = String::new();
                for (interval, counter) in &interval_updates {
                    let total_count = counter.load(Ordering::Relaxed);
                    let period_count = total_count - *interval_period_counts.get(interval).unwrap_or(&0);
                    interval_period_counts.insert(interval.clone(), total_count); // Update last count
                    interval_stats.push_str(&format!("{}: {}(+{}), ",
                        interval,
                        total_count,
                        period_count));
                }
                if !interval_stats.is_empty() {
                    interval_stats.truncate(interval_stats.len() - 2); // Remove trailing comma and space
                    info!("[{}] Interval update statistics: {}", timestamp, interval_stats);
                }

                // Output symbol updates
                {
                    let symbol_map = symbol_updates.lock().await;
                    // Collect updates for all symbols
                    let mut symbol_stats = Vec::new();
                    for (symbol, counter) in symbol_map.iter() {
                        let total_count = counter.load(Ordering::Relaxed);
                        let period_count = total_count - *symbol_period_counts.get(symbol).unwrap_or(&0);
                        symbol_period_counts.insert(symbol.clone(), total_count); // Update last count
                        // Only add symbols with updates
                        if period_count > 0 {
                            symbol_stats.push((symbol.clone(), total_count, period_count));
                        }
                    }

                    // Sort by update count
                    symbol_stats.sort_by(|a, b| b.2.cmp(&a.2));

                    // Log to file
                    if !symbol_stats.is_empty() {
                        // Send log message first, then output statistics
                        let log_message = format!("STATS [{}] Symbol update count: {}", timestamp, symbol_stats.len());
                        let _ = log_tx.send(log_message).await;

                        // Only show top 20 most active symbols
                        let display_count = symbol_stats.len().min(20);
                        let mut active_symbols_str = String::new();
                        for (i, (symbol, total, period)) in symbol_stats.iter().take(display_count).enumerate() {
                            active_symbols_str.push_str(&format!("{}: {}(+{}){}",
                                symbol.to_uppercase(),
                                total,
                                period,
                                if i < display_count - 1 { ", " } else { "" }));
                        }
                        info!("[{}] Most active symbols (top {}): {}", timestamp, display_count, active_symbols_str);
                    }
                }

                // Output update count for each symbol's each interval
                let symbol_interval_updates = SYMBOL_INTERVAL_UPDATES.lock().await;
                // Collect updates for all symbol intervals
                let mut symbol_interval_stats = Vec::new();

                for (key, counter) in symbol_interval_updates.iter() {
                    let total_count = counter.load(Ordering::Relaxed);

                    // Only add symbol intervals with updates
                    if total_count > 0 {
                        symbol_interval_stats.push((key.clone(), total_count));
                    }
                }

                // Sort by update count
                symbol_interval_stats.sort_by(|a, b| b.1.cmp(&a.1));

                // Log to file
                if !symbol_interval_stats.is_empty() {
                    // Send log message first, then output statistics
                    let log_message = format!("STATS [{}] Symbol interval update count: {}", timestamp, symbol_interval_stats.len());
                    let _ = log_tx.send(log_message).await;

                    // Only show top 50 most active symbol intervals
                    let display_count = symbol_interval_stats.len().min(50);

                    // Group by interval
                    let mut interval_groups: HashMap<String, Vec<(String, usize)>> = HashMap::new();

                    for (key, total) in symbol_interval_stats.iter().take(display_count) {
                        let parts: Vec<&str> = key.split('/').collect();
                        if parts.len() == 2 {
                            let symbol = parts[0].to_string();
                            let interval = parts[1].to_string();

                            let group = interval_groups.entry(interval).or_insert_with(Vec::new);
                            group.push((symbol, *total));
                        }
                    }

                    // Output by interval
                    for (interval, symbols) in interval_groups.iter() {
                        let mut interval_symbols_str = String::new();
                        for (i, (symbol, total)) in symbols.iter().enumerate() {
                            interval_symbols_str.push_str(&format!("{}: {}{}",
                                symbol.to_uppercase(),
                                total,
                                if i < symbols.len() - 1 { ", " } else { "" }));
                        }
                        info!("[{}] Interval {} update statistics (top {}): {}",
                            timestamp,
                            interval,
                            symbols.len(),
                            interval_symbols_str);
                    }
                }
            }
        });

        // Start a separate task to output latest kline info for each connection
        tokio::spawn(async move {
            loop {
                // Wait for statistics interval
                sleep(Duration::from_secs(STATS_INTERVAL)).await;

                // Get current timestamp
                let now = chrono::Local::now();
                let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();

                // Output latest kline info for each connection
                let latest_klines = LATEST_KLINES.lock().await;
                for conn_id in 0..connections_count {
                    if let Some(conn_klines) = latest_klines.get(&conn_id) {
                        // Only show one latest kline info for each connection
                        if !conn_klines.is_empty() {
                            // Sort by timestamp to get latest kline
                            let mut klines: Vec<_> = conn_klines.iter().collect();
                            klines.sort_by(|a, b| b.1.timestamp.cmp(&a.1.timestamp));

                            if let Some((_, kline_info)) = klines.first() {
                                info!("[{}] Connection {} latest kline: {} {} Close: {} Vol: {}",
                                    timestamp,
                                    conn_id + 1,
                                    kline_info.symbol,
                                    kline_info.interval,
                                    kline_info.close,
                                    kline_info.volume
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}
