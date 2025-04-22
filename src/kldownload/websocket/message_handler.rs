use crate::kldownload::db::Database;
use crate::kldownload::error::Result;
use crate::kldownload::models::Kline;
use crate::kldownload::websocket::models::{KlineResponse, WebSocketStreamType};
use futures_util::StreamExt;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, Mutex as TokioMutex};

// Global variables to track latest klines and update counts
pub static LATEST_KLINES: once_cell::sync::Lazy<TokioMutex<HashMap<usize, HashMap<String, KlineInfo>>>> =
    once_cell::sync::Lazy::new(|| TokioMutex::new(HashMap::new()));

pub static SYMBOL_INTERVAL_UPDATES: once_cell::sync::Lazy<TokioMutex<HashMap<String, Arc<AtomicUsize>>>> =
    once_cell::sync::Lazy::new(|| TokioMutex::new(HashMap::new()));

/// Kline information for tracking latest updates
#[derive(Debug, Clone)]
pub struct KlineInfo {
    pub symbol: String,
    pub interval: String,
    pub close: String,
    pub volume: String,
    pub timestamp: i64,
}

/// Handle kline data from WebSocket
pub async fn handle_kline_data(db: &Database, kline_resp: &KlineResponse) -> Result<()> {
    let symbol = kline_resp.symbol.to_lowercase();
    let interval = kline_resp.kline.interval.clone();
    let is_closed = kline_resp.kline.is_closed;

    // Convert WebSocket kline data to database Kline model
    let kline_data = &kline_resp.kline;
    let kline = Kline {
        open_time: kline_data.start_time,
        open: kline_data.open.clone(),
        high: kline_data.high.clone(),
        low: kline_data.low.clone(),
        close: kline_data.close.clone(),
        volume: kline_data.volume.clone(),
        close_time: kline_data.end_time,
        quote_asset_volume: kline_data.quote_volume.clone(),
        number_of_trades: kline_data.number_of_trades,
        taker_buy_base_asset_volume: kline_data.taker_buy_volume.clone(),
        taker_buy_quote_asset_volume: kline_data.taker_buy_quote_volume.clone(),
        ignore: "0".to_string(), // Add ignore field
    };

    // If it's a 1-minute kline, add data to kline aggregator
    if interval == "1m" {
        // Get kline aggregator instance
        if let Some(aggregator) = crate::kldownload::aggregator::KlineAggregator::get_instance() {
            // Add kline data to aggregator
            aggregator.add_1m_kline(&symbol, kline.clone());
        } else {
            error!("Failed to get kline aggregator instance");
        }
    }

    // Determine whether to insert or update based on is_closed
    if is_closed {
        // If kline is closed, insert new record
        debug!("Inserting closed kline: symbol={}, interval={}, start_time={}", symbol, interval, kline_data.start_time);
        db.insert_kline(&symbol, &interval, &kline)?;
    } else {
        // If kline is not closed, update existing record
        debug!("Updating unclosed kline: symbol={}, interval={}, start_time={}", symbol, interval, kline_data.start_time);
        db.update_kline(&symbol, &interval, &kline)?;
    }

    Ok(())
}

/// Process WebSocket messages
pub async fn process_messages(
    connection_id: usize,
    mut ws_stream: WebSocketStreamType,
    db: Arc<Database>,
    total_updates: Arc<AtomicUsize>,
    connection_updates: Arc<AtomicUsize>,
    symbol_updates: Arc<TokioMutex<std::collections::HashMap<String, Arc<AtomicUsize>>>>,
    interval_updates: std::collections::HashMap<String, Arc<AtomicUsize>>,
    log_tx: Sender<String>,
) -> Result<()> {
    info!("Starting message processing for connection {}", connection_id + 1);

    // Clone log sender
    let log_tx_clone = log_tx.clone();

    loop {
        tokio::select! {
            Some(msg) = ws_stream.next() => {
                match msg {
                    Ok(msg) => {
                        if msg.is_text() {
                            let text = msg.into_text().unwrap_or_default();

                            // Log all messages for debugging
                            let log_message = format!("Connection {}: {}", connection_id + 1, text);
                            if let Err(e) = log_tx_clone.send(log_message).await {
                                error!("Connection {}: Failed to send message to logging task: {}", connection_id + 1, e);
                            }

                            // No longer printing message previews to reduce log volume

                            // Only log received raw text messages at debug level
                            // No longer printing content of each message, only counting
                            static RECEIVED_MESSAGES: AtomicUsize = AtomicUsize::new(0);
                            let msg_count = RECEIVED_MESSAGES.fetch_add(1, Ordering::Relaxed);
                            if log::log_enabled!(log::Level::Debug) && msg_count % 1000 == 0 {
                                debug!("Connection {} has received {} messages", connection_id + 1, msg_count);
                            }

                            // Try to parse as kline response
                            let kline_parse_result = serde_json::from_str::<KlineResponse>(&text);
                            if let Ok(kline_resp) = kline_parse_result {
                                // Only process continuous kline events, consistent with API docs and test program
                                if kline_resp.event_type == "continuous_kline" {
                                    // Update counters
                                    let count = total_updates.fetch_add(1, Ordering::Relaxed);
                                    connection_updates.fetch_add(1, Ordering::Relaxed);

                                    // Update latest kline info in global variable
                                    let kline_info = KlineInfo {
                                        symbol: kline_resp.symbol.clone(),
                                        interval: kline_resp.kline.interval.clone(),
                                        close: kline_resp.kline.close.clone(),
                                        volume: kline_resp.kline.volume.clone(),
                                        timestamp: kline_resp.kline.start_time,
                                    };

                                    // Use symbol_interval as key to distinguish klines for different pairs and intervals
                                    let key = format!("{}/{}", kline_resp.symbol.to_lowercase(), kline_resp.kline.interval);

                                    // Update latest kline info
                                    let mut latest_klines = LATEST_KLINES.lock().await;
                                    let conn_klines = latest_klines.entry(connection_id).or_insert_with(HashMap::new);
                                    conn_klines.insert(key.clone(), kline_info);

                                    // Update update count for each contract's each interval
                                    let mut symbol_interval_updates = SYMBOL_INTERVAL_UPDATES.lock().await;
                                    let counter = symbol_interval_updates.entry(key.clone()).or_insert_with(|| {
                                        Arc::new(AtomicUsize::new(0))
                                    });
                                    counter.fetch_add(1, Ordering::Relaxed);

                                    // No longer printing each kline update, only showing update count in statistics
                                    // Print log every 1000 updates
                                    if count % 1000 == 0 {
                                        info!("Connection {} processed {} kline updates, latest: {} {} Close: {}",
                                            connection_id + 1,
                                            count,
                                            kline_resp.symbol,
                                            kline_resp.kline.interval,
                                            kline_resp.kline.close
                                        );
                                    }

                                    // Update symbol counter
                                    let symbol = kline_resp.symbol.to_lowercase();
                                    {
                                        let mut symbol_map = symbol_updates.lock().await;
                                        if let Some(counter) = symbol_map.get(&symbol) {
                                            counter.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            // If symbol not in map, add it
                                            symbol_map.insert(symbol.clone(), Arc::new(AtomicUsize::new(1)));
                                        }
                                    }

                                    // Update interval counter
                                    let interval = kline_resp.kline.interval.clone();
                                    if let Some(counter) = interval_updates.get(&interval) {
                                        counter.fetch_add(1, Ordering::Relaxed);
                                    }

                                    // Process kline data - determine whether to insert or update based on is_closed
                                    if let Err(e) = handle_kline_data(&db, &kline_resp).await {
                                        error!("Failed to process kline data: {}", e);
                                    }

                                    // No longer duplicating logs, already handled above
                                } else {
                                    // Log non-continuous kline events
                                    debug!("Connection {} received non-continuous kline event: {}", connection_id + 1, kline_resp.event_type);
                                }
                            } else {
                                // Log parsing errors
                                if let Err(e) = &kline_parse_result {
                                    info!("Connection {} failed to parse KlineResponse: {}", connection_id + 1, e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        error!("Connection {} WebSocket error: {}", connection_id + 1, e);
                        break;
                    }
                }
            },
            else => {
                info!("Message processing for connection {} has ended", connection_id + 1);
                break;
            }
        }
    }

    info!("Connection {} closed", connection_id + 1);
    Ok(())
}
