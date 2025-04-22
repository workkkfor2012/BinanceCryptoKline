// WebSocket client main module
use crate::kldownload::db::Database;
use crate::kldownload::error::{AppError, Result};
use crate::kldownload::websocket::config::{ContinuousKlineConfig, MAX_STREAMS_PER_CONNECTION};
use crate::kldownload::websocket::connection::ConnectionManager;
use crate::kldownload::websocket::message_handler::process_messages;
use crate::kldownload::websocket::models::WebSocketConnection;
use crate::kldownload::websocket::stats::StatsManager;

use log::{info, error, warn};
use std::collections::HashMap;
use std::fs::{OpenOptions, create_dir_all};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use futures_util::future::join_all;

/// Continuous kline WebSocket client
pub struct ContinuousKlineClient {
    config: ContinuousKlineConfig,
    db: Arc<Database>,
    connections: Vec<WebSocketConnection>,
    // Statistics data
    total_updates: Arc<AtomicUsize>,
    symbol_updates: Arc<TokioMutex<HashMap<String, Arc<AtomicUsize>>>>,
    interval_updates: HashMap<String, Arc<AtomicUsize>>,
}

impl ContinuousKlineClient {
    /// Create a new WebSocket client
    pub fn new(config: ContinuousKlineConfig, db: Arc<Database>) -> Self {
        // Initialize symbol update counters
        let mut symbol_updates_map = HashMap::new();
        for symbol in &config.symbols {
            symbol_updates_map.insert(symbol.to_lowercase(), Arc::new(AtomicUsize::new(0)));
        }

        // Initialize interval update counters
        let mut interval_updates_map = HashMap::new();
        for interval in &config.intervals {
            interval_updates_map.insert(interval.clone(), Arc::new(AtomicUsize::new(0)));
        }

        Self {
            config,
            db,
            connections: Vec::new(),
            total_updates: Arc::new(AtomicUsize::new(0)),
            symbol_updates: Arc::new(TokioMutex::new(symbol_updates_map)),
            interval_updates: interval_updates_map,
        }
    }

    /// Start WebSocket client
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting continuous kline WebSocket client");

        // Create log directory
        let log_dir = Path::new("./logs");
        if !log_dir.exists() {
            create_dir_all(log_dir).map_err(|e| AppError::IoError(e))?;
        }

        // Create log file
        let log_file_path = log_dir.join("continuous_kline_messages.log");
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .map_err(|e| AppError::IoError(e))?;

        info!("Logging ContinuousKline WebSocket messages to {}", log_file_path.display());

        // Create log channel
        let (log_tx, mut log_rx) = mpsc::channel::<String>(10000);

        // Start log task
        let log_file_clone = Arc::new(TokioMutex::new(log_file));
        tokio::spawn(async move {
            while let Some(message) = log_rx.recv().await {
                let mut file = log_file_clone.lock().await;
                let _ = writeln!(file, "{}", message);
                let _ = file.flush();
            }
        });

        // Show proxy configuration
        if self.config.use_proxy {
            info!("Using proxy: {}:{}", self.config.proxy_addr, self.config.proxy_port);
        } else {
            info!("Not using proxy, connecting directly");
        }

        // Show subscription information
        info!("Number of symbols: {}", self.config.symbols.len());
        info!("Number of intervals: {}", self.config.intervals.len());

        // Prepare all streams to subscribe
        let all_streams = self.prepare_streams();
        let total_streams = all_streams.len();
        info!("Total streams to subscribe: {}", total_streams);

        // Assign streams by interval, each connection handles one interval
        let intervals = &self.config.intervals;
        let interval_count = intervals.len();

        // Determine connection count, one connection per interval
        let connection_count = interval_count;
        info!("Assigning by interval, {} intervals, using {} connections", interval_count, connection_count);

        // Calculate number of symbols per interval
        let symbols_per_interval = self.config.symbols.len();

        // Calculate maximum streams per connection
        let max_streams_per_connection = symbols_per_interval;

        // Check if streams per connection exceeds MAX_STREAMS_PER_CONNECTION
        if max_streams_per_connection > MAX_STREAMS_PER_CONNECTION {
            warn!("Each connection needs to handle {} streams, exceeding maximum streams per connection: {}",
                  max_streams_per_connection, MAX_STREAMS_PER_CONNECTION);

            // Calculate actual number of symbols that can be handled
            let actual_symbols = MAX_STREAMS_PER_CONNECTION;
            info!("Will only process {} symbols", actual_symbols);
        }

        // Calculate actual number of symbols that can be handled
        let actual_symbols = std::cmp::min(symbols_per_interval, MAX_STREAMS_PER_CONNECTION);

        info!("Each connection handles all symbols for one interval, {} streams per connection", actual_symbols);
        info!("Total streams: {}, actually processing {} streams ({} intervals * {} symbols)",
              total_streams, actual_symbols * interval_count, interval_count, actual_symbols);

        // Create stream list for each connection
        let mut connection_streams: Vec<Vec<String>> = vec![Vec::new(); connection_count];

        // Assign streams by interval
        for (i, interval) in intervals.iter().enumerate() {
            // Only process first actual_symbols symbols
            for symbol in self.config.symbols.iter().take(actual_symbols) {
                // Use continuous kline format
                // Binance WebSocket subscription format: <symbol>_perpetual@continuousKline_<interval>
                let stream_name = format!("{}_perpetual@continuousKline_{}", symbol.to_lowercase(), interval);
                connection_streams[i].push(stream_name);
            }
        }

        // Create connection manager
        let connection_manager = ConnectionManager::new(
            self.config.use_proxy,
            self.config.proxy_addr.clone(),
            self.config.proxy_port,
        );

        // Create and connect all WebSocket connections
        for i in 0..connection_count {
            // Use streams we've already assigned
            let batch_streams = &connection_streams[i];

            // Skip if this connection has no streams to handle
            if batch_streams.is_empty() {
                continue;
            }

            info!("Creating connection {} (with {} streams)", i + 1, batch_streams.len());

            let mut reconnect_attempts = 0;
            loop {
                match connection_manager.connect_and_subscribe(i, batch_streams.to_vec()).await {
                    Ok(connection) => {
                        info!("WebSocket connection {} established and subscribed", i + 1);

                        // Print symbols and intervals subscribed by this connection
                        let mut symbol_interval_map: HashMap<String, Vec<String>> = HashMap::new();

                        // Parse stream names to extract symbol and interval information
                        for stream in &connection.streams {
                            // Stream format: symbol_perpetual@continuousKline_interval
                            if let Some(parts) = stream.split_once("_perpetual@continuousKline_") {
                                let symbol = parts.0.to_uppercase();
                                let interval = parts.1.to_string();

                                symbol_interval_map.entry(symbol)
                                    .or_insert_with(Vec::new)
                                    .push(interval);
                            }
                        }

                        // Print subscription details
                        // info!("Connection {} subscription details:", i + 1);
                        // for (symbol, intervals) in &symbol_interval_map {
                        //     info!("  Symbol: {}, Intervals: {}", symbol, intervals.join(", "));
                        // }
                        info!("Connection {} subscribed to {} symbols, {} streams", i + 1, symbol_interval_map.len(), connection.streams.len());

                        // Add to connection list
                        self.connections.push(connection);
                        break;
                    },
                    Err(e) => {
                        reconnect_attempts += 1;
                        error!("Connection {} failed to connect (attempt {}/10): {}", i + 1, reconnect_attempts, e);
                        if reconnect_attempts >= 10 {
                            return Err(AppError::WebSocketError(format!("Connection {} failed to connect, maximum retry attempts reached", i + 1)));
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }

        // Collect update counters for each connection
        let connection_updates: Vec<Arc<AtomicUsize>> = self.connections.iter()
            .map(|conn| Arc::clone(&conn.update_count))
            .collect();

        // Create statistics manager
        let stats_manager = StatsManager::new(
            Arc::clone(&self.total_updates),
            self.connections.len(),
            connection_updates.clone(),
            Arc::clone(&self.symbol_updates),
            self.interval_updates.clone(),
            log_tx.clone(),
        );

        // Start statistics task
        stats_manager.start_stats_task().await;

        // Process messages from all connections
        self.process_all_connections(log_tx.clone()).await
    }

    /// Prepare all streams to subscribe
    fn prepare_streams(&self) -> Vec<String> {
        let mut stream_names = Vec::new();

        // Create subscription for each symbol and interval
        for symbol in &self.config.symbols {
            for interval in &self.config.intervals {
                // Use continuous kline format
                // Binance WebSocket subscription format: <symbol>_perpetual@continuousKline_<interval>
                let stream_name = format!("{}_perpetual@continuousKline_{}", symbol.to_lowercase(), interval);
                stream_names.push(stream_name);
            }
        }

        stream_names
    }

    /// Process messages from all WebSocket connections
    async fn process_all_connections(&mut self, log_tx: mpsc::Sender<String>) -> Result<()> {
        if self.connections.is_empty() {
            return Err(AppError::WebSocketError("No WebSocket connections available".to_string()));
        }

        // Create processing tasks
        let mut tasks = Vec::new();
        for (i, connection) in self.connections.iter_mut().enumerate() {
            if let Some(ws_stream) = connection.ws_stream.take() {
                let db = Arc::clone(&self.db);
                let total_updates = Arc::clone(&self.total_updates);
                let connection_updates = Arc::clone(&connection.update_count);
                let symbol_updates = Arc::clone(&self.symbol_updates);
                let interval_updates = self.interval_updates.clone();
                let log_tx_clone = log_tx.clone();

                // Start message processing task
                let task = tokio::spawn(async move {
                    if let Err(e) = process_messages(
                        i,
                        ws_stream,
                        db,
                        total_updates,
                        connection_updates,
                        symbol_updates,
                        interval_updates,
                        log_tx_clone,
                    ).await {
                        error!("Connection {} message processing error: {}", i + 1, e);
                    }
                });

                tasks.push(task);
            }
        }

        // Wait for all tasks to complete
        join_all(tasks).await;

        Ok(())
    }
}

/// Test continuous kline client
#[cfg(test)]
pub struct TestContinuousKlineClient {
    // Test client implementation
}

#[cfg(test)]
impl TestContinuousKlineClient {
    /// Create default configuration
    pub fn default_config() -> ContinuousKlineConfig {
        ContinuousKlineConfig {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols: vec!["btcusdt".to_string(), "ethusdt".to_string()],
            intervals: vec!["1m".to_string()],
        }
    }

    /// Create a new test client
    pub fn new(_config: ContinuousKlineConfig, _db: Arc<Database>) -> Self {
        Self {}
    }

    /// Start test client
    pub async fn start(&mut self) -> Result<()> {
        // Test client startup logic
        Ok(())
    }
}
