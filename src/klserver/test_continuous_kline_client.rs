use super::db::Database;
use super::error::{AppError, Result};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
// use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_rustls::{TlsConnector, rustls::{ClientConfig, RootCertStore}};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    tungstenite::{protocol::Message, protocol::WebSocketConfig},
    client_async_with_config,
};
use futures_util::{SinkExt, StreamExt};
use url::Url;
use rustls_native_certs;
use http;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex; // Add Mutex for thread-safe mutable access

// Binance WebSocket base URL
const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws";

// Maximum subscriptions per WebSocket connection
// Set to a small value so each connection only subscribes to a few symbols
const MAX_STREAMS_PER_CONNECTION: usize = 2; // 5 connections * 2 symbols * 1 interval = 10 streams total, 2 streams per connection

// Reconnect delay (seconds)
const RECONNECT_DELAY: u64 = 1;

// Maximum reconnect attempts (5分钟 × 60秒 ÷ 1秒 = 300次)
const MAX_RECONNECT_ATTEMPTS: u32 = 300;

// Heartbeat interval (seconds)
const HEARTBEAT_INTERVAL: u64 = 30;

// Statistics output interval (seconds)
const STATS_INTERVAL: u64 = 60;

/// Import WebSocket client configuration
pub use crate::kldownload::websocket::ContinuousKlineConfig;

/// Local test configuration
#[derive(Debug, Clone)]
pub struct TestContinuousKlineConfig {
    /// Whether to use proxy
    #[allow(dead_code)]
    pub use_proxy: bool,
    /// Proxy address
    pub proxy_addr: String,
    /// Proxy port
    pub proxy_port: u16,
    /// List of symbols to subscribe to
    pub symbols: Vec<String>,
    /// List of intervals to subscribe to
    pub intervals: Vec<String>,
}

impl Default for TestContinuousKlineConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols: vec![
                "btcusdt".to_string(),
                "ethusdt".to_string(),
                "bnbusdt".to_string(),
                "xrpusdt".to_string(),
                "adausdt".to_string(),
                "dogeusdt".to_string(),
                "solusdt".to_string(),
                "dotusdt".to_string(),
                "maticusdt".to_string(),
                "ltcusdt".to_string(),
            ], // Subscribe to ten symbols
            intervals: vec!["1m".to_string()], // Only subscribe to 1-minute interval
        }
    }
}

/// WebSocket subscription message
#[derive(Debug, Serialize, Deserialize)]
struct SubscribeMessage {
    method: String,
    params: Vec<String>,
    id: i32,
}

/// WebSocket subscription response
#[derive(Debug, Serialize, Deserialize)]
struct SubscribeResponse {
    result: Option<serde_json::Value>,
    id: i32,
}

/// Test subscription response
#[derive(Debug, Serialize, Deserialize)]
struct TestSubscribeResponse {
    id: i32,
    result: Option<Vec<String>>,
    error: Option<serde_json::Value>,
}

/// WebSocket kline data response
#[derive(Debug, Serialize, Deserialize)]
struct KlineResponse {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "ps")]
    symbol: String,
    #[serde(rename = "ct")]
    contract_type: String,
    #[serde(rename = "k")]
    kline: KlineData,
}

/// Kline data structure
#[derive(Debug, Serialize, Deserialize)]
struct KlineData {
    #[serde(rename = "t")]
    start_time: i64,
    #[serde(rename = "T")]
    end_time: i64,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "f")]
    first_trade_id: i64,
    #[serde(rename = "L")]
    last_trade_id: i64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    number_of_trades: i64,
    #[serde(rename = "x")]
    is_closed: bool,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "V")]
    taker_buy_volume: String,
    #[serde(rename = "Q")]
    taker_buy_quote_volume: String,
    #[serde(rename = "B")]
    ignore: String,
}

/// WebSocket connection
struct WebSocketConnection {
    id: usize,
    #[allow(dead_code)]
    streams: Vec<String>,
    ws_stream: Option<tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<tokio_socks::tcp::Socks5Stream<tokio::net::TcpStream>>>>,
}

/// Continuous kline WebSocket client
pub struct ContinuousKlineClient {
    config: ContinuousKlineConfig,
    db: Arc<Database>,
    connections: Vec<WebSocketConnection>,
    // Statistics data
    total_updates: Arc<AtomicUsize>,
    #[allow(dead_code)]
    symbol_updates: HashMap<String, Arc<AtomicUsize>>,
    #[allow(dead_code)]
    interval_updates: HashMap<String, Arc<AtomicUsize>>,
}

impl ContinuousKlineClient {
    /// Create a new WebSocket client
    pub fn new(config: ContinuousKlineConfig, db: Arc<Database>) -> Self {
        Self {
            config,
            db,
            connections: Vec::new(),
            total_updates: Arc::new(AtomicUsize::new(0)),
            symbol_updates: HashMap::new(),
            interval_updates: HashMap::new(),
        }
    }

    /// Provide a default configuration for testing
    pub fn default_config() -> ContinuousKlineConfig {
        ContinuousKlineConfig {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols: vec![
                "btcusdt".to_string(),
                "ethusdt".to_string(),
                "bnbusdt".to_string(),
                "xrpusdt".to_string(),
                "adausdt".to_string(),
                "dogeusdt".to_string(),
                "solusdt".to_string(),
                "dotusdt".to_string(),
                "maticusdt".to_string(),
                "ltcusdt".to_string(),
            ], // Subscribe to ten symbols
            intervals: vec!["1m".to_string()], // Only subscribe to 1-minute interval
        }
    }

    /// Start WebSocket client
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting continuous kline WebSocket client");
        info!("Using proxy: {}:{}", self.config.proxy_addr, self.config.proxy_port);
        info!("Number of symbols: {}", self.config.symbols.len());
        info!("Number of intervals: {}", self.config.intervals.len());

        // Create logging channel
        let (log_tx, mut log_rx) = mpsc::channel::<String>(1000); // Channel for log messages

        // Spawn logging task
        tokio::spawn(async move {
            let log_dir = Path::new("./logs");
            if let Err(e) = tokio::fs::create_dir_all(log_dir).await {
                error!("Failed to create log directory: {}", e);
                return; // Exit task if directory creation fails
            }
            let log_path = log_dir.join("continuous_kline_messages.log");
            let file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
            {
                Ok(file) => Arc::new(Mutex::new(file)), // Wrap file in Arc<Mutex>
                Err(e) => {
                    error!("Failed to open log file {}: {}", log_path.display(), e);
                    return; // Exit task if file opening fails
                }
            };
            info!("Logging ContinuousKline WebSocket messages to {}", log_path.display());

            while let Some(message) = log_rx.recv().await {
                let file_clone = Arc::clone(&file); // Clone the Arc for the blocking task
                // Use spawn_blocking for file writing to avoid blocking the async runtime
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    let mut file = file_clone.lock().unwrap(); // Lock the mutex to get mutable access
                    writeln!(file, "{}", message)
                }).await.unwrap() { // unwrap() the Result from spawn_blocking
                    error!("Failed to write to log file: {}", e);
                    // Continue receiving messages even if a write fails
                }
            }
            info!("ContinuousKline logging task finished."); // This will be printed when all senders are dropped
        });

        // Prepare all streams to subscribe
        let all_streams = self.prepare_streams();
        let total_streams = all_streams.len();
        info!("Total streams to subscribe: {}", total_streams);

        // Calculate required connections
        let connection_count = (total_streams + MAX_STREAMS_PER_CONNECTION - 1) / MAX_STREAMS_PER_CONNECTION;
        info!("Will use {} WebSocket connections", connection_count);

        // Create connections and subscribe in batches
        for i in 0..connection_count {
            let start_idx = i * MAX_STREAMS_PER_CONNECTION;
            let end_idx = std::cmp::min((i + 1) * MAX_STREAMS_PER_CONNECTION, total_streams);
            let batch_streams = all_streams[start_idx..end_idx].to_vec();

            info!("Creating connection {} (with {} streams)", i + 1, batch_streams.len());

            let mut reconnect_attempts = 0;
            loop {
                match self.connect_and_subscribe(i, batch_streams.clone()).await {
                    Ok(connection) => {
                        info!("WebSocket connection {} established and subscribed", i + 1);
                        self.connections.push(connection);
                        break;
                    }
                    Err(e) => {
                        error!("Connection {} failed to establish or subscribe: {}", i + 1, e);
                        reconnect_attempts += 1;

                        if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                            error!("Connection {} reached maximum reconnect attempts, giving up", i + 1);
                            return Err(AppError::WebSocketError(format!("Connection {} reached maximum reconnect attempts", i + 1)));
                        }

                        let delay = RECONNECT_DELAY; // 固定1秒间隔，不使用指数退避
                        info!("Will reconnect in {} seconds...", delay);
                        sleep(Duration::from_secs(delay)).await;
                    }
                }
            }
        }

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

    /// Connect to WebSocket server and subscribe to streams
    async fn connect_and_subscribe(&self, id: usize, streams: Vec<String>) -> Result<WebSocketConnection> {
        info!("Connection {} connecting to Binance WebSocket server...", id + 1);

        // Use base URL
        let url = Url::parse(BINANCE_WS_URL)?;
        let host = url.host_str().ok_or_else(|| AppError::WebSocketError("Invalid URL host".to_string()))?;
        let port = url.port_or_known_default().ok_or_else(|| AppError::WebSocketError("Invalid URL port".to_string()))?;
        let target_addr = format!("{}:{}", host, port);

        info!("Connection {} target server: {}", id + 1, target_addr);

        // 1. Establish TCP connection through SOCKS5 proxy
        let proxy_addr_str = format!("{}:{}", self.config.proxy_addr, self.config.proxy_port);
        let proxy_addr: SocketAddr = proxy_addr_str.parse()?;
        let socks_stream = Socks5Stream::connect(proxy_addr, target_addr.clone()).await
            .map_err(|e| AppError::WebSocketError(format!("SOCKS5 connection failed: {}", e)))?;
        info!("Connection {} TCP connection established successfully through SOCKS5 proxy", id + 1);

        // 2. Establish TLS connection on top of TCP stream
        // Load system root certificates
        let mut root_cert_store = RootCertStore::empty();
        let certs = rustls_native_certs::load_native_certs()
            .map_err(|e| AppError::WebSocketError(format!("Failed to load platform certificates: {}", e)))?;
        for cert in certs {
            root_cert_store.add(&rustls::Certificate(cert.0))
                .map_err(|e| AppError::WebSocketError(format!("Failed to add certificate to store: {}", e)))?;
        }

        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = rustls::ServerName::try_from(host)
            .map_err(|_| AppError::WebSocketError(format!("Invalid DNS name: {}", host)))?;

        let tls_stream = connector.connect(server_name, socks_stream).await
            .map_err(|e| AppError::WebSocketError(format!("TLS connection failed: {}", e)))?;
        info!("Connection {} TLS connection established successfully", id + 1);

        // 3. Perform WebSocket handshake on top of TLS stream
        let ws_config = WebSocketConfig {
            max_message_size: Some(64 << 20), // 64 MiB
            max_frame_size: Some(16 << 20),   // 16 MiB
            accept_unmasked_frames: false,
            ..Default::default()
        };

        let request = http::Request::builder()
            .method("GET")
            .uri(url.as_str())
            .header("Host", host)
            .header("User-Agent", "Rust Kline Client/1.0")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .body(())?;

        info!("Connection {} performing WebSocket handshake...", id + 1);
        let (ws_stream, response) = client_async_with_config(request, tls_stream, Some(ws_config)).await
            .map_err(|e| AppError::WebSocketError(format!("WebSocket handshake failed: {}", e)))?;

        info!("Connection {} WebSocket handshake successful, server response: {:?}", id + 1, response.status());

        // 4. Subscribe to streams
        let mut connection = WebSocketConnection {
            id,
            streams: streams.clone(),
            ws_stream: Some(ws_stream),
        };

        // Create subscription message
        let subscribe_msg = SubscribeMessage {
            method: "SUBSCRIBE".to_string(),
            params: streams,
            id: (id + 1) as i32,
        };

        // Send subscription message
        let subscribe_text = serde_json::to_string(&subscribe_msg)?;
        info!("Connection {} sending subscription request: {}", id + 1, subscribe_text);

        if let Some(ws_stream) = &mut connection.ws_stream {
            ws_stream.send(Message::Text(subscribe_text.clone())).await
                .map_err(|e| AppError::WebSocketError(format!("Failed to send subscription message: {}", e)))?;
            info!("Connection {} subscription request sent", id + 1);
        }

        Ok(connection)
    }

    /// Process messages from all WebSocket connections
    async fn process_all_connections(&mut self, log_tx: Sender<String>) -> Result<()> {
        if self.connections.is_empty() {
            return Err(AppError::WebSocketError("No WebSocket connections available".to_string()));
        }

        // Create statistics output task
        let total_updates = Arc::clone(&self.total_updates);
        let connections_count = self.connections.len();
        let stats_task = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(STATS_INTERVAL)).await;
                let update_count = total_updates.load(Ordering::Relaxed);
                info!("WebSocket statistics: Total connections: {}, Total updates: {}, Average updates per connection: {}",
                      connections_count,
                      update_count,
                      if connections_count > 0 { update_count / connections_count } else { 0 });
            }
        });

        // Move connections to a new vector to resolve lifetime issues
        let mut connections = Vec::new();
        std::mem::swap(&mut connections, &mut self.connections);

        // Create a processing task for each connection
        let mut handles = Vec::new();
        let db = self.db.clone();
        let total_updates = Arc::clone(&self.total_updates);

        for mut connection in connections {
            let _db = db.clone();
            let total_updates = total_updates.clone();
            let log_tx_clone = log_tx.clone(); // Clone log_tx for each spawned task

            let handle = tokio::spawn(async move {
                let connection_id = connection.id + 1;
                info!("Starting message processing for connection {}", connection_id);

                if let Some(mut ws_stream) = connection.ws_stream.take() {
                    let mut last_ping_time = std::time::Instant::now();

                    loop {
                        tokio::select! {
                            Some(msg) = ws_stream.next() => {
                                match msg {
                                    Ok(msg) => {
                                        if msg.is_text() {
                                            let text = msg.into_text().unwrap_or_default();

                                            // Send message to logging task, including connection ID
                                            let log_message = format!("Connection {}: {}", connection_id, text);
                                            if let Err(e) = log_tx_clone.send(log_message).await {
                                                error!("Connection {}: Failed to send message to logging task: {}", connection_id, e);
                                                // If sending fails, we can choose to stop processing messages or continue
                                                // Here we choose to continue, just logging the error
                                            }

                                            // Log received raw text message
                                            debug!("Connection {} received text message: {}", connection_id, text);

                                            // Try to parse as kline response
                                            if let Ok(kline_resp) = serde_json::from_str::<KlineResponse>(&text) {
                                                // Only process continuous kline events
                                                if kline_resp.event_type == "continuous_kline" {
                                                    // Update counter
                                                    total_updates.fetch_add(1, Ordering::Relaxed);

                                                    // K线更新处理（已移除控制台输出）

                                                    // Update counter
                                                    total_updates.fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        error!("Connection {} WebSocket error: {}", connection_id, e);
                                        break;
                                    }
                                }
                            },
                            _ = sleep(Duration::from_secs(1)) => {
                                // Check if it's time to send a ping
                                let now = std::time::Instant::now();
                                if now.duration_since(last_ping_time).as_secs() >= HEARTBEAT_INTERVAL {
                                    debug!("Connection {} sending ping", connection_id);
                                    if let Err(e) = ws_stream.send(Message::Ping(vec![])).await {
                                        error!("Connection {} failed to send ping: {}", connection_id, e);
                                        break;
                                    }
                                    last_ping_time = now;
                                }
                            }
                        }
                    }
                }

                info!("Connection {} message processing ended", connection_id);
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Task join error: {}", e);
            }
        }

        // Cancel stats task
        stats_task.abort();

        Ok(())
    }
}
