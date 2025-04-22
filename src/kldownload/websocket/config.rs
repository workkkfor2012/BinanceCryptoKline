use serde::{Deserialize, Serialize};

// Binance WebSocket URL
pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws";

// Statistics output interval in seconds
pub const STATS_INTERVAL: u64 = 30;

// Maximum streams per connection
// In production, this would be higher, but for testing we use a lower value
// to make logs more readable and simplify testing
pub const MAX_STREAMS_PER_CONNECTION: usize = 1;

// Maximum number of reconnect attempts
#[allow(dead_code)]
pub const MAX_RECONNECT_ATTEMPTS: usize = 5;

// Reconnect delay in milliseconds
#[allow(dead_code)]
pub const RECONNECT_DELAY_MS: u64 = 5000;

// Ping interval in seconds
#[allow(dead_code)]
pub const PING_INTERVAL_SECONDS: u64 = 30;

// Maximum connections
#[allow(dead_code)]
pub const MAX_CONNECTIONS: usize = 6; // One connection per interval, 6 intervals total

/// WebSocket client configuration
#[derive(Debug, Clone)]
pub struct ContinuousKlineConfig {
    /// Whether to use proxy
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

impl Default for ContinuousKlineConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols: vec!["btcusdt".to_string()],
            intervals: vec!["1m".to_string(), "5m".to_string(), "30m".to_string(), "4h".to_string(), "1d".to_string(), "1w".to_string()],
        }
    }
}

/// WebSocket subscription message
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeMessage {
    pub method: String,
    pub params: Vec<String>,
    pub id: i32,
}

/// Create subscription message
pub fn create_subscribe_message(streams: Vec<String>, id: i32) -> SubscribeMessage {
    SubscribeMessage {
        method: "SUBSCRIBE".to_string(),
        params: streams,
        id,
    }
}
