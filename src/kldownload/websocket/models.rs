// WebSocket消息和K线数据模块
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio_tungstenite::WebSocketStream;
use tokio_rustls::client::TlsStream;
use tokio_socks::tcp::Socks5Stream;
use tokio::net::TcpStream;

/// K线数据结构
#[derive(Debug, Serialize, Deserialize)]
pub struct KlineData {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub end_time: i64,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: String,
    #[serde(rename = "B")]
    pub ignore: String,
}

/// K线响应结构
#[derive(Debug, Serialize, Deserialize)]
pub struct KlineResponse {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "ps")]
    pub symbol: String,
    #[serde(rename = "ct")]
    pub contract_type: String,
    #[serde(rename = "k")]
    pub kline: KlineData,
}

/// WebSocket连接类型
pub type WebSocketStreamType = WebSocketStream<TlsStream<Socks5Stream<TcpStream>>>;

/// WebSocket连接
pub struct WebSocketConnection {
    #[allow(dead_code)]
    pub id: usize,
    pub streams: Vec<String>,
    pub ws_stream: Option<WebSocketStreamType>,
    // 添加计数器记录每个连接的更新数
    pub update_count: Arc<AtomicUsize>,
}

impl WebSocketConnection {
    /// 创建新的WebSocket连接
    pub fn new(id: usize, streams: Vec<String>) -> Self {
        Self {
            id,
            streams,
            ws_stream: None,
            update_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

