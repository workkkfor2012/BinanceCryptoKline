use serde_json::json;

/// 币安WebSocket URL
pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/stream";

/// 每个连接的最大流数量
pub const MAX_STREAMS_PER_CONNECTION: usize = 1;

/// 连续合约K线配置
#[derive(Clone)]
pub struct ContinuousKlineConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    /// 代理地址
    pub proxy_addr: String,
    /// 代理端口
    pub proxy_port: u16,
    /// 交易对列表
    pub symbols: Vec<String>,
    /// K线周期列表
    pub intervals: Vec<String>,
}

/// 创建订阅消息
pub fn create_subscribe_message(streams: &[String]) -> String {
    json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    })
    .to_string()
}
