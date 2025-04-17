use serde::{Deserialize, Serialize};
// 移除未使用的导入

/// Represents a Binance kline/candlestick
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    /// Kline open time
    pub open_time: i64,
    /// Open price
    pub open: String,
    /// High price
    pub high: String,
    /// Low price
    pub low: String,
    /// Close price
    pub close: String,
    /// Volume
    pub volume: String,
    /// Kline close time
    pub close_time: i64,
    /// Quote asset volume
    pub quote_asset_volume: String,
    /// Number of trades
    pub number_of_trades: i64,
    /// Taker buy base asset volume
    pub taker_buy_base_asset_volume: String,
    /// Taker buy quote asset volume
    pub taker_buy_quote_asset_volume: String,
    /// Ignore
    pub ignore: String,
}

impl Kline {
    /// Convert a raw kline array from Binance API to a Kline struct
    pub fn from_raw_kline(raw: &[serde_json::Value]) -> Option<Self> {
        if raw.len() < 12 {
            return None;
        }

        // Helper function to convert Value to String
        let to_string = |v: &serde_json::Value| -> Option<String> {
            if v.is_string() {
                Some(v.as_str()?.to_string())
            } else if v.is_number() {
                Some(v.to_string())
            } else {
                None
            }
        };

        Some(Kline {
            open_time: raw[0].as_i64()?,
            open: to_string(&raw[1])?,
            high: to_string(&raw[2])?,
            low: to_string(&raw[3])?,
            close: to_string(&raw[4])?,
            volume: to_string(&raw[5])?,
            close_time: raw[6].as_i64()?,
            quote_asset_volume: to_string(&raw[7])?,
            number_of_trades: raw[8].as_i64()?,
            taker_buy_base_asset_volume: to_string(&raw[9])?,
            taker_buy_quote_asset_volume: to_string(&raw[10])?,
            ignore: to_string(&raw[11]).unwrap_or_default(),
        })
    }

    /// Convert to CSV record
    pub fn to_csv_record(&self) -> Vec<String> {
        vec![
            self.open_time.to_string(),
            self.open.clone(),
            self.high.clone(),
            self.low.clone(),
            self.close.clone(),
            self.volume.clone(),
            self.close_time.to_string(),
            self.quote_asset_volume.clone(),
            self.number_of_trades.to_string(),
            self.taker_buy_base_asset_volume.clone(),
            self.taker_buy_quote_asset_volume.clone(),
            self.ignore.clone(),
        ]
    }

    /// Get CSV headers
    pub fn csv_headers() -> Vec<String> {
        vec![
            "open_time".to_string(),
            "open".to_string(),
            "high".to_string(),
            "low".to_string(),
            "close".to_string(),
            "volume".to_string(),
            "close_time".to_string(),
            "quote_asset_volume".to_string(),
            "number_of_trades".to_string(),
            "taker_buy_base_asset_volume".to_string(),
            "taker_buy_quote_asset_volume".to_string(),
            "ignore".to_string(),
        ]
    }
}

/// Represents a trading symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Symbol {
    pub symbol: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub base_asset: String,
    #[serde(default)]
    pub quote_asset: String,
    #[serde(default)]
    pub margin_asset: String,
    #[serde(default = "default_i32")]
    pub price_precision: i32,
    #[serde(default = "default_i32")]
    pub quantity_precision: i32,
    #[serde(default = "default_i32")]
    pub base_asset_precision: i32,
    #[serde(default = "default_i32")]
    pub quote_precision: i32,
    #[serde(default)]
    pub underlying_type: String,
    #[serde(default)]
    pub underlying_sub_type: Vec<String>,
    #[serde(default = "default_i32")]
    pub settle_plan: i32,
    #[serde(default)]
    pub trigger_protect: String,
    #[serde(default)]
    pub filters: Vec<serde_json::Value>,
    #[serde(default)]
    pub order_type: Vec<String>,
    #[serde(default)]
    pub time_in_force: Vec<String>,
    #[serde(default)]
    pub liquidation_fee: String,
    #[serde(default)]
    pub market_take_bound: String,
    // 添加其他可能的字段
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Default value for i32 fields
fn default_i32() -> i32 {
    0
}

/// Exchange information response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    #[serde(default)]
    pub timezone: String,
    #[serde(default)]
    pub server_time: i64,
    #[serde(default)]
    pub rate_limits: Vec<RateLimit>,
    #[serde(default)]
    pub exchange_filters: Vec<serde_json::Value>,
    pub symbols: Vec<Symbol>,
    // 添加其他可能的字段
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Rate limit information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    #[serde(default)]
    pub rate_limit_type: String,
    #[serde(default)]
    pub interval: String,
    #[serde(default = "default_i32")]
    pub interval_num: i32,
    #[serde(default = "default_i32")]
    pub limit: i32,
    // 添加其他可能的字段
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Download task for a specific symbol and time range
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub symbol: String,
    pub interval: String,
    pub start_time: i64,
    pub end_time: i64,
    pub limit: i32,
}

/// Download result
#[derive(Debug, Clone)]
pub struct DownloadResult {
    pub symbol: String,
    pub interval: String,
    pub klines: Vec<Kline>,
    // 这些字段在当前代码中未使用，但可能在未来的扩展中使用
    #[allow(dead_code)]
    pub start_time: i64,
    #[allow(dead_code)]
    pub end_time: i64,
}
