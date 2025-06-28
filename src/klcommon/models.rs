use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::instrument;

/// 表示币安K线/蜡烛图 - 数据库存储格式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    /// K线开盘时间
    pub open_time: i64,
    /// 开盘价
    pub open: String,
    /// 最高价
    pub high: String,
    /// 最低价
    pub low: String,
    /// 收盘价
    pub close: String,
    /// 成交量
    pub volume: String,
    /// K线收盘时间
    pub close_time: i64,
    /// 报价资产成交量
    pub quote_asset_volume: String,
    /// 成交笔数
    pub number_of_trades: i64,
    /// 主动买入基础资产成交量
    pub taker_buy_base_asset_volume: String,
    /// 主动买入报价资产成交量
    pub taker_buy_quote_asset_volume: String,
    /// 忽略
    pub ignore: String,
}

/// 币安原始AggTrade数据 (简化)
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceRawAggTrade {
    #[serde(rename = "s")]
    pub symbol: String,      // 交易对
    #[serde(rename = "p")]
    pub price: String,       // 成交价格 (字符串形式)
    #[serde(rename = "q")]
    pub quantity: String,    // 成交量 (字符串形式)
    #[serde(rename = "T")]
    pub trade_time: i64,     // 成交时间 (毫秒时间戳)
    #[serde(rename = "a")]
    pub agg_id: i64,         // 归集成交ID
    #[serde(rename = "f")]
    pub first_trade_id: i64, // 被归集的首个交易ID
    #[serde(rename = "l")]
    pub last_trade_id: i64,  // 被归集的末次交易ID
    #[serde(rename = "m")]
    pub is_buyer_maker: bool, // 买方是否是做市方
}

/// 解析后的应用内部AggTrade数据
#[derive(Debug, Clone)]
pub struct AppAggTrade {
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    pub timestamp_ms: i64,
    pub is_buyer_maker: bool,
}

/// K线Bar数据 - 内存中的K线表示形式
#[derive(Debug, Clone)]
pub struct KlineBar {
    pub symbol: String,
    pub period_ms: i64,      // K线周期，单位毫秒
    pub open_time_ms: i64,   // K线开盘时间戳 (毫秒)
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,         // 成交量
    pub turnover: f64,       // 成交额 (price * quantity)
    pub number_of_trades: i64,
    pub taker_buy_volume: f64,
    pub taker_buy_quote_volume: f64,
}

/// K线Bar内部数据结构 (不包含symbol和period_ms)
#[derive(Debug, Clone)]
pub struct KlineBarDataInternal {
    pub open_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
    pub number_of_trades: i64,
    pub taker_buy_volume: f64,
    pub taker_buy_quote_volume: f64,
}

/// 将内部K线数据转换为标准K线Bar
impl KlineBarDataInternal {
    #[instrument(target = "KlineBarDataInternal", skip_all)]
    pub fn to_kline_bar(&self, symbol: &str, period_ms: i64) -> KlineBar {
        KlineBar {
            symbol: symbol.to_string(),
            period_ms,
            open_time_ms: self.open_time_ms,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            turnover: self.turnover,
            number_of_trades: self.number_of_trades,
            taker_buy_volume: self.taker_buy_volume,
            taker_buy_quote_volume: self.taker_buy_quote_volume,
        }
    }
}

/// 将K线Bar转换为标准Kline (用于数据库存储)
impl KlineBar {
    #[instrument(target = "KlineBar", skip_all)]
    pub fn to_kline(&self) -> Kline {
        Kline {
            open_time: self.open_time_ms,
            open: self.open.to_string(),
            high: self.high.to_string(),
            low: self.low.to_string(),
            close: self.close.to_string(),
            volume: self.volume.to_string(),
            close_time: self.open_time_ms + self.period_ms - 1,
            quote_asset_volume: self.turnover.to_string(),
            number_of_trades: self.number_of_trades,
            taker_buy_base_asset_volume: self.taker_buy_volume.to_string(),
            taker_buy_quote_asset_volume: self.taker_buy_quote_volume.to_string(),
            ignore: "0".to_string(),
        }
    }
}

impl Kline {
    /// 从原始K线数据创建K线对象
    pub fn from_raw_kline(raw: &[serde_json::Value]) -> Option<Self> {
        if raw.len() < 12 {
            return None;
        }

        Some(Self {
            open_time: raw[0].as_i64()?,
            open: raw[1].as_str()?.to_string(),
            high: raw[2].as_str()?.to_string(),
            low: raw[3].as_str()?.to_string(),
            close: raw[4].as_str()?.to_string(),
            volume: raw[5].as_str()?.to_string(),
            close_time: raw[6].as_i64()?,
            quote_asset_volume: raw[7].as_str()?.to_string(),
            number_of_trades: raw[8].as_i64()?,
            taker_buy_base_asset_volume: raw[9].as_str()?.to_string(),
            taker_buy_quote_asset_volume: raw[10].as_str()?.to_string(),
            ignore: raw[11].as_str()?.to_string(),
        })
    }

    /// 转换为CSV记录
    #[instrument(target = "Kline", skip_all)]
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
}

/// 表示交易对
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
    /// 合约类型，对应API返回的contractType字段
    #[serde(default, rename = "contractType")]
    pub contract_type: String,
    // 添加其他可能的字段
    #[serde(skip)]
    pub extra: Option<HashMap<String, serde_json::Value>>,
}

/// 费率限制信息
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
    pub extra: HashMap<String, serde_json::Value>,
}

/// 交易所信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    #[serde(default)]
    pub exchange_filters: Vec<serde_json::Value>,
    #[serde(default)]
    pub rate_limits: Vec<RateLimit>,
    #[serde(default)]
    pub server_time: i64,
    #[serde(default)]
    pub assets: Vec<serde_json::Value>,
    #[serde(default)]
    pub symbols: Vec<Symbol>,
    #[serde(default)]
    pub timezone: String,
    // 添加其他可能的字段
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// 下载任务，用于特定交易对和时间范围
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub transaction_id: u64,     // ✨ [新增] 事务ID，用于业务追踪
    pub symbol: String,
    pub interval: String,
    pub start_time: Option<i64>,  // 可选，如果为None则下载最新1000根K线
    pub end_time: Option<i64>,    // 可选，如果为None则使用当前时间
    pub limit: usize,            // 每次请求的K线数量
}

/// 下载结果
#[derive(Debug, Clone)]
pub struct DownloadResult {
    pub symbol: String,
    pub interval: String,
    pub klines: Vec<Kline>,
}

/// i32字段的默认值
// #[instrument] 移除：这是serde反序列化的内部细节，被调用数百次产生噪音
fn default_i32() -> i32 {
    0
}

/// K线数据结构（WebSocket）
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

impl KlineData {
    /// 转换为标准K线格式
    pub fn to_kline(&self) -> Kline {
        Kline {
            open_time: self.start_time,
            open: self.open.clone(),
            high: self.high.clone(),
            low: self.low.clone(),
            close: self.close.clone(),
            volume: self.volume.clone(),
            close_time: self.end_time,
            quote_asset_volume: self.quote_volume.clone(),
            number_of_trades: self.number_of_trades,
            taker_buy_base_asset_volume: self.taker_buy_volume.clone(),
            taker_buy_quote_asset_volume: self.taker_buy_quote_volume.clone(),
            ignore: self.ignore.clone(),
        }
    }
}
