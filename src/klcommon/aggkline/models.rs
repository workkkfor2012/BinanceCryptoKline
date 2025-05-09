// Actor模型数据结构 - 用于K线合成系统
use serde::Deserialize;
use crate::klcommon::{Kline, Database, Result};
use crate::klcommon::websocket::MessageHandler;
use std::sync::Arc;
use std::future::Future;

// 临时的消息处理器，用于替代旧的AggTradeMessageHandler
pub struct DummyMessageHandler {
    pub db: Arc<Database>,
}

impl MessageHandler for DummyMessageHandler {
    fn handle_message(&self, _connection_id: usize, _text: String) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
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

/// K线Bar数据
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
