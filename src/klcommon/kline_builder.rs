// K线构建器模块 - 从归集交易数据生成K线
use crate::klcommon::{AppError, Kline, Result};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// 归集交易数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggTrade {
    /// 交易对
    pub symbol: String,
    /// 归集成交ID
    pub agg_id: i64,
    /// 成交价格
    pub price: String,
    /// 成交数量
    pub quantity: String,
    /// 被归集的首个交易ID
    pub first_trade_id: i64,
    /// 被归集的末次交易ID
    pub last_trade_id: i64,
    /// 成交时间
    pub trade_time: i64,
    /// 买方是否是做市方
    pub is_buyer_maker: bool,
}

/// K线构建器 - 用于从归集交易数据构建K线
#[derive(Debug)]
pub struct KlineBuilder {
    /// 交易对
    pub symbol: String,
    /// 时间周期
    pub interval: String,
    /// 开盘时间
    pub open_time: i64,
    /// 收盘时间
    pub close_time: i64,
    /// 开盘价
    pub open: Option<String>,
    /// 最高价
    pub high: Option<String>,
    /// 最低价
    pub low: Option<String>,
    /// 收盘价
    pub close: Option<String>,
    /// 成交量
    pub volume: f64,
    /// 报价资产成交量
    pub quote_volume: f64,
    /// 成交笔数
    pub number_of_trades: i64,
    /// 主动买入基础资产成交量
    pub taker_buy_volume: f64,
    /// 主动买入报价资产成交量
    pub taker_buy_quote_volume: f64,
}

impl KlineBuilder {
    /// 创建新的K线构建器
    pub fn new(symbol: String, interval: String, open_time: i64) -> Self {
        // 计算收盘时间
        let interval_ms = interval_to_milliseconds(&interval);
        let close_time = open_time + interval_ms - 1;

        Self {
            symbol,
            interval,
            open_time,
            close_time,
            open: None,
            high: None,
            low: None,
            close: None,
            volume: 0.0,
            quote_volume: 0.0,
            number_of_trades: 0,
            taker_buy_volume: 0.0,
            taker_buy_quote_volume: 0.0,
        }
    }

    /// 添加交易数据
    pub fn add_trade(&mut self, trade: &AggTrade) {
        // 解析价格和数量
        let price = trade.price.parse::<f64>().unwrap_or(0.0);
        let quantity = trade.quantity.parse::<f64>().unwrap_or(0.0);
        let quote_quantity = price * quantity;

        // 设置开盘价（第一笔交易）
        if self.open.is_none() {
            self.open = Some(trade.price.clone());
        }

        // 更新最高价
        if let Some(ref high) = self.high {
            if price > high.parse::<f64>().unwrap_or(0.0) {
                self.high = Some(trade.price.clone());
            }
        } else {
            self.high = Some(trade.price.clone());
        }

        // 更新最低价
        if let Some(ref low) = self.low {
            if price < low.parse::<f64>().unwrap_or(0.0) {
                self.low = Some(trade.price.clone());
            }
        } else {
            self.low = Some(trade.price.clone());
        }

        // 更新收盘价（最后一笔交易）
        self.close = Some(trade.price.clone());

        // 累加成交量和成交额
        self.volume += quantity;
        self.quote_volume += quote_quantity;
        self.number_of_trades += 1;

        // 累加主动买入/卖出成交量
        if !trade.is_buyer_maker {
            // 主动买入
            self.taker_buy_volume += quantity;
            self.taker_buy_quote_volume += quote_quantity;
        }
    }

    /// 构建K线
    pub fn build(self) -> Result<Kline> {
        // 检查是否有足够的数据构建K线
        if self.open.is_none() || self.high.is_none() || self.low.is_none() || self.close.is_none() {
            return Err(AppError::AggregationError("K线数据不完整，无法构建".to_string()));
        }

        Ok(Kline {
            open_time: self.open_time,
            open: self.open.unwrap(),
            high: self.high.unwrap(),
            low: self.low.unwrap(),
            close: self.close.unwrap(),
            volume: self.volume.to_string(),
            close_time: self.close_time,
            quote_asset_volume: self.quote_volume.to_string(),
            number_of_trades: self.number_of_trades,
            taker_buy_base_asset_volume: self.taker_buy_volume.to_string(),
            taker_buy_quote_asset_volume: self.taker_buy_quote_volume.to_string(),
            ignore: "0".to_string(),
        })
    }

    /// 检查K线是否为空（没有交易数据）
    pub fn is_empty(&self) -> bool {
        self.open.is_none()
    }
}

/// 将时间间隔转换为毫秒数
/// 例如: "1m" -> 60000, "1h" -> 3600000
pub fn interval_to_milliseconds(interval: &str) -> i64 {
    let last_char = interval.chars().last().unwrap_or('m');
    let value: i64 = interval[..interval.len() - 1].parse().unwrap_or(1);

    match last_char {
        'm' => value * 60 * 1000,        // 分钟
        'h' => value * 60 * 60 * 1000,   // 小时
        'd' => value * 24 * 60 * 60 * 1000, // 天
        'w' => value * 7 * 24 * 60 * 60 * 1000, // 周
        _ => value * 60 * 1000,  // 默认为分钟
    }
}

/// 从归集交易生成K线
pub fn process_agg_trade(trade: &AggTrade, kline_map: &mut HashMap<i64, KlineBuilder>, interval: &str) -> Result<Vec<(i64, bool)>> {
    // 计算K线周期
    let interval_ms = interval_to_milliseconds(interval);
    let open_time = (trade.trade_time / interval_ms) * interval_ms;

    // 获取或创建K线构建器
    let kline_builder = kline_map.entry(open_time).or_insert_with(|| {
        KlineBuilder::new(
            trade.symbol.clone(),
            interval.to_string(),
            open_time,
        )
    });

    // 添加交易数据
    kline_builder.add_trade(trade);

    // 检查是否有已完成的K线
    let current_time = trade.trade_time;
    let mut completed_klines = Vec::new();

    // 找出所有已完成的K线（当前时间已经超过K线的收盘时间）
    for (&k_open_time, builder) in kline_map.iter() {
        if !builder.is_empty() && current_time > builder.close_time {
            // 这个K线已经完成
            completed_klines.push((k_open_time, true));
        }
    }

    Ok(completed_klines)
}

/// 解析归集交易消息
pub fn parse_agg_trade_message(text: &str) -> Result<Option<AggTrade>> {
    // 解析JSON
    let json: serde_json::Value = serde_json::from_str(text)?;

    // 检查是否是归集交易消息
    // 组合流格式: {"stream":"btcusdt@aggTrade","data":{...}}
    if let (Some(stream), Some(data)) = (json["stream"].as_str(), json["data"].as_object()) {
        if stream.contains("@aggTrade") {
            // 提取交易对
            let parts: Vec<&str> = stream.split('@').collect();
            let symbol = if !parts.is_empty() { parts[0].to_uppercase() } else { "UNKNOWN".to_string() };

            // 提取归集交易数据
            let agg_id = data.get("a").and_then(|v| v.as_i64()).unwrap_or(0);
            let price = data.get("p").and_then(|v| v.as_str()).unwrap_or("0").to_string();
            let quantity = data.get("q").and_then(|v| v.as_str()).unwrap_or("0").to_string();
            let first_trade_id = data.get("f").and_then(|v| v.as_i64()).unwrap_or(0);
            let last_trade_id = data.get("l").and_then(|v| v.as_i64()).unwrap_or(0);
            let trade_time = data.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
            let is_buyer_maker = data.get("m").and_then(|v| v.as_bool()).unwrap_or(false);

            return Ok(Some(AggTrade {
                symbol,
                agg_id,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                trade_time,
                is_buyer_maker,
            }));
        }
    }

    // 不是归集交易消息
    Ok(None)
}
