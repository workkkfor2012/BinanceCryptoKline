// K线生成器 - 从交易数据生成K线
use crate::klcommon::aggkline::models::{AppAggTrade, KlineBar, KlineBarDataInternal};
// use log::debug;

/// K线生成器 - 用于从交易数据生成特定周期的K线
#[derive(Debug)]
pub struct KlineGenerator {
    /// 交易对
    pub symbol: String,
    /// 周期 (毫秒)
    pub period_ms: i64,
    /// 当前K线数据
    pub current_bar: Option<KlineBarDataInternal>,
}

impl KlineGenerator {
    /// 创建新的K线生成器
    pub fn new(symbol: String, period_ms: i64) -> Self {
        Self {
            symbol,
            period_ms,
            current_bar: None,
        }
    }

    /// 使用交易数据更新K线
    ///
    /// # 参数
    /// * `trade` - 交易数据
    ///
    /// # 返回值
    /// * `Some(KlineBar)` - 如果K线完成，返回完成的K线
    /// * `None` - 如果K线未完成
    pub fn update_with_trade(&mut self, trade: &AppAggTrade) -> Option<KlineBar> {
        let trade_price = trade.price;
        let trade_quantity = trade.quantity;
        let trade_turnover = trade_price * trade_quantity;
        let trade_timestamp_ms = trade.timestamp_ms;

        // 计算当前trade所属K线的开盘时间
        let kline_open_time_ms = (trade_timestamp_ms / self.period_ms) * self.period_ms;

        let mut completed_kline_to_return = None;

        match &mut self.current_bar {
            Some(current) => {
                if kline_open_time_ms == current.open_time_ms {
                    // 仍在当前K线周期内
                    current.high = current.high.max(trade_price);
                    current.low = current.low.min(trade_price);
                    current.close = trade_price;
                    current.volume += trade_quantity;
                    current.turnover += trade_turnover;
                    current.number_of_trades += 1;

                    // 更新主动买入/卖出成交量
                    if !trade.is_buyer_maker {
                        // 主动买入
                        current.taker_buy_volume += trade_quantity;
                        current.taker_buy_quote_volume += trade_turnover;
                    }
                } else {
                    // 新的K线周期开始了，旧K线完成
                    let completed_bar_data = current.clone();
                    completed_kline_to_return = Some(completed_bar_data.to_kline_bar(&self.symbol, self.period_ms));

                    // 初始化新的K线
                    *current = KlineBarDataInternal {
                        open_time_ms: kline_open_time_ms,
                        open: trade_price,
                        high: trade_price,
                        low: trade_price,
                        close: trade_price,
                        volume: trade_quantity,
                        turnover: trade_turnover,
                        number_of_trades: 1,
                        taker_buy_volume: if !trade.is_buyer_maker { trade_quantity } else { 0.0 },
                        taker_buy_quote_volume: if !trade.is_buyer_maker { trade_turnover } else { 0.0 },
                    };
                }
            }
            None => {
                // 第一笔交易，初始化K线
                self.current_bar = Some(KlineBarDataInternal {
                    open_time_ms: kline_open_time_ms,
                    open: trade_price,
                    high: trade_price,
                    low: trade_price,
                    close: trade_price,
                    volume: trade_quantity,
                    turnover: trade_turnover,
                    number_of_trades: 1,
                    taker_buy_volume: if !trade.is_buyer_maker { trade_quantity } else { 0.0 },
                    taker_buy_quote_volume: if !trade.is_buyer_maker { trade_turnover } else { 0.0 },
                });
            }
        }

        completed_kline_to_return
    }
}
