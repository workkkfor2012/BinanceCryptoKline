use crate::klcommon::{AppError, Database, Kline, Result};
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use chrono::Utc;

/// 将时间间隔转换为毫秒数
/// 例如: "1m" -> 60000, "1h" -> 3600000
fn interval_to_milliseconds(interval: &str) -> i64 {
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

/// K线聚合器
pub struct KlineAggregator {
    db: Arc<Database>,
    intervals: Vec<String>,
    symbols: Vec<String>,
}

impl KlineAggregator {
    /// 创建新的K线聚合器
    pub fn new(db: Arc<Database>, intervals: Vec<String>, symbols: Vec<String>) -> Self {
        Self {
            db,
            intervals,
            symbols,
        }
    }

    /// 启动聚合任务
    pub async fn start(&self) -> Result<()> {
        info!("启动K线聚合任务");

        // 创建定时器，每秒执行一次聚合
        let mut timer = interval(Duration::from_secs(1));

        loop {
            timer.tick().await;

            // 获取当前时间
            let now = Utc::now().timestamp_millis();

            // 对每个交易对和周期执行聚合
            for symbol in &self.symbols {
                for interval in &self.intervals {
                    if interval == "1m" {
                        continue; // 跳过1分钟周期，它是基础数据
                    }

                    // 使用特定的日志目标，便于分离日志
                    let target = format!("aggregator_{}", interval);

                    // 执行聚合
                    match self.aggregate_klines(symbol, interval, now) {
                        Ok(count) => {
                            if count > 0 {
                                log::log!(target: &target, log::Level::Info,
                                    "{}/{}: 聚合了 {} 条K线", symbol, interval, count);
                            }
                        },
                        Err(e) => {
                            log::log!(target: &target, log::Level::Error,
                                "{}/{}: 聚合K线失败: {}", symbol, interval, e);
                        }
                    }
                }
            }
        }
    }

    /// 聚合K线数据
    fn aggregate_klines(&self, symbol: &str, target_interval: &str, current_time: i64) -> Result<usize> {
        // 获取目标周期的毫秒数
        let target_ms = interval_to_milliseconds(target_interval);

        // 计算需要聚合的时间范围
        let current_period_start = (current_time / target_ms) * target_ms;
        let previous_period_start = current_period_start - target_ms;

        // 获取1分钟K线数据
        let source_klines = self.db.get_klines_in_range(
            symbol,
            "1m",
            previous_period_start,
            current_time
        )?;

        if source_klines.is_empty() {
            return Ok(0);
        }

        // 按周期分组
        let mut period_klines: Vec<Vec<&Kline>> = Vec::new();
        let mut current_group: Vec<&Kline> = Vec::new();
        let mut current_group_start = (source_klines[0].open_time / target_ms) * target_ms;

        for kline in &source_klines {
            let kline_period_start = (kline.open_time / target_ms) * target_ms;

            if kline_period_start != current_group_start && !current_group.is_empty() {
                period_klines.push(current_group);
                current_group = Vec::new();
                current_group_start = kline_period_start;
            }

            current_group.push(kline);
        }

        if !current_group.is_empty() {
            period_klines.push(current_group);
        }

        // 聚合每个周期的K线
        let mut aggregated_klines = Vec::new();

        for group in period_klines {
            if group.is_empty() {
                continue;
            }

            let period_start = (group[0].open_time / target_ms) * target_ms;
            let period_end = period_start + target_ms - 1;

            // 聚合K线
            let aggregated = self.aggregate_kline_group(group, period_start, period_end)?;
            aggregated_klines.push(aggregated);
        }

        // 保存聚合后的K线
        if !aggregated_klines.is_empty() {
            self.db.save_klines(symbol, target_interval, &aggregated_klines)?;
        }

        Ok(aggregated_klines.len())
    }

    /// 聚合一组K线
    fn aggregate_kline_group(&self, klines: Vec<&Kline>, open_time: i64, close_time: i64) -> Result<Kline> {
        if klines.is_empty() {
            return Err(AppError::AggregationError("无法聚合空的K线组".to_string()));
        }

        // 获取开盘价（第一根K线的开盘价）
        let open = klines[0].open.clone();

        // 获取收盘价（最后一根K线的收盘价）
        let close = klines.last().unwrap().close.clone();

        // 计算最高价和最低价
        let mut high = klines[0].high.clone();
        let mut low = klines[0].low.clone();
        let mut volume = 0.0;
        let mut quote_asset_volume = 0.0;
        let mut number_of_trades = 0;
        let mut taker_buy_base_asset_volume = 0.0;
        let mut taker_buy_quote_asset_volume = 0.0;

        for kline in &klines {
            // 更新最高价
            if kline.high.parse::<f64>().unwrap_or(0.0) > high.parse::<f64>().unwrap_or(0.0) {
                high = kline.high.clone();
            }

            // 更新最低价
            if kline.low.parse::<f64>().unwrap_or(0.0) < low.parse::<f64>().unwrap_or(0.0) {
                low = kline.low.clone();
            }

            // 累加成交量和成交额
            volume += kline.volume.parse::<f64>().unwrap_or(0.0);
            quote_asset_volume += kline.quote_asset_volume.parse::<f64>().unwrap_or(0.0);
            number_of_trades += kline.number_of_trades;
            taker_buy_base_asset_volume += kline.taker_buy_base_asset_volume.parse::<f64>().unwrap_or(0.0);
            taker_buy_quote_asset_volume += kline.taker_buy_quote_asset_volume.parse::<f64>().unwrap_or(0.0);
        }

        // 创建聚合后的K线
        let aggregated = Kline {
            open_time,
            open,
            high,
            low,
            close,
            volume: volume.to_string(),
            close_time,
            quote_asset_volume: quote_asset_volume.to_string(),
            number_of_trades,
            taker_buy_base_asset_volume: taker_buy_base_asset_volume.to_string(),
            taker_buy_quote_asset_volume: taker_buy_quote_asset_volume.to_string(),
            ignore: "0".to_string(),
        };

        Ok(aggregated)
    }
}
