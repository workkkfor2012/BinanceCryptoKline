// K线合成处理器
use crate::kldownload::db::Database;
use crate::kldownload::models::Kline;
use crate::kldownload::error::Result;
use log::{info, error};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{self, Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;
use once_cell::sync::Lazy;

// 全局单例实例
static INSTANCE: Lazy<StdMutex<Option<Arc<KlineAggregator>>>> = Lazy::new(|| {
    StdMutex::new(None)
});

// 全局变量，存储每个交易对的最新1分钟K线
static LATEST_1M_KLINES: Lazy<StdMutex<HashMap<String, Vec<Kline>>>> = Lazy::new(|| {
    StdMutex::new(HashMap::new())
});

// 支持的周期列表
const SUPPORTED_INTERVALS: [&str; 5] = ["5m", "30m", "4h", "1d", "1w"];

// 每个周期对应的分钟数
const INTERVAL_MINUTES: [i64; 5] = [5, 30, 240, 1440, 10080];

// 每个周期包含多少1分钟K线
const KLINES_PER_INTERVAL: [usize; 5] = [5, 30, 240, 1440, 10080];

#[derive(Clone)]
pub struct KlineAggregator {
    db: Arc<Database>,
    running: Arc<AtomicBool>,
}

impl KlineAggregator {
    /// 创建新的K线合成器
    pub fn new(db: Arc<Database>) -> Self {
        let instance = Self {
            db,
            running: Arc::new(AtomicBool::new(false)),
        };

        // 设置全局实例
        let arc_instance = Arc::new(instance.clone());
        let mut global_instance = INSTANCE.lock().unwrap();
        *global_instance = Some(arc_instance);

        instance
    }

    /// 获取全局实例
    pub fn get_instance() -> Option<Arc<Self>> {
        let instance = INSTANCE.lock().unwrap();
        instance.clone()
    }

    /// 添加1分钟K线数据
    pub fn add_1m_kline(&self, symbol: &str, kline: Kline) {
        let symbol_lower = symbol.to_lowercase();

        // 获取锁
        let mut klines_map = LATEST_1M_KLINES.lock().unwrap();

        // 获取或创建该交易对的K线列表
        let klines = klines_map.entry(symbol_lower.clone()).or_insert_with(|| Vec::new());

        // 检查是否已存在相同时间戳的K线
        let existing_index = klines.iter().position(|k| k.open_time == kline.open_time);

        // 先保存开始时间，因为后面会移动kline
        let _open_time = kline.open_time; // 使用下划线前缀表示有意未使用
        if let Some(index) = existing_index {
            // 更新现有K线
            klines[index] = kline;
        } else {
            // 添加新K线
            klines.push(kline);

            // 按时间排序
            klines.sort_by_key(|k| k.open_time);

            // 只保留最近的10080个K线（一周的数据）
            if klines.len() > 10080 {
                let excess = klines.len() - 10080;
                klines.drain(0..excess);
            }
        }
    }

    /// 启动K线合成任务
    pub async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            info!("K线合成器已经在运行");

            return Ok(());
        }

        self.running.store(true, Ordering::SeqCst);

        let db = self.db.clone();
        let running = self.running.clone();

        // 启动合成任务
        tokio::spawn(async move {
            info!("K线合成器已启动");

            let mut interval = time::interval(Duration::from_secs(1));

            // 每30秒输出一次合成器状态
            let mut status_counter = 0;

            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                status_counter += 1;

                // 获取当前时间戳
                let now = chrono::Utc::now().timestamp_millis();

                // 每30秒输出一次合成器状态
                if status_counter >= 30 {
                    // 简化状态日志，不再输出详细信息
                    status_counter = 0;
                }

                // 获取所有交易对的1分钟K线
                let klines_map = LATEST_1M_KLINES.lock().unwrap();

                // 为每个交易对合成K线
                for (symbol, klines) in klines_map.iter() {
                    if klines.is_empty() {
                        continue;
                    }

                    // 简化日志，不再输出每个交易对的处理信息

                    // 合成各个周期的K线
                    for (i, interval) in SUPPORTED_INTERVALS.iter().enumerate() {
                        let minutes = INTERVAL_MINUTES[i];
                        let _required_klines = KLINES_PER_INTERVAL[i]; // 不使用这个变量，但保留以便将来可能需要
                        // 在测试阶段，即使没有足够的K线数据也进行合成
                        // 正常情况下应该是: if klines.len() >= _required_klines
                        if klines.len() > 0 { // 只要有数据就合成
                            // 简化日志，不再输出开始合成的信息
                            match Self::aggregate_klines(klines, minutes) {
                                Ok(aggregated_klines) => {
                                    // 合成结果的简化日志已移至下面的判断逻辑中
                                    for agg_kline in aggregated_klines {
                                        // 判断是否需要插入或更新
                                        let is_closed = Self::is_interval_closed(agg_kline.open_time, minutes, now);

                                        // 获取当前合成次数
                                        static AGGREGATION_COUNTS: Lazy<StdMutex<HashMap<String, usize>>> = Lazy::new(|| {
                                            StdMutex::new(HashMap::new())
                                        });

                                        let key = format!("{}-{}", symbol, interval);
                                        let mut counts = AGGREGATION_COUNTS.lock().unwrap();
                                        let count = counts.entry(key.clone()).or_insert(0);
                                        *count += 1;

                                        if is_closed {
                                            // 已关闭的K线，插入新记录
                                            if let Err(e) = db.insert_kline(symbol, interval, &agg_kline) {
                                                error!("插入合成K线失败, 交易对={}, 周期={}, 错误={}", symbol, interval, e);
                                            } else {
                                                info!("合成结果: 交易对={}, 周期={}, 操作=插入, 合成次数={}",
                                                      symbol, interval, *count);
                                            }
                                        } else {
                                            // 未关闭的K线，更新现有记录
                                            if let Err(e) = db.update_kline(symbol, interval, &agg_kline) {
                                                error!("更新合成K线失败, 交易对={}, 周期={}, 错误={}", symbol, interval, e);
                                            } else {
                                                info!("合成结果: 交易对={}, 周期={}, 操作=更新, 合成次数={}",
                                                      symbol, interval, *count);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("合成K线失败, 交易对={}, 周期={}, 错误={}", symbol, interval, e);
                                }
                            }
                        }
                    }
                }
            }

            info!("K线合成器已停止");
        });

        Ok(())
    }

    /// 停止K线合成任务
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("K线合成器停止中...");
    }

    /// 合成K线
    fn aggregate_klines(klines: &[Kline], minutes: i64) -> Result<Vec<Kline>> {
        let mut result = Vec::new();

        // 按指定周期分组
        let mut groups: HashMap<i64, Vec<&Kline>> = HashMap::new();

        for kline in klines {
            // 计算周期时间戳（向下取整到最近的周期开始时间）
            let interval_ms = minutes * 60 * 1000;
            let timestamp = (kline.open_time / interval_ms) * interval_ms;

            groups.entry(timestamp).or_default().push(kline);
        }

        // 为每个周期创建一个K线
        for (timestamp, group) in groups {
            if !group.is_empty() {
                // 获取第一个和最后一个K线
                let first = group[0];
                let last = group.last().unwrap();

                // 计算最高价和最低价
                let high = group.iter()
                    .map(|k| k.high.parse::<f64>().unwrap_or(0.0))
                    .fold(f64::MIN, |a, b| a.max(b))
                    .to_string();

                let low = group.iter()
                    .map(|k| k.low.parse::<f64>().unwrap_or(f64::MAX))
                    .fold(f64::MAX, |a, b| a.min(b))
                    .to_string();

                // 计算成交量和成交额
                let volume: f64 = group.iter()
                    .map(|k| k.volume.parse::<f64>().unwrap_or(0.0))
                    .sum();

                let quote_volume: f64 = group.iter()
                    .map(|k| k.quote_asset_volume.parse::<f64>().unwrap_or(0.0))
                    .sum();

                let taker_buy_volume: f64 = group.iter()
                    .map(|k| k.taker_buy_base_asset_volume.parse::<f64>().unwrap_or(0.0))
                    .sum();

                let taker_buy_quote_volume: f64 = group.iter()
                    .map(|k| k.taker_buy_quote_asset_volume.parse::<f64>().unwrap_or(0.0))
                    .sum();

                // 计算成交笔数
                let trades: i64 = group.iter()
                    .map(|k| k.number_of_trades)
                    .sum();

                // 创建合成K线
                let aggregated_kline = Kline {
                    open_time: timestamp,
                    open: first.open.clone(),
                    high,
                    low,
                    close: last.close.clone(),
                    volume: volume.to_string(),
                    close_time: timestamp + (minutes * 60 * 1000) - 1, // 周期结束时间
                    quote_asset_volume: quote_volume.to_string(),
                    number_of_trades: trades,
                    taker_buy_base_asset_volume: taker_buy_volume.to_string(),
                    taker_buy_quote_asset_volume: taker_buy_quote_volume.to_string(),
                    ignore: "0".to_string(),
                };

                result.push(aggregated_kline);
            }
        }

        Ok(result)
    }

    /// 判断指定周期的K线是否已关闭
    fn is_interval_closed(open_time: i64, minutes: i64, current_time: i64) -> bool {
        let interval_ms = minutes * 60 * 1000;
        let close_time = open_time + interval_ms - 1;

        // 如果当前时间已经超过了K线的结束时间，则认为K线已关闭
        let is_closed = current_time > close_time;

        // 简化日志，不再输出详细的时间信息
        is_closed
    }
}

