// src/engine/mod.rs
pub mod events;
pub mod tracker;
pub mod web_server; // web_server.rs 移到 engine 模块下

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracker::DirtyTracker;
pub use events::AppEvent;
use crate::klcommon::{db::Database, models::Kline as DbKline, AggregateConfig};
use anyhow::Result;

// KlineState 保持不变，但它是 engine 的核心状态
#[derive(Debug, Clone, Default)]
pub struct KlineState {
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
    pub trade_count: i64,
    pub taker_buy_volume: f64,
    pub taker_buy_turnover: f64,
    pub is_final: bool,
    pub is_initialized: bool,
}

pub struct KlineEngine {
    config: Arc<AggregateConfig>,
    db: Arc<Database>,

    // 核心状态
    kline_states: Vec<KlineState>,
    kline_expirations: Vec<i64>,
    dirty_tracker: DirtyTracker,

    // 通信
    event_rx: mpsc::Receiver<AppEvent>,
    state_watch_tx: watch::Sender<events::StateUpdate>,

    // 索引
    symbol_to_offset: HashMap<String, usize>,
    index_to_symbol: Vec<String>,  // 新增：反向索引，从global_index到symbol
    next_symbol_index: usize,      // 新增：追踪下一个可用的品种索引
    periods: Arc<Vec<String>>,
}

// KlineEngine 的 impl 块将在后续步骤中添加
impl KlineEngine {
    pub fn new(
        config: Arc<AggregateConfig>,
        db: Arc<Database>,
        initial_klines: HashMap<(String, String), DbKline>,
        all_symbols_sorted: &[String],
        event_rx: mpsc::Receiver<AppEvent>,
        state_watch_tx: watch::Sender<events::StateUpdate>,
    ) -> Result<Self> {
        let num_periods = config.supported_intervals.len();
        let num_initial_symbols = all_symbols_sorted.len();
        let total_slots = config.max_symbols * num_periods;
        let kline_states = vec![KlineState::default(); total_slots];
        let mut symbol_to_offset = HashMap::with_capacity(all_symbols_sorted.len());

        for (global_index, symbol) in all_symbols_sorted.iter().enumerate() {
            let offset = global_index * num_periods;
            symbol_to_offset.insert(symbol.clone(), offset);
            for (period_idx, period) in config.supported_intervals.iter().enumerate() {
                let _kline_offset = offset + period_idx;
                if let Some(_db_kline) = initial_klines.get(&(symbol.clone(), period.clone())) {
                    // ... 将 db_kline 转换为 KlineState 并填充到 kline_states[kline_offset]
                    // 这部分逻辑可以从旧的 Worker::new 方法中迁移过来
                }
            }
        }

        Ok(Self {
            config: config.clone(),
            db,
            kline_states,
            kline_expirations: vec![0; total_slots], // 需要正确初始化
            dirty_tracker: DirtyTracker::new(total_slots),
            event_rx,
            state_watch_tx,
            symbol_to_offset,
            index_to_symbol: all_symbols_sorted.to_vec(), // 初始化反向索引
            next_symbol_index: num_initial_symbols,       // 初始化为当前品种数量
            periods: Arc::new(config.supported_intervals.clone()),
        })
    }

    pub async fn run(&mut self) {
        use tokio::time::{interval, Duration};

        let mut clock_timer = interval(Duration::from_secs(1)); // 简化的1秒滴答
        let mut persist_timer = interval(Duration::from_secs(5)); // 简化为5秒间隔

        loop {
            tokio::select! {
                Some(event) = self.event_rx.recv() => self.handle_event(event),
                _ = clock_timer.tick() => self.process_clock_tick(),
                _ = persist_timer.tick() => {
                    self.publish_and_persist_changes();
                },
                else => break,
            }
        }
    }

    fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::AggTrade(trade) => self.process_trade(*trade),
            AppEvent::AddSymbol { symbol, first_kline_open_time } => {
                use tracing::{info, warn};

                if self.symbol_to_offset.contains_key(&symbol) {
                    warn!(target: "品种管理", symbol = %symbol, "尝试添加已存在的品种，已忽略");
                    return;
                }

                if self.next_symbol_index >= self.config.max_symbols {
                    warn!(target: "品种管理", symbol = %symbol, max_symbols = self.config.max_symbols, "已达到最大品种数，无法添加新品种");
                    return;
                }

                let global_index = self.next_symbol_index;
                info!(target: "品种管理", symbol = %symbol, global_index, "分配新索引并添加新品种");

                // 计算偏移量
                let offset = global_index * self.periods.len();

                // 更新品种到偏移量的映射
                self.symbol_to_offset.insert(symbol.clone(), offset);
                self.index_to_symbol.push(symbol.clone()); // 更新反向索引

                // 为新品种初始化所有周期的K线状态
                for (period_idx, period) in self.periods.iter().enumerate() {
                    let kline_offset = offset + period_idx;

                    if let Some(period_ms) = parse_period_to_ms(period) {
                        let aligned_open_time = get_aligned_time(first_kline_open_time, period_ms);

                        // 初始化K线状态（使用默认值，等待第一笔交易数据）
                        self.kline_states[kline_offset] = KlineState {
                            open_time: aligned_open_time,
                            open: 0.0,
                            high: 0.0,
                            low: 0.0,
                            close: 0.0,
                            volume: 0.0,
                            turnover: 0.0,
                            trade_count: 0,
                            taker_buy_volume: 0.0,
                            taker_buy_turnover: 0.0,
                            is_final: false,
                            is_initialized: true,
                        };

                        // 设置过期时间
                        self.kline_expirations[kline_offset] = aligned_open_time + period_ms;

                        // 标记为脏数据
                        self.dirty_tracker.mark_dirty(kline_offset);
                    }
                }

                self.next_symbol_index += 1; // 更新下一个可用索引
                info!(target: "品种管理", symbol = %symbol, offset, "新品种初始化完成");
            }
        }
    }

    fn process_trade(&mut self, trade: crate::klcommon::websocket::AggTradeData) {
        use tracing::{debug, warn};

        // 1. 获取品种的偏移量
        let Some(&offset) = self.symbol_to_offset.get(&trade.symbol) else {
            debug!(target: "K线聚合", symbol = %trade.symbol, "收到未知品种的交易数据，忽略");
            return;
        };

        let trade_time = trade.event_time_ms;
        let price = trade.price;
        let quantity = trade.quantity;
        let turnover = price * quantity;

        // 2. 遍历所有周期
        for (period_idx, period) in self.periods.iter().enumerate() {
            let kline_offset = offset + period_idx;

            // 获取周期的毫秒数
            let period_ms = match parse_period_to_ms(period) {
                Some(ms) => ms,
                None => {
                    warn!(target: "K线聚合", period = %period, "无法解析周期，跳过");
                    continue;
                }
            };

            // 计算对齐的开盘时间
            let aligned_open_time = get_aligned_time(trade_time, period_ms);

            let kline = &mut self.kline_states[kline_offset];

            // 修改：只在对齐时间大于当前K线开盘时间时才翻转
            if aligned_open_time > kline.open_time {
                // 翻转到新的K线
                kline.open_time = aligned_open_time;
                kline.open = price;
                kline.high = price;
                kline.low = price;
                kline.close = price;
                kline.volume = quantity;
                kline.turnover = turnover;
                kline.trade_count = 1;

                // 更新过期时间
                self.kline_expirations[kline_offset] = aligned_open_time + period_ms;
            } else if aligned_open_time == kline.open_time {
                // 更新现有K线
                kline.high = kline.high.max(price);
                kline.low = kline.low.min(price);
                kline.close = price;
                kline.volume += quantity;
                kline.turnover += turnover;
                kline.trade_count += 1;
            }
            // 如果 aligned_open_time < kline.open_time，则自动忽略这笔延迟交易

            // 标记为脏数据
            self.dirty_tracker.mark_dirty(kline_offset);
        }
    }

    fn process_clock_tick(&mut self) {
        use tracing::debug;

        let current_time = chrono::Utc::now().timestamp_millis();
        let num_symbols = self.symbol_to_offset.len();
        let num_periods = self.periods.len();

        // 遍历所有已初始化的K线
        for symbol_idx in 0..num_symbols {
            let offset = symbol_idx * num_periods;

            for period_idx in 0..num_periods {
                let kline_offset = offset + period_idx;
                let expiration_time = self.kline_expirations[kline_offset];

                // 检查是否到期
                if expiration_time > 0 && current_time >= expiration_time {
                    let kline = &mut self.kline_states[kline_offset];

                    // 如果K线有数据（不是默认状态），则需要翻转
                    if kline.open_time > 0 {
                        debug!(target: "时钟驱动",
                            kline_offset,
                            open_time = kline.open_time,
                            expiration_time,
                            current_time,
                            "K线到期，准备翻转"
                        );

                        // 获取周期毫秒数
                        let period = &self.periods[period_idx];
                        if let Some(period_ms) = parse_period_to_ms(period) {
                            // 计算新的开盘时间
                            let new_open_time = get_aligned_time(current_time, period_ms);

                            // 翻转K线（保持最后价格作为新的开盘价）
                            let last_close = kline.close;
                            *kline = KlineState {
                                open_time: new_open_time,
                                open: last_close,
                                high: last_close,
                                low: last_close,
                                close: last_close,
                                volume: 0.0,
                                turnover: 0.0,
                                trade_count: 0,
                                taker_buy_volume: 0.0,
                                taker_buy_turnover: 0.0,
                                is_final: false,
                                is_initialized: true,
                            };

                            // 更新过期时间
                            self.kline_expirations[kline_offset] = new_open_time + period_ms;

                            // 标记为脏数据
                            self.dirty_tracker.mark_dirty(kline_offset);
                        }
                    }
                }
            }
        }
    }
    
    fn publish_and_persist_changes(&mut self) {
        let dirty_indices = self.dirty_tracker.collect_and_reset();
        if dirty_indices.is_empty() {
            return;
        }

        // 1. **推送状态更新 (高性能)**
        let update = events::StateUpdate {
            // 这是唯一一次受控的克隆，将可变状态转换为共享快照
            kline_snapshot: Arc::new(self.kline_states.clone()),
            dirty_indices: Arc::new(dirty_indices.clone()), // 克隆索引列表，开销很小
        };
        self.state_watch_tx.send(update).ok();

        // 2. **持久化** (复用 dirty_indices)
        self.persist_changes_with_indices(dirty_indices);
    }

    fn persist_changes_with_indices(&mut self, dirty_indices: Vec<usize>) {
        use tracing::{debug, error};

        if dirty_indices.is_empty() {
            return;
        }

        debug!(target: "持久化", count = dirty_indices.len(), "开始持久化脏数据");

        let mut klines_to_save: Vec<(String, String, DbKline)> = Vec::with_capacity(dirty_indices.len());

        // 构建需要保存的K线数据
        for &kline_offset in &dirty_indices {
            let kline = &self.kline_states[kline_offset];

            // 跳过未初始化的K线
            if kline.open_time == 0 {
                continue;
            }

            // 计算品种索引和周期索引
            let num_periods = self.periods.len();
            let symbol_idx = kline_offset / num_periods;
            let period_idx = kline_offset % num_periods;

            // O(1) 复杂度的反向查找
            let symbol = if let Some(s) = self.index_to_symbol.get(symbol_idx) {
                s.clone() // clone是必要的，因为要和DbKline一起移动所有权
            } else {
                error!(target: "持久化", kline_offset, symbol_idx, "无法找到对应的品种名称");
                continue;
            };

            let period = &self.periods[period_idx];

            // 构建数据库K线对象
            let db_kline = DbKline {
                open_time: kline.open_time,
                open: kline.open.to_string(),
                high: kline.high.to_string(),
                low: kline.low.to_string(),
                close: kline.close.to_string(),
                volume: kline.volume.to_string(),
                close_time: kline.open_time + parse_period_to_ms(period).unwrap_or(60000) - 1,
                quote_asset_volume: kline.turnover.to_string(),
                number_of_trades: kline.trade_count,
                taker_buy_base_asset_volume: kline.taker_buy_volume.to_string(),
                taker_buy_quote_asset_volume: kline.taker_buy_turnover.to_string(),
                ignore: "0".to_string(),
            };

            klines_to_save.push((symbol, period.clone(), db_kline));
        }

        if !klines_to_save.is_empty() {
            debug!(target: "持久化", count = klines_to_save.len(), "准备批量写入数据库");

            let db_clone = self.db.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = db_clone.upsert_klines_batch(klines_to_save) {
                    error!(target: "持久化", error = ?e, "批量写入数据库失败");
                } else {
                    debug!(target: "持久化", "批量写入数据库成功");
                }
            });
        }
    }
}

// 辅助函数：解析周期字符串为毫秒数
fn parse_period_to_ms(period: &str) -> Option<i64> {
    match period {
        "1m" => Some(60 * 1000),
        "3m" => Some(3 * 60 * 1000),
        "5m" => Some(5 * 60 * 1000),
        "15m" => Some(15 * 60 * 1000),
        "30m" => Some(30 * 60 * 1000),
        "1h" => Some(60 * 60 * 1000),
        "2h" => Some(2 * 60 * 60 * 1000),
        "4h" => Some(4 * 60 * 60 * 1000),
        "6h" => Some(6 * 60 * 60 * 1000),
        "8h" => Some(8 * 60 * 60 * 1000),
        "12h" => Some(12 * 60 * 60 * 1000),
        "1d" => Some(24 * 60 * 60 * 1000),
        "3d" => Some(3 * 24 * 60 * 60 * 1000),
        "1w" => Some(7 * 24 * 60 * 60 * 1000),
        "1M" => Some(30 * 24 * 60 * 60 * 1000), // 近似值
        _ => None,
    }
}

// 辅助函数：获取对齐的时间戳
fn get_aligned_time(timestamp_ms: i64, interval_ms: i64) -> i64 {
    (timestamp_ms / interval_ms) * interval_ms
}
