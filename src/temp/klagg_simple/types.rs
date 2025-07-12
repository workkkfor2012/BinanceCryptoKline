// src/klagg_simple/types.rs

//! 定义了K线聚合模块的核心数据结构和公共契约。

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

// --- 1. 内部计算使用的数据结构 ---

/// 从WebSocket流入的原始交易数据，保留所有字段以备将来使用。
#[derive(Debug, Clone)]
pub struct Trade {
    pub price: f64,
    pub quantity: f64,
    pub is_buyer_maker: bool,
    pub trade_time: i64, 
    pub agg_trade_id: u64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
}

/// Actor内部用于聚合计算的K线状态。
#[derive(Debug, Clone, Default)]
pub(super) struct KlineState {
    pub symbol_index: usize,
    pub period_index: usize,
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
    // is_persisted 已经被 is_dirty 取代，因为 is_dirty 更通用
}

// --- 2. 公共共享数据结构 (模块的API) ---

/// 对外暴露的、用于双缓冲区的原子K线数据结构。
/// 使用 #[repr(C, align(64))] 确保按CPU缓存行对齐，避免False Sharing。
#[repr(C, align(64))]
#[derive(Default)]
pub struct AtomicKlineData {
    symbol_index: AtomicU64,
    period_index: AtomicU64,
    open_time: AtomicI64,
    open: AtomicU64,
    high: AtomicU64,
    low: AtomicU64,
    close: AtomicU64,
    volume: AtomicU64,
    turnover: AtomicU64,
    trade_count: AtomicI64,
    taker_buy_volume: AtomicU64,
    taker_buy_turnover: AtomicU64,
    is_final: AtomicBool,
    is_dirty: AtomicBool, // <-- 新增字段
    // 13*8 (104) + 2 = 106 bytes. Next multiple of 64 is 128. Padding = 128 - 106 = 22.
    _padding: [u8; 22],
}

impl AtomicKlineData {
    /// 内部使用：将 `KlineState` 的数据原子化地存储到共享内存。
    #[inline]
    pub(super) fn store(&self, k: &KlineState) {
        self.symbol_index
            .store(k.symbol_index as u64, Ordering::Relaxed);
        self.period_index
            .store(k.period_index as u64, Ordering::Relaxed);
        self.open_time.store(k.open_time, Ordering::Relaxed);
        self.open.store(k.open.to_bits(), Ordering::Relaxed);
        self.high.store(k.high.to_bits(), Ordering::Relaxed);
        self.low.store(k.low.to_bits(), Ordering::Relaxed);
        self.close.store(k.close.to_bits(), Ordering::Relaxed);
        self.volume.store(k.volume.to_bits(), Ordering::Relaxed);
        self.turnover.store(k.turnover.to_bits(), Ordering::Relaxed);
        self.trade_count.store(k.trade_count, Ordering::Relaxed);
        self.taker_buy_volume
            .store(k.taker_buy_volume.to_bits(), Ordering::Relaxed);
        self.taker_buy_turnover
            .store(k.taker_buy_turnover.to_bits(), Ordering::Relaxed);
        self.is_final.store(k.is_final, Ordering::Relaxed);
        // 新K线或更新后的K线总是脏的
        self.is_dirty.store(true, Ordering::Relaxed);
    }

    /// 公共API：从共享内存中加载数据，转换为普通K线结构。
    #[inline]
    pub fn load(&self) -> KlineData {
        KlineData {
            symbol_index: self.symbol_index.load(Ordering::Relaxed) as usize,
            period_index: self.period_index.load(Ordering::Relaxed) as usize,
            open_time: self.open_time.load(Ordering::Relaxed),
            open: f64::from_bits(self.open.load(Ordering::Relaxed)),
            high: f64::from_bits(self.high.load(Ordering::Relaxed)),
            low: f64::from_bits(self.low.load(Ordering::Relaxed)),
            close: f64::from_bits(self.close.load(Ordering::Relaxed)),
            volume: f64::from_bits(self.volume.load(Ordering::Relaxed)),
            turnover: f64::from_bits(self.turnover.load(Ordering::Relaxed)),
            trade_count: self.trade_count.load(Ordering::Relaxed),
            taker_buy_volume: f64::from_bits(self.taker_buy_volume.load(Ordering::Relaxed)),
            taker_buy_turnover: f64::from_bits(self.taker_buy_turnover.load(Ordering::Relaxed)),
            is_final: self.is_final.load(Ordering::Relaxed),
            is_dirty: self.is_dirty.load(Ordering::Relaxed),
        }
    }
    
    /// 公共API：原子地设置 is_dirty 标志
    #[inline]
    pub fn set_dirty(&self, value: bool) {
        self.is_dirty.store(value, Ordering::Relaxed);
    }
}

/// 对外暴露的普通K线数据结构，是模块输出的最终形式。
#[derive(Debug, Clone, Default)]
pub struct KlineData {
    pub symbol_index: usize,
    pub period_index: usize,
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
    pub is_dirty: bool, // <-- 新增字段
}

// --- 3. Watchdog & Health Monitoring Types ---

use std::sync::Arc;

/// 用于Watchdog的共享健康状态数组。
/// Arc使其可以在多个任务间共享。
/// Vec的索引直接对应 symbol_index。
/// AtomicU64存储了最后一次心跳的Unix毫秒时间戳。
pub type HealthArray = Arc<Vec<AtomicU64>>;