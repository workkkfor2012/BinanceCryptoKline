//! K线聚合系统的核心数据类型
//! 
//! 本模块定义了系统中使用的所有核心数据结构，特别注意内存对齐以提高性能。

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// 归集交易数据 - 从WebSocket接收的原始数据解析后的结构
/// 
/// 使用 #[repr(C)] 确保内存布局的可预测性，提高缓存效率
#[repr(C)]
#[derive(Debug, Clone)]
pub struct AggTradeData {
    /// 交易品种
    pub symbol: String,
    /// 成交价格
    pub price: f64,
    /// 成交数量
    pub quantity: f64,
    /// 成交时间戳（毫秒）
    pub timestamp_ms: i64,
    /// 买方是否为做市商
    pub is_buyer_maker: bool,
    /// 归集交易ID
    pub agg_trade_id: i64,
    /// 首个交易ID
    pub first_trade_id: i64,
    /// 最后交易ID
    pub last_trade_id: i64,
}

impl AggTradeData {
    /// 从币安原始归集交易数据创建
    pub fn from_binance_raw(raw: &crate::klcommon::websocket::BinanceRawAggTrade) -> Self {
        Self {
            symbol: raw.symbol.clone(),
            price: raw.price.parse().unwrap_or(0.0),
            quantity: raw.quantity.parse().unwrap_or(0.0),
            timestamp_ms: raw.trade_time as i64,
            is_buyer_maker: raw.is_buyer_maker,
            agg_trade_id: raw.aggregate_trade_id as i64,
            first_trade_id: raw.first_trade_id as i64,
            last_trade_id: raw.last_trade_id as i64,
        }
    }
}

/// K线数据 - 内存对齐优化的K线结构
/// 
/// 使用 #[repr(C, align(64))] 确保结构体按64字节对齐（CPU缓存行大小）
/// 这样可以避免false sharing，提高并发性能
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct KlineData {
    /// 品种索引
    pub symbol_index: u32,
    /// 周期索引
    pub period_index: u32,
    /// K线开盘时间戳（毫秒）
    pub open_time: i64,
    /// 开盘价
    pub open: f64,
    /// 最高价
    pub high: f64,
    /// 最低价
    pub low: f64,
    /// 收盘价
    pub close: f64,
    /// 成交量
    pub volume: f64,
    /// 成交额
    pub turnover: f64,
    /// 成交笔数
    pub trade_count: i64,
    /// 主动买入成交量
    pub taker_buy_volume: f64,
    /// 主动买入成交额
    pub taker_buy_turnover: f64,
    /// 是否已最终形成（该周期是否已结束）
    pub is_final: bool,
    /// 填充字节，确保结构体大小为64字节的倍数
    _padding: [u8; 7],
}

impl Default for KlineData {
    fn default() -> Self {
        Self {
            symbol_index: 0,
            period_index: 0,
            open_time: 0,
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
            _padding: [0; 7],
        }
    }
}

impl KlineData {
    /// 创建新的K线数据
    pub fn new(symbol_index: u32, period_index: u32, open_time: i64) -> Self {
        Self {
            symbol_index,
            period_index,
            open_time,
            ..Default::default()
        }
    }
    
    /// 转换为数据库存储格式
    pub fn to_kline(&self, _symbol: &str, interval: &str) -> crate::klcommon::Kline {
        crate::klcommon::Kline {
            open_time: self.open_time,
            open: self.open.to_string(),
            high: self.high.to_string(),
            low: self.low.to_string(),
            close: self.close.to_string(),
            volume: self.volume.to_string(),
            close_time: self.open_time + crate::klcommon::api::interval_to_milliseconds(interval) - 1,
            quote_asset_volume: self.turnover.to_string(),
            number_of_trades: self.trade_count,
            taker_buy_base_asset_volume: self.taker_buy_volume.to_string(),
            taker_buy_quote_asset_volume: self.taker_buy_turnover.to_string(),
            ignore: "0".to_string(),
        }
    }
    
    /// 检查是否为空K线（没有交易数据）
    pub fn is_empty(&self) -> bool {
        self.volume == 0.0 && self.trade_count == 0
    }
    
    /// 重置K线数据（用于新周期开始）
    pub fn reset(&mut self, new_open_time: i64) {
        self.open_time = new_open_time;
        self.open = 0.0;
        self.high = 0.0;
        self.low = 0.0;
        self.close = 0.0;
        self.volume = 0.0;
        self.turnover = 0.0;
        self.trade_count = 0;
        self.taker_buy_volume = 0.0;
        self.taker_buy_turnover = 0.0;
        self.is_final = false;
    }
}

/// 原子K线数据 - 用于无锁并发访问
/// 
/// 使用原子类型确保在多线程环境下的数据一致性
#[repr(C, align(64))]
pub struct AtomicKlineData {
    pub symbol_index: AtomicU64,  // 使用u64存储u32，便于原子操作
    pub period_index: AtomicU64,
    pub open_time: AtomicI64,
    pub open: AtomicU64,          // 使用u64存储f64的位表示
    pub high: AtomicU64,
    pub low: AtomicU64,
    pub close: AtomicU64,
    pub volume: AtomicU64,
    pub turnover: AtomicU64,
    pub trade_count: AtomicI64,
    pub taker_buy_volume: AtomicU64,
    pub taker_buy_turnover: AtomicU64,
    pub is_final: AtomicBool,
}

impl AtomicKlineData {
    /// 创建新的原子K线数据
    pub fn new() -> Self {
        Self {
            symbol_index: AtomicU64::new(0),
            period_index: AtomicU64::new(0),
            open_time: AtomicI64::new(0),
            open: AtomicU64::new(0),
            high: AtomicU64::new(0),
            low: AtomicU64::new(0),
            close: AtomicU64::new(0),
            volume: AtomicU64::new(0),
            turnover: AtomicU64::new(0),
            trade_count: AtomicI64::new(0),
            taker_buy_volume: AtomicU64::new(0),
            taker_buy_turnover: AtomicU64::new(0),
            is_final: AtomicBool::new(false),
        }
    }
    
    /// 从普通K线数据加载
    pub fn load_from(&self, data: &KlineData) {
        self.symbol_index.store(data.symbol_index as u64, Ordering::Relaxed);
        self.period_index.store(data.period_index as u64, Ordering::Relaxed);
        self.open_time.store(data.open_time, Ordering::Relaxed);
        self.open.store(data.open.to_bits(), Ordering::Relaxed);
        self.high.store(data.high.to_bits(), Ordering::Relaxed);
        self.low.store(data.low.to_bits(), Ordering::Relaxed);
        self.close.store(data.close.to_bits(), Ordering::Relaxed);
        self.volume.store(data.volume.to_bits(), Ordering::Relaxed);
        self.turnover.store(data.turnover.to_bits(), Ordering::Relaxed);
        self.trade_count.store(data.trade_count, Ordering::Relaxed);
        self.taker_buy_volume.store(data.taker_buy_volume.to_bits(), Ordering::Relaxed);
        self.taker_buy_turnover.store(data.taker_buy_turnover.to_bits(), Ordering::Relaxed);
        self.is_final.store(data.is_final, Ordering::Relaxed);
    }
    
    /// 转换为普通K线数据
    pub fn to_kline_data(&self) -> KlineData {
        KlineData {
            symbol_index: self.symbol_index.load(Ordering::Relaxed) as u32,
            period_index: self.period_index.load(Ordering::Relaxed) as u32,
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
            _padding: [0; 7],
        }
    }
}

impl Default for AtomicKlineData {
    fn default() -> Self {
        Self::new()
    }
}

/// 时间周期信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodInfo {
    /// 周期字符串（如 "1m", "5m", "1h"）
    pub interval: String,
    /// 周期索引
    pub index: u32,
    /// 周期毫秒数
    pub duration_ms: i64,
}

impl PeriodInfo {
    /// 创建新的周期信息
    pub fn new(interval: String, index: u32) -> Self {
        let duration_ms = crate::klcommon::api::interval_to_milliseconds(&interval);
        Self {
            interval,
            index,
            duration_ms,
        }
    }
}

/// 品种信息
#[derive(Debug, Clone)]
pub struct SymbolInfo {
    /// 品种名称
    pub symbol: String,
    /// 品种索引
    pub index: u32,
    /// 上市时间（首个1分钟K线时间）
    pub listing_time: i64,
}

/// 系统配置常量
pub mod constants {
    /// 默认缓冲区切换间隔（毫秒）
    pub const DEFAULT_BUFFER_SWAP_INTERVAL_MS: u64 = 1000;
    
    /// 默认持久化间隔（毫秒）
    pub const DEFAULT_PERSISTENCE_INTERVAL_MS: u64 = 5000;
    
    /// 默认支持的最大品种数
    pub const DEFAULT_MAX_SYMBOLS: usize = 10000;
    
    /// 默认支持的时间周期
    pub const DEFAULT_INTERVALS: &[&str] = &["1m", "5m", "30m", "1h", "4h", "1d", "1w"];
    
    /// CPU缓存行大小
    pub const CACHE_LINE_SIZE: usize = 64;
}
