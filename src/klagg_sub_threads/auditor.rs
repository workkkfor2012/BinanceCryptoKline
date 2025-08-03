//! K线数据完整性审计器
//!
//! 这个模块实现了K线数据的完整性审计功能，用于检测数据丢失、重复或异常。
//! 只有在启用 `full-audit` feature 时才会编译此模块。

// [新增] 整个文件内容都应该被条件编译包裹（如果它只在审计模式下使用）
#![cfg(feature = "full-audit")]

use tokio::sync::{mpsc, watch};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn, instrument};
use std::collections::{HashMap, HashSet};

use crate::klagg_sub_threads::KlineData;

/// 审计统计信息
#[derive(Debug, Default)]
struct AuditStats {
    total_klines_processed: u64,
    symbols_tracked: HashSet<String>,
    periods_tracked: HashSet<String>,
    data_gaps_detected: u64,
    duplicate_data_detected: u64,
    timestamp_anomalies: u64,
}

impl AuditStats {
    fn record_kline(&mut self, kline: &KlineData) {
        self.total_klines_processed += 1;
        self.symbols_tracked.insert(kline.symbol.clone());
        self.periods_tracked.insert(kline.period.clone());
    }
    
    fn record_gap(&mut self) {
        self.data_gaps_detected += 1;
    }
    
    fn record_duplicate(&mut self) {
        self.duplicate_data_detected += 1;
    }
    
    fn record_timestamp_anomaly(&mut self) {
        self.timestamp_anomalies += 1;
    }
    
    fn log_summary(&self) {
        info!(
            target: "数据完整性审计",
            log_type = "audit_summary",
            total_klines = self.total_klines_processed,
            unique_symbols = self.symbols_tracked.len(),
            unique_periods = self.periods_tracked.len(),
            data_gaps = self.data_gaps_detected,
            duplicates = self.duplicate_data_detected,
            timestamp_anomalies = self.timestamp_anomalies,
            "数据完整性审计统计摘要"
        );
    }
}

/// K线数据的审计上下文
#[derive(Debug)]
struct KlineAuditContext {
    last_timestamp: i64,
    last_close_price: f64,
    sequence_number: u64,
}

impl KlineAuditContext {
    fn new() -> Self {
        Self {
            last_timestamp: 0,
            last_close_price: 0.0,
            sequence_number: 0,
        }
    }
    
    fn update(&mut self, kline: &KlineData) {
        self.last_timestamp = kline.open_time;
        self.last_close_price = kline.close;
        self.sequence_number += 1;
    }
}

// [新增] 完整性审计器任务的实现
#[instrument(target = "数据完整性审计", skip_all, name = "completeness_auditor_task")]
pub async fn run_completeness_auditor_task(
    mut kline_rx: mpsc::Receiver<KlineData>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!(target: "数据完整性审计", "数据完整性审计器任务启动");
    
    let mut stats = AuditStats::default();
    let mut audit_contexts: HashMap<String, KlineAuditContext> = HashMap::new();
    let mut report_interval = interval(Duration::from_secs(300)); // 每5分钟报告一次
    
    loop {
        tokio::select! {
            // 检查关闭信号
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(target: "数据完整性审计", "收到关闭信号，完整性审计器正在退出");
                    break;
                }
            },
            
            // 接收K线数据进行审计
            Some(kline) = kline_rx.recv() => {
                debug!(
                    target: "数据完整性审计",
                    symbol = %kline.symbol,
                    period = %kline.period,
                    open_time = kline.open_time,
                    "收到K线数据进行审计"
                );
                
                // 记录统计信息
                stats.record_kline(&kline);
                
                // 执行审计检查
                audit_kline_data(&kline, &mut audit_contexts, &mut stats);
            },
            
            // 定期报告统计信息
            _ = report_interval.tick() => {
                stats.log_summary();
            },
        }
    }
    
    // 最终统计报告
    stats.log_summary();
    info!(target: "数据完整性审计", "数据完整性审计器任务已退出");
}

/// 审计单个K线数据
fn audit_kline_data(
    kline: &KlineData,
    contexts: &mut HashMap<String, KlineAuditContext>,
    stats: &mut AuditStats,
) {
    let key = format!("{}:{}", kline.symbol, kline.period);
    let context = contexts.entry(key.clone()).or_insert_with(KlineAuditContext::new);
    
    // 检查时间戳连续性
    if context.last_timestamp > 0 {
        let expected_interval = get_period_interval_ms(&kline.period);
        let actual_interval = kline.open_time - context.last_timestamp;
        
        if actual_interval > expected_interval * 2 {
            warn!(
                target: "数据完整性审计",
                symbol = %kline.symbol,
                period = %kline.period,
                expected_interval_ms = expected_interval,
                actual_interval_ms = actual_interval,
                last_timestamp = context.last_timestamp,
                current_timestamp = kline.open_time,
                log_type = "data_gap",
                "检测到数据间隙：时间间隔异常"
            );
            stats.record_gap();
        } else if actual_interval == 0 {
            warn!(
                target: "数据完整性审计",
                symbol = %kline.symbol,
                period = %kline.period,
                timestamp = kline.open_time,
                log_type = "duplicate_data",
                "检测到重复数据：相同时间戳"
            );
            stats.record_duplicate();
        }
    }
    
    // 检查价格合理性
    if kline.open <= 0.0 || kline.high <= 0.0 || kline.low <= 0.0 || kline.close <= 0.0 {
        error!(
            target: "数据完整性审计",
            symbol = %kline.symbol,
            period = %kline.period,
            open = kline.open,
            high = kline.high,
            low = kline.low,
            close = kline.close,
            log_type = "invalid_price",
            "检测到无效价格数据"
        );
    }
    
    // 检查OHLC逻辑关系
    if kline.high < kline.low || 
       kline.high < kline.open || kline.high < kline.close ||
       kline.low > kline.open || kline.low > kline.close {
        error!(
            target: "数据完整性审计",
            symbol = %kline.symbol,
            period = %kline.period,
            open = kline.open,
            high = kline.high,
            low = kline.low,
            close = kline.close,
            log_type = "ohlc_logic_error",
            "检测到OHLC逻辑错误"
        );
    }
    
    // 检查成交量合理性
    if kline.volume < 0.0 {
        error!(
            target: "数据完整性审计",
            symbol = %kline.symbol,
            period = %kline.period,
            volume = kline.volume,
            log_type = "invalid_volume",
            "检测到无效成交量数据"
        );
    }
    
    // 更新审计上下文
    context.update(kline);
}

/// 根据周期字符串获取对应的毫秒间隔
fn get_period_interval_ms(period: &str) -> i64 {
    match period {
        "1m" => 60 * 1000,
        "3m" => 3 * 60 * 1000,
        "5m" => 5 * 60 * 1000,
        "15m" => 15 * 60 * 1000,
        "30m" => 30 * 60 * 1000,
        "1h" => 60 * 60 * 1000,
        "2h" => 2 * 60 * 60 * 1000,
        "4h" => 4 * 60 * 60 * 1000,
        "6h" => 6 * 60 * 60 * 1000,
        "8h" => 8 * 60 * 60 * 1000,
        "12h" => 12 * 60 * 60 * 1000,
        "1d" => 24 * 60 * 60 * 1000,
        "3d" => 3 * 24 * 60 * 60 * 1000,
        "1w" => 7 * 24 * 60 * 60 * 1000,
        "1M" => 30 * 24 * 60 * 60 * 1000, // 近似值
        _ => {
            warn!(
                target: "数据完整性审计",
                period = %period,
                "未知的K线周期，使用默认间隔"
            );
            60 * 1000 // 默认1分钟
        }
    }
}
