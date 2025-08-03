//! K线生命周期事件校验器
//!
//! 这个模块实现了K线生命周期事件的校验逻辑，用于调试和验证K线状态转换的正确性。
//! 只有在启用 `full-audit` feature 时才会编译此模块。

// [新增] 整个文件内容都应该被条件编译包裹（如果它只在审计模式下使用）
#![cfg(feature = "full-audit")]

use tokio::sync::{broadcast, watch};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn, instrument};
use std::collections::HashMap;

// [新增] 定义生命周期事件的触发器
#[derive(Debug, Clone, Copy)]
pub enum LifecycleTrigger {
    Trade,
    Clock,
}

// [新增] 定义生命周期事件的数据结构
#[derive(Debug, Clone)]
pub struct KlineLifecycleEvent {
    pub timestamp_ms: i64,
    pub global_symbol_index: usize,
    pub period_index: usize,
    pub kline_offset: usize,
    pub old_kline_state: crate::klagg_sub_threads::KlineState,
    pub new_kline_state: crate::klagg_sub_threads::KlineState,
    pub trigger: LifecycleTrigger,
}

/// 生命周期事件统计信息
#[derive(Debug, Default)]
struct LifecycleStats {
    total_events: u64,
    trade_triggered: u64,
    clock_triggered: u64,
    state_transitions: HashMap<String, u64>,
}

impl LifecycleStats {
    fn record_event(&mut self, event: &KlineLifecycleEvent) {
        self.total_events += 1;
        
        match event.trigger {
            LifecycleTrigger::Trade => self.trade_triggered += 1,
            LifecycleTrigger::Clock => self.clock_triggered += 1,
        }
        
        let transition_key = format!(
            "{:?} -> {:?}",
            event.old_kline_state, event.new_kline_state
        );
        *self.state_transitions.entry(transition_key).or_insert(0) += 1;
    }
    
    fn log_summary(&self) {
        info!(
            target: "生命周期审计",
            log_type = "audit_summary",
            total_events = self.total_events,
            trade_triggered = self.trade_triggered,
            clock_triggered = self.clock_triggered,
            "生命周期事件统计摘要"
        );
        
        for (transition, count) in &self.state_transitions {
            debug!(
                target: "生命周期审计",
                transition = %transition,
                count = count,
                "状态转换统计"
            );
        }
    }
}

// [新增] 校验器任务的实现
#[instrument(target = "生命周期审计", skip_all, name = "lifecycle_validator_task")]
pub async fn run_lifecycle_validator_task(
    mut event_rx: broadcast::Receiver<KlineLifecycleEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!(
        target: "生命周期审计",
        log_type = "audit_startup",
        "🔍 生命周期校验器任务启动 - 开始监听K线生命周期事件"
    );
    info!(
        target: "生命周期审计",
        log_type = "audit_startup",
        report_interval_seconds = 60,
        "✅ 生命周期校验器配置完成 - 每60秒报告一次统计信息"
    );

    let mut stats = LifecycleStats::default();
    let mut report_interval = interval(Duration::from_secs(60)); // 每分钟报告一次统计
    
    loop {
        tokio::select! {
            // 检查关闭信号
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(target: "生命周期审计", "收到关闭信号，生命周期校验器正在退出");
                    break;
                }
            },
            
            // 接收生命周期事件
            result = event_rx.recv() => {
                match result {
                    Ok(event) => {
                        debug!(
                            target: "生命周期审计",
                            timestamp_ms = event.timestamp_ms,
                            global_symbol_index = event.global_symbol_index,
                            period_index = event.period_index,
                            kline_offset = event.kline_offset,
                            trigger = ?event.trigger,
                            old_state = ?event.old_kline_state,
                            new_state = ?event.new_kline_state,
                            "收到生命周期事件"
                        );
                        
                        // 记录统计信息
                        stats.record_event(&event);
                        
                        // 执行校验逻辑
                        validate_lifecycle_event(&event);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        warn!(target: "生命周期审计", "生命周期事件通道已关闭，校验器退出");
                        break;
                    },
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            target: "生命周期审计",
                            skipped_events = skipped,
                            log_type = "performance_alert",
                            "生命周期事件接收滞后，跳过了部分事件"
                        );
                    },
                }
            },
            
            // 定期报告统计信息
            _ = report_interval.tick() => {
                stats.log_summary();
            },
        }
    }
    
    // 最终统计报告
    stats.log_summary();
    info!(target: "生命周期审计", "生命周期校验器任务已退出");
}

/// 校验单个生命周期事件的逻辑
fn validate_lifecycle_event(event: &KlineLifecycleEvent) {
    // 基本的状态转换校验
    match (&event.old_kline_state, &event.new_kline_state) {
        // 检查不合理的状态转换
        (old, new) if std::ptr::eq(old, new) => {
            // 状态没有变化，这可能是正常的更新操作
            debug!(
                target: "生命周期审计",
                global_symbol_index = event.global_symbol_index,
                period_index = event.period_index,
                "K线状态未发生变化（可能是数据更新）"
            );
        },
        _ => {
            // 状态发生了变化，记录转换
            debug!(
                target: "生命周期审计",
                global_symbol_index = event.global_symbol_index,
                period_index = event.period_index,
                old_state = ?event.old_kline_state,
                new_state = ?event.new_kline_state,
                trigger = ?event.trigger,
                "检测到K线状态转换"
            );
        }
    }
    
    // 时间戳合理性检查
    let current_time = chrono::Utc::now().timestamp_millis();
    if event.timestamp_ms > current_time + 60000 { // 允许1分钟的时间偏差
        warn!(
            target: "生命周期审计",
            event_timestamp = event.timestamp_ms,
            current_timestamp = current_time,
            log_type = "validation_warning",
            "生命周期事件时间戳异常：事件时间超前当前时间过多"
        );
    }
}
