//! Gateway模块 - 中心化网关架构实现
//!
//! 实现"优化的中心网关(Optimized Gateway)"架构，包括：
//! 1. gateway_task: 定时拉取Worker数据，聚合并分发快照
//! 2. db_writer_task: 从Gateway消费数据并持久化
//! 3. GlobalKlines: 全局K线状态快照数据结构

use super::{KlineData, WorkerReadHandle};
use crate::klcommon::{
    db::Database,
    models::Kline as DbKline,
    AggregateConfig, WatchdogV2,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, error, info, instrument, warn};

/// Gateway 和消费者之间传递的全局K线状态快照
#[derive(Clone, Default, Debug)]
pub struct GlobalKlines {
    /// 使用 Vec<KlineData> 而不是 Vec<KlineState>
    /// 因为 KlineData 已经包含了所有需要的信息，并且是纯数据结构，
    /// 而 KlineState 包含一些 worker 内部状态，不适合向外暴露。
    pub klines: Vec<KlineData>,
    /// 可选: 增加一个时间戳，表示快照生成的时间
    pub snapshot_time_ms: i64,
}

/// Gateway任务 - 中心化聚合与分发
#[instrument(target = "网关任务", skip_all, name = "gateway_task")]
pub async fn gateway_task(
    worker_handles: Arc<Vec<WorkerReadHandle>>,
    klines_watch_tx: watch::Sender<Arc<GlobalKlines>>,
    db_queue_tx: mpsc::Sender<Arc<GlobalKlines>>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    _watchdog: Arc<WatchdogV2>,
) {
    let num_workers = worker_handles.len();
    let pull_interval = Duration::from_millis(config.gateway.pull_interval_ms);
    let pull_timeout = Duration::from_millis(config.gateway.pull_timeout_ms);
    let timeout_threshold = config.gateway.timeout_alert_threshold;
    let mut interval = tokio::time::interval(pull_interval);

    // --- 【核心修改】: 使用单缓冲重用模式，只在启动时分配一次内存 ---
    let total_kline_slots = config.max_symbols * config.supported_intervals.len();
    let mut buffer_a = vec![KlineData::default(); total_kline_slots];

    // 用于优雅降级的缓存
    let mut last_good_snapshots: Vec<Vec<KlineData>> = vec![vec![]; num_workers];
    let mut timeout_counters = vec![0usize; num_workers];

    // [诊断日志开始]
    let db_queue_capacity = db_queue_tx.capacity();
    let mut log_interval = tokio::time::interval(Duration::from_secs(5));
    log_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // [诊断日志结束]

    info!(target: "网关任务", "网关任务已启动 (单缓冲重用模式)，Worker数量: {}, 总K线槽位: {}, DB队列容量: {}", num_workers, total_kline_slots, db_queue_capacity);

    loop {
        tokio::select! {
            // [诊断日志开始]
            _ = log_interval.tick() => {
                let db_queue_len = db_queue_capacity - db_queue_tx.capacity();
                let percentage = (db_queue_len as f32 / db_queue_capacity as f32) * 100.0;

                // 使用 info 级别以便观察队列状态
                info!(
                    target: "队列监控",
                    queue_name = "Gateway_to_DBWriter",
                    len = db_queue_len,
                    capacity = db_queue_capacity,
                    usage_percent = format!("{:.2}%", percentage),
                    "数据库写入队列状态"
                );

                if percentage > 80.0 {
                    warn!(
                        target: "队列监控",
                        queue_name = "Gateway_to_DBWriter",
                        len = db_queue_len,
                        "数据库写入队列使用率超过80%!"
                    );
                }
            },
            // [诊断日志结束]

            _ = interval.tick() => {
                let cycle_start = Instant::now();

                // [诊断日志开始] - 添加网关周期开始日志
                info!(target: "网关任务", "开始Gateway聚合周期");
                // [诊断日志结束]

                // 1. 并发拉取数据
                let futures = worker_handles.iter().map(|h| {
                    tokio::time::timeout(pull_timeout, h.request_snapshot())
                });
                let results = futures::future::join_all(futures).await;

                // [诊断日志开始] - 添加Worker响应统计
                let success_count = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
                let error_count = results.iter().filter(|r| matches!(r, Ok(Err(_)))).count();
                let timeout_count = results.iter().filter(|r| matches!(r, Err(_))).count();
                info!(target: "网关任务",
                    success_count, error_count, timeout_count,
                    "Worker响应统计"
                );
                // [诊断日志结束]

                // --- 【核心修改】: 聚合数据到 buffer_a ---
                for (i, res) in results.into_iter().enumerate() {
                    match res {
                        Ok(Ok(snapshot)) => {
                            // 成功
                            for delta in &snapshot {
                                let kline_offset = delta.global_symbol_index * config.supported_intervals.len() + delta.period_index;
                                if kline_offset < buffer_a.len() {
                                    // 写入到 buffer_a
                                    buffer_a[kline_offset] = delta.clone();
                                }
                            }
                            last_good_snapshots[i] = snapshot;
                            timeout_counters[i] = 0;
                        }
                        Ok(Err(e)) => {
                            // Worker内部错误
                            warn!(target: "网关任务", worker_id = i, error = ?e, "从Worker获取快照失败");
                            // 使用上次缓存数据进行降级：将last_good_snapshots[i]写入buffer_a
                            for delta in &last_good_snapshots[i] {
                                let kline_offset = delta.global_symbol_index * config.supported_intervals.len() + delta.period_index;
                                if kline_offset < buffer_a.len() {
                                    buffer_a[kline_offset] = delta.clone();
                                }
                            }
                        }
                        Err(_) => {
                            // 超时
                            warn!(target: "网关任务", worker_id = i, "从Worker获取快照超时");
                            timeout_counters[i] += 1;
                            if timeout_counters[i] >= timeout_threshold {
                                error!(target: "网关任务", worker_id = i, count = timeout_counters[i], "Worker已连续多次超时!");
                            }
                            // 使用上次缓存数据进行降级：将last_good_snapshots[i]写入buffer_a
                            for delta in &last_good_snapshots[i] {
                                let kline_offset = delta.global_symbol_index * config.supported_intervals.len() + delta.period_index;
                                if kline_offset < buffer_a.len() {
                                    buffer_a[kline_offset] = delta.clone();
                                }
                            }
                        }
                    }
                }

                // --- 【核心修改】: 移除swap逻辑，直接克隆buffer_a来发送 ---
                let snapshot_to_send = Arc::new(GlobalKlines {
                    klines: buffer_a.clone(), // 克隆数据以供发送，避免了重新分配Vec
                    snapshot_time_ms: chrono::Utc::now().timestamp_millis(),
                });

                // 3a. 发送到实时通道
                if klines_watch_tx.send(snapshot_to_send.clone()).is_err() {
                    warn!(target: "网关任务", "实时(watch)通道已关闭，网关可能无法正常服务");
                }

                // 3b. 发送到持久化通道
                if let Err(e) = db_queue_tx.try_send(snapshot_to_send) {
                     warn!(target: "网关任务", error = ?e, "持久化(mpsc)通道发送失败(可能已满或关闭)");
                }

                // --- 【核心修改】: 清空缓冲区以备下次使用，而不是重新分配 ---
                // 这确保了我们始终在重用同一块内存
                buffer_a.iter_mut().for_each(|kline| *kline = KlineData::default());

                let cycle_duration = cycle_start.elapsed();
                debug!(target: "网关任务", duration_ms = cycle_duration.as_millis(), "Gateway聚合周期完成");
            },
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }
    warn!(target: "网关任务", "网关任务已退出");
}

/// 数据库写入任务 - 重构后的持久化任务
#[instrument(target = "持久化任务", skip_all, name="db_writer_task")]
pub async fn db_writer_task(
    db: Arc<Database>,
    mut db_queue_rx: mpsc::Receiver<Arc<GlobalKlines>>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    mut shutdown_rx: watch::Receiver<bool>,
    _watchdog: Arc<WatchdogV2>,
) {
    info!(target: "持久化任务", "数据库写入任务已启动 (新架构)");

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    // 优雅关闭：处理队列中所有剩余消息
                    while let Ok(snapshot) = db_queue_rx.try_recv() {
                         persist_snapshot(db.clone(), snapshot, &index_to_symbol, &periods).await;
                    }
                    break;
                }
            },
            Some(snapshot) = db_queue_rx.recv() => {
                 persist_snapshot(db.clone(), snapshot, &index_to_symbol, &periods).await;
            }
        }
    }
    warn!(target: "持久化任务", "数据库写入任务已退出");
}

/// 辅助函数，封装持久化逻辑
async fn persist_snapshot(
    db: Arc<Database>,
    snapshot: Arc<GlobalKlines>,
    index_to_symbol: &Arc<RwLock<Vec<String>>>,
    periods: &Arc<Vec<String>>,
) {
    let index_guard = index_to_symbol.read().await;
    
    // 写入合并逻辑：只保存有意义的（非默认）K线
    let klines_to_save: Vec<(String, String, DbKline)> = snapshot.klines.iter()
        .filter(|k| k.open_time > 0)  // 过滤掉默认/空的K线
        .filter_map(|kline_data| {
            // 将 KlineData 转换为 (symbol, interval, DbKline)
            if kline_data.global_symbol_index < index_guard.len()
                && kline_data.period_index < periods.len() {

                let symbol = index_guard[kline_data.global_symbol_index].clone();
                let period = periods[kline_data.period_index].clone();

                let db_kline = DbKline {
                    open_time: kline_data.open_time,
                    open: kline_data.open.to_string(),
                    high: kline_data.high.to_string(),
                    low: kline_data.low.to_string(),
                    close: kline_data.close.to_string(),
                    volume: kline_data.volume.to_string(),
                    close_time: kline_data.open_time + crate::klcommon::api::interval_to_milliseconds(&period) - 1,
                    quote_asset_volume: kline_data.turnover.to_string(),
                    number_of_trades: kline_data.trade_count,
                    taker_buy_base_asset_volume: kline_data.taker_buy_volume.to_string(),
                    taker_buy_quote_asset_volume: kline_data.taker_buy_turnover.to_string(),
                    ignore: "0".to_string(),
                };

                Some((symbol, period, db_kline))
            } else {
                None
            }
        }).collect();

    if !klines_to_save.is_empty() {
        let count = klines_to_save.len();
        if let Err(e) = db.upsert_klines_batch(klines_to_save) {
            error!(target: "持久化任务", error = ?e, "持久化K线到数据库失败");
        } else {
            debug!(target: "持久化任务", count, "成功持久化K线数据");
        }
    }
}
