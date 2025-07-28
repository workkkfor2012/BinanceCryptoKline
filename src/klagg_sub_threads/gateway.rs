//! Gateway模块 - 中心化网关架构实现
//!
//! 实现"优化的中心网关(Optimized Gateway)"架构，包括：
//! 1. gateway_task: 定时拉取Worker数据，聚合并分发快照
//! 2. db_writer_task: 从Gateway消费数据并持久化
//! 3. GlobalKlines: 全局K线状态快照数据结构

use super::{DeltaBatch, AggregatorReadHandle};
use crate::klcommon::{
    db::Database,
    models::Kline as DbKline,
    AggregateConfig, WatchdogV2,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{error, info, instrument, warn, trace};

static BATCH_ID_COUNTER: AtomicU64 = AtomicU64::new(0);



/// Gateway任务 - 中心化聚合与分发
#[instrument(target = "网关任务", skip_all, name = "gateway_task")]
pub async fn gateway_task(
    // [修改] 参数类型更新，但保持 Vec 结构以简化调用方代码
    worker_handles: Arc<Vec<AggregatorReadHandle>>,
    klines_watch_tx: watch::Sender<Arc<DeltaBatch>>, // [修改]
    db_queue_tx: mpsc::Sender<Arc<DeltaBatch>>,     // [修改]
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    _watchdog: Arc<WatchdogV2>,
) {
    // [修改] 从 Vec 中获取唯一的 handle
    let handle = worker_handles.get(0).expect("应该有且只有一个聚合器句柄").clone();
    let pull_timeout = Duration::from_millis(config.gateway.pull_timeout_ms);

    // [核心修改] 从定时对齐模式改为高频轮询模式
    let pull_interval_ms = config.gateway.pull_interval_ms;
    let mut interval = tokio::time::interval(Duration::from_millis(pull_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!(
        target: "网关任务",
        pull_interval_ms,
        "网关任务已启动 (高频轮询模式)，将每 {} 毫秒拉取一次增量数据", pull_interval_ms
    );

    // 添加10秒统计计时器
    let mut stats_interval = tokio::time::interval(Duration::from_secs(10));
    stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut batches_pulled_count = 0u64;
    let mut klines_processed_count = 0u64;

    loop {
        tokio::select! {
            biased; // 优先检查关闭信号
            _ = shutdown_rx.changed() => if *shutdown_rx.borrow() { break; },
            _ = stats_interval.tick() => {
                info!(
                    target: "网关任务",
                    log_type = "low_freq",
                    "周期性统计: batches_pulled_per_10s={}, klines_processed_per_10s={}",
                    batches_pulled_count, klines_processed_count
                );
                batches_pulled_count = 0;
                klines_processed_count = 0;
            },
            _ = interval.tick() => {
                 // 时间到，执行拉取逻辑
                 match tokio::time::timeout(pull_timeout, handle.request_deltas()).await {
                    Ok(Ok(deltas)) => {
                        // [新增日志] 无论数据是否为空，都记录本次拉取的结果
                        trace!(target: "网关任务", kline_count = deltas.len(), "从聚合器拉取到增量数据");
                        if !deltas.is_empty() {
                            let deltas_len = deltas.len();
                            // [最终决策] 增加批次ID溢出归零的完备性处理
                            let batch_id = BATCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
                            if batch_id == u64::MAX {
                                warn!(target: "网关任务", "批次ID计数器已溢出并重置为0");
                                BATCH_ID_COUNTER.store(0, Ordering::Relaxed);
                            }

                            let delta_batch_arc = Arc::new(DeltaBatch {
                                klines: deltas,
                                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                                batch_id,
                                size: deltas_len,
                            });

                            // 更新统计计数
                            batches_pulled_count += 1;
                            klines_processed_count += deltas_len as u64;

                            // 高频发送给 Web 服务器
                            if klines_watch_tx.send(delta_batch_arc.clone()).is_err() {
                                warn!(target: "网关任务", "实时(watch)通道已关闭");
                            }

                            // 高频发送给 DB 写入任务（DB任务自己会节流）
                            if let Err(e) = db_queue_tx.try_send(delta_batch_arc) {
                                match e {
                                    mpsc::error::TrySendError::Full(batch) => {
                                        warn!(target: "网关任务", batch_id = batch.batch_id, batch_size = batch.size, "持久化队列已满，此批次数据被丢弃！");
                                        // TODO: 在此增加监控指标，例如: METRICS.db_batches_dropped.inc();
                                    },
                                    mpsc::error::TrySendError::Closed(_) => {
                                        error!(target: "网关任务", "持久化通道已关闭，系统出现严重故障");
                                        break; // 退出循环
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(target: "网关任务", error = ?e, "从聚合器获取增量数据失败");
                    }
                    Err(_) => { // 这是 tokio::time::timeout 返回的超时错误
                        warn!(target: "网关任务", "从聚合器获取增量数据超时");
                    }
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
    mut db_queue_rx: mpsc::Receiver<Arc<DeltaBatch>>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    mut shutdown_rx: watch::Receiver<bool>,
    _watchdog: Arc<WatchdogV2>,
) {
    info!(target: "持久化任务", "数据库写入任务已启动 (带节流缓冲模式)");

    // [核心修改] 内部缓冲区，用于"储蓄"高频到来的数据
    let mut accumulated_klines: Vec<super::KlineData> = Vec::with_capacity(4096);

    // [核心修改] 将定时对齐逻辑迁移到这里
    const TARGET_SECOND_OF_MINUTE: u32 = 30;
    const MINUTE_IN_MILLIS: i64 = 60_000;
    const TARGET_MILLIS_OF_MINUTE: i64 = (TARGET_SECOND_OF_MINUTE as i64) * 1000;

    loop {
        // 1. 计算到下一个持久化时间点需要休眠多久
        let now_ms = chrono::Utc::now().timestamp_millis();
        let millis_into_minute = now_ms % MINUTE_IN_MILLIS;
        let sleep_millis = if millis_into_minute < TARGET_MILLIS_OF_MINUTE {
            TARGET_MILLIS_OF_MINUTE - millis_into_minute
        } else {
            MINUTE_IN_MILLIS - millis_into_minute + TARGET_MILLIS_OF_MINUTE
        };
        let sleep_duration = Duration::from_millis(sleep_millis.max(1) as u64); // 至少休眠1ms

        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    // 优雅关闭：处理队列中所有剩余消息
                    while let Ok(batch) = db_queue_rx.try_recv() {
                         accumulated_klines.extend(batch.klines.iter().cloned());
                    }
                    // 执行最后一次写入
                    if !accumulated_klines.is_empty() {
                        info!(target: "持久化任务", kline_count = accumulated_klines.len(), "执行最终的持久化操作");
                        persist_kline_data(db.clone(), &accumulated_klines, &index_to_symbol, &periods).await;
                    }
                    break;
                }
            },
            // 持续从高频通道接收数据并存入缓冲区
            Some(batch) = db_queue_rx.recv() => {
                 accumulated_klines.extend(batch.klines.iter().cloned());
            },
            // 定时器触发，执行批量写入
            _ = tokio::time::sleep(sleep_duration) => {
                if !accumulated_klines.is_empty() {
                    // 使用 std::mem::take 原子地取出缓冲区所有权并替换为空Vec，避免阻塞
                    let klines_to_persist = std::mem::take(&mut accumulated_klines);
                    info!(target: "持久化任务", kline_count = klines_to_persist.len(), "定时触发，开始批量持久化");
                    persist_kline_data(db.clone(), &klines_to_persist, &index_to_symbol, &periods).await;
                }
            }
        }
    }
    warn!(target: "持久化任务", "数据库写入任务已退出");
}

/// [核心修改] 辅助函数，封装持久化逻辑，现在接收 &[KlineData]
async fn persist_kline_data(
    db: Arc<Database>,
    klines: &[super::KlineData], // 修改输入类型
    index_to_symbol: &Arc<RwLock<Vec<String>>>,
    periods: &Arc<Vec<String>>,
) {
    if klines.is_empty() {
        return;
    }

    let index_guard = index_to_symbol.read().await;

    let klines_to_save: Vec<(String, String, DbKline)> = klines.iter()
        .filter(|k| k.open_time > 0)
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
                error!(target: "持久化任务",
                       global_symbol_index = kline_data.global_symbol_index,
                       "发现无效的 global_symbol_index，该条数据被丢弃");
                None
            }
        })
        .collect();

    if !klines_to_save.is_empty() {
        if let Err(e) = db.upsert_klines_batch(klines_to_save) {
            error!(target: "持久化任务", error = ?e, "持久化K线到数据库失败");
        }
    }
}


