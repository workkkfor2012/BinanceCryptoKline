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



/// [V8 重命名] Gateway任务 - 仅负责Web推送，不再处理数据库持久化
#[instrument(target = "网关任务", skip_all, name = "gateway_task_for_web")]
pub async fn gateway_task_for_web(
    // [修改] 参数类型更新，但保持 Vec 结构以简化调用方代码
    worker_handles: Arc<Vec<AggregatorReadHandle>>,
    klines_watch_tx: watch::Sender<Arc<DeltaBatch>>, // [修改]
    // [V8 移除] db_queue_tx: mpsc::Sender<Arc<DeltaBatch>>,     // [移除] 不再处理数据库队列
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
                        // 【修改此处】
                        let kline_count = deltas.len();
                        trace!(target: "网关任务", kline_count, "从聚合器拉取到增量数据");

                        if !deltas.is_empty() {
                            let batch_id = BATCH_ID_COUNTER.load(Ordering::Relaxed); // 提前读取，用于日志

                            // 增加详细的数据内容摘要日志
                            trace!(
                                target: "网关数据内容", // 使用新target便于过滤
                                batch_id,
                                kline_count,
                                // 记录批次中第一条和最后一条K线的关键信息
                                first_kline = ?deltas.first(),
                                last_kline = ?deltas.last(),
                                "增量批次内容摘要"
                            );

                            // [最终决策] 增加批次ID溢出归零的完备性处理
                            let batch_id = BATCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed); // batch_id在这里才递增
                            if batch_id == u64::MAX {
                                warn!(target: "网关任务", "批次ID计数器已溢出并重置为0");
                                BATCH_ID_COUNTER.store(0, Ordering::Relaxed);
                            }

                            let delta_batch_arc = Arc::new(DeltaBatch {
                                klines: deltas,
                                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                                batch_id,
                                size: kline_count,
                            });

                            // 更新统计计数
                            batches_pulled_count += 1;
                            klines_processed_count += kline_count as u64;

                            // [V8 修改] 仅发送给 Web 服务器，不再处理数据库队列
                            if klines_watch_tx.send(delta_batch_arc).is_err() {
                                warn!(target: "网关任务", "实时(watch)通道已关闭");
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

/// [V8 修改] 辅助函数，封装持久化逻辑，现在接收 &[KlineData]
async fn persist_kline_data(
    db: Arc<Database>,
    klines: &[super::KlineData], // 修改输入类型
    _index_to_symbol: &Arc<RwLock<Vec<String>>>, // [V8] 不再需要，但保留参数兼容性
    _periods: &Arc<Vec<String>>, // [V8] 不再需要，但保留参数兼容性
) {
    if klines.is_empty() {
        return;
    }

    let klines_to_save: Vec<(String, String, DbKline)> = klines.iter()
        .filter(|k| k.open_time > 0)
        .map(|kline_data| {
            // [V8 简化] 直接使用 KlineData 中的 symbol 和 period 字段
            let db_kline = DbKline {
                open_time: kline_data.open_time,
                open: kline_data.open.to_string(),
                high: kline_data.high.to_string(),
                low: kline_data.low.to_string(),
                close: kline_data.close.to_string(),
                volume: kline_data.volume.to_string(),
                close_time: kline_data.open_time + crate::klcommon::api::interval_to_milliseconds(&kline_data.period) - 1,
                quote_asset_volume: kline_data.turnover.to_string(),
                number_of_trades: kline_data.trade_count,
                taker_buy_base_asset_volume: kline_data.taker_buy_volume.to_string(),
                taker_buy_quote_asset_volume: kline_data.taker_buy_turnover.to_string(),
                ignore: "0".to_string(),
            };

            (kline_data.symbol.clone(), kline_data.period.clone(), db_kline)
        })
        .collect();

    if !klines_to_save.is_empty() {
        // [日志增强] 在这里添加实际写入数据库前的日志
        info!(
            target: "持久化任务",
            count = klines_to_save.len(),
            "准备批量写入 {} 条K线数据到数据库",
            klines_to_save.len()
        );
        if let Err(e) = db.upsert_klines_batch(klines_to_save) {
            error!(target: "持久化任务", error = ?e, "持久化K线到数据库失败");
        }
    }
}

/// [V8 新增] 高优先级持久化任务 - 处理已完成的K线
#[instrument(target = "持久化任务", skip_all, name = "finalized_writer_task")]
pub async fn finalized_writer_task(
    db: Arc<Database>,
    mut finalized_kline_rx: mpsc::Receiver<super::KlineData>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    config: Arc<AggregateConfig>, // [V8] 新增config参数
    mut shutdown_rx: watch::Receiver<bool>,
    _watchdog: Arc<WatchdogV2>,
) {
    info!(target: "持久化任务", "高优先级 Finalized-Writer 任务已启动");

    // [V8] 从配置读取参数
    let buffer_max_size = config.persistence.finalized_buffer_size;
    let flush_interval = Duration::from_millis(config.persistence.finalized_flush_interval_ms);

    let mut buffer: Vec<super::KlineData> = Vec::with_capacity(buffer_max_size);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // 优雅关闭分支
            _ = shutdown_rx.changed() => {
                info!(target: "持久化任务", "收到关闭信号，开始优雅关闭高优先级持久化任务");

                // [V8 关键] 处理缓冲区中的剩余数据
                while let Ok(kline) = finalized_kline_rx.try_recv() {
                    buffer.push(kline);
                }

                if !buffer.is_empty() {
                    info!(target: "持久化任务", count = buffer.len(), "优雅关闭：持久化缓冲区中的剩余数据");
                    persist_kline_data(db.clone(), &buffer, &index_to_symbol, &periods).await;
                }

                break;
            }

            // 接收已完成的K线
            Some(kline) = finalized_kline_rx.recv() => {
                buffer.push(kline);

                // 如果缓冲区满了，立即刷盘
                if buffer.len() >= buffer_max_size {
                    trace!(target: "持久化任务", count = buffer.len(), "缓冲区已满，立即刷盘");
                    persist_kline_data(db.clone(), &buffer, &index_to_symbol, &periods).await;
                    buffer.clear();
                }
            }

            // 定时刷盘
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    trace!(target: "持久化任务", count = buffer.len(), "定时刷盘");
                    persist_kline_data(db.clone(), &buffer, &index_to_symbol, &periods).await;
                    buffer.clear();
                }
            }
        }
    }

    warn!(target: "持久化任务", "高优先级 Finalized-Writer 任务已退出");
}

/// [V8 新增] 低优先级持久化任务 - 处理进行中K线的快照
#[instrument(target = "持久化任务", skip_all, name = "snapshot_writer_task")]
pub async fn snapshot_writer_task(
    db: Arc<Database>,
    aggregator_handle: AggregatorReadHandle,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    mut clock_rx: watch::Receiver<i64>,
    mut shutdown_rx: watch::Receiver<bool>,
    _watchdog: Arc<WatchdogV2>,
) {
    info!(target: "持久化任务", "低优先级 Snapshot-Writer 任务已启动");

    loop {
        tokio::select! {
            // 优雅关闭分支
            _ = shutdown_rx.changed() => {
                info!(target: "持久化任务", "收到关闭信号，低优先级持久化任务开始退出");
                break;
            }

            // 时钟触发分支
            result = clock_rx.changed() => {
                if result.is_err() {
                    warn!(target: "持久化任务", "时钟通道已关闭，低优先级持久化任务退出");
                    break;
                }

                trace!(target: "持久化任务", "收到时钟信号，开始拉取进行中K线快照");

                match aggregator_handle.request_full_snapshot().await {
                    Ok(klines) => {
                        let snapshots: Vec<_> = klines.into_iter().filter(|k| !k.is_final).collect();
                        if !snapshots.is_empty() {
                            info!(target: "持久化任务", count = snapshots.len(), "拉取到 {} 条进行中K线快照进行持久化", snapshots.len());
                            persist_kline_data(db.clone(), &snapshots, &index_to_symbol, &periods).await;
                        }
                    },
                    Err(e) => {
                        error!(target: "持久化任务", "拉取全量快照失败: {}", e);
                    }
                }
            },
        }
    }
    warn!(target: "持久化任务", "低优先级 Snapshot-Writer 任务已退出");
}


