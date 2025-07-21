//! 高性能K线聚合服务 (完全分区模型) - Worker实现
//!
//! ## 设计核心
//! 1.  **Worker**: 聚合逻辑的核心，运行在专属的、绑核的计算线程上。
//! 2.  **run_io_loop**: Worker的I/O部分，运行在共享的I/O线程池中。
//! 3.  **通道通信**: 计算与I/O之间通过MPSC通道解耦，实现无锁通信。
//! 4.  **本地缓存**: Worker内部使用`symbol->local_index`的本地缓存，实现热路径O(1)查找。

// SIMD优化通过feature flag控制

pub mod web_server;

#[cfg(test)]
mod tests;

use crate::klcommon::{
    api::{get_aligned_time, interval_to_milliseconds},
    db::Database,
    error::{AppError, Result},
    log,
    models::Kline as DbKline,
    websocket::{
        AggTradeData, AggTradeMessageHandler, MessageHandler,
    },
    ComponentStatus, HealthReport, HealthReporter, WatchdogV2, // 引入 health 模块
    AggregateConfig,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::time::{interval, sleep, Duration, Instant};

/// 性能统计数据
#[derive(Debug)]
struct PerformanceStats {
    total_calls: u64,
    total_duration_nanos: u128,
    total_processed_klines: u64,
    min_duration_nanos: u128,
    max_duration_nanos: u128,
    last_reset_time: Instant,
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            total_calls: 0,
            total_duration_nanos: 0,
            total_processed_klines: 0,
            min_duration_nanos: u128::MAX,
            max_duration_nanos: 0,
            last_reset_time: Instant::now(),
        }
    }
}
use tracing::{debug, error, info, instrument, trace, warn};

#[cfg(feature = "simd")]
use wide::{CmpEq, CmpLt};

// --- 1. 类型定义 ---

/// 用于封装 AddSymbol 指令携带的初始K线数据
#[derive(Debug, Clone, Copy)]
pub struct InitialKlineData {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

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

#[derive(Debug, Clone, Default)]
pub struct KlineData {
    pub global_symbol_index: usize,
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
    pub is_updated: bool,
}

#[derive(Debug)]
pub enum WorkerCmd {
    AddSymbol {
        symbol: String,
        global_index: usize,
        initial_data: InitialKlineData,
        ack: oneshot::Sender<std::result::Result<(), String>>,
    },
}

#[derive(Debug)]
pub enum WsCmd {
    Subscribe(Vec<String>),
}

#[derive(Clone)]
pub struct WorkerReadHandle {
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
}

impl std::fmt::Debug for WorkerReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerReadHandle")
            .field("snapshot_req_tx", &"<mpsc::Sender>")
            .finish()
    }
}

impl WorkerReadHandle {
    pub async fn request_snapshot(&self) -> Result<Vec<KlineData>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.snapshot_req_tx
            .send(response_tx)
            .await
            .map_err(|e| AppError::ChannelClosed(format!("Failed to send snapshot request: {}", e)))?;
        response_rx
            .await
            .map_err(|e| AppError::ChannelClosed(format!("Failed to receive snapshot response: {}", e)))
    }
}

// --- 2. 计算任务健康报告 ---

struct ComputationHealthReporter {
    worker_id: usize,
    last_activity: Arc<AtomicI64>, // 复用之前的心跳原子变量
    timeout: Duration,
}

#[async_trait]
impl HealthReporter for ComputationHealthReporter {
    fn name(&self) -> String {
        format!("Computation-Worker-{}", self.worker_id)
    }

    async fn report(&self) -> HealthReport {
        let last_activity_ms = self.last_activity.load(Ordering::Relaxed);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let elapsed_ms = now_ms.saturating_sub(last_activity_ms);

        let (status, message) = if last_activity_ms == 0 {
            (ComponentStatus::Warning, Some("尚未开始活动".to_string()))
        } else if elapsed_ms > self.timeout.as_millis() as i64 {
            (
                ComponentStatus::Error,
                Some(format!("超过 {} 秒无活动", self.timeout.as_secs())),
            )
        } else {
            (ComponentStatus::Ok, None)
        };

        HealthReport {
            component_name: self.name(),
            status,
            message,
            details: [(
                "last_activity_ago_ms".to_string(),
                serde_json::json!(elapsed_ms),
            )]
            .into(),
        }
    }
}

// --- 3. I/O 任务健康报告 ---

#[derive(Default)]
struct IoLoopMetrics {
    messages_received: AtomicU64,
    last_message_timestamp: AtomicI64,
    reconnects: AtomicU64,
}

struct IoHealthReporter {
    worker_id: usize,
    metrics: Arc<IoLoopMetrics>,
    // 如果超过这个时间没有收到任何消息，就发出警告
    no_message_warning_threshold: Duration,
}

#[async_trait]
impl HealthReporter for IoHealthReporter {
    fn name(&self) -> String {
        format!("IO-Worker-{}", self.worker_id)
    }

    async fn report(&self) -> HealthReport {
        let last_msg_ts = self.metrics.last_message_timestamp.load(Ordering::Relaxed);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let elapsed_ms = now_ms.saturating_sub(last_msg_ts);

        let (status, message) =
            if last_msg_ts > 0 && elapsed_ms > self.no_message_warning_threshold.as_millis() as i64
            {
                (
                    ComponentStatus::Warning,
                    Some(format!(
                        "超过 {} 秒未收到WebSocket消息",
                        self.no_message_warning_threshold.as_secs()
                    )),
                )
            } else {
                (ComponentStatus::Ok, None)
            };

        let reconnects = self.metrics.reconnects.load(Ordering::Relaxed);
        let final_status = if reconnects > 10 { ComponentStatus::Warning } else { status };

        HealthReport {
            component_name: self.name(),
            status: final_status,
            message,
            details: [
                ("reconnects".to_string(), serde_json::json!(reconnects)),
                ("last_message_ago_ms".to_string(), serde_json::json!(elapsed_ms)),
            ].into(),
        }
    }
}

// --- 4. 持久化任务健康报告 ---

#[derive(Default)]
struct PersistenceMetrics {
    last_successful_run: Option<Instant>,
    last_batch_size: usize,
    last_run_duration_ms: u128,
}

struct PersistenceHealthReporter {
    metrics: Arc<Mutex<PersistenceMetrics>>,
    // 如果超过这个时间没有成功运行，就发出警告
    max_interval: Duration,
}

#[async_trait]
impl HealthReporter for PersistenceHealthReporter {
    fn name(&self) -> String {
        "Persistence-Task".to_string()
    }
    async fn report(&self) -> HealthReport {
        let guard = self.metrics.lock().unwrap();
        let (status, message) = match guard.last_successful_run {
            Some(last_run) if last_run.elapsed() > self.max_interval => (
                ComponentStatus::Warning,
                Some(format!(
                    "超过 {} 秒未成功持久化",
                    self.max_interval.as_secs()
                )),
            ),
            None => (ComponentStatus::Warning, Some("尚未执行过持久化".to_string())),
            _ => (ComponentStatus::Ok, None),
        };

        HealthReport {
            component_name: self.name(),
            status,
            message,
            details: [
                ("last_batch_size".to_string(), serde_json::json!(guard.last_batch_size)),
                ("last_run_duration_ms".to_string(), serde_json::json!(guard.last_run_duration_ms)),
            ].into(),
        }
    }
}

// --- 5. Worker 核心实现 (计算部分) ---

pub struct Worker {
    worker_id: usize,
    periods: Arc<Vec<String>>,
    // 【核心修改】新增与 kline_states 平行的到期时间数组
    kline_expirations: Vec<i64>,
    partition_start_index: usize,
    kline_states: Vec<KlineState>,

    // [新增] 用于暂存"抢跑"交易的等待区。
    // Key: K线周期的 open_time, Value: 该周期的交易列表
    pending_trades: HashMap<i64, Vec<AggTradeData>>,

    // [新增] 等待区保护机制：限制暂存交易的总数量，防止内存泄漏
    pending_trades_count: usize,
    max_pending_trades: usize,

    snapshot_buffers: (Vec<KlineData>, Vec<KlineData>),
    active_buffer_index: usize,
    local_symbol_cache: HashMap<String, usize>,
    managed_symbols_count: usize,
    cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    snapshot_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    ws_cmd_tx: mpsc::Sender<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
    last_clock_tick: i64,
    clock_rx: watch::Receiver<i64>,

    // 性能统计
    performance_stats: PerformanceStats,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    #[instrument(target = "计算核心", level = "info", skip_all, fields(worker_id, initial_symbols = assigned_symbols.len()))]
    pub async fn new(
        worker_id: usize,
        partition_start_index: usize,
        assigned_symbols: &[String],
        symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
        periods: Arc<Vec<String>>,
        cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
        clock_rx: watch::Receiver<i64>,
        initial_klines: Arc<HashMap<(String, String), DbKline>>,
    ) -> Result<(Self, mpsc::Receiver<WsCmd>, mpsc::Receiver<AggTradeData>)> {
        let (snapshot_req_tx, snapshot_req_rx) = mpsc::channel(8);
        let (ws_cmd_tx, ws_cmd_rx) = mpsc::channel(8);
        let (trade_tx, trade_rx) = mpsc::channel(10240);
        let num_periods = periods.len();
        let is_special_worker = cmd_rx.is_some();

        let initial_capacity_symbols = if is_special_worker {
            10000 - partition_start_index
        } else {
            assigned_symbols.len()
        };

        let total_slots = initial_capacity_symbols * num_periods;

        let mut kline_states = vec![KlineState::default(); total_slots];
        let mut kline_expirations = vec![i64::MAX; total_slots];
        let snapshot_buffers = (vec![KlineData::default(); total_slots], vec![KlineData::default(); total_slots]);

        let mut local_symbol_cache = HashMap::with_capacity(assigned_symbols.len());

        let guard = symbol_to_global_index.read().await;
        for symbol in assigned_symbols {
            if let Some(&global_index) = guard.get(symbol) {
                let local_index = global_index - partition_start_index;
                local_symbol_cache.insert(symbol.clone(), local_index);

                let parse_or_warn = |value: &str, field_name: &str| -> f64 {
                    value.parse().unwrap_or_else(|e| {
                        warn!(
                            target: "计算核心",
                            worker_id,
                            symbol,
                            field_name,
                            error = ?e,
                            "解析DbKline字段失败，使用0.0作为默认值"
                        );
                        0.0
                    })
                };

                for (period_idx, period) in periods.iter().enumerate() {
                    let kline_offset = local_index * num_periods + period_idx;

                    // --- [核心修改] 使用 match 强制处理初始化失败的情况 ---
                    match initial_klines.get(&(symbol.clone(), period.clone())) {
                        Some(db_kline) => {
                            // 成功找到初始数据，填充状态
                            let kline_state = KlineState {
                                open_time: db_kline.open_time,
                                open: parse_or_warn(&db_kline.open, "open"),
                                high: parse_or_warn(&db_kline.high, "high"),
                                low: parse_or_warn(&db_kline.low, "low"),
                                close: parse_or_warn(&db_kline.close, "close"),
                                volume: parse_or_warn(&db_kline.volume, "volume"),
                                turnover: parse_or_warn(&db_kline.quote_asset_volume, "turnover"),
                                trade_count: db_kline.number_of_trades,
                                taker_buy_volume: parse_or_warn(&db_kline.taker_buy_base_asset_volume, "taker_buy_volume"),
                                taker_buy_turnover: parse_or_warn(&db_kline.taker_buy_quote_asset_volume, "taker_buy_turnover"),
                                is_final: false,
                                is_initialized: true,
                            };

                            if kline_offset < kline_states.len() {
                                trace!(
                                    target: "计算核心",
                                    worker_id,
                                    symbol,
                                    period,
                                    open_time = kline_state.open_time,
                                    "成功应用初始K线数据到Worker状态"
                                );
                                kline_states[kline_offset] = kline_state;
                                let interval_ms = interval_to_milliseconds(period);
                                kline_expirations[kline_offset] = db_kline.open_time + interval_ms;
                            } else {
                                // 这是一个不太可能发生的内部逻辑错误，但同样需要硬失败
                                let err_msg = format!(
                                    "Worker-{} 初始化时计算的K线偏移量越界！Symbol: {}, Period: {}, LocalIndex: {}, Offset: {}, Capacity: {}",
                                    worker_id, symbol, period, local_index, kline_offset, kline_states.len()
                                );
                                error!(target: "计算核心", log_type="assertion", "{}", err_msg);
                                return Err(AppError::InitializationError(err_msg).into());
                            }
                        }
                        None => {
                            // --- 失败路径：未找到初始数据，立即报错并退出 ---
                            let err_msg = format!(
                                "Worker-{} 初始化失败：未能从数据源获取品种 '{}' 周期 '{}' 的初始K线数据。请检查数据库或数据补齐逻辑。",
                                worker_id, symbol, period
                            );
                            error!(target: "计算核心", log_type="FATAL", "{}", err_msg);
                            return Err(AppError::InitializationError(err_msg).into());
                        }
                    }
                }
            }
        }

        let worker = Self {
            worker_id,
            periods,
            kline_expirations,
            partition_start_index,
            kline_states,
            pending_trades: HashMap::new(),
            pending_trades_count: 0,
            max_pending_trades: 50000,
            snapshot_buffers,
            active_buffer_index: 0,
            local_symbol_cache,
            managed_symbols_count: assigned_symbols.len(),
            cmd_rx,
            snapshot_req_tx,
            snapshot_req_rx,
            ws_cmd_tx,
            trade_tx,
            last_clock_tick: 0,
            clock_rx,
            performance_stats: PerformanceStats {
                last_reset_time: Instant::now(),
                min_duration_nanos: u128::MAX,
                ..Default::default()
            },
        };

        info!(target: "计算核心", log_type="low_freq", "Worker 实例已创建并完成初始状态填充和索引构建");
        Ok((worker, ws_cmd_rx, trade_rx))
    }

    pub fn get_read_handle(&self) -> WorkerReadHandle {
        WorkerReadHandle {
            snapshot_req_tx: self.snapshot_req_tx.clone(),
        }
    }

    pub fn get_trade_sender(&self) -> mpsc::Sender<AggTradeData> {
        self.trade_tx.clone()
    }

    #[instrument(target = "计算核心", skip_all, name = "run_computation_loop", fields(worker_id = self.worker_id))]
    pub async fn run_computation_loop(
        &mut self,
        mut shutdown_rx: watch::Receiver<bool>,
        mut trade_rx: mpsc::Receiver<AggTradeData>,
        watchdog: Arc<WatchdogV2>, // 接收 Watchdog
    ) {
        // 创建并注册自己的健康报告者
        let health_probe = Arc::new(AtomicI64::new(0));
        let reporter = Arc::new(ComputationHealthReporter {
            worker_id: self.worker_id,
            last_activity: health_probe.clone(),
            timeout: Duration::from_secs(60), // 与旧的 timeout 一致
        });
        watchdog.register(reporter);

        info!(target: "计算核心", log_type="low_freq", "计算循环开始");
        health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
        let is_special_worker = self.cmd_rx.is_some();

        loop {
            tokio::select! {
                biased;  // 关键字：按顺序轮询，不随机选择

                // 最高优先级：关闭信号
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { break; }
                },

                // 第二优先级：时钟处理（修改后的优先级）
                Ok(_) = self.clock_rx.changed() => {
                    let time = *self.clock_rx.borrow();
                    if time > 0 {
                        self.last_clock_tick = time;
                        debug!(time, "收到时钟滴答");
                        self.process_clock_tick(time);
                    }
                    health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                },

                // 第三优先级：交易数据处理
                Some(trade) = trade_rx.recv() => {
                    let start_time = std::time::Instant::now();
                    trace!(target: "计算核心", symbol = %trade.symbol, price = trade.price, "收到交易数据");
                    self.process_trade(trade);
                    let processing_time = start_time.elapsed();

                    // 记录处理延迟 (仅在 debug 模式下)
                    if processing_time.as_micros() > 100 {
                        debug!(target: "性能监控",
                            processing_time_us = processing_time.as_micros(),
                            "交易处理耗时较长"
                        );
                    }

                    health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                },
                Some(response_tx) = self.snapshot_req_rx.recv() => {
                    debug!(target: "计算核心", "收到快照请求");
                    self.process_snapshot_request(response_tx);
                },
                Some(cmd) = async { if let Some(rx) = self.cmd_rx.as_mut() { rx.recv().await } else { std::future::pending().await } }, if is_special_worker => {
                    debug!(target: "计算核心", ?cmd, "收到Worker指令");
                    self.process_command(cmd).await;
                },
            }
        }
        warn!(target: "计算核心", "计算循环退出");
    }

    #[instrument(target = "计算核心", level = "trace", skip(self, trade), fields(symbol = %trade.symbol, price = trade.price))]
    fn process_trade(&mut self, trade: AggTradeData) {
        let local_index = match self.local_symbol_cache.get(&trade.symbol) {
            Some(&idx) => idx,
            None => {
                warn!(target: "计算核心", symbol = %trade.symbol, "收到未被此Worker管理的品种的交易数据，已忽略");
                return;
            }
        };

        let num_periods = self.periods.len();
        let base_offset = local_index * num_periods;
        if base_offset >= self.kline_states.len() {
            error!(
                log_type = "assertion",
                symbol = %trade.symbol,
                local_index,
                base_offset,
                kline_states_len = self.kline_states.len(),
                "计算出的K线偏移量超出预分配容量，数据可能已损坏！"
            );
            return;
        }

        trace!(target: "计算核心", symbol=%trade.symbol, local_index, "开始处理聚合交易");
        let write_buffer = if self.active_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };
        let global_index = local_index + self.partition_start_index;

        for period_idx in 0..num_periods {
            let interval = &self.periods[period_idx];
            let trade_period_start = get_aligned_time(trade.timestamp_ms, interval);
            let kline_offset = base_offset + period_idx;
            let kline = &mut self.kline_states[kline_offset];

            // [核心修改] 重构K线处理逻辑
            if kline.is_initialized && kline.open_time == trade_period_start {
                // --- 路径1 (热路径): 交易属于当前活跃K线，直接更新 ---
                kline.high = kline.high.max(trade.price);
                kline.low = kline.low.min(trade.price);
                kline.close = trade.price;
                kline.volume += trade.quantity;
                kline.turnover += trade.price * trade.quantity;
                kline.trade_count += 1;
                if !trade.is_buyer_maker {
                    kline.taker_buy_volume += trade.quantity;
                    kline.taker_buy_turnover += trade.price * trade.quantity;
                }
            } else if kline.is_initialized && trade_period_start > kline.open_time {
                // --- 路径2 (竞争场景): 交易"抢跑"，属于下一个K线周期，将其暂存 ---

                // [内存保护] 检查等待区是否已达到容量上限
                if self.pending_trades_count >= self.max_pending_trades {
                    error!(
                        target: "计算核心",
                        log_type = "memory_protection",
                        symbol = %trade.symbol,
                        interval = interval,
                        pending_count = self.pending_trades_count,
                        max_pending = self.max_pending_trades,
                        trade_time_ms = trade.timestamp_ms,
                        current_kline_open_time = kline.open_time,
                        trade_kline_open_time = trade_period_start,
                        "等待区已满！疑似时钟系统故障导致交易持续抢跑。为防止内存泄漏，丢弃此交易。"
                    );
                    // 直接丢弃交易，不进入等待区，防止内存无限增长
                    continue;
                }

                warn!(
                    target: "计算核心",
                    symbol = %trade.symbol,
                    interval = interval,
                    trade_time_ms = trade.timestamp_ms,
                    current_kline_open_time = kline.open_time,
                    trade_kline_open_time = trade_period_start,
                    pending_count = self.pending_trades_count,
                    "交易抢跑：时钟滴答延迟，交易数据先于周期切换到达。已暂存。"
                );

                self.pending_trades
                    .entry(trade_period_start)
                    .or_default()
                    .push(trade.clone()); // 必须Clone，因为一个trade可能属于多个周期的未来K线

                // [内存保护] 更新暂存交易计数
                self.pending_trades_count += 1;
            } else {
                // --- 路径3 (其他情况): K线未初始化或收到陈旧交易，记录并忽略 ---
                 warn!(
                    target: "计算核心",
                    symbol = %trade.symbol,
                    interval = interval,
                    trade_time_ms = trade.timestamp_ms,
                    is_initialized = kline.is_initialized,
                    "收到与当前活跃K线周期不匹配的交易数据，已忽略。Trade period: {}, Kline period: {}",
                    trade_period_start,
                    kline.open_time
                );
                // 直接进入下一次循环，不更新任何东西
                continue;
            }

            // 更新快照缓冲区的逻辑保持不变
            write_buffer[kline_offset] = KlineData {
                global_symbol_index: global_index, period_index: period_idx,
                open_time: kline.open_time, open: kline.open, high: kline.high, low: kline.low,
                close: kline.close, volume: kline.volume, turnover: kline.turnover,
                trade_count: kline.trade_count, taker_buy_volume: kline.taker_buy_volume,
                taker_buy_turnover: kline.taker_buy_turnover, is_final: kline.is_final,
                is_updated: true,
            };
        }
    }

    #[instrument(target = "计算核心", level = "debug", skip(self), fields(current_time))]
    fn process_clock_tick(&mut self, current_time: i64) {
        let start_time = std::time::Instant::now();

        let pending_trades_snapshot = std::mem::take(&mut self.pending_trades);
        self.pending_trades_count = 0;

        if !pending_trades_snapshot.is_empty() {
            debug!(
                target: "计算核心",
                captured_pending_periods = pending_trades_snapshot.len(),
                "处理时钟滴答：捕获到抢跑交易快照，包含 {} 个不同的开盘时间点。",
                pending_trades_snapshot.len()
            );
        }

        let write_buffer = if self.active_buffer_index == 0 {
            &mut self.snapshot_buffers.0
        } else {
            &mut self.snapshot_buffers.1
        };

        let num_periods = self.periods.len();
        let total_managed_kline_slots = self.managed_symbols_count * num_periods;

        // 根据feature flag选择SIMD或标准实现
        let (processed_klines, implementation_type) = {
            #[cfg(feature = "simd")]
            {
                let processed = self.process_clock_tick_simd(current_time, pending_trades_snapshot, num_periods, total_managed_kline_slots);
                (processed, "SIMD")
            }
            #[cfg(not(feature = "simd"))]
            {
                let processed = self.process_clock_tick_standard(current_time, pending_trades_snapshot, num_periods, total_managed_kline_slots);
                (processed, "Standard")
            }
        };

        let duration = start_time.elapsed();

        // 更新性能统计
        self.update_performance_stats(duration, processed_klines, total_managed_kline_slots, implementation_type);
    }

    /// 标准线性扫描实现
    #[instrument(target = "计算核心", level = "debug", skip(self, pending_trades_snapshot))]
    fn process_clock_tick_standard(
        &mut self,
        current_time: i64,
        pending_trades_snapshot: HashMap<i64, Vec<AggTradeData>>,
        num_periods: usize,
        total_managed_kline_slots: usize
    ) -> usize {
        let write_buffer = if self.active_buffer_index == 0 {
            &mut self.snapshot_buffers.0
        } else {
            &mut self.snapshot_buffers.1
        };

        let mut processed_count = 0;
        // 【标准实现】对整个K线到期时间数组进行一次线性扫描
        for kline_offset in 0..total_managed_kline_slots {
            let expiration_time = self.kline_expirations[kline_offset];

            // 检查K线是否已到期 (并且不是未初始化的哨兵值)
            if expiration_time != i64::MAX && current_time >= expiration_time {
                // 额外检查，确保我们不会重复处理已经标记为final的K线
                if self.kline_states[kline_offset].is_initialized && !self.kline_states[kline_offset].is_final {
                    processed_count += 1;

                    // 从偏移量反向推导周期和品种信息
                    let period_idx = kline_offset % num_periods;
                    let local_idx = kline_offset / num_periods;
                    let interval = &self.periods[period_idx];
                    let interval_ms = interval_to_milliseconds(interval);

                    // 内联K线处理逻辑以避免借用检查问题
                    let kline = &mut self.kline_states[kline_offset];

                    // === 阶段一：终结旧K线 ===
                    kline.is_final = true;

                    let snapshot_kline = &mut write_buffer[kline_offset];
                    *snapshot_kline = KlineData {
                        global_symbol_index: local_idx + self.partition_start_index,
                        period_index: period_idx,
                        open_time: kline.open_time, open: kline.open, high: kline.high, low: kline.low,
                        close: kline.close, volume: kline.volume, turnover: kline.turnover,
                        trade_count: kline.trade_count, taker_buy_volume: kline.taker_buy_volume,
                        taker_buy_turnover: kline.taker_buy_turnover,
                        is_final: true, is_updated: true,
                    };

                    // === 阶段二：播种新K线并应用暂存数据 ===
                    let next_open_time = kline.open_time + interval_ms;
                    let last_close = kline.close;

                    *kline = KlineState {
                        open_time: next_open_time,
                        open: last_close, high: last_close, low: last_close, close: last_close,
                        volume: 0.0, turnover: 0.0, trade_count: 0,
                        taker_buy_volume: 0.0, taker_buy_turnover: 0.0,
                        is_final: false, is_initialized: true,
                    };

                    // 更新平行的到期时间数组
                    self.kline_expirations[kline_offset] = next_open_time + interval_ms;

                    if let Some(trades_to_apply) = pending_trades_snapshot.get(&next_open_time) {
                        for trade in trades_to_apply {
                            if get_aligned_time(trade.timestamp_ms, interval) == kline.open_time {
                                kline.high = kline.high.max(trade.price);
                                kline.low = kline.low.min(trade.price);
                                kline.close = trade.price;
                                kline.volume += trade.quantity;
                                kline.turnover += trade.price * trade.quantity;
                                kline.trade_count += 1;
                                if !trade.is_buyer_maker {
                                    kline.taker_buy_volume += trade.quantity;
                                    kline.taker_buy_turnover += trade.price * trade.quantity;
                                }
                            }
                        }
                    }
                }
            }
        }

        processed_count
    }

    /// SIMD优化实现
    #[cfg(feature = "simd")]
    #[instrument(target = "计算核心", level = "debug", skip(self, pending_trades_snapshot))]
    fn process_clock_tick_simd(
        &mut self,
        current_time: i64,
        pending_trades_snapshot: HashMap<i64, Vec<AggTradeData>>,
        num_periods: usize,
        total_managed_kline_slots: usize
    ) -> usize {
        let write_buffer = if self.active_buffer_index == 0 {
            &mut self.snapshot_buffers.0
        } else {
            &mut self.snapshot_buffers.1
        };
        use wide::{i64x4, CmpEq, CmpLt};

        const SIMD_WIDTH: usize = 4;
        let current_time_vec = i64x4::splat(current_time);
        let sentinel_vec = i64x4::splat(i64::MAX);

        let mut processed_count = 0;

        // 处理完整的SIMD块
        let full_blocks = total_managed_kline_slots / SIMD_WIDTH;
        for block_idx in 0..full_blocks {
            let base_offset = block_idx * SIMD_WIDTH;

            // 加载4个到期时间
            let mut expirations = [0i64; SIMD_WIDTH];
            for i in 0..SIMD_WIDTH {
                expirations[i] = self.kline_expirations[base_offset + i];
            }
            let expiration_vec = i64x4::from(expirations);

            // SIMD比较：检查哪些K线已到期且不是哨兵值
            let expired_mask = expiration_vec.cmp_lt(current_time_vec) | expiration_vec.cmp_eq(current_time_vec);
            let not_sentinel_mask = !expiration_vec.cmp_eq(sentinel_vec);
            let process_mask = expired_mask & not_sentinel_mask;

            // 如果整个块都没有到期的K线，跳过
            if !process_mask.any() {
                continue;
            }

            // 处理块中到期的K线
            for i in 0..SIMD_WIDTH {
                if process_mask.as_array_ref()[i] != 0 {
                    let kline_offset = base_offset + i;

                    if self.kline_states[kline_offset].is_initialized && !self.kline_states[kline_offset].is_final {
                        processed_count += 1;

                        let period_idx = kline_offset % num_periods;
                        let local_idx = kline_offset / num_periods;
                        let interval = &self.periods[period_idx];
                        let interval_ms = interval_to_milliseconds(interval);

                        // 内联K线处理逻辑
                        let kline = &mut self.kline_states[kline_offset];

                        // === 阶段一：终结旧K线 ===
                        kline.is_final = true;

                        let snapshot_kline = &mut write_buffer[kline_offset];
                        *snapshot_kline = KlineData {
                            global_symbol_index: local_idx + self.partition_start_index,
                            period_index: period_idx,
                            open_time: kline.open_time, open: kline.open, high: kline.high, low: kline.low,
                            close: kline.close, volume: kline.volume, turnover: kline.turnover,
                            trade_count: kline.trade_count, taker_buy_volume: kline.taker_buy_volume,
                            taker_buy_turnover: kline.taker_buy_turnover,
                            is_final: true, is_updated: true,
                        };

                        // === 阶段二：播种新K线并应用暂存数据 ===
                        let next_open_time = kline.open_time + interval_ms;
                        let last_close = kline.close;

                        *kline = KlineState {
                            open_time: next_open_time,
                            open: last_close, high: last_close, low: last_close, close: last_close,
                            volume: 0.0, turnover: 0.0, trade_count: 0,
                            taker_buy_volume: 0.0, taker_buy_turnover: 0.0,
                            is_final: false, is_initialized: true,
                        };

                        // 更新平行的到期时间数组
                        self.kline_expirations[kline_offset] = next_open_time + interval_ms;

                        if let Some(trades_to_apply) = pending_trades_snapshot.get(&next_open_time) {
                            for trade in trades_to_apply {
                                if get_aligned_time(trade.timestamp_ms, interval) == kline.open_time {
                                    kline.high = kline.high.max(trade.price);
                                    kline.low = kline.low.min(trade.price);
                                    kline.close = trade.price;
                                    kline.volume += trade.quantity;
                                    kline.turnover += trade.price * trade.quantity;
                                    kline.trade_count += 1;
                                    if !trade.is_buyer_maker {
                                        kline.taker_buy_volume += trade.quantity;
                                        kline.taker_buy_turnover += trade.price * trade.quantity;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 处理剩余的K线（不足一个SIMD块的部分）
        let remaining_start = full_blocks * SIMD_WIDTH;
        for kline_offset in remaining_start..total_managed_kline_slots {
            let expiration_time = self.kline_expirations[kline_offset];

            if expiration_time != i64::MAX && current_time >= expiration_time {
                if self.kline_states[kline_offset].is_initialized && !self.kline_states[kline_offset].is_final {
                    processed_count += 1;

                    let period_idx = kline_offset % num_periods;
                    let local_idx = kline_offset / num_periods;
                    let interval = &self.periods[period_idx];
                    let interval_ms = interval_to_milliseconds(interval);

                    // 内联K线处理逻辑
                    let kline = &mut self.kline_states[kline_offset];

                    // === 阶段一：终结旧K线 ===
                    kline.is_final = true;

                    let snapshot_kline = &mut write_buffer[kline_offset];
                    *snapshot_kline = KlineData {
                        global_symbol_index: local_idx + self.partition_start_index,
                        period_index: period_idx,
                        open_time: kline.open_time, open: kline.open, high: kline.high, low: kline.low,
                        close: kline.close, volume: kline.volume, turnover: kline.turnover,
                        trade_count: kline.trade_count, taker_buy_volume: kline.taker_buy_volume,
                        taker_buy_turnover: kline.taker_buy_turnover,
                        is_final: true, is_updated: true,
                    };

                    // === 阶段二：播种新K线并应用暂存数据 ===
                    let next_open_time = kline.open_time + interval_ms;
                    let last_close = kline.close;

                    *kline = KlineState {
                        open_time: next_open_time,
                        open: last_close, high: last_close, low: last_close, close: last_close,
                        volume: 0.0, turnover: 0.0, trade_count: 0,
                        taker_buy_volume: 0.0, taker_buy_turnover: 0.0,
                        is_final: false, is_initialized: true,
                    };

                    // 更新平行的到期时间数组
                    self.kline_expirations[kline_offset] = next_open_time + interval_ms;

                    if let Some(trades_to_apply) = pending_trades_snapshot.get(&next_open_time) {
                        for trade in trades_to_apply {
                            if get_aligned_time(trade.timestamp_ms, interval) == kline.open_time {
                                kline.high = kline.high.max(trade.price);
                                kline.low = kline.low.min(trade.price);
                                kline.close = trade.price;
                                kline.volume += trade.quantity;
                                kline.turnover += trade.price * trade.quantity;
                                kline.trade_count += 1;
                                if !trade.is_buyer_maker {
                                    kline.taker_buy_volume += trade.quantity;
                                    kline.taker_buy_turnover += trade.price * trade.quantity;
                                }
                            }
                        }
                    }
                }
            }
        }

        processed_count
    }





    #[instrument(target = "计算核心", level = "debug", skip(self, cmd), fields(command_type = std::any::type_name::<WorkerCmd>()))]
    async fn process_command(&mut self, cmd: WorkerCmd) {
        match cmd {
            WorkerCmd::AddSymbol { symbol, global_index, initial_data, ack } => {
                let local_index = global_index - self.partition_start_index;
                let max_local_index = self.kline_states.len() / self.periods.len();

                if local_index >= max_local_index {
                    error!(
                        log_type = "assertion",
                        symbol, global_index, local_index, max_local_index,
                        "计算出的本地索引超出预分配容量！"
                    );
                    let _ = ack.send(Err("Local index exceeds pre-allocated boundary".to_string()));
                    return;
                }

                info!(target: "计算核心", %symbol, global_index, local_index, ?initial_data, "正在动态添加新品种并创建种子K线");
                self.local_symbol_cache.insert(symbol.clone(), local_index);
                self.managed_symbols_count = (local_index + 1).max(self.managed_symbols_count);

                if self.last_clock_tick > 0 {
                    let num_periods = self.periods.len();
                    let base_offset = local_index * num_periods;
                    let turnover = initial_data.close * initial_data.volume; // 估算 turnover

                    for period_idx in 0..num_periods {
                        let interval = &self.periods[period_idx];
                        let aligned_open_time = get_aligned_time(self.last_clock_tick, interval);
                        let kline_offset = base_offset + period_idx;

                        if kline_offset < self.kline_states.len() {
                            self.kline_states[kline_offset] = KlineState {
                                open_time: aligned_open_time,
                                open: initial_data.open,
                                high: initial_data.high,
                                low: initial_data.low,
                                close: initial_data.close,
                                volume: initial_data.volume,
                                turnover,
                                trade_count: 0,
                                taker_buy_volume: 0.0,
                                taker_buy_turnover: 0.0,
                                is_final: false,
                                is_initialized: true,
                            };

                            // 【修改】为新品种设置初始到期时间
                            let interval_ms = interval_to_milliseconds(interval);
                            self.kline_expirations[kline_offset] = aligned_open_time + interval_ms;

                            trace!(target: "计算核心", %symbol, %interval, open_time = aligned_open_time, "为新品种创建了种子K线");
                        }
                    }
                } else {
                    warn!(target: "计算核心", %symbol, "尚未收到任何时钟滴答，无法为新品种创建种子K线");
                }

                if self.ws_cmd_tx.send(WsCmd::Subscribe(vec![symbol.clone()])).await.is_err() {
                    warn!(target: "计算核心", %symbol, "向I/O任务发送订阅命令失败");
                    let _ = ack.send(Err("Failed to send subscribe command to I/O task".to_string()));
                    return;
                }

                let _ = ack.send(Ok(()));
            }
        }
    }
    
    #[instrument(target = "计算核心", level = "debug", skip(self, response_tx))]
    fn process_snapshot_request(&mut self, response_tx: oneshot::Sender<Vec<KlineData>>) {
        let read_buffer_index = self.active_buffer_index;
        self.active_buffer_index = 1 - self.active_buffer_index;

        let read_buffer = if read_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };

        let updated_data: Vec<KlineData> = read_buffer
            .iter()
            .filter(|kd| kd.is_updated && kd.open_time > 0)
            .cloned()
            .collect();

        let updated_count = updated_data.len();
        read_buffer.iter_mut().filter(|kd| kd.is_updated).for_each(|kd| kd.is_updated = false);
        
        debug!(target: "计算核心", updated_kline_count = updated_count, "快照已生成，准备发送");
        let _ = response_tx.send(updated_data);
    }

    /// 更新性能统计数据
    fn update_performance_stats(&mut self, duration: Duration, processed_klines: usize, total_klines: usize, implementation: &str) {
        let duration_nanos = duration.as_nanos();

        self.performance_stats.total_calls += 1;
        self.performance_stats.total_duration_nanos += duration_nanos;
        self.performance_stats.total_processed_klines += processed_klines as u64;

        if duration_nanos < self.performance_stats.min_duration_nanos {
            self.performance_stats.min_duration_nanos = duration_nanos;
        }
        if duration_nanos > self.performance_stats.max_duration_nanos {
            self.performance_stats.max_duration_nanos = duration_nanos;
        }

        // 每60秒输出一次统计报告
        let now = Instant::now();
        if now.duration_since(self.performance_stats.last_reset_time).as_secs() >= 60 {
            self.log_performance_summary(implementation, total_klines);
            self.reset_performance_stats();
        }
    }

    /// 输出性能统计摘要
    fn log_performance_summary(&self, implementation: &str, total_klines: usize) {
        if self.performance_stats.total_calls == 0 {
            return;
        }

        let avg_duration_nanos = self.performance_stats.total_duration_nanos / self.performance_stats.total_calls as u128;
        let avg_processed_per_call = self.performance_stats.total_processed_klines as f64 / self.performance_stats.total_calls as f64;
        let total_duration_secs = self.performance_stats.total_duration_nanos as f64 / 1_000_000_000.0;
        let calls_per_second = self.performance_stats.total_calls as f64 / 60.0; // 60秒统计周期

        info!(
            target: "性能分析",
            log_type = "performance_summary",
            worker_id = self.worker_id,
            implementation = implementation,

            // 调用统计
            total_calls = self.performance_stats.total_calls,
            calls_per_second = calls_per_second,

            // 时间统计 (纳秒)
            total_duration_nanos = self.performance_stats.total_duration_nanos,
            avg_duration_nanos = avg_duration_nanos,
            min_duration_nanos = self.performance_stats.min_duration_nanos,
            max_duration_nanos = self.performance_stats.max_duration_nanos,

            // 时间统计 (微秒，便于阅读)
            avg_duration_micros = avg_duration_nanos / 1000,
            min_duration_micros = self.performance_stats.min_duration_nanos / 1000,
            max_duration_micros = self.performance_stats.max_duration_nanos / 1000,

            // K线处理统计
            total_klines = total_klines,
            total_processed_klines = self.performance_stats.total_processed_klines,
            avg_processed_per_call = avg_processed_per_call,
            processing_efficiency = if total_klines > 0 {
                avg_processed_per_call / total_klines as f64 * 100.0
            } else {
                0.0
            },

            // 吞吐量统计
            klines_scanned_per_second = if total_duration_secs > 0.0 {
                (self.performance_stats.total_calls as f64 * total_klines as f64) / total_duration_secs
            } else {
                0.0
            },

            "【性能统计-60秒】Worker-{} {} 实现: 调用{}次 平均耗时{:.1}μs ({}~{}μs) 处理{:.1}条/次 扫描{:.0}条/秒",
            self.worker_id,
            implementation,
            self.performance_stats.total_calls,
            avg_duration_nanos / 1000,
            self.performance_stats.min_duration_nanos / 1000,
            self.performance_stats.max_duration_nanos / 1000,
            avg_processed_per_call,
            if total_duration_secs > 0.0 {
                (self.performance_stats.total_calls as f64 * total_klines as f64) / total_duration_secs
            } else {
                0.0
            }
        );
    }

    /// 重置性能统计数据
    fn reset_performance_stats(&mut self) {
        self.performance_stats = PerformanceStats {
            last_reset_time: Instant::now(),
            min_duration_nanos: u128::MAX,
            ..Default::default()
        };
    }
}

// --- 3. I/O 任务实现 ---

#[instrument(target = "I/O核心", skip_all, name="run_io_loop", fields(worker_id))]
pub async fn run_io_loop(
    worker_id: usize,
    initial_symbols: Vec<String>, // 保持参数名不变
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut ws_cmd_rx: mpsc::Receiver<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
    watchdog: Arc<WatchdogV2>, // [修改] 接收 watchdog
) {
    let metrics = Arc::new(IoLoopMetrics::default());
    let reporter = Arc::new(IoHealthReporter {
        worker_id,
        metrics: metrics.clone(),
        no_message_warning_threshold: Duration::from_secs(60),
    });
    watchdog.register(reporter);

    info!(target: "I/O核心", log_type = "low_freq", "I/O 循环启动");

    // 【核心改进 1】: 在循环外部维护一个可变的、代表全量订阅状态的列表。
    // initial_symbols 的所有权被移交，成为 managed_symbols 的初始状态。
    let mut managed_symbols = initial_symbols;

    // --- 新增状态：事务性确认机制 ---
    // 为每个订阅请求生成唯一的ID
    let mut next_request_id: u64 = 1;

    // 追踪待确认的订阅: key是请求ID, value是 (请求的品种列表, 发送时间)
    let mut pending_subscriptions: std::collections::HashMap<u64, (Vec<String>, std::time::Instant)> = std::collections::HashMap::new();

    // 定义订阅超时时间
    const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(30);

    // 创建一个中转通道，因为 AggTradeMessageHandler 需要 UnboundedSender
    let (unbounded_tx, mut unbounded_rx) = tokio::sync::mpsc::unbounded_channel::<AggTradeData>();

    // 启动一个任务来转发数据从 unbounded 到 bounded 通道
    let trade_tx_clone = trade_tx.clone();
    let metrics_clone = metrics.clone();
    log::context::spawn_instrumented(async move {
        while let Some(trade) = unbounded_rx.recv().await {
            // 更新 I/O 指标
            metrics_clone.messages_received.fetch_add(1, Ordering::Relaxed);
            metrics_clone.last_message_timestamp.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
            if trade_tx_clone.send(trade).await.is_err() {
                warn!(target: "I/O核心", "计算任务的trade通道已关闭，I/O数据转发任务退出");
                break;
            }
        }
    });

    // 创建一个消息处理器，它会将解析后的数据发给计算线程
    let handler = Arc::new(AggTradeMessageHandler::with_trade_sender(
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        unbounded_tx,
    ));

    // 使用 ConnectionManager 来处理连接逻辑
    let connection_manager = crate::klcommon::websocket::ConnectionManager::new(
        config.websocket.use_proxy,
        config.websocket.proxy_host.clone(),
        config.websocket.proxy_port,
    );

    // [MODIFIED] 主要的重连循环
    'reconnect_loop: loop {
        if *shutdown_rx.borrow() {
            warn!(target: "I/O核心", "I/O 循环因关闭信号而退出");
            break 'reconnect_loop;
        }

        // 重连时清空待确认池，因为旧连接的请求已经无效
        pending_subscriptions.clear();

        // 【核心改进 2】: 在重连时，使用自己维护的 managed_symbols，而不是原始的 initial_symbols。
        // 这样可以恢复包括动态添加品种在内的完整订阅状态。

        // -- 修正后的逻辑：确保第一个订阅也走事务性确认流程 --
        if managed_symbols.is_empty() && !*shutdown_rx.borrow() {
            // 当没有可管理的品种时，不尝试连接，而是直接等待指令。
            // 这简化了逻辑，因为所有订阅请求（无论是第一个还是后续的）
            // 都将在连接建立后的 'message_loop' 中统一处理。
            info!(target: "I/O核心", "当前无订阅品种，等待订阅指令...");
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => continue 'reconnect_loop,
                // 直接在 'message_loop' 外处理第一个订阅指令
                Some(cmd) = ws_cmd_rx.recv() => {
                    if let WsCmd::Subscribe(new_symbols) = cmd {
                        // 收到第一个订阅命令时，不要立即订阅，
                        // 而是将其作为初始品种，让重连接口去处理。
                        // 这确保了第一个订阅也走事务性确认流程。
                        managed_symbols.extend(new_symbols);
                        // 跳到下一次循环，此时 managed_symbols 不再为空，会正常建立连接和订阅
                        continue 'reconnect_loop;
                    }
                },
            };
        }

        // 从这里开始，逻辑与原来一致
        let streams_to_subscribe = managed_symbols.clone();

        if streams_to_subscribe.is_empty() && !*shutdown_rx.borrow() {
            sleep(Duration::from_secs(1)).await;
            continue 'reconnect_loop;
        }

        info!(target: "I/O核心", symbol_count = streams_to_subscribe.len(), "准备启动 AggTrade WebSocket 客户端");
        metrics.reconnects.fetch_add(1, Ordering::Relaxed);

        // [MODIFIED] 连接一次
        let agg_trade_streams: Vec<String> = streams_to_subscribe.iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();

        // [修改逻辑] 调用 connect 时不再传递任何参数
        let mut ws = match connection_manager.connect().await {
            Ok(ws) => ws,
            Err(e) => {
                error!(target: "I/O核心", error = ?e, "WebSocket 连接失败，将在延迟后重试");
                sleep(Duration::from_secs(5)).await;
                continue 'reconnect_loop;
            }
        };

        // [修改逻辑] 在这里发送初始订阅消息，使用事务性确认机制
        if !agg_trade_streams.is_empty() {
            // 生成唯一的请求ID
            let request_id = next_request_id;
            next_request_id += 1;

            // 构造带有唯一ID的订阅消息
            let subscribe_msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": agg_trade_streams,
                "id": request_id
            }).to_string();

            let frame = fastwebsockets::Frame::text(subscribe_msg.into_bytes().into());

            if let Err(e) = ws.write_frame(frame).await {
                error!(target: "I/O核心", request_id, error = ?e, "发送初始订阅命令失败，准备重连...");
                sleep(Duration::from_secs(5)).await;
                continue 'reconnect_loop;
            } else {
                info!(target: "I/O核心",
                    log_type = "low_freq",
                    worker_id = worker_id,
                    request_id,
                    streams = ?agg_trade_streams,
                    stream_count = agg_trade_streams.len(),
                    "🔗 初始订阅命令已发送，等待服务器确认..."
                );

                // 将初始订阅也加入待确认池
                // 注意：这里我们需要从 agg_trade_streams 中提取出原始的 symbol 名称
                let initial_symbols_for_confirmation: Vec<String> = streams_to_subscribe.clone();
                pending_subscriptions.insert(request_id, (initial_symbols_for_confirmation, std::time::Instant::now()));
            }
        }

        info!(target: "I/O核心", "WebSocket 连接成功建立，开始监听消息...");

        // [MODIFIED] 统一的事件循环
        'message_loop: loop {
            tokio::select! {
                biased;
                // 监听关闭信号
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        warn!(target: "I/O核心", "I/O 循环因关闭信号而退出");
                        break 'reconnect_loop;
                    }
                },
                // 定期检查订阅超时
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    // 复制需要移除的ID，避免在遍历时修改HashMap
                    let mut timed_out_ids = Vec::new();
                    for (id, (_, sent_at)) in &pending_subscriptions {
                        if sent_at.elapsed() > SUBSCRIPTION_TIMEOUT {
                            timed_out_ids.push(*id);
                        }
                    }

                    if !timed_out_ids.is_empty() {
                        error!(target: "I/O核心",
                            ?timed_out_ids,
                            timeout_seconds = SUBSCRIPTION_TIMEOUT.as_secs(),
                            "发现订阅请求超时！连接可能已损坏，将强制重连以恢复状态一致性。"
                        );
                        // 清理超时的请求
                        for id in timed_out_ids {
                            pending_subscriptions.remove(&id);
                        }
                        // 最安全的做法是跳出消息循环，触发整个重连逻辑
                        break 'message_loop;
                    }
                },
                // 监听传入的 WebSocket 消息
                result = ws.read_frame() => {
                    match result {
                        Ok(frame) => {
                            match frame.opcode {
                                fastwebsockets::OpCode::Text => {
                                    let text = String::from_utf8_lossy(&frame.payload).to_string();

                                    // --- 订阅确认逻辑 ---
                                    // 尝试解析为通用的JSON Value
                                    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&text) {
                                        // 检查是否是订阅响应 (包含 id 和 result 字段)
                                        if let Some(id) = json_value.get("id").and_then(|v| v.as_u64()) {
                                            if json_value.get("result").is_some() {
                                                // 这是一个确认/错误消息
                                                if let Some((confirmed_symbols, _)) = pending_subscriptions.remove(&id) {
                                                    // 成功收到确认！
                                                    info!(target: "I/O核心",
                                                        request_id = id,
                                                        confirmed_count = confirmed_symbols.len(),
                                                        symbols = ?confirmed_symbols,
                                                        "✅ 订阅确认成功，更新本地状态"
                                                    );
                                                    // ✨ 原子性保证：只有在收到确认后，才更新 managed_symbols
                                                    managed_symbols.extend(confirmed_symbols);
                                                } else {
                                                    // 收到了一个我们没有记录的ID的响应，可能是一个旧的或重复的响应
                                                    warn!(target: "I/O核心", request_id = id, "收到一个未知的或过期的订阅确认ID");
                                                }
                                                // 确认消息处理完毕，跳过后续的交易处理
                                                continue 'message_loop;
                                            }
                                            // 这里可以进一步处理特定错误码
                                            if let Some(error_code) = json_value.get("code").and_then(|v| v.as_i64()) {
                                                error!(target: "I/O核心", request_id = id, error_code, msg = ?json_value.get("msg"), "订阅请求失败");
                                                // 从待确认池中移除失败的请求，防止内存泄漏
                                                pending_subscriptions.remove(&id);
                                                continue 'message_loop;
                                            }
                                        }
                                    }

                                    // 如果不是确认消息，则按原逻辑处理交易数据
                                    metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                                    metrics.last_message_timestamp.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                                    if let Err(e) = handler.handle_message(worker_id, text).await {
                                        warn!(target: "I/O核心", error = ?e, "消息处理失败");
                                    }
                                },
                                fastwebsockets::OpCode::Close => {
                                    info!(target: "I/O核心", "服务器关闭了连接，准备重连...");
                                    break 'message_loop;
                                }
                                fastwebsockets::OpCode::Ping => {
                                    debug!(target: "I/O核心", "收到 Ping, 自动回复 Pong");
                                    let _ = ws.write_frame(fastwebsockets::Frame::pong(frame.payload)).await;
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            warn!(target: "I/O核心", error = ?e, "读取 WebSocket 帧失败，连接可能已断开，准备重连...");
                            break 'message_loop;
                        }
                    }
                },
                // 监听来自计算线程的命令
                Some(cmd) = ws_cmd_rx.recv() => {
                    if let WsCmd::Subscribe(new_symbols) = cmd {
                        // --- 订阅逻辑重构 ---

                        // 1. 如果没有新品种，则什么都不做
                        if new_symbols.is_empty() {
                            continue;
                        }

                        // 2. 生成唯一的请求ID
                        let request_id = next_request_id;
                        next_request_id += 1;

                        // 3. 构造带有新流和唯一ID的订阅消息
                        let new_streams: Vec<_> = new_symbols.iter().map(|s| format!("{}@aggTrade", s.to_lowercase())).collect();
                        let subscribe_msg = serde_json::json!({
                            "method": "SUBSCRIBE",
                            "params": new_streams,
                            "id": request_id
                        }).to_string();

                        info!(target: "I/O核心",
                            request_id,
                            count = new_symbols.len(),
                            symbols = ?new_symbols,
                            "发送动态订阅指令，等待服务器确认..."
                        );
                        let frame = fastwebsockets::Frame::text(subscribe_msg.into_bytes().into());

                        // 4. 发送订阅请求
                        if let Err(e) = ws.write_frame(frame).await {
                            error!(target: "I/O核心", request_id, error = ?e, "发送动态订阅命令失败，准备重连...");
                            // 发送失败，跳出消息循环以触发重连
                            break 'message_loop;
                        } else {
                            // 5. 将请求存入"待确认"池
                            // 只有在数据成功写入TCP缓冲区后，才认为它处于 pending 状态
                            pending_subscriptions.insert(request_id, (new_symbols, std::time::Instant::now()));
                        }
                    }
                }
            }
        }
        // 如果 message_loop 中断，则会在这里稍作等待后重新进入 reconnect_loop
        sleep(Duration::from_secs(5)).await;
    }
}


// --- 4. 后台任务 (持久化) ---

#[instrument(target = "持久化任务", skip_all, name="persistence_task")]
pub async fn persistence_task(
    db: Arc<Database>,
    worker_handles: Arc<Vec<WorkerReadHandle>>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    watchdog: Arc<WatchdogV2>, // [修改] 接收 watchdog
) {
    let metrics = Arc::new(Mutex::new(PersistenceMetrics::default()));
    let reporter = Arc::new(PersistenceHealthReporter {
        metrics: metrics.clone(),
        max_interval: Duration::from_millis(config.persistence_interval_ms * 3), // e.g., 3倍于周期
    });
    watchdog.register(reporter);

    let persistence_interval = Duration::from_millis(config.persistence_interval_ms);
    let mut interval = interval(persistence_interval);
    info!(target: "持久化任务", log_type = "low_freq", interval_ms = persistence_interval.as_millis(), "持久化任务已启动");

    loop {
        tokio::select! {
            _ = interval.tick() => {
                perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods, metrics.clone()).await;
            },
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(target: "持久化任务", "收到关闭信号，执行最后一次持久化...");
                    perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods, metrics.clone()).await;
                    info!(target: "持久化任务", "最后一次持久化完成");
                    break;
                }
            }
        }
    }
    warn!(target: "持久化任务", "持久化任务已退出");
}

#[instrument(target = "持久化任务", skip_all, name="perform_persistence_cycle")]
async fn perform_persistence_cycle(
    db: &Arc<Database>,
    worker_handles: &Arc<Vec<WorkerReadHandle>>,
    index_to_symbol: &Arc<RwLock<Vec<String>>>,
    periods: &Arc<Vec<String>>,
    metrics: Arc<Mutex<PersistenceMetrics>>, // [修改] 接收 metrics
) {
    let cycle_start = Instant::now();

    let handles = worker_handles.iter().enumerate().map(|(id, h)| async move {
        (id, h.request_snapshot().await)
    });
    let results = futures::future::join_all(handles).await;

    let mut klines_to_save = Vec::new();
    let index_to_symbol_guard = index_to_symbol.read().await;

    for (worker_id, result) in results {
        match result {
            Ok(snapshot) => {
                debug!(target: "持久化任务", worker_id, kline_count = snapshot.len(), "成功从Worker获取快照");
                for kline_data in snapshot {
                    if let (Some(symbol), Some(interval)) = (
                        index_to_symbol_guard.get(kline_data.global_symbol_index),
                        periods.get(kline_data.period_index),
                    ) {
                        let db_kline = DbKline {
                            open_time: kline_data.open_time,
                            open: kline_data.open.to_string(),
                            high: kline_data.high.to_string(),
                            low: kline_data.low.to_string(),
                            close: kline_data.close.to_string(),
                            volume: kline_data.volume.to_string(),
                            close_time: kline_data.open_time + interval_to_milliseconds(interval) - 1,
                            quote_asset_volume: kline_data.turnover.to_string(),
                            number_of_trades: kline_data.trade_count,
                            taker_buy_base_asset_volume: kline_data.taker_buy_volume.to_string(),
                            taker_buy_quote_asset_volume: kline_data.taker_buy_turnover.to_string(),
                            ignore: "0".to_string(),
                        };
                        klines_to_save.push((symbol.clone(), interval.clone(), db_kline));
                    }
                }
            }
            Err(e) => {
                warn!(target: "持久化任务", worker_id, error = ?e, "从Worker获取快照失败");
            }
        }
    }

    if !klines_to_save.is_empty() {
        let count = klines_to_save.len();
        info!(target: "持久化任务", log_type="beacon", kline_count = count, "开始执行批量K线数据持久化");
        if let Err(e) = db.upsert_klines_batch(klines_to_save) {
            error!(target: "持久化任务", error = ?e, "批量K线数据持久化失败");
        } else {
            info!(target: "持久化任务", log_type="beacon", kline_count = count, "批量K线数据持久化成功");
            // [修改] 更新指标
            let mut guard = metrics.lock().unwrap();
            guard.last_successful_run = Some(Instant::now());
            guard.last_batch_size = count;
            guard.last_run_duration_ms = cycle_start.elapsed().as_millis();
        }
    } else {
        debug!(target: "持久化任务", "本次持久化周期内无更新的K线数据");
        // 即使没有数据，也算成功运行了一次
        let mut guard = metrics.lock().unwrap();
        guard.last_successful_run = Some(Instant::now());
        guard.last_batch_size = 0;
        guard.last_run_duration_ms = cycle_start.elapsed().as_millis();
    }
}