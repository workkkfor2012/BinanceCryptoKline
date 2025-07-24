//! 高性能K线聚合服务 (完全分区模型) - Worker实现
//!
//! ## 设计核心
//! 1.  **Worker**: 聚合逻辑的核心，运行在专属的、绑核的计算线程上。
//! 2.  **run_io_loop**: Worker的I/O部分，运行在共享的I/O线程池中。
//! 3.  **通道通信**: 计算与I/O之间通过MPSC通道解耦，实现无锁通信。
//! 4.  **本地缓存**: Worker内部使用`symbol->local_index`的本地缓存，实现热路径O(1)查找。

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
    ComponentStatus, HealthReport, HealthReporter, WatchdogV2,
    AggregateConfig,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::time::{interval, sleep, Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn};

// --- 1. 类型定义 ---

/// 一个通用的、用于管理双缓冲快照和脏数据跟踪的结构体。
///
/// 此结构体为高性能场景设计，它与一个外部的"脏位图" (`Vec<bool>`) 协同工作，
/// 以实现 O(1) 的更新记录，避免了在热路径上进行任何搜索或排序。
#[derive(Debug)]
pub struct Snapshotter<T>
where
    T: Clone + Default,
{
    /// 双缓冲，元组的第一个元素是缓冲区A，第二个是缓冲区B。
    buffers: (Vec<T>, Vec<T>),
    /// `active_buffer_index` 指向当前用于写入的缓冲区 (0 或 1)。
    active_buffer_index: usize,
    /// 脏表，仅记录了本周期内被更新过的槽位的【唯一】索引。
    updated_indices: Vec<usize>,
}

impl<T> Snapshotter<T>
where
    T: Clone + Default,
{
    /// 创建一个新的 `Snapshotter` 实例。
    pub fn new(capacity: usize, initial_dirty_capacity: usize) -> Self {
        Self {
            buffers: (vec![T::default(); capacity], vec![T::default(); capacity]),
            active_buffer_index: 0,
            updated_indices: Vec::with_capacity(initial_dirty_capacity),
        }
    }

    /// 记录一次数据更新。
    pub fn record_update(&mut self, index: usize, data: T, is_dirty_flag: &mut bool) {
        let write_buffer = if self.active_buffer_index == 0 {
            &mut self.buffers.0
        } else {
            &mut self.buffers.1
        };

        if !*is_dirty_flag {
            self.updated_indices.push(index);
            *is_dirty_flag = true;
        }

        // 我们相信调用者能保证索引有效。
        write_buffer[index] = data;
    }

    /// 提取快照，并为下个周期做准备。
    /// 这是一个原子操作，保证了读取的快照是完整和一致的。
    pub fn take_snapshot(&mut self, slots_is_updated_flags: &mut [bool]) -> Vec<T> {
        // 1. 交换缓冲区。当前 active_buffer 变为 read_buffer。
        let read_buffer_index = self.active_buffer_index;
        self.active_buffer_index = 1 - self.active_buffer_index;

        // 2. 从刚刚冻结的、稳定的 read_buffer 中提取数据。
        let read_buffer = if read_buffer_index == 0 {
            &self.buffers.0
        } else {
            &self.buffers.1
        };

        let updated_data: Vec<T> = self
            .updated_indices
            .iter()
            .map(|&index| read_buffer[index].clone())
            .collect();

        // 3. 重置外部的脏位图标志。
        for &index in &self.updated_indices {
            if let Some(flag) = slots_is_updated_flags.get_mut(index) {
                *flag = false;
            }
        }

        // 4. 清空内部脏表，为新的写周期做准备。
        self.updated_indices.clear();

        updated_data
    }
}

/// 用于封装 AddSymbol 指令携带的初始K线数据
#[derive(Debug, Clone, Copy)]
pub struct InitialKlineData {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
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
    pub trade_count: i64, // 这个字段现在是区分临时K线的关键：0表示临时，>0表示有真实交易
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
    // is_updated 字段被移除，现在 KlineData 是纯粹的数据载体
}

#[derive(Debug)]
pub enum WorkerCmd {
    AddSymbol {
        symbol: String,
        global_index: usize,
        initial_data: InitialKlineData,
        /// 用于精确定义"创世K线"开盘时间的事件时间戳
        first_kline_open_time: i64,
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

// --- 2. 健康报告相关结构体 (保持不变) ---

struct ComputationHealthReporter {
    worker_id: usize,
    last_activity: Arc<AtomicI64>,
    timeout: Duration,
}

#[async_trait]
impl HealthReporter for ComputationHealthReporter {
    fn name(&self) -> String { format!("Computation-Worker-{}", self.worker_id) }
    async fn report(&self) -> HealthReport {
        let last_activity_ms = self.last_activity.load(Ordering::Relaxed);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let elapsed_ms = now_ms.saturating_sub(last_activity_ms);
        let (status, message) = if last_activity_ms == 0 {
            (ComponentStatus::Warning, Some("尚未开始活动".to_string()))
        } else if elapsed_ms > self.timeout.as_millis() as i64 {
            (ComponentStatus::Error, Some(format!("超过 {} 秒无活动", self.timeout.as_secs())))
        } else {
            (ComponentStatus::Ok, None)
        };
        HealthReport {
            component_name: self.name(),
            status,
            message,
            details: [("last_activity_ago_ms".to_string(), serde_json::json!(elapsed_ms))].into(),
        }
    }
}

#[derive(Default)]
struct IoLoopMetrics {
    messages_received: AtomicU64,
    last_message_timestamp: AtomicI64,
    reconnects: AtomicU64,
}

struct IoHealthReporter {
    worker_id: usize,
    metrics: Arc<IoLoopMetrics>,
    no_message_warning_threshold: Duration,
}

#[async_trait]
impl HealthReporter for IoHealthReporter {
    fn name(&self) -> String { format!("IO-Worker-{}", self.worker_id) }
    async fn report(&self) -> HealthReport {
        let last_msg_ts = self.metrics.last_message_timestamp.load(Ordering::Relaxed);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let elapsed_ms = now_ms.saturating_sub(last_msg_ts);
        let (status, message) = if last_msg_ts > 0 && elapsed_ms > self.no_message_warning_threshold.as_millis() as i64 {
            (ComponentStatus::Warning, Some(format!("超过 {} 秒未收到WebSocket消息", self.no_message_warning_threshold.as_secs())))
        } else {
            (ComponentStatus::Ok, None)
        };
        let reconnects = self.metrics.reconnects.load(Ordering::Relaxed);
        let final_status = if reconnects > 10 { ComponentStatus::Warning } else { status };
        HealthReport {
            component_name: self.name(),
            status: final_status,
            message,
            details: [("reconnects".to_string(), serde_json::json!(reconnects)), ("last_message_ago_ms".to_string(), serde_json::json!(elapsed_ms))].into(),
        }
    }
}

#[derive(Default)]
struct PersistenceMetrics {
    last_successful_run: Option<Instant>,
    last_batch_size: usize,
    last_run_duration_ms: u128,
}

struct PersistenceHealthReporter {
    metrics: Arc<Mutex<PersistenceMetrics>>,
    max_interval: Duration,
}

#[async_trait]
impl HealthReporter for PersistenceHealthReporter {
    fn name(&self) -> String { "Persistence-Task".to_string() }
    async fn report(&self) -> HealthReport {
        let guard = self.metrics.lock().unwrap();
        let (status, message) = match guard.last_successful_run {
            Some(last_run) if last_run.elapsed() > self.max_interval => (ComponentStatus::Warning, Some(format!("超过 {} 秒未成功持久化", self.max_interval.as_secs()))),
            None => (ComponentStatus::Warning, Some("尚未执行过持久化".to_string())),
            _ => (ComponentStatus::Ok, None),
        };
        HealthReport {
            component_name: self.name(), status, message,
            details: [("last_batch_size".to_string(), serde_json::json!(guard.last_batch_size)), ("last_run_duration_ms".to_string(), serde_json::json!(guard.last_run_duration_ms))].into(),
        }
    }
}

// --- 3. Worker 核心实现 ---

// 【重构】: 使用 Snapshotter 替代原有的快照机制
pub struct Worker {
    worker_id: usize,
    periods: Arc<Vec<String>>,
    kline_expirations: Vec<i64>,
    partition_start_index: usize,
    kline_states: Vec<KlineState>,

    // --- [核心修改] ---
    /// 与 Snapshotter 协同工作，跟踪每个K线槽位是否在本轮快照中被更新过。
    kline_is_updated: Vec<bool>,
    /// 封装了双缓冲和脏表逻辑的快照器。
    snapshotter: Snapshotter<KlineData>,
    // --- [旧字段被移除] ---
    // snapshot_buffers: (Vec<KlineData>, Vec<KlineData>),
    // active_buffer_index: usize,
    // updated_indices: Vec<usize>,

    local_symbol_cache: HashMap<String, usize>,
    managed_symbols_count: usize,
    cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    snapshot_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    ws_cmd_tx: mpsc::Sender<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
    last_clock_tick: i64,
    clock_rx: watch::Receiver<i64>,
    // [保留] 动态计算的创世周期索引
    genesis_period_index: usize,
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

        // [新增] 动态查找最小周期的索引，不再硬编码
        let genesis_period_index = periods
            .iter()
            .enumerate()
            .min_by_key(|(_, p)| interval_to_milliseconds(p))
            .map(|(idx, _)| idx)
            .unwrap_or(0); // 如果 periods 为空则默认为0，但应由配置加载保证其不为空

        // [新增] 增加日志记录，明确启动时使用的创世周期
        if let Some(genesis_period) = periods.get(genesis_period_index) {
            info!(
                target: "计算核心",
                worker_id,
                genesis_period = %genesis_period,
                genesis_period_index,
                "动态确定新品种的创世周期"
            );
        } else if !periods.is_empty() {
             // 这种情况理论上不应发生，但作为防御性日志
            warn!(
                target: "计算核心",
                worker_id,
                genesis_period_index,
                periods_len = periods.len(),
                "计算出的创世周期索引无效！"
            );
        }

        let initial_capacity_symbols = if is_special_worker {
            10000 - partition_start_index
        } else {
            assigned_symbols.len()
        };

        let total_slots = initial_capacity_symbols * num_periods;

        let mut kline_states = vec![KlineState::default(); total_slots];
        let mut kline_expirations = vec![i64::MAX; total_slots];

        // 【核心修改】: 初始化 Snapshotter 和 kline_is_updated
        let initial_dirty_capacity = assigned_symbols.len().max(256);
        let snapshotter = Snapshotter::new(total_slots, initial_dirty_capacity);
        let kline_is_updated = vec![false; total_slots];

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

        // 【重构】: 使用新的 Snapshotter 架构
        let worker = Self {
            worker_id,
            periods,
            kline_expirations,
            partition_start_index,
            kline_states,
            kline_is_updated, // [新增]
            snapshotter,      // [新增]
            local_symbol_cache,
            managed_symbols_count: assigned_symbols.len(),
            cmd_rx,
            snapshot_req_tx,
            snapshot_req_rx,
            ws_cmd_tx,
            trade_tx,
            last_clock_tick: 0,
            clock_rx,
            genesis_period_index, // [保留]
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
        watchdog: Arc<WatchdogV2>,
    ) {
        let health_probe = Arc::new(AtomicI64::new(0));
        let reporter = Arc::new(ComputationHealthReporter {
            worker_id: self.worker_id,
            last_activity: health_probe.clone(),
            timeout: Duration::from_secs(60),
        });
        watchdog.register(reporter);
        info!(target: "计算核心", log_type="low_freq", "计算循环开始");
        health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
        let is_special_worker = self.cmd_rx.is_some();
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { break; }
                },
                Some(trade) = trade_rx.recv() => {
                    trace!(target: "计算核心", symbol = %trade.symbol, price = trade.price, "收到交易数据");
                    self.process_trade(trade);
                    health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                },
                Ok(_) = self.clock_rx.changed() => {
                    let time = *self.clock_rx.borrow();
                    if time > 0 {
                        self.last_clock_tick = time;
                        debug!(time, "收到时钟滴答");
                        self.process_clock_tick(time);
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
            None => { return; }
        };

        let num_periods = self.periods.len();
        let base_offset = local_index * num_periods;

        for period_idx in 0..num_periods {
            let interval = &self.periods[period_idx];
            let kline_offset = base_offset + period_idx;

            if kline_offset >= self.kline_states.len() {
                 error!(log_type = "assertion", symbol = %trade.symbol, "process_trade: K线偏移量越界！");
                continue;
            }

            let trade_period_start = get_aligned_time(trade.timestamp_ms, interval);

            let kline_open_time = self.kline_states[kline_offset].open_time;

            if trade_period_start == kline_open_time {
                // --- 路径1 (热路径): 更新当前K线 ---
                let final_close = {
                    let kline = &mut self.kline_states[kline_offset];

                    // [修改] 检查 trade_count 是否为 0，以此判断是否为第一笔交易
                    if kline.trade_count == 0 {
                        // 这是本周期的第一笔真实交易，设置OHLCV
                        kline.open = trade.price;
                        kline.high = trade.price;
                        kline.low = trade.price;
                        kline.volume = trade.quantity;
                        kline.turnover = trade.price * trade.quantity;
                        if !trade.is_buyer_maker {
                            kline.taker_buy_volume = trade.quantity;
                            kline.taker_buy_turnover = trade.price * trade.quantity;
                        } else {
                            // 确保在没有买方主动成交时清零
                            kline.taker_buy_volume = 0.0;
                            kline.taker_buy_turnover = 0.0;
                        }
                    } else {
                        // 非首笔交易，执行常规更新
                        kline.high = kline.high.max(trade.price);
                        kline.low = kline.low.min(trade.price);
                        kline.volume += trade.quantity;
                        kline.turnover += trade.price * trade.quantity;
                        if !trade.is_buyer_maker {
                            kline.taker_buy_volume += trade.quantity;
                            kline.taker_buy_turnover += trade.price * trade.quantity;
                        }
                    }
                    // 统一更新 close 和 trade_count
                    kline.trade_count += 1; // 计数器总是在增加
                    // 统一更新 close
                    kline.close = trade.price;
                    kline.close
                };

                // 以非最终状态(is_final=false)更新快照，确保数据实时性
                self.finalize_and_snapshot_kline(kline_offset, final_close, false);

            } else if trade_period_start > kline_open_time {
                // --- 路径2 (切换路径): 交易驱动K线切换 ---
                // 调用统一的切换函数，传入交易数据
                self.rollover_kline(kline_offset, trade_period_start, Some(&trade));
            } else {
                // --- 路径3 (忽略): 陈旧或未初始化的交易 ---
                trace!(target: "计算核心", symbol = %trade.symbol, "忽略不匹配的交易");
            }
        }
    }

    #[instrument(target = "计算核心", level = "debug", skip(self), fields(current_time))]
    fn process_clock_tick(&mut self, current_time: i64) {
        let num_periods = self.periods.len();
        let total_managed_kline_slots = self.managed_symbols_count * num_periods;

        // 遍历所有管理的K线槽位
        for kline_offset in 0..total_managed_kline_slots {
            // 检查当前时间是否超过了K线的预期到期时间
            if current_time >= self.kline_expirations[kline_offset] {
                let kline = &self.kline_states[kline_offset];

                // 如果K线未初始化，则跳过
                if !kline.is_initialized { continue; }

                // 计算理论上的下一个周期的开盘时间
                let interval = &self.periods[kline_offset % num_periods];
                let interval_ms = interval_to_milliseconds(interval);
                let next_open_time = kline.open_time + interval_ms;

                // 调用统一的切换函数，不传入交易数据(None)
                // rollover_kline内部的幂等性检查会处理与事件驱动的并发问题
                self.rollover_kline(kline_offset, next_open_time, None);
            }
        }
    }








    /// 核心K线切换函数，处理K线终结与播种，保证幂等性，并填充空缺K线。
    fn rollover_kline(&mut self, kline_offset: usize, new_open_time: i64, trade_opt: Option<&AggTradeData>) {
        // 幂等性保护：如果目标时间不比当前K线开盘时间晚，则直接返回。
        if new_open_time <= self.kline_states[kline_offset].open_time {
            return;
        }

        let period_idx = kline_offset % self.periods.len();
        let interval = &self.periods[period_idx].clone(); // .clone()是为了后续避免借用冲突
        let interval_ms = interval_to_milliseconds(interval);

        let mut current_open_time = self.kline_states[kline_offset].open_time;
        let last_close = self.kline_states[kline_offset].close;

        // --- 核心修改：循环填充空缺的K线 (Gap Filling) ---
        while current_open_time + interval_ms < new_open_time {
            let next_empty_open_time = current_open_time + interval_ms;

            // 终结当前周期的K线，并将其标记为final
            self.finalize_and_snapshot_kline(kline_offset, last_close, true);

            // 播种一根新的、空的K线
            self.seed_kline(kline_offset, next_empty_open_time, last_close, None);

            // 更新循环变量，为处理下一个可能的空洞做准备
            current_open_time = next_empty_open_time;
        }

        // --- 最后一步：处理最终的目标K线 (new_open_time) ---
        // 终结上一根K线（可能是循环生成的最后一根空K线，也可能是原始K线）
        self.finalize_and_snapshot_kline(kline_offset, last_close, true);

        // 播种最终的目标K线，它可能由交易触发(Some(trade))，也可能由时钟触发(None)
        self.seed_kline(kline_offset, new_open_time, last_close, trade_opt);
    }

    /// 辅助函数：终结当前K线并写入快照
    /// is_final: 标记这是否是一根完整的、已结束的K线
    fn finalize_and_snapshot_kline(&mut self, kline_offset: usize, final_close: f64, is_final: bool) {
        let old_kline = &mut self.kline_states[kline_offset];

        // 如果是标记为final的调用，并且K线已经是final状态，则跳过
        if old_kline.is_final && is_final { return; }

        old_kline.close = final_close;
        old_kline.is_final = is_final;

        let local_idx = kline_offset / self.periods.len();
        let period_idx = kline_offset % self.periods.len();
        let global_symbol_index = local_idx + self.partition_start_index;

        // 1. 创建要快照的数据
        let kline_data = KlineData {
            global_symbol_index,
            period_index: period_idx,
            open_time: old_kline.open_time,
            open: old_kline.open,
            high: old_kline.high,
            low: old_kline.low,
            close: old_kline.close,
            volume: old_kline.volume,
            turnover: old_kline.turnover,
            trade_count: old_kline.trade_count,
            taker_buy_volume: old_kline.taker_buy_volume,
            taker_buy_turnover: old_kline.taker_buy_turnover,
            is_final,
        };

        // 2. 将更新操作委托给 snapshotter
        self.snapshotter.record_update(
            kline_offset,
            kline_data,
            &mut self.kline_is_updated[kline_offset],
        );
    }

    /// 辅助函数：播种一根新的K线
    fn seed_kline(&mut self, kline_offset: usize, open_time: i64, last_close: f64, trade_opt: Option<&AggTradeData>) {
        let new_kline_state = match trade_opt {
            Some(trade) => KlineState { // 事件驱动：K线是确定的
                open_time, open: trade.price, high: trade.price, low: trade.price, close: trade.price,
                volume: trade.quantity, turnover: trade.price * trade.quantity,
                trade_count: 1, // 由真实交易创建，trade_count从1开始
                taker_buy_volume: if !trade.is_buyer_maker { trade.quantity } else { 0.0 },
                taker_buy_turnover: if !trade.is_buyer_maker { trade.price * trade.quantity } else { 0.0 },
                is_final: false, is_initialized: true,
            },
            None => KlineState { // 时钟驱动 / 空K线：K线是临时的
                open_time, open: last_close, high: last_close, low: last_close, close: last_close,
                volume: 0.0, turnover: 0.0,
                trade_count: 0, // 时钟驱动创建，trade_count为0自然表示"临时"
                taker_buy_volume: 0.0, taker_buy_turnover: 0.0,
                is_final: false, is_initialized: true,
            },
        };

        self.kline_states[kline_offset] = new_kline_state;

        let period_idx = kline_offset % self.periods.len();
        let interval_ms = interval_to_milliseconds(&self.periods[period_idx]);
        self.kline_expirations[kline_offset] = open_time + interval_ms;
    }

    #[instrument(target = "计算核心", level = "debug", skip(self, cmd), fields(command_type = std::any::type_name::<WorkerCmd>()))]
    async fn process_command(&mut self, cmd: WorkerCmd) {
        match cmd {
            WorkerCmd::AddSymbol { symbol, global_index, initial_data, first_kline_open_time, ack } => {
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

                info!(target: "计算核心", %symbol, global_index, local_index, ?initial_data, event_time = first_kline_open_time, "动态添加新品种(最终方案)");
                self.local_symbol_cache.insert(symbol.clone(), local_index);
                self.managed_symbols_count = (local_index + 1).max(self.managed_symbols_count);

                // 使用从 SymbolManager 传递过来的精确事件时间戳
                if first_kline_open_time > 0 {
                    let num_periods = self.periods.len();
                    let base_offset = local_index * num_periods;

                    // --- 最终方案：基于事件时间的隔离处理 ---

                    // 1. 对"创世周期"进行特殊处理
                    // [修改] 使用动态计算出的创世周期索引
                    let genesis_period_idx = self.genesis_period_index;

                    // 克隆以避免借用冲突，因为 self 在后续循环中仍被可变借用
                    let genesis_interval = match self.periods.get(genesis_period_idx) {
                        Some(p) => p.clone(),
                        None => {
                            // 这种情况几乎不可能发生，因为索引是在 new 中从同一个 periods 计算的
                            error!(target: "计算核心", log_type="assertion", %symbol, "创世周期索引无效，无法添加新品种！");
                            let _ = ack.send(Err("Invalid genesis period index.".to_string()));
                            return;
                        }
                    };
                    let genesis_kline_offset = base_offset + genesis_period_idx;

                    let genesis_open_time = get_aligned_time(first_kline_open_time, &genesis_interval);

                    if genesis_kline_offset < self.kline_states.len() {
                        self.kline_states[genesis_kline_offset] = KlineState {
                            open_time: genesis_open_time,
                            open: initial_data.open,
                            high: initial_data.high,
                            low: initial_data.low,
                            close: initial_data.close,
                            volume: initial_data.volume,
                            turnover: initial_data.turnover, // 使用真实成交额
                            trade_count: 1, // 作为一个"非空"的明确标记
                            taker_buy_volume: 0.0,  // Ticker数据无此信息
                            taker_buy_turnover: 0.0, // Ticker数据无此信息
                            is_final: false,
                            is_initialized: true,
                        };
                        let interval_ms = interval_to_milliseconds(&genesis_interval);
                        self.kline_expirations[genesis_kline_offset] = genesis_open_time + interval_ms;
                        // 立即快照，使其可被持久化
                        self.finalize_and_snapshot_kline(genesis_kline_offset, initial_data.close, false);
                        trace!(target: "计算核心", %symbol, %genesis_interval, open_time = genesis_open_time, "为新品种创建了[创世]K线");
                    }

                    // 2. 对所有其他长周期，使用健壮的"播种"逻辑 (从1开始)
                    for period_idx in 1..num_periods {
                        let other_interval = self.periods[period_idx].clone();
                        let other_kline_offset = base_offset + period_idx;
                        let aligned_open_time = get_aligned_time(first_kline_open_time, &other_interval);

                        if other_kline_offset < self.kline_states.len() {
                            // 调用标准 seed_kline，使用最可信的 close 价播种，并将 trade_count 置为0
                            self.seed_kline(other_kline_offset, aligned_open_time, initial_data.close, None);
                            trace!(target: "计算核心", %symbol, %other_interval, open_time = aligned_open_time, "为新品种[播种]了标准长周期K线");
                        }
                    }
                } else {
                    warn!(target: "计算核心", %symbol, "收到的 first_kline_open_time 无效，无法为新品种创建种子K线");
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
        // 1. 从 snapshotter 获取快照。此调用是原子的。
        let updated_data = self.snapshotter.take_snapshot(&mut self.kline_is_updated);

        let valid_data: Vec<KlineData> = updated_data
            .into_iter()
            .filter(|k| k.open_time > 0)
            .collect();

        debug!(target = "计算核心", candidate_kline_count = valid_data.len(), "快照数据已收集，准备发送");

        // 2. 发送数据
        if response_tx.send(valid_data).is_err() {
            warn!(target = "计算核心", "快照发送失败，接收端可能已关闭。由于快照已被提取，本轮数据将丢失。");
        }
    }


}

// --- 4. I/O 任务实现 (简化版) ---

#[instrument(target = "I/O核心", skip_all, name="run_io_loop", fields(worker_id))]
pub async fn run_io_loop(
    worker_id: usize,
    initial_symbols: Vec<String>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut ws_cmd_rx: mpsc::Receiver<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
    watchdog: Arc<WatchdogV2>,
) {
    let metrics = Arc::new(IoLoopMetrics::default());
    let reporter = Arc::new(IoHealthReporter {
        worker_id,
        metrics: metrics.clone(),
        no_message_warning_threshold: Duration::from_secs(60),
    });
    watchdog.register(reporter);

    info!(target: "I/O核心", log_type = "low_freq", "I/O 循环启动");

    let mut managed_symbols = initial_symbols;

    // 【简化】: 移除复杂的事务性确认机制，使用简单的WebSocket连接
    let (unbounded_tx, mut unbounded_rx) = tokio::sync::mpsc::unbounded_channel::<AggTradeData>();

    let trade_tx_clone = trade_tx.clone();
    let metrics_clone = metrics.clone();
    log::context::spawn_instrumented(async move {
        while let Some(trade) = unbounded_rx.recv().await {
            metrics_clone.messages_received.fetch_add(1, Ordering::Relaxed);
            metrics_clone.last_message_timestamp.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
            if trade_tx_clone.send(trade).await.is_err() {
                warn!(target: "I/O核心", "计算任务的trade通道已关闭，I/O数据转发任务退出");
                break;
            }
        }
    });

    let handler = Arc::new(AggTradeMessageHandler::with_trade_sender(
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        unbounded_tx,
    ));

    let connection_manager = crate::klcommon::websocket::ConnectionManager::new(
        config.websocket.use_proxy,
        config.websocket.proxy_host.clone(),
        config.websocket.proxy_port,
    );

    // 【简化】: 简单的重连循环
    'reconnect_loop: loop {
        if *shutdown_rx.borrow() {
            warn!(target: "I/O核心", "I/O 循环因关闭信号而退出");
            break 'reconnect_loop;
        }

        if managed_symbols.is_empty() && !*shutdown_rx.borrow() {
            info!(target: "I/O核心", "当前无订阅品种，等待订阅指令...");
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => continue 'reconnect_loop,
                Some(cmd) = ws_cmd_rx.recv() => {
                    let WsCmd::Subscribe(new_symbols) = cmd;
                    managed_symbols.extend(new_symbols);
                    continue 'reconnect_loop;
                },
            };
        }

        let streams_to_subscribe = managed_symbols.clone();
        if streams_to_subscribe.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue 'reconnect_loop;
        }

        info!(target: "I/O核心", symbol_count = streams_to_subscribe.len(), "准备启动 AggTrade WebSocket 客户端");
        metrics.reconnects.fetch_add(1, Ordering::Relaxed);

        let agg_trade_streams: Vec<String> = streams_to_subscribe.iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();

        let mut ws = match connection_manager.connect().await {
            Ok(ws) => ws,
            Err(e) => {
                error!(target: "I/O核心", error = ?e, "WebSocket 连接失败，将在延迟后重试");
                sleep(Duration::from_secs(5)).await;
                continue 'reconnect_loop;
            }
        };

        // 【简化】: 直接发送订阅消息，不使用事务性确认
        if !agg_trade_streams.is_empty() {
            let subscribe_msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": agg_trade_streams
            }).to_string();

            let frame = fastwebsockets::Frame::text(subscribe_msg.into_bytes().into());
            if let Err(e) = ws.write_frame(frame).await {
                error!(target: "I/O核心", error = ?e, "发送初始订阅命令失败，准备重连...");
                sleep(Duration::from_secs(5)).await;
                continue 'reconnect_loop;
            }
            info!(target: "I/O核心", stream_count = agg_trade_streams.len(), "初始订阅命令已发送");
        }

        info!(target: "I/O核心", "WebSocket 连接成功建立，开始监听消息...");

        // 【简化】: 简单的消息循环
        'message_loop: loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        warn!(target: "I/O核心", "I/O 循环因关闭信号而退出");
                        break 'reconnect_loop;
                    }
                },
                result = ws.read_frame() => {
                    match result {
                        Ok(frame) => {
                            match frame.opcode {
                                fastwebsockets::OpCode::Text => {
                                    let text = String::from_utf8_lossy(&frame.payload).to_string();
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
                Some(cmd) = ws_cmd_rx.recv() => {
                    let WsCmd::Subscribe(new_symbols) = cmd;
                    if new_symbols.is_empty() {
                        continue;
                    }

                    let new_streams: Vec<_> = new_symbols.iter().map(|s| format!("{}@aggTrade", s.to_lowercase())).collect();
                    let subscribe_msg = serde_json::json!({
                        "method": "SUBSCRIBE",
                        "params": new_streams
                    }).to_string();

                    info!(target: "I/O核心", count = new_symbols.len(), symbols = ?new_symbols, "发送动态订阅指令");
                    let frame = fastwebsockets::Frame::text(subscribe_msg.into_bytes().into());

                    if let Err(e) = ws.write_frame(frame).await {
                        error!(target: "I/O核心", error = ?e, "发送动态订阅命令失败，准备重连...");
                        break 'message_loop;
                    } else {
                        managed_symbols.extend(new_symbols);
                    }
                }
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}


// --- 5. 后台任务 (持久化) ---

/// 定义非最终K线（进行中）的持久化频率。
/// 例如，值为5表示每5个持久化周期才将"进行中"的K线更新写入一次数据库。
/// 这可以显著降低在高频交易期间的数据库写入压力。
const ONGOING_KLINE_PERSIST_FREQUENCY: u64 = 5;

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
    info!(
        target: "持久化任务",
        log_type = "low_freq",
        interval_ms = persistence_interval.as_millis(),
        ongoing_persist_freq_cycles = ONGOING_KLINE_PERSIST_FREQUENCY,
        "持久化任务已启动，采用差异化降频策略"
    );

    // 新增周期计数器
    let mut cycle_counter: u64 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // 修改：传递计数器和force标志(false)
                cycle_counter = cycle_counter.wrapping_add(1);
                perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods, metrics.clone(), cycle_counter, false).await;
            },
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(target: "持久化任务", "收到关闭信号，执行最后一次强制全量持久化...");
                    // 修改：传递计数器和force标志(true)以确保所有数据都被保存
                    perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods, metrics.clone(), cycle_counter, true).await;
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
    metrics: Arc<Mutex<PersistenceMetrics>>,
    // 修改：接收新参数
    cycle_count: u64,
    force_save_ongoing: bool,
) {
    let cycle_start = Instant::now();

    let handles = worker_handles.iter().enumerate().map(|(id, h)| async move {
        (id, h.request_snapshot().await)
    });
    let results = futures::future::join_all(handles).await;

    // 修改：创建两个独立的Vec来分类K线
    let mut final_klines_to_save = Vec::new();
    let mut ongoing_klines_to_save = Vec::new();
    let index_to_symbol_guard = index_to_symbol.read().await;

    for (worker_id, result) in results {
        match result {
            Ok(snapshot) => {
                trace!(target: "持久化任务", worker_id, kline_count = snapshot.len(), "成功从Worker获取快照");
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

                        // 核心修改：根据 is_final 标志进行分类
                        if kline_data.is_final {
                            final_klines_to_save.push((symbol.clone(), interval.clone(), db_kline));
                        } else {
                            ongoing_klines_to_save.push((symbol.clone(), interval.clone(), db_kline));
                        }
                    }
                }
            }
            Err(e) => {
                warn!(target: "持久化任务", worker_id, error = ?e, "从Worker获取快照失败");
            }
        }
    }

    let mut total_saved_count = 0;

    // --- 1. 持久化最终确定的K线 (高优先级，每次都执行) ---
    if !final_klines_to_save.is_empty() {
        let count = final_klines_to_save.len();
        info!(target: "持久化任务", log_type="beacon", kline_type = "final", count, "开始持久化最终K线 (高优先级)");
        if let Err(e) = db.upsert_klines_batch(final_klines_to_save) {
            error!(target: "持久化任务", error = ?e, "持久化最终K线失败");
        } else {
            total_saved_count += count;
        }
    }

    // --- 2. 有条件地持久化进行中的K线 (低优先级，降频执行) ---
    let should_save_ongoing = force_save_ongoing || (cycle_count % ONGOING_KLINE_PERSIST_FREQUENCY == 0);

    if !ongoing_klines_to_save.is_empty() {
        if should_save_ongoing {
            let count = ongoing_klines_to_save.len();
            info!(target: "持久化任务", log_type="beacon", kline_type = "ongoing", count, cycle = cycle_count, forced = force_save_ongoing, "开始持久化进行中K线 (低优先级)");
            if let Err(e) = db.upsert_klines_batch(ongoing_klines_to_save) {
                error!(target: "持久化任务", error = ?e, "持久化进行中K线失败");
            } else {
                total_saved_count += count;
            }
        } else {
            // 仅在trace级别记录跳过，避免日志刷屏
            trace!(target: "持久化任务", kline_type="ongoing", count = ongoing_klines_to_save.len(), cycle = cycle_count, "跳过本次进行中K线的持久化");
        }
    }

    // --- 3. 更新监控指标 ---
    if total_saved_count > 0 {
        // 更新指标
        let mut guard = metrics.lock().unwrap();
        guard.last_successful_run = Some(Instant::now());
        guard.last_batch_size = total_saved_count;
        guard.last_run_duration_ms = cycle_start.elapsed().as_millis();
    } else {
        // 即使没有数据写入，也更新运行时间戳，表示任务仍在正常调度
        debug!(target: "持久化任务", "本次持久化周期内无数据写入数据库");
        let mut guard = metrics.lock().unwrap();
        guard.last_successful_run = Some(Instant::now());
        guard.last_batch_size = 0;
        guard.last_run_duration_ms = cycle_start.elapsed().as_millis();
    }
}