//! 高性能K线聚合服务 (中心化聚合器模型) - KlineAggregator实现
//!
//! ## 设计核心
//! 1.  **KlineAggregator**: 聚合逻辑的核心，运行在单一的计算线程上。
//! 2.  **run_io_loop**: I/O部分，运行在单一的I/O线程中。
//! 3.  **通道通信**: 计算与I/O之间通过MPSC通道解耦，实现无锁通信。
//! 4.  **全量热路径缓存**: 内部使用`symbol->global_index`的全量缓存，实现热路径O(1)查找。

pub mod web_server;

#[cfg(test)]
mod tests;

// 新增: Gateway 模块，用于组织新代码
mod gateway;

// 新增: 导出新的任务
pub use gateway::{db_writer_task, gateway_task};



use crate::klcommon::{
    api::{get_aligned_time, interval_to_milliseconds},
    error::{AppError, Result},
    models::Kline as DbKline,
    websocket::{
        self, WebSocketClient,
    },
    ComponentStatus, HealthReport, HealthReporter, WatchdogV2,
    AggregateConfig,
};
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::time::Duration;
use tracing::{debug, error, info, instrument, trace, warn};

// --- 1. 类型定义 ---

//=============================================================================
// 入口优化：新的数据结构定义
//=============================================================================

/// 用于从原始WebSocket &[u8]载荷进行零拷贝反序列化的结构体。
/// 字段直接借用于原始的 &[u8] buffer，生命周期为 'a。
#[derive(Deserialize)]
pub struct RawTradePayload<'a> {
    #[serde(rename = "e")]
    pub event_type: &'a str, // 保留此字段用于快速过滤
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "p")]
    pub price: &'a str,
    #[serde(rename = "q")]
    pub quantity: &'a str,
    #[serde(rename = "T")]
    pub timestamp_ms: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// 发送到计算核心的、极度轻量化的交易数据。
/// 它是 `Copy` 类型，跨线程传递开销极小。
#[derive(Debug, Clone, Copy)]
pub struct AggTradePayload {
    pub global_symbol_index: usize,
    pub price: f64,
    pub quantity: f64,
    pub timestamp_ms: i64,
    pub is_buyer_maker: bool,
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

#[derive(Debug, Clone, Default, Copy)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// 增量数据批次，作为系统内部数据交换的基本单元。
/// 整个结构体将被 Arc 包裹以实现零克隆共享。
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeltaBatch {
    /// 统一命名为 klines，与 KlineData 保持一致
    pub klines: Vec<KlineData>,
    /// 批次创建的Unix毫秒时间戳
    pub timestamp_ms: i64,
    /// 用于端到端追踪和日志关联的唯一批次ID，设为必选字段。
    pub batch_id: u64,
    /// 新增批次大小字段，便于监控和调试，避免消费者重复调用 .len()。
    pub size: usize,
}

#[derive(Debug)]
pub enum WorkerCmd {
    /// 添加新品种，ack返回新索引
    AddSymbol {
        symbol: String,
        initial_data: InitialKlineData,
        /// 用于精确定义"创世K线"开盘时间的事件时间戳
        first_kline_open_time: i64,
        ack: oneshot::Sender<std::result::Result<usize, String>>,
    },
    /// 移除品种，ack返回(被删除的旧索引, 变更集)
    RemoveSymbol {
        symbol: String,
        ack: oneshot::Sender<std::result::Result<(usize, Vec<(String, usize)>), String>>,
    },
}

#[derive(Debug)]
pub enum WsCmd {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

#[derive(Clone)]
pub struct AggregatorReadHandle {
    full_snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    deltas_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
}

impl std::fmt::Debug for AggregatorReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregatorReadHandle")
            .field("full_snapshot_req_tx", &"<mpsc::Sender>")
            .field("deltas_req_tx", &"<mpsc::Sender>")
            .finish()
    }
}

impl AggregatorReadHandle {
    pub async fn request_full_snapshot(&self) -> Result<Vec<KlineData>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.full_snapshot_req_tx
            .send(response_tx)
            .await
            .map_err(|e| AppError::ChannelClosed(format!("Failed to send full snapshot request: {}", e)))?;
        response_rx
            .await
            .map_err(|e| AppError::ChannelClosed(format!("Failed to receive full snapshot response: {}", e)))
    }

    pub async fn request_deltas(&self) -> Result<Vec<KlineData>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.deltas_req_tx
            .send(response_tx)
            .await
            .map_err(|e| AppError::ChannelClosed(format!("Failed to send deltas request: {}", e)))?;
        response_rx
            .await
            .map_err(|e| AppError::ChannelClosed(format!("Failed to receive deltas response: {}", e)))
    }


}

// --- 2. 健康报告相关结构体 (保持不变) ---

struct ComputationHealthReporter {
    last_activity: Arc<AtomicI64>,
    timeout: Duration,
}

#[async_trait]
impl HealthReporter for ComputationHealthReporter {
    fn name(&self) -> String { "Computation-Aggregator".to_string() }
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
    metrics: Arc<IoLoopMetrics>,
    no_message_warning_threshold: Duration,
}

#[async_trait]
impl HealthReporter for IoHealthReporter {
    fn name(&self) -> String { "IO-Loop".to_string() }
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



// --- 3. Worker 核心实现 ---

// 【重构】: 使用脏标记机制替代缓冲区机制
pub struct KlineAggregator {
    periods: Arc<Vec<String>>,     // [保留] 用于非热路径，如日志和初始化
    period_milliseconds: Vec<i64>, // [新增] 核心计算的毫秒数来源 (Single Source of Truth)
    kline_expirations: Vec<i64>,
    kline_states: Vec<KlineState>,

    // --- [核心修改] ---
    /// 快速检查K线是否脏的标记位图。
    dirty_flags: Vec<bool>,
    /// 存储所有脏K线索引的向量，用于快速遍历。
    dirty_indices: Vec<usize>,
    // --- [旧字段被移除] ---
    // deltas_buffer: Vec<KlineData>,
    // deltas_buffer_capacity: usize,
    // kline_is_updated: Vec<bool>,
    // snapshotter: Snapshotter<KlineData>,

    /// [修改] local_symbol_cache 语义变更为全量热路径缓存 (symbol -> global_index)
    local_symbol_cache: HashMap<String, usize>,
    /// [新增] 内部反向缓存 (global_index -> symbol)
    global_index_to_symbol_cache: Vec<String>,
    managed_symbols_count: usize,
    cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
    full_snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    full_snapshot_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    deltas_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    deltas_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    ws_cmd_tx: mpsc::Sender<WsCmd>,
    trade_tx: mpsc::Sender<AggTradePayload>,
    last_clock_tick: i64,
    clock_rx: watch::Receiver<i64>,
    // [保留] 动态计算的创世周期索引
    genesis_period_index: usize,
}

impl KlineAggregator {
    #[allow(clippy::too_many_arguments)]
    #[instrument(target = "计算核心", level = "info", skip_all, fields(initial_symbols = assigned_symbols.len()))]
    pub async fn new(
        assigned_symbols: &[String],
        symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
        periods: Arc<Vec<String>>,
        cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
        clock_rx: watch::Receiver<i64>,
        initial_klines: Arc<HashMap<(String, String), DbKline>>,
        config: &AggregateConfig,
    ) -> Result<(Self, mpsc::Receiver<WsCmd>, mpsc::Receiver<AggTradePayload>)> {
        let (full_snapshot_req_tx, full_snapshot_req_rx) = mpsc::channel(8);
        let (deltas_req_tx, deltas_req_rx) = mpsc::channel(8);
        let (ws_cmd_tx, ws_cmd_rx) = mpsc::channel(8);
        let (trade_tx, trade_rx) = mpsc::channel::<AggTradePayload>(10240);
        let num_periods = periods.len();

        // [核心修改] 在初始化时一次性计算所有周期的毫秒数
        let period_milliseconds: Vec<i64> = periods
            .iter()
            .map(|p| interval_to_milliseconds(p))
            .collect();

        // [新增] 动态查找最小周期的索引，使用预计算的毫秒数
        let genesis_period_index = period_milliseconds
            .iter()
            .enumerate()
            .min_by_key(|(_, &ms)| ms)
            .map(|(idx, _)| idx)
            .unwrap_or(0); // 如果 periods 为空则默认为0，但应由配置加载保证其不为空

        // [新增] 增加日志记录，明确启动时使用的创世周期
        if let Some(genesis_period) = periods.get(genesis_period_index) {
            info!(
                target: "计算核心",
                genesis_period = %genesis_period,
                genesis_period_index,
                "动态确定新品种的创世周期"
            );
        } else if !periods.is_empty() {
             // 这种情况理论上不应发生，但作为防御性日志
            warn!(
                target: "计算核心",
                genesis_period_index,
                periods_len = periods.len(),
                "计算出的创世周期索引无效！"
            );
        }

        // [修改] 使用配置中的 max_symbols，更加灵活健壮
        let capacity_symbols = config.max_symbols;
        let total_slots = capacity_symbols * num_periods;

        let mut kline_states = vec![KlineState::default(); total_slots];
        let mut kline_expirations = vec![i64::MAX; total_slots];

        // --- [核心修改] 初始化稀疏集 ---
        let mut dirty_flags = vec![false; total_slots];
        // 预分配容量，减少后续push时的重分配可能
        let mut dirty_indices = Vec::with_capacity(assigned_symbols.len() * num_periods);

        let mut local_symbol_cache = HashMap::with_capacity(assigned_symbols.len());
        // [新增] 初始化反向缓存
        let mut global_index_to_symbol_cache = vec![String::new(); capacity_symbols];

        let guard = symbol_to_global_index.read().await;
        for symbol in assigned_symbols {
            if let Some(&global_index) = guard.get(symbol) {
                // [修改] 直接填充全量缓存 (symbol -> global_index)
                local_symbol_cache.insert(symbol.clone(), global_index);
                // [新增] 填充反向缓存
                if global_index < global_index_to_symbol_cache.len() {
                    global_index_to_symbol_cache[global_index] = symbol.clone();
                }

                let parse_or_warn = |value: &str, field_name: &str| -> f64 {
                    value.parse().unwrap_or_else(|e| {
                        warn!(
                            target: "计算核心",
                            symbol,
                            field_name,
                            error = ?e,
                            "解析DbKline字段失败，使用0.0作为默认值"
                        );
                        0.0
                    })
                };

                for (period_idx, period) in periods.iter().enumerate() {
                    // [修改] 初始化 kline_states 等数组时，直接使用 global_index
                    let kline_offset = global_index * num_periods + period_idx;

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
                                    symbol,
                                    period,
                                    open_time = kline_state.open_time,
                                    "成功应用初始K线数据到KlineAggregator状态"
                                );
                                kline_states[kline_offset] = kline_state;
                                // [核心优化] 初始化kline_expirations时，直接使用预计算的毫秒数
                                let interval_ms = period_milliseconds[period_idx];
                                kline_expirations[kline_offset] = db_kline.open_time + interval_ms;

                                // --- [核心修改] 初始化时标记为脏 ---
                                if !dirty_flags[kline_offset] { // 理论上初始化时总为false，但保留检查是个好习惯
                                    dirty_flags[kline_offset] = true;
                                    dirty_indices.push(kline_offset);
                                    trace!(
                                        target: "计算核心",
                                        symbol,
                                        period,
                                        global_index,
                                        kline_offset,
                                        "初始化时设置脏标记"
                                    );
                                }
                            } else {
                                // 这是一个不太可能发生的内部逻辑错误，但同样需要硬失败
                                let err_msg = format!(
                                    "KlineAggregator 初始化时计算的K线偏移量越界！Symbol: {}, Period: {}, GlobalIndex: {}, Offset: {}, Capacity: {}",
                                    symbol, period, global_index, kline_offset, kline_states.len()
                                );
                                error!(target: "计算核心", log_type="assertion", "{}", err_msg);
                                return Err(AppError::InitializationError(err_msg).into());
                            }
                        }
                        None => {
                            // --- 失败路径：未找到初始数据，立即报错并退出 ---
                            let err_msg = format!(
                                "KlineAggregator 初始化失败：未能从数据源获取品种 '{}' 周期 '{}' 的初始K线数据。请检查数据库或数据补齐逻辑。",
                                symbol, period
                            );
                            error!(target: "计算核心", log_type="FATAL", "{}", err_msg);
                            return Err(AppError::InitializationError(err_msg).into());
                        }
                    }
                }
            }
        }

        let initial_dirty_count = dirty_indices.len();

        let aggregator = Self {
            periods,
            period_milliseconds, // [新增] 存储预计算结果
            kline_expirations,
            kline_states,
            dirty_flags,      // [核心修改]
            dirty_indices,    // [核心修改]
            local_symbol_cache,
            global_index_to_symbol_cache, // [新增]
            managed_symbols_count: assigned_symbols.len(),
            cmd_rx,
            full_snapshot_req_tx,
            full_snapshot_req_rx,
            deltas_req_tx,
            deltas_req_rx,
            ws_cmd_tx,
            trade_tx,
            last_clock_tick: 0,
            clock_rx,
            genesis_period_index, // [保留]
        };
        info!(
            target: "计算核心",
            log_type="low_freq",
            initial_dirty_count,
            total_slots,
            "KlineAggregator 实例已创建并完成初始状态填充和索引构建"
        );
        Ok((aggregator, ws_cmd_rx, trade_rx))
    }

    pub fn get_read_handle(&self) -> AggregatorReadHandle {
        AggregatorReadHandle {
            full_snapshot_req_tx: self.full_snapshot_req_tx.clone(),
            deltas_req_tx: self.deltas_req_tx.clone(),
        }
    }

    pub fn get_trade_sender(&self) -> mpsc::Sender<AggTradePayload> {
        self.trade_tx.clone()
    }

    #[instrument(target = "计算核心", skip_all, name = "run_aggregation_loop", fields())]
    pub async fn run_aggregation_loop(
        &mut self,
        mut shutdown_rx: watch::Receiver<bool>,
        mut trade_rx: mpsc::Receiver<AggTradePayload>,
        watchdog: Arc<WatchdogV2>,
    ) {
        let health_probe = Arc::new(AtomicI64::new(0));
        let reporter = Arc::new(ComputationHealthReporter {
            last_activity: health_probe.clone(),
            timeout: Duration::from_secs(60),
        });
        watchdog.register(reporter);
        info!(target: "计算核心", log_type="low_freq", "聚合循环开始");
        health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
        let has_cmd_channel = self.cmd_rx.is_some();

        // 添加10秒统计计时器
        let mut stats_interval = tokio::time::interval(Duration::from_secs(10));
        stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut trades_count = 0u64;
        let mut klines_updated_count = 0u64;

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { break; }
                },
                _ = stats_interval.tick() => {
                    info!(
                        target: "计算核心",
                        log_type = "low_freq",
                        "周期性统计: trades_per_10s={}, klines_updated_per_10s={}",
                        trades_count, klines_updated_count
                    );
                    trades_count = 0;
                    klines_updated_count = 0;
                },
                Some(trade) = trade_rx.recv() => {
                    trace!(target: "计算核心", global_index = trade.global_symbol_index, price = trade.price, "收到交易数据");
                    self.process_trade(trade);
                    trades_count += 1;
                    klines_updated_count += 1; // 简化统计，每个交易都可能更新K线
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
                Some(response_tx) = self.full_snapshot_req_rx.recv() => {
                    debug!(target: "计算核心", "收到全量快照请求");
                    self.process_full_snapshot_request(response_tx);
                },
                Some(response_tx) = self.deltas_req_rx.recv() => {
                    debug!(target: "计算核心", "收到增量数据请求");
                    self.process_deltas_request(response_tx);
                },
                Some(cmd) = async { if let Some(rx) = self.cmd_rx.as_mut() { rx.recv().await } else { std::future::pending().await } }, if has_cmd_channel => {
                    debug!(target: "计算核心", ?cmd, "收到聚合器指令");
                    self.process_command(cmd).await;
                },
            }
        }
        warn!(target: "计算核心", "聚合循环退出");
    }

    #[instrument(target = "计算核心", level = "trace", skip(self, trade), fields(global_index = trade.global_symbol_index, price = trade.price))]
    fn process_trade(&mut self, trade: AggTradePayload) {
        // [核心简化] 不再需要哈希查找，直接使用索引！
        let global_index = trade.global_symbol_index;

        let num_periods = self.period_milliseconds.len(); // <-- 从新字段获取长度
        let base_offset = global_index * num_periods;

        for period_idx in 0..num_periods {
            let kline_offset = base_offset + period_idx;

            if kline_offset >= self.kline_states.len() {
                 error!(log_type = "assertion", global_index, period_idx, "process_trade: K线偏移量越界！");
                continue;
            }

            // [核心优化] 直接使用毫秒数进行对齐计算，无字符串操作，无函数调用
            let interval_ms = self.period_milliseconds[period_idx];
            // 防御性编程：避免除以0的潜在panic
            if interval_ms == 0 { continue; }
            let trade_period_start = (trade.timestamp_ms / interval_ms) * interval_ms;

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
                trace!(target: "计算核心", global_index, trade_period_start, kline_open_time, "忽略不匹配的交易");
            }
        }
    }

    #[instrument(target = "计算核心", level = "debug", skip(self), fields(current_time))]
    fn process_clock_tick(&mut self, current_time: i64) {
        // current_time 是由全局时钟发送的、略微延迟的整分钟时间戳
        // 我们需要计算出它对应的理论"开盘/收盘"时间点
        let aligned_minute_time = (current_time / 60_000) * 60_000;

        let num_periods = self.period_milliseconds.len(); // <-- 从新字段获取
        // 只检查已激活的品种数量，避免对未使用的槽位进行无效计算
        let num_managed_symbols = self.managed_symbols_count;

        // 1. 遍历所有支持的周期，找出在当前分钟需要检查的周期
        for period_idx in 0..num_periods {
            let interval_ms = self.period_milliseconds[period_idx]; // <-- 直接获取毫秒数

            // 2. 如果当前分钟是该周期的整数倍，则该周期需要检查
            if interval_ms > 0 && aligned_minute_time % interval_ms == 0 {
                // 3. 遍历所有品种属于该周期的K线槽位
                for symbol_idx in 0..num_managed_symbols {
                    let kline_offset = symbol_idx * num_periods + period_idx;

                    // 4. 检查这根K线是否真的到期了
                    // 这个检查是必要的，因为交易驱动可能已经提前滚动了K线
                    if self.kline_expirations[kline_offset] <= current_time {
                        let kline = &self.kline_states[kline_offset];
                        if !kline.is_initialized { continue; }

                        // [核心优化] 直接进行对齐计算，不再需要period字符串或外部函数
                        let next_open_time = (current_time / interval_ms) * interval_ms;

                        // 调用统一的切换函数，它能处理空洞填充等复杂情况
                        self.rollover_kline(kline_offset, next_open_time, None);
                    }
                }
            }
        }
    }








    /// 核心K线切换函数，处理K线终结与播种，保证幂等性，并填充空缺K线。
    fn rollover_kline(&mut self, kline_offset: usize, new_open_time: i64, trade_opt: Option<&AggTradePayload>) {
        // 幂等性保护：如果目标时间不比当前K线开盘时间晚，则直接返回。
        if new_open_time <= self.kline_states[kline_offset].open_time {
            return;
        }

        let period_idx = kline_offset % self.period_milliseconds.len();
        let interval_ms = self.period_milliseconds[period_idx]; // <-- 直接获取，避免了之前的 .clone() 和函数调用

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

        if kline_offset < self.dirty_flags.len() {
            // --- [核心修改] 只有当标记不是脏的时候，才去设置它并记录索引 ---
            if !self.dirty_flags[kline_offset] {
                self.dirty_flags[kline_offset] = true;
                self.dirty_indices.push(kline_offset);

                // 这条日志现在只会在状态从 "干净" -> "脏" 的瞬间打印一次
                let global_symbol_index = kline_offset / self.periods.len();
                let period_index = kline_offset % self.periods.len();
                trace!(
                    target: "计算核心",
                    global_symbol_index,
                    period_index,
                    kline_offset,
                    is_final,
                    "设置脏标记"
                );
            }
        } else {
            error!(
                target: "计算核心",
                log_type = "assertion",
                kline_offset,
                dirty_flags_len = self.dirty_flags.len(),
                "finalize_and_snapshot_kline: 偏移量越界！"
            );
        }
    }

    /// 辅助函数：播种一根新的K线
    fn seed_kline(&mut self, kline_offset: usize, open_time: i64, last_close: f64, trade_opt: Option<&AggTradePayload>) {
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

        let period_idx = kline_offset % self.period_milliseconds.len();
        let interval_ms = self.period_milliseconds[period_idx]; // <-- 直接获取
        self.kline_expirations[kline_offset] = open_time + interval_ms;
    }

    #[instrument(target = "计算核心", level = "debug", skip(self, cmd), fields(command_type = std::any::type_name::<WorkerCmd>()))]
    async fn process_command(&mut self, cmd: WorkerCmd) {
        match cmd {
            WorkerCmd::AddSymbol { symbol, initial_data, first_kline_open_time, ack } => {
                // 在数组末尾添加新品种
                let new_global_index = self.managed_symbols_count;
                let max_global_index = self.kline_states.len() / self.periods.len();

                if new_global_index >= max_global_index {
                    error!(
                        log_type = "assertion",
                        symbol, new_global_index, max_global_index,
                        "全局索引超出预分配容量！"
                    );
                    let _ = ack.send(Err("Global index exceeds pre-allocated boundary".to_string()));
                    return;
                }

                info!(target: "计算核心", %symbol, new_global_index, ?initial_data, event_time = first_kline_open_time, "动态添加新品种(中心化模式)");

                // 更新缓存
                self.local_symbol_cache.insert(symbol.clone(), new_global_index);
                self.global_index_to_symbol_cache[new_global_index] = symbol.clone();
                self.managed_symbols_count += 1;

                // 初始化K线数据
                if first_kline_open_time > 0 {
                    let num_periods = self.periods.len();
                    let base_offset = new_global_index * num_periods;

                    // 1. 对"创世周期"进行特殊处理
                    let genesis_period_idx = self.genesis_period_index;
                    let genesis_interval = match self.periods.get(genesis_period_idx) {
                        Some(p) => p.clone(),
                        None => {
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
                            turnover: initial_data.turnover,
                            trade_count: 1,
                            taker_buy_volume: 0.0,
                            taker_buy_turnover: 0.0,
                            is_final: false,
                            is_initialized: true,
                        };
                        let interval_ms = self.period_milliseconds[self.genesis_period_index];
                        self.kline_expirations[genesis_kline_offset] = genesis_open_time + interval_ms;
                        self.finalize_and_snapshot_kline(genesis_kline_offset, initial_data.close, false);
                        trace!(target: "计算核心", %symbol, %genesis_interval, open_time = genesis_open_time, "为新品种创建了[创世]K线");
                    }

                    // 2. 对所有其他长周期，使用健壮的"播种"逻辑
                    for period_idx in 1..num_periods {
                        let other_interval = self.periods[period_idx].clone();
                        let other_kline_offset = base_offset + period_idx;
                        let aligned_open_time = get_aligned_time(first_kline_open_time, &other_interval);

                        if other_kline_offset < self.kline_states.len() {
                            self.seed_kline(other_kline_offset, aligned_open_time, initial_data.close, None);
                            trace!(target: "计算核心", %symbol, %other_interval, open_time = aligned_open_time, "为新品种[播种]了标准长周期K线");
                        }
                    }
                } else {
                    warn!(target: "计算核心", %symbol, "收到的 first_kline_open_time 无效，无法为新品种创建种子K线");
                }

                // 发送订阅命令
                if self.ws_cmd_tx.send(WsCmd::Subscribe(vec![symbol.clone()])).await.is_err() {
                    warn!(target: "计算核心", %symbol, "向I/O任务发送订阅命令失败");
                    let _ = ack.send(Err("Failed to send subscribe command to I/O task".to_string()));
                    return;
                }

                let _ = ack.send(Ok(new_global_index));
            }

            WorkerCmd::RemoveSymbol { symbol, ack } => {
                let removed_index = match self.local_symbol_cache.remove(&symbol) {
                    Some(idx) => idx,
                    None => {
                        let _ = ack.send(Err(format!("Symbol '{}' not found.", symbol)));
                        return;
                    }
                };

                info!(target: "计算核心", %symbol, removed_index, "开始移除品种并前移数据...");
                let num_periods = self.periods.len();
                let last_active_index = self.managed_symbols_count - 1;

                // 1. 数据前移
                if removed_index < last_active_index {
                    let move_start_index = (removed_index + 1) * num_periods;
                    let dest_index = removed_index * num_periods;
                    let move_count = (last_active_index - removed_index) * num_periods;

                    self.kline_states.copy_within(move_start_index..move_start_index + move_count, dest_index);
                    self.kline_expirations.copy_within(move_start_index..move_start_index + move_count, dest_index);
                    self.dirty_flags.copy_within(move_start_index..move_start_index + move_count, dest_index);
                }

                self.managed_symbols_count -= 1;

                // 2. 更新缓存和构建变更集
                self.global_index_to_symbol_cache.remove(removed_index);
                let mut index_changes = Vec::new();
                for i in removed_index..self.managed_symbols_count {
                    let sym_to_update = &self.global_index_to_symbol_cache[i];
                    if let Some(entry) = self.local_symbol_cache.get_mut(sym_to_update) {
                        *entry = i;
                        index_changes.push((sym_to_update.clone(), i));
                    }
                }

                // 3. 修正脏索引
                self.dirty_indices.retain_mut(|dirty_idx| {
                    let symbol_idx = *dirty_idx / num_periods;
                    if symbol_idx < removed_index {
                        true
                    } else if symbol_idx == removed_index {
                        false
                    } else {
                        *dirty_idx -= num_periods;
                        true
                    }
                });

                // 4. 发送取消订阅命令
                if self.ws_cmd_tx.send(WsCmd::Unsubscribe(vec![symbol.clone()])).await.is_err() {
                    warn!(target: "计算核心", %symbol, "向I/O任务发送取消订阅命令失败");
                }

                let _ = ack.send(Ok((removed_index, index_changes)));
            }
        }
    }
    
    #[instrument(target = "计算核心", level = "debug", skip(self, response_tx))]
    fn process_full_snapshot_request(&mut self, response_tx: oneshot::Sender<Vec<KlineData>>) {
        let snapshot_start = std::time::Instant::now();

        // 预估容量以避免多次内存重分配
        let estimated_size = self.kline_states.iter()
            .filter(|state| state.is_initialized && state.open_time > 0)
            .count();

        let mut full_snapshot = Vec::with_capacity(estimated_size);

        full_snapshot.extend(
            self.kline_states.iter()
                .enumerate()
                .filter(|(_, state)| state.is_initialized && state.open_time > 0)
                .map(|(offset, state)| {
                    let global_symbol_index = offset / self.periods.len();
                    let period_index = offset % self.periods.len();

                    KlineData {
                        global_symbol_index,
                        period_index,
                        open_time: state.open_time,
                        open: state.open,
                        high: state.high,
                        low: state.low,
                        close: state.close,
                        volume: state.volume,
                        turnover: state.turnover,
                        trade_count: state.trade_count,
                        taker_buy_volume: state.taker_buy_volume,
                        taker_buy_turnover: state.taker_buy_turnover,
                        is_final: state.is_final,
                    }
                })
        );

        info!(target: "计算核心",
              snapshot_size = full_snapshot.len(),
              generation_time_ms = snapshot_start.elapsed().as_millis(),
              "生成全量快照完成 (同步模式)");

        if response_tx.send(full_snapshot).is_err() {
            warn!(target: "计算核心", "全量快照发送失败，请求方可能已关闭。");
        }
    }

    #[instrument(target = "计算核心", level = "debug", skip(self, response_tx))]
    fn process_deltas_request(&mut self, response_tx: oneshot::Sender<Vec<KlineData>>) {
        trace!(target: "计算核心", "收到增量数据请求，开始扫描脏标记...");

        // --- [核心修改] ---
        // 1. 如果没有脏数据，快速返回
        if self.dirty_indices.is_empty() {
            if response_tx.send(Vec::new()).is_err() {
                 warn!(target: "计算核心", "增量数据请求方已关闭（无脏数据）");
            }
            return;
        }

        // 2. 原子地拿走脏索引列表的所有权，为下一次拉取周期准备一个空的列表。
        let indices_to_process = std::mem::take(&mut self.dirty_indices);

        // 3. 基于这个（通常很小的）索引列表，高效地打包数据。
        let final_states_snapshot: Vec<KlineData> = indices_to_process
            .iter()
            .map(|&i| { // i 是一个脏索引
                let state = &self.kline_states[i];
                let global_symbol_index = i / self.periods.len();
                let period_index = i % self.periods.len();
                KlineData {
                    global_symbol_index,
                    period_index,
                    open_time: state.open_time,
                    open: state.open,
                    high: state.high,
                    low: state.low,
                    close: state.close,
                    volume: state.volume,
                    turnover: state.turnover,
                    trade_count: state.trade_count,
                    taker_buy_volume: state.taker_buy_volume,
                    taker_buy_turnover: state.taker_buy_turnover,
                    is_final: state.is_final,
                }
            })
            .collect();

        trace!(target: "计算核心", dirty_kline_found = final_states_snapshot.len(), "增量数据打包完成，准备发送");

        if response_tx.send(final_states_snapshot).is_err() {
            error!(
                target: "计算核心",
                log_type = "DATA_LOSS",
                lost_data_count = indices_to_process.len(),
                "增量数据发送失败，本轮更新已在计算核心丢失！请立即检查网关或下游消费者状态！"
            );
            // 即使发送失败，我们也不把索引还回去了。现在需要清理标记位图。
        }

        // 4. (关键) 高效清理标记位图，为下一轮做准备
        // 这个循环的开销是 O(N_dirty)，远小于 O(N_total)
        // [微调采纳] 直接消费Vec(move)，而不是通过引用迭代(&)，语义更清晰。
        for index in indices_to_process {
            if index < self.dirty_flags.len() { // 安全检查
                self.dirty_flags[index] = false;
            }
        }
    }


}

// --- 4. I/O 任务实现 (简化版) ---

#[instrument(target = "I/O核心", skip_all, name="run_io_loop")]
pub async fn run_io_loop(
    initial_symbols: Vec<String>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut ws_cmd_rx: mpsc::Receiver<WsCmd>,
    // [修改] 不再直接传递 trade_tx，而是传递已经配置好的 handler
    handler: Arc<websocket::AggTradeMessageHandler>,
    watchdog: Arc<WatchdogV2>,
) {
    let metrics = Arc::new(IoLoopMetrics::default());
    let reporter = Arc::new(IoHealthReporter {
        metrics: metrics.clone(),
        no_message_warning_threshold: Duration::from_secs(60),
    });
    watchdog.register(reporter);

    info!(target: "I/O核心", log_type = "low_freq", "I/O 循环启动 (入口优化版)");

    // [删除] 不再在这里创建 MessageHandler
    // let handler = ...

    // 创建并配置 AggTradeClient，传入已经创建好的 handler
    let agg_trade_config = websocket::AggTradeConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
        symbols: initial_symbols,
    };
    let mut client = websocket::AggTradeClient::new_with_handler(agg_trade_config, handler);

    // 3. 获取命令发送端
    let command_tx = client.get_command_sender().expect("客户端应已创建命令发送器");

    // 4. 在后台启动客户端，它会自我管理连接、重连和消息处理
    tokio::spawn(async move {
        if let Err(e) = client.start().await {
            error!(target: "I/O核心", error = ?e, "AggTradeClient 意外退出");
        }
    });

    // 5. I/O 任务的主循环现在只负责转发命令
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            },
            Some(cmd) = ws_cmd_rx.recv() => {
                match cmd {
                    WsCmd::Subscribe(new_symbols) => {
                        if new_symbols.is_empty() { continue; }

                        // 将应用层的 WsCmd 转换为通用层的 WsCommand
                        let command = websocket::WsCommand::Subscribe(new_symbols);
                        if command_tx.send(command).await.is_err() {
                            error!(target: "I/O核心", "向 AggTradeClient 发送订阅指令失败，通道已关闭");
                            break;
                        }
                    }
                    WsCmd::Unsubscribe(remove_symbols) => {
                        if remove_symbols.is_empty() { continue; }

                        // 将应用层的 WsCmd 转换为通用层的 WsCommand
                        let command = websocket::WsCommand::Unsubscribe(remove_symbols);
                        if command_tx.send(command).await.is_err() {
                            error!(target: "I/O核心", "向 AggTradeClient 发送取消订阅指令失败，通道已关闭");
                            break;
                        }
                    }
                }
            }
        }
    }
    warn!(target: "I/O核心", "I/O 循环任务已退出");
}


// --- 5. 后台任务 (持久化) ---



