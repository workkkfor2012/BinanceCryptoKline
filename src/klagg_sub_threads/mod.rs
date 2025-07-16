//! 高性能K线聚合服务 (完全分区模型) - Worker实现
//!
//! ## 设计核心
//! 1.  **Worker**: 聚合逻辑的核心，运行在专属的、绑核的计算线程上。
//! 2.  **run_io_loop**: Worker的I/O部分，运行在共享的I/O线程池中。
//! 3.  **通道通信**: 计算与I/O之间通过MPSC通道解耦，实现无锁通信。
//! 4.  **本地缓存**: Worker内部使用`symbol->local_index`的本地缓存，实现热路径O(1)查找。

pub mod web_server;

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
use tracing::{debug, error, info, instrument, trace, warn};

// --- 1. 类型定义 ---

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
    partition_start_index: usize,
    kline_states: Vec<KlineState>,
    snapshot_buffers: (Vec<KlineData>, Vec<KlineData>),
    active_buffer_index: usize,
    local_symbol_cache: HashMap<String, usize>,
    managed_symbols_count: usize,
    cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    snapshot_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    ws_cmd_tx: mpsc::Sender<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
    clock_rx: watch::Receiver<i64>,
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

        let kline_states = vec![KlineState::default(); total_slots];
        let snapshot_buffers = (vec![KlineData::default(); total_slots], vec![KlineData::default(); total_slots]);

        let mut local_symbol_cache = HashMap::with_capacity(assigned_symbols.len());
        let guard = symbol_to_global_index.read().await;
        for symbol in assigned_symbols {
            if let Some(&global_index) = guard.get(symbol) {
                let local_index = global_index - partition_start_index;
                local_symbol_cache.insert(symbol.clone(), local_index);
            }
        }

        let worker = Self {
            worker_id,
            periods,
            partition_start_index,
            kline_states,
            snapshot_buffers,
            active_buffer_index: 0,
            local_symbol_cache,
            managed_symbols_count: assigned_symbols.len(),
            cmd_rx,
            snapshot_req_tx,
            snapshot_req_rx,
            ws_cmd_tx,
            trade_tx,
            clock_rx,
        };
        
        info!(target: "计算核心", log_type="low_freq", "Worker 实例已创建");
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
                biased;
                _ = shutdown_rx.changed() => { if *shutdown_rx.borrow() { break; } },
                Some(trade) = trade_rx.recv() => {
                    trace!(target: "计算核心", symbol = %trade.symbol, price = trade.price, "收到交易数据");
                    self.process_trade(trade);
                    health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                },
                Ok(_) = self.clock_rx.changed() => {
                    let time = *self.clock_rx.borrow();
                    if time > 0 { 
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
            None => {
                warn!(target: "计算核心", symbol = %trade.symbol, "收到未被此Worker管理的品种的交易数据，已忽略");
                return;
            }
        };

        let num_periods = self.periods.len();
        let base_offset = local_index * num_periods;
        if base_offset >= self.kline_states.len() {
            // This indicates a severe logic error
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

            if !kline.is_initialized || kline.open_time != trade_period_start {
                *kline = KlineState {
                    open_time: trade_period_start, open: trade.price, high: trade.price, low: trade.price, close: trade.price,
                    volume: trade.quantity, turnover: trade.price * trade.quantity, trade_count: 1,
                    taker_buy_volume: if !trade.is_buyer_maker { trade.quantity } else { 0.0 },
                    taker_buy_turnover: if !trade.is_buyer_maker { trade.price * trade.quantity } else { 0.0 },
                    is_final: false, is_initialized: true,
                };
            } else {
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
        let num_periods = self.periods.len();
        let write_buffer = if self.active_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };

        for local_idx in 0..self.managed_symbols_count {
            for period_idx in 0..num_periods {
                let kline_offset = local_idx * num_periods + period_idx;
                let kline = &mut self.kline_states[kline_offset];

                if kline.is_initialized && !kline.is_final {
                    let interval = &self.periods[period_idx];
                    let interval_ms = interval_to_milliseconds(interval);
                    if current_time >= kline.open_time + interval_ms {
                        kline.is_final = true;
                        let snapshot_kline = &mut write_buffer[kline_offset];
                        snapshot_kline.is_final = true;
                        snapshot_kline.is_updated = true;
                    }
                }
            }
        }
    }

    #[instrument(target = "计算核心", level = "debug", skip(self, cmd), fields(command_type = std::any::type_name::<WorkerCmd>()))]
    async fn process_command(&mut self, cmd: WorkerCmd) {
        match cmd {
            WorkerCmd::AddSymbol { symbol, global_index, ack } => {
                let local_index = global_index - self.partition_start_index;
                let max_local_index = self.kline_states.len() / self.periods.len();

                if local_index >= max_local_index {
                    error!(
                        log_type = "assertion",
                        symbol, global_index, local_index, max_local_index,
                        "计算出的本地索引超出预分配容量，这是一个严重的逻辑错误！"
                    );
                    let _ = ack.send(Err("Local index exceeds pre-allocated boundary".to_string()));
                    return;
                }
                
                info!(target: "计算核心", %symbol, global_index, local_index, "正在动态添加新品种");
                self.local_symbol_cache.insert(symbol.clone(), local_index);
                self.managed_symbols_count = (local_index + 1).max(self.managed_symbols_count);

                if self.ws_cmd_tx.send(WsCmd::Subscribe(vec![symbol.clone()])).await.is_err() {
                    warn!(target: "计算核心", %symbol, "向I/O任务发送订阅命令失败，通道可能已关闭");
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
}

// --- 3. I/O 任务实现 ---

#[instrument(target = "I/O核心", skip_all, name="run_io_loop", fields(worker_id))]
pub async fn run_io_loop(
    worker_id: usize,
    initial_symbols: Vec<String>,
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

        // 初始订阅流
        let streams_to_subscribe = if initial_symbols.is_empty() {
            // 如果一开始没有品种，就等待指令
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => continue 'reconnect_loop,
                Some(cmd) = ws_cmd_rx.recv() => {
                    if let WsCmd::Subscribe(new_symbols) = cmd {
                        new_symbols
                    } else {
                        vec![]
                    }
                },
            }
        } else {
            initial_symbols.clone()
        };

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

        // [修改逻辑] 在这里发送初始订阅消息，而不是在 connect_once 内部。
        if !agg_trade_streams.is_empty() {
            let subscribe_msg = crate::klcommon::websocket::create_subscribe_message(&agg_trade_streams);
            let frame = fastwebsockets::Frame::text(subscribe_msg.into_bytes().into());

            if let Err(e) = ws.write_frame(frame).await {
                error!(target: "I/O核心", error = ?e, "发送初始订阅命令失败，准备重连...");
                sleep(Duration::from_secs(5)).await;
                continue 'reconnect_loop;
            } else {
                info!(target: "I/O核心",
                    log_type = "low_freq",
                    worker_id = worker_id,
                    streams = ?agg_trade_streams,
                    stream_count = agg_trade_streams.len(),
                    "🔗 初始订阅命令已发送"
                );
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
                // 监听传入的 WebSocket 消息
                result = ws.read_frame() => {
                    match result {
                        Ok(frame) => {
                            match frame.opcode {
                                fastwebsockets::OpCode::Text => {
                                    let text = String::from_utf8_lossy(&frame.payload).to_string();
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
                        let new_streams: Vec<_> = new_symbols.iter().map(|s| format!("{}@aggTrade", s.to_lowercase())).collect();

                        // [MODIFIED] 发送订阅命令，而不是重启
                        let subscribe_msg = crate::klcommon::websocket::create_subscribe_message(&new_streams);

                        info!(target: "I/O核心",
                            count = new_symbols.len(),
                            symbols = ?new_symbols,
                            streams = ?new_streams,
                            subscribe_msg = %subscribe_msg,
                            "收到动态订阅指令"
                        );
                        let frame = fastwebsockets::Frame::text(subscribe_msg.into_bytes().into());

                        if let Err(e) = ws.write_frame(frame).await {
                            error!(target: "I/O核心", error = ?e, "发送动态订阅命令失败，准备重连...");
                            break 'message_loop;
                        } else {
                            info!(target: "I/O核心",
                                log_type = "low_freq",
                                worker_id = worker_id,
                                new_streams = ?new_streams,
                                stream_count = new_streams.len(),
                                "🔗 动态订阅命令发送成功"
                            );
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