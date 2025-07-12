//! 高性能K线聚合服务 (完全分区模型) - Worker实现
//!
//! ## 设计核心
//! 1.  **Worker**: 聚合逻辑的核心，运行在专属的、绑核的计算线程上。
//! 2.  **run_io_loop**: Worker的I/O部分，运行在共享的I/O线程池中。
//! 3.  **通道通信**: 计算与I/O之间通过MPSC通道解耦，实现无锁通信。
//! 4.  **本地缓存**: Worker内部使用`symbol->local_index`的本地缓存，实现热路径O(1)查找。

use crate::klcommon::{
    api::{get_aligned_time, interval_to_milliseconds},
    context::Instrumented,
    db::Database,
    error::{AppError, Result},
    models::Kline as DbKline,
    websocket::{
        AggTradeClient, AggTradeConfig, AggTradeData,
        AggTradeMessageHandler, WebSocketClient,
    },
    AggregateConfig,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::time::{interval, sleep, Duration};
use tracing::{debug, error, info, trace, warn};
use tracing_futures::Instrument;
use kline_macros::perf_profile;

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
        // ack通道现在传递Result，以通知成功或失败
        ack: oneshot::Sender<std::result::Result<(), String>>,
    },
}

#[derive(Debug)]
pub enum WsCmd {
    // [MODIFIED] 订阅指令现在只包含原始的symbol列表
    Subscribe(Vec<String>),
}

#[derive(Clone)]
pub struct WorkerReadHandle {
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
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

// --- 2. Worker 核心实现 (计算部分) ---

pub struct Worker {
    worker_id: usize,
    periods: Arc<Vec<String>>,
    partition_start_index: usize,
    kline_states: Vec<KlineState>,
    snapshot_buffers: (Vec<KlineData>, Vec<KlineData>),
    active_buffer_index: usize,
    local_symbol_cache: HashMap<String, usize>,
    managed_symbols_count: usize,
    cmd_rx: Option<mpsc::Receiver<Instrumented<WorkerCmd>>>,
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
    snapshot_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    ws_cmd_tx: mpsc::Sender<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
    clock_rx: watch::Receiver<i64>,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    // [MODIFIED] Worker::new is now an async function
    pub async fn new(
        worker_id: usize,
        partition_start_index: usize,
        assigned_symbols: &[String],
        symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
        periods: Arc<Vec<String>>,
        cmd_rx: Option<mpsc::Receiver<Instrumented<WorkerCmd>>>,
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
        info!(worker_id, initial_capacity_symbols, total_slots, "Allocating state buffers.");

        let kline_states = vec![KlineState::default(); total_slots];
        let snapshot_buffers = (vec![KlineData::default(); total_slots], vec![KlineData::default(); total_slots]);

        let mut local_symbol_cache = HashMap::with_capacity(assigned_symbols.len());
        // [MODIFIED] No longer uses block_on, now it awaits the read lock.
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

    pub async fn run_computation_loop(
        &mut self,
        mut shutdown_rx: watch::Receiver<bool>,
        mut trade_rx: mpsc::Receiver<AggTradeData>,
        health_probe: Arc<AtomicI64>,
    ) {
        info!(log_type = "low_freq", worker_id = self.worker_id, "计算循环启动...");
        health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
        let is_special_worker = self.cmd_rx.is_some();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => { if *shutdown_rx.borrow() { info!(worker_id = self.worker_id, "Shutdown signal received."); break; } },
                Some(trade) = trade_rx.recv() => {
                    self.process_trade(trade);
                    health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                },
                Ok(_) = self.clock_rx.changed() => {
                    let time = *self.clock_rx.borrow();
                    if time > 0 { self.process_clock_tick(time); }
                    health_probe.store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
                },
                Some(response_tx) = self.snapshot_req_rx.recv() => { self.process_snapshot_request(response_tx); },
                Some(instrumented_cmd) = async { if let Some(rx) = self.cmd_rx.as_mut() { rx.recv().await } else { std::future::pending().await } }, if is_special_worker => {
                    // 解构为 payload 和 span，避免借用冲突
                    let (cmd, span) = instrumented_cmd.into_parts();
                    // 使用 instrument() 方法将 future 包装在 Span 中执行
                    self.process_command(cmd).instrument(span).await;
                },
            }
        }
        info!(log_type = "low_freq", worker_id = self.worker_id, "计算循环结束");
    }

    #[perf_profile(
        name = "trade_processing",
        level = "trace",
        skip_all,
        fields(
            worker_id = self.worker_id,
            symbol = %trade.symbol,
            price = trade.price,
            quantity = trade.quantity
        )
    )]
    fn process_trade(&mut self, trade: AggTradeData) {
        let local_index = match self.local_symbol_cache.get(&trade.symbol) {
            Some(&idx) => idx,
            None => {
                warn!(worker_id = self.worker_id, symbol = %trade.symbol, "收到未缓存品种的交易数据，丢弃");
                return;
            }
        };

        let num_periods = self.periods.len();
        let base_offset = local_index * num_periods;
        if base_offset >= self.kline_states.len() {
            error!(worker_id = self.worker_id, symbol = %trade.symbol, local_index, "计算的本地索引超出边界！");
            return;
        }

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

    #[perf_profile(skip_all, fields(worker_id = self.worker_id, time = current_time))]
    fn process_clock_tick(&mut self, current_time: i64) {
        trace!(worker_id = self.worker_id, time = current_time, "Processing clock tick");
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

    // [MODIFIED] Now sends an acknowledgement back to the SymbolManager
    async fn process_command(&mut self, cmd: WorkerCmd) {
        info!(worker_id = self.worker_id, "处理指令: {:?}", cmd);
        match cmd {
            WorkerCmd::AddSymbol { symbol, global_index, ack } => {
                let local_index = global_index - self.partition_start_index;
                let max_local_index = self.kline_states.len() / self.periods.len();

                if local_index >= max_local_index {
                    error!(worker_id = self.worker_id, %symbol, global_index, local_index, max_local_index, "新品种本地索引超出预分配边界！");
                    let _ = ack.send(Err("Local index exceeds pre-allocated boundary".to_string()));
                    return;
                }

                self.local_symbol_cache.insert(symbol.clone(), local_index);
                self.managed_symbols_count = (local_index + 1).max(self.managed_symbols_count);
                info!(worker_id = self.worker_id, %symbol, global_index, local_index, "新品种状态初始化完成");

                // Send command to I/O task to subscribe to the new symbol
                let cmd = WsCmd::Subscribe(vec![symbol.clone()]);
                if self.ws_cmd_tx.send(cmd).await.is_err() {
                    error!(worker_id = self.worker_id, %symbol, "向I/O任务发送订阅指令失败");
                    let _ = ack.send(Err("Failed to send subscribe command to I/O task".to_string()));
                    return;
                }
                info!(worker_id = self.worker_id, %symbol, "订阅指令已发送到I/O任务");

                // 发送成功确认信号
                if ack.send(Ok(())).is_err() {
                    warn!(worker_id = self.worker_id, %symbol, "向品种管理器发送确认信号失败，可能已超时");
                }
            }
        }
    }

    #[perf_profile(skip_all, fields(worker_id = self.worker_id))]
    fn process_snapshot_request(&mut self, response_tx: oneshot::Sender<Vec<KlineData>>) {
        trace!(worker_id = self.worker_id, "Processing snapshot request");
        let read_buffer_index = self.active_buffer_index;
        self.active_buffer_index = 1 - self.active_buffer_index;

        let read_buffer = if read_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };

        let updated_data: Vec<KlineData> = read_buffer
            .iter()
            .filter(|kd| kd.is_updated && kd.open_time > 0)
            .cloned()
            .collect();

        read_buffer.iter_mut().filter(|kd| kd.is_updated).for_each(|kd| kd.is_updated = false);

        if response_tx.send(updated_data).is_err() {
            warn!(worker_id = self.worker_id, "Snapshot request channel closed before sending response.");
        }
    }
}

// --- 3. I/O 任务实现 ---

// [MODIFIED] run_io_loop is now more robust against reconnections
pub async fn run_io_loop(
    worker_id: usize,
    initial_symbols: Vec<String>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut ws_cmd_rx: mpsc::Receiver<WsCmd>,
    trade_tx: mpsc::Sender<AggTradeData>,
) {
    info!(log_type = "low_freq", worker_id, "I/O循环启动...");
    // [NEW] This list tracks all symbols this I/O loop is responsible for.
    let mut managed_symbols = initial_symbols;

    // 创建一个转换通道，将 AggTradeData 转发到计算线程
    let (klcommon_sender, mut klcommon_receiver) = tokio::sync::mpsc::unbounded_channel::<AggTradeData>();

    // 启动转换任务
    let trade_tx_clone = trade_tx.clone();
    tokio::spawn(async move {
        while let Some(trade) = klcommon_receiver.recv().await {
            if trade_tx_clone.send(trade).await.is_err() {
                trace!("Computation loop trade channel closed.");
                break;
            }
        }
    });
    
    loop {
        // 检查是否有品种需要处理
        if managed_symbols.is_empty() {
            info!(worker_id, "No symbols assigned to this worker, waiting for commands...");

            // 等待命令或关闭信号
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(worker_id, "I/O loop received shutdown signal.");
                        return;
                    }
                },
                Some(cmd) = ws_cmd_rx.recv() => {
                    info!(worker_id, ?cmd, "I/O task received command.");
                    match cmd {
                        WsCmd::Subscribe(new_symbols) => {
                            // 更新管理的品种列表
                            managed_symbols.extend(new_symbols.clone());
                            info!(worker_id, new_count = managed_symbols.len(), "Updated managed symbols list.");
                            // 继续到下一次循环尝试连接
                            continue;
                        }
                    }
                },
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    // 定期检查是否有新的品种分配
                    continue;
                }
            }
        }

        // [MODIFIED] Always use the current `managed_symbols` list for connections.
        let _streams_to_subscribe: Vec<String> = managed_symbols
            .iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();

        let agg_trade_config = AggTradeConfig {
            use_proxy: config.websocket.use_proxy,
            proxy_addr: config.websocket.proxy_host.clone(),
            proxy_port: config.websocket.proxy_port,
            symbols: managed_symbols.clone(), // This is used by some generic websocket parts
        };
        
        let handler = Arc::new(AggTradeMessageHandler::with_trade_sender(
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            klcommon_sender.clone(),
        ));
        let mut client = AggTradeClient::new_with_handler(agg_trade_config, handler);
        
        // 启动 WebSocket 客户端
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(worker_id, "I/O loop received shutdown signal.");
                    return;
                }
            },
            result = client.start() => {
                if let Err(e) = result {
                    error!(worker_id, error = %e, "WebSocket client failed.");
                }
                // WebSocket 连接断开，等待重连
                sleep(Duration::from_secs(1)).await;
            },
            Some(cmd) = ws_cmd_rx.recv() => {
                info!(worker_id, ?cmd, "I/O task received command.");
                match cmd {
                    WsCmd::Subscribe(new_symbols) => {
                        // 更新管理的品种列表
                        managed_symbols.extend(new_symbols.clone());
                        info!(worker_id, new_count = managed_symbols.len(), "Updated managed symbols list.");
                        // 注意：新的订阅将在下次重连时生效
                        info!(worker_id, "New symbols will be subscribed on next reconnection.");
                    }
                }
            },
        }

        // 等待重连
        tokio::select! {
            _ = sleep(Duration::from_secs(5)) => {
                info!(worker_id, "Attempting to reconnect WebSocket...");
            },
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(worker_id, "I/O loop received shutdown signal during retry wait. Exiting.");
                    return;
                }
            }
        }
    }
}


// --- 4. 后台任务 (持久化) ---

pub async fn persistence_task(
    db: Arc<Database>,
    worker_handles: Arc<Vec<WorkerReadHandle>>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let persistence_interval = Duration::from_millis(config.persistence_interval_ms);
    let mut interval = interval(persistence_interval);
    info!(log_type = "low_freq", interval_ms = config.persistence_interval_ms, "持久化任务启动");

    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(log_type = "low_freq", "持久化任务收到关闭信号，执行最终写入...");
                    perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods).await;
                    break;
                }
            }
        }
        perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods).await;
    }
    info!(log_type = "low_freq", "持久化任务结束");
}

#[perf_profile(
    name = "persistence_cycle_transaction",
    level = "info",
    skip_all,
    fields(
        tx_id = format!("persist:{}", chrono::Utc::now().timestamp_millis()),
        log_type = "high_freq"
    )
)]
async fn perform_persistence_cycle(
    db: &Arc<Database>,
    worker_handles: &Arc<Vec<WorkerReadHandle>>,
    index_to_symbol: &Arc<RwLock<Vec<String>>>,
    periods: &Arc<Vec<String>>,
) {
    debug!("开始持久化周期...");
    let handles = worker_handles.iter().map(|h| h.request_snapshot());
    let results = futures::future::join_all(handles).await;

    let mut klines_to_save = Vec::new();
    let index_to_symbol_guard = index_to_symbol.read().await;

    for result in results {
        if let Ok(snapshot) = result {
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
        } else if let Err(e) = result {
            error!("从Worker获取快照失败: {}", e);
        }
    }

    if !klines_to_save.is_empty() {
        let count = klines_to_save.len();
        info!(log_type = "low_freq", count, "保存K线数据到数据库...");
        if let Err(e) = db.upsert_klines_batch(klines_to_save) {
            error!(error = %e, "保存K线数据到数据库失败");
        } else {
            info!(log_type = "low_freq", count, "成功保存{}条K线数据", count);
        }
    } else {
        debug!("本周期无更新的K线数据需要持久化");
    }
}