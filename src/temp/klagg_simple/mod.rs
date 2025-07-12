//! 高性能K线聚合服务 (完全分区模型)
//!
//! ## 设计核心
//! 1.  **静态分区**: 数据按品种的全局索引分区，每个分区由一个独立的Worker线程全权负责。
//! 2.  **无共享写入**: Worker内部的计算和状态更新无任何跨线程锁争用。
//! 3.  **CPU亲和性**: 每个Worker被设计为可以绑定到单个CPU核心，最大化缓存局部性。
//! 4.  **主动快照推送**: 外部服务通过`WorkerReadHandle`请求数据，Worker主动过滤并推送已更新的数据快照。

use crate::klcommon::{
    api::{get_aligned_time, interval_to_milliseconds},
    context::spawn_instrumented,
    db::Database,
    error::Result,
    models::Kline as DbKline,
    server_time_sync::ServerTimeSyncManager,
    websocket::{AggTradeClient, AggTradeConfig, AggTradeData, MessageHandler},
    AggregateConfig,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, trace, warn};

// --- 1. 类型定义 ---

/// Worker 内部用于聚合计算的K线状态。
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

/// 用于在快照中传输的K线数据。
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
    /// 标志此数据在上次快照后是否被更新过
    pub is_updated: bool,
}

/// 发送给特殊 Worker (Worker 3) 的指令
#[derive(Debug, Clone)]
pub enum WorkerCmd {
    AddSymbol {
        symbol: String,
        global_index: usize,
    },
}

/// Worker 暴露给外部的、线程安全的只读句柄。
#[derive(Clone)]
pub struct WorkerReadHandle {
    /// 用于发送“快照请求”的通道，请求本身是一个 oneshot::Sender 用于接收响应。
    snapshot_req_tx: mpsc::Sender<oneshot::Sender<Vec<KlineData>>>,
}

impl WorkerReadHandle {
    /// 异步请求一次已更新数据的快照。
    pub async fn request_snapshot(&self) -> Result<Vec<KlineData>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.snapshot_req_tx.send(response_tx).await.map_err(|e| {
            crate::klcommon::AppError::ChannelClosed(format!(
                "Failed to send snapshot request to worker: {}",
                e
            ))
        })?;
        response_rx.await.map_err(|e| {
            crate::klcommon::AppError::ChannelClosed(format!(
                "Failed to receive snapshot response from worker: {}",
                e
            ))
        })
    }
}

// --- 2. Worker 核心实现 ---

/// K线聚合的工作单元 (Worker)。
pub struct Worker {
    // --- 身份与配置 ---
    worker_id: usize,
    /// 分区内负责的品种名，主要用于 WebSocket 订阅和添加新品种
    partition_symbols: Vec<String>,
    periods: Arc<Vec<String>>,
    config: Arc<AggregateConfig>,
    /// 此 Worker 负责的全局索引起始点，用于直接计算本地索引
    partition_start_index: usize,

    // --- 内部状态 (非并发安全) ---
    kline_states: Vec<KlineState>,
    snapshot_buffers: (Vec<KlineData>, Vec<KlineData>),
    active_buffer_index: usize,
    /// 从外部传入只读的全局映射，用于从 symbol string 定位 global_index
    symbol_to_global_index: Arc<HashMap<String, usize>>,

    // --- 通信通道 ---
    cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
    snapshot_req_rx: mpsc::Receiver<oneshot::Sender<Vec<KlineData>>>,
    clock_rx: watch::Receiver<i64>,
}

impl Worker {
    /// 启动 Worker 任务，并返回其只读句柄。
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        worker_id: usize,
        partition_symbols: Vec<String>,
        partition_start_index: usize,
        symbol_to_global_index: Arc<HashMap<String, usize>>,
        periods: Arc<Vec<String>>,
        config: Arc<AggregateConfig>,
        cmd_rx: Option<mpsc::Receiver<WorkerCmd>>,
        clock_rx: watch::Receiver<i64>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<WorkerReadHandle> {
        // a. 为外部通信创建句柄
        let (snapshot_req_tx, snapshot_req_rx) = mpsc::channel(8);
        let handle = WorkerReadHandle { snapshot_req_tx };

        // b. 初始化 Worker 状态
        let num_periods = periods.len();
        let partition_symbols_len = partition_symbols.len();
        
        let initial_capacity_symbols = if worker_id == 3 {
            10000 - partition_start_index 
        } else {
            partition_symbols_len
        };

        let total_slots = initial_capacity_symbols * num_periods;
        info!(worker_id, partition_symbols_len, initial_capacity_symbols, total_slots, "Allocating state buffers.");

        let kline_states = vec![KlineState::default(); total_slots];
        let snapshot_buffers = (
            vec![KlineData::default(); total_slots],
            vec![KlineData::default(); total_slots],
        );

        let mut worker = Self {
            worker_id,
            partition_symbols,
            periods,
            config,
            partition_start_index,
            kline_states,
            snapshot_buffers,
            active_buffer_index: 0,
            symbol_to_global_index,
            cmd_rx,
            snapshot_req_rx,
            clock_rx,
        };

        // c. 启动核心的 Worker 异步任务
        spawn_instrumented(async move {
            if let Err(e) = worker.task_loop(shutdown_rx).await {
                error!(worker_id, error = %e, "Worker task loop exited with error.");
            } else {
                info!(worker_id, "Worker task loop exited gracefully.");
            }
        });

        // d. 返回句柄
        Ok(handle)
    }

    /// Worker 的主事件循环。
    async fn task_loop(&mut self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
        info!(
            worker_id = self.worker_id,
            symbols_count = self.partition_symbols.len(),
            "Worker starting..."
        );

        let (trade_tx, mut trade_rx) = mpsc::channel::<AggTradeData>(10240);
        self.start_websocket_client(trade_tx, shutdown_rx.clone()).await;

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => { if *shutdown_rx.borrow() { info!(worker_id = self.worker_id, "Shutdown signal received."); break; } },
                Some(trade) = trade_rx.recv() => { self.process_trade(trade); },
                Ok(_) = self.clock_rx.changed() => { let time = *self.clock_rx.borrow(); if time > 0 { self.process_clock_tick(time); } },
                Some(response_tx) = self.snapshot_req_rx.recv() => { self.process_snapshot_request(response_tx); },
                Some(cmd) = async { if let Some(rx) = self.cmd_rx.as_mut() { rx.recv().await } else { std::future::pending().await } } => { self.process_command(cmd).await; },
            }
        }
        Ok(())
    }

    /// 启动此 Worker 专属的 WebSocket 客户端。
    async fn start_websocket_client(&self, trade_tx: mpsc::Sender<AggTradeData>, mut shutdown_rx: watch::Receiver<bool>) {
        struct WorkerMessageHandler { trade_tx: mpsc::Sender<AggTradeData> }
        impl MessageHandler for WorkerMessageHandler {
            fn handle_message(&self, _: usize, msg: String) -> impl std::future::Future<Output = Result<()>> + Send {
                async move {
                    // **注意**: 币安的 aggTrade 流直接就是交易对象 JSON，不是数组
                    if let Ok(trade) = serde_json::from_str::<AggTradeData>(&msg) {
                        if self.trade_tx.send(trade).await.is_err() {
                            trace!("Worker internal trade channel closed.");
                        }
                    }
                    Ok(())
                }
            }
        }

        let config_clone = self.config.clone();
        let symbols_clone = self.partition_symbols.clone();
        let worker_id = self.worker_id;

        spawn_instrumented(async move {
            let agg_trade_config = AggTradeConfig {
                use_proxy: config_clone.websocket.use_proxy,
                proxy_addr: config_clone.websocket.proxy_host.clone(),
                proxy_port: config_clone.websocket.proxy_port,
                symbols: symbols_clone,
            };
            let handler = Arc::new(WorkerMessageHandler { trade_tx });
            let mut client = AggTradeClient::new_with_handler(agg_trade_config, handler);
            tokio::select! {
                res = client.start() => { if let Err(e) = res { error!(worker_id, error = %e, "WebSocket client failed."); } },
                _ = shutdown_rx.changed() => { if *shutdown_rx.borrow() { info!(worker_id, "WebSocket client received shutdown signal."); } }
            }
        });
    }

    /// 处理单笔交易数据
    fn process_trade(&mut self, trade: AggTradeData) {
        trace!(worker_id = self.worker_id, symbol = %trade.symbol, "Processing trade");
        let global_index = if let Some(idx) = self.symbol_to_global_index.get(&trade.symbol) {
            *idx
        } else {
            warn!(worker_id = self.worker_id, symbol = %trade.symbol, "Received trade for unknown symbol");
            return;
        };

        let local_index = global_index - self.partition_start_index;

        let num_periods = self.periods.len();
        let base_offset = local_index * num_periods;
        
        if base_offset >= self.kline_states.len() {
             error!(worker_id = self.worker_id, symbol = %trade.symbol, global_index, local_index, "Calculated local_index is out of bounds!");
            return;
        }

        let write_buffer = if self.active_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };

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
                kline.high = kline.high.max(trade.price); kline.low = kline.low.min(trade.price);
                kline.close = trade.price; kline.volume += trade.quantity;
                kline.turnover += trade.price * trade.quantity; kline.trade_count += 1;
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

    /// 处理时钟节拍，用于完结K线
    fn process_clock_tick(&mut self, current_time: i64) {
        trace!(worker_id = self.worker_id, time = current_time, "Processing clock tick");
        let num_periods = self.periods.len();
        let write_buffer = if self.active_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };
        
        let num_symbols_in_partition = self.partition_symbols.len();
        for local_idx in 0..num_symbols_in_partition {
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

    /// 处理外部的快照拉取请求
    fn process_snapshot_request(&mut self, response_tx: oneshot::Sender<Vec<KlineData>>) {
        trace!(worker_id = self.worker_id, "Processing snapshot request");
        let read_buffer_index = self.active_buffer_index;
        self.active_buffer_index = 1 - self.active_buffer_index;

        let read_buffer = if read_buffer_index == 0 { &mut self.snapshot_buffers.0 } else { &mut self.snapshot_buffers.1 };

        let updated_data: Vec<KlineData> = read_buffer.iter().filter(|kd| kd.is_updated && kd.open_time > 0).cloned().collect();
        read_buffer.iter_mut().filter(|kd| kd.is_updated).for_each(|kd| kd.is_updated = false);

        if response_tx.send(updated_data).is_err() {
            warn!(worker_id = self.worker_id, "Snapshot request channel closed before sending response.");
        }
    }

    /// (仅Worker 3) 处理指令
    async fn process_command(&mut self, cmd: WorkerCmd) {
        info!(worker_id = self.worker_id, "Processing command: {:?}", cmd);
        match cmd {
            WorkerCmd::AddSymbol { symbol, global_index } => {
                let local_index = global_index - self.partition_start_index;
                self.partition_symbols.push(symbol.clone());

                let num_periods = self.periods.len();
                let base_offset = local_index * num_periods;

                if base_offset >= self.kline_states.len() {
                    error!(worker_id = self.worker_id, symbol, global_index, local_index, "New symbol local_index is out of pre-allocated bounds!");
                    return;
                }

                for period_idx in 0..num_periods {
                    let offset = base_offset + period_idx;
                    self.kline_states[offset] = KlineState::default();
                    self.snapshot_buffers.0[offset] = KlineData::default();
                    self.snapshot_buffers.1[offset] = KlineData::default();
                }

                info!(worker_id = self.worker_id, symbol, global_index, local_index, "New symbol initialized locally.");
                warn!("TODO: Dynamic WebSocket subscription for new symbols is not yet implemented.");
            }
        }
    }
}

// --- 3. 后台任务 ---

/// 全局唯一的持久化任务。
pub async fn persistence_task(
    db: Arc<Database>,
    worker_handles: Arc<Vec<WorkerReadHandle>>,
    index_to_symbol: Arc<Vec<String>>,
    periods: Arc<Vec<String>>,
    config: Arc<AggregateConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let persistence_interval = Duration::from_millis(config.persistence_interval_ms);
    let mut interval = interval(persistence_interval);
    info!(interval_ms = config.persistence_interval_ms, "Persistence task starting.");

    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Persistence task received shutdown signal, performing final write...");
                    perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods).await;
                    break;
                }
            }
        }
        perform_persistence_cycle(&db, &worker_handles, &index_to_symbol, &periods).await;
    }
    info!("Persistence task finished.");
}

/// 执行一次完整的持久化周期：从所有Worker拉取数据并写入数据库。
async fn perform_persistence_cycle(
    db: &Arc<Database>,
    worker_handles: &Arc<Vec<WorkerReadHandle>>,
    index_to_symbol: &Arc<Vec<String>>,
    periods: &Arc<Vec<String>>,
) {
    debug!("Starting persistence cycle...");
    let handles = worker_handles.iter().map(|h| h.request_snapshot());
    let results = futures::future::join_all(handles).await;

    let mut klines_to_save = Vec::new();
    for result in results {
        if let Ok(snapshot) = result {
            for kline_data in snapshot {
                if let (Some(symbol), Some(interval)) = (
                    index_to_symbol.get(kline_data.global_symbol_index),
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
            error!("Failed to get snapshot from a worker: {}", e);
        }
    }

    if !klines_to_save.is_empty() {
        let count = klines_to_save.len();
        info!(count, "Saving klines to database...");
        if let Err(e) = db.upsert_klines_batch(klines_to_save) {
            error!(error = %e, "Failed to save klines to database.");
        } else {
            info!(count, "Successfully saved klines.");
        }
    } else {
        debug!("No updated klines to persist in this cycle.");
    }
}