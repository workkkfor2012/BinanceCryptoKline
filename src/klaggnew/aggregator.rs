// src/aggregator.rs

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::interval;
use tracing::{debug, info, instrument, span, warn, Instrument, Level};

// --- Common Data Structures (public, to be used by other modules) ---
#[derive(Debug, Clone, Deserialize)]
pub struct AggTradeData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "q")]
    pub quantity: f64,
    #[serde(rename = "T")]
    pub timestamp_ms: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone)]
pub struct KlineData {
    pub symbol_index: u32,
    pub period_index: u32,
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_final: bool,
}

// --- Helper Functions (internal to the module) ---
fn get_period_duration_ms(period: &str) -> i64 {
    let last_char = period.chars().last().unwrap_or('m');
    let value: i64 = period[..period.len() - 1].parse().unwrap_or(1);

    match last_char {
        'm' => value * 60 * 1000,        // 分钟
        'h' => value * 60 * 60 * 1000,   // 小时
        'd' => value * 24 * 60 * 60 * 1000, // 天
        'w' => value * 7 * 24 * 60 * 60 * 1000, // 周
        _ => value * 60 * 1000,  // 默认为分钟
    }
}

fn get_aligned_time(timestamp: i64, duration: i64) -> i64 {
    // 简化版本：直接按duration对齐
    (timestamp / duration) * duration
}

// --- Module 1: RealtimeAggregator ---
// The struct is public, but its fields are private, enforcing encapsulation.
pub struct RealtimeAggregator {
    is_running: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    
    // Internal state
    config: Arc<AggregatorConfig>,
    buffer_a: Arc<Mutex<Vec<KlineData>>>,
    buffer_b: Arc<Mutex<Vec<KlineData>>>,
    is_a_the_write_buffer: Arc<AtomicBool>,
    snapshot_ready_notify: Arc<Notify>,
}

// Configuration struct for better organization
struct AggregatorConfig {
    symbol_to_index: HashMap<String, u32>,
    pub(crate) index_to_symbol: HashMap<u32, String>,
    period_to_index: HashMap<String, u32>,
    pub(crate) index_to_period: HashMap<u32, String>,
    num_periods: usize,
    websocket_url: String,
}

impl RealtimeAggregator {
    #[instrument(name="aggregator_init", skip_all, err)]
    pub fn new(
        symbols: Vec<String>,
        periods: Vec<String>,
        max_symbols: usize,
        websocket_url: String,
    ) -> Result<Self> {
        info!(event_name = "RealtimeAggregatorInitStarted");
        
        let mut symbol_to_index = HashMap::new();
        let mut index_to_symbol = HashMap::new();
        let mut period_to_index = HashMap::new();
        let mut index_to_period = HashMap::new();

        // 初始化品种映射
        for (index, symbol) in symbols.iter().enumerate() {
            symbol_to_index.insert(symbol.clone(), index as u32);
            index_to_symbol.insert(index as u32, symbol.clone());
        }

        // 初始化周期映射
        for (index, period) in periods.iter().enumerate() {
            period_to_index.insert(period.clone(), index as u32);
            index_to_period.insert(index as u32, period.clone());
        }

        let config = Arc::new(AggregatorConfig {
            symbol_to_index,
            index_to_symbol,
            period_to_index,
            index_to_period,
            num_periods: periods.len(),
            websocket_url,
        });

        let total_slots = max_symbols * config.num_periods;
        let initial_buffer: Vec<KlineData> = (0..total_slots)
            .map(|i| KlineData::new((i / config.num_periods) as u32, (i % config.num_periods) as u32))
            .collect();
        
        Ok(Self {
            is_running: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            config,
            buffer_a: Arc::new(Mutex::new(initial_buffer.clone())),
            buffer_b: Arc::new(Mutex::new(initial_buffer)),
            is_a_the_write_buffer: Arc::new(AtomicBool::new(true)),
            snapshot_ready_notify: Arc::new(Notify::new()),
        })
    }
    
    #[instrument(name="aggregator_start", skip(self))]
    pub async fn start(&self) {
        if self.is_running.swap(true, Ordering::SeqCst) {
            warn!("RealtimeAggregator already running");
            return;
        }
        info!("Starting RealtimeAggregator");

        let self_clone = self.clone_for_task();
        tokio::spawn(async move {
            self_clone.run_websocket_loop().await;
        }.instrument(span!(Level::INFO, "websocket_task")));

        let self_clone = self.clone_for_task();
        tokio::spawn(async move {
            self_clone.run_buffer_swap_loop().await;
        }.instrument(span!(Level::INFO, "buffer_swap_task")));
    }

    #[instrument(name="aggregator_stop", skip(self))]
    pub async fn stop(&self) {
        if !self.is_running.swap(false, Ordering::SeqCst) {
            return;
        }
        info!("Stopping RealtimeAggregator");
        self.shutdown_notify.notify_waiters();
        // 等待一小段时间让任务完成
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // --- Public API ---
    #[instrument(name="get_kline_snapshot", skip(self))]
    pub async fn get_kline_snapshot(&self) -> Vec<KlineData> {
        // 获取读缓冲区（非写缓冲区）
        let is_a_write = self.is_a_the_write_buffer.load(Ordering::Relaxed);
        if is_a_write {
            // A是写缓冲区，读B
            self.buffer_b.lock().await.clone()
        } else {
            // B是写缓冲区，读A
            self.buffer_a.lock().await.clone()
        }
    }

    pub async fn wait_for_new_snapshot(&self) {
        self.snapshot_ready_notify.notified().await;
    }

    // Helper to provide necessary info to other modules without exposing all state
    pub fn get_metadata(&self) -> (HashMap<u32, String>, HashMap<u32, String>) {
        (self.config.index_to_symbol.clone(), self.config.index_to_period.clone())
    }

    // --- Internal Logic ---
    // A private method to create a lightweight clone for spawning tasks
    fn clone_for_task(&self) -> Arc<Self> {
        Arc::new(Self {
             is_running: self.is_running.clone(),
             shutdown_notify: self.shutdown_notify.clone(),
             config: self.config.clone(),
             buffer_a: self.buffer_a.clone(),
             buffer_b: self.buffer_b.clone(),
             is_a_the_write_buffer: self.is_a_the_write_buffer.clone(),
             snapshot_ready_notify: self.snapshot_ready_notify.clone(),
        })
    }
    
    // --- Internal Implementation ---
    async fn run_websocket_loop(self: Arc<Self>) {
        info!("WebSocket loop started");
        loop {
            tokio::select! {
                _ = self.shutdown_notify.notified() => {
                    info!("WebSocket loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // 模拟WebSocket数据接收
                    debug!("Simulating WebSocket data reception");
                }
            }
        }
    }

    async fn run_buffer_swap_loop(self: Arc<Self>) {
        info!("Buffer swap loop started");
        let mut interval = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = self.shutdown_notify.notified() => {
                    info!("Buffer swap loop shutting down");
                    break;
                }
                _ = interval.tick() => {
                    self.swap_buffers().await;
                }
            }
        }
    }

    async fn swap_buffers(&self) {
        debug!("Swapping buffers");
        // 切换写缓冲区
        self.is_a_the_write_buffer.fetch_xor(true, Ordering::SeqCst);
        // 通知有新快照可用
        self.snapshot_ready_notify.notify_waiters();
    }
}


// Implementations for KlineData and helper functions
impl KlineData {
    fn new(symbol_index: u32, period_index: u32) -> Self {
        Self {
            symbol_index,
            period_index,
            open_time: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            is_final: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.volume == 0.0 && self.open_time == 0
    }
}