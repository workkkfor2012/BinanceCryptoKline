//! 自调度双缓冲K线存储模块
//! 
//! 实现高性能的双缓冲K线数据存储，支持无锁并发读写操作。

use crate::klaggregate::{SymbolMetadataRegistry, KlineData, AtomicKlineData};
use crate::klcommon::{Result, AppError};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{RwLock, Notify};
use tokio::time::{interval, Duration, Instant};
use tracing::{info, debug, warn, instrument, Instrument};

/// 双缓冲K线存储
pub struct BufferedKlineStore {
    /// 符号元数据注册表
    symbol_registry: Arc<SymbolMetadataRegistry>,
    
    /// 写缓冲区（当前正在写入的缓冲区）
    write_buffer: Arc<RwLock<Vec<AtomicKlineData>>>,
    
    /// 读缓冲区（当前可以读取的缓冲区）
    read_buffer: Arc<RwLock<Vec<AtomicKlineData>>>,
    
    /// 缓冲区切换间隔（毫秒）
    swap_interval_ms: u64,
    
    /// 切换计数器
    swap_count: Arc<AtomicU64>,

    /// 调度器运行状态
    scheduler_running: Arc<AtomicBool>,
    
    /// 停止信号
    stop_signal: Arc<Notify>,
    
    /// 新快照就绪通知
    snapshot_ready_notify: Arc<Notify>,
    
    /// 总存储槽数量
    total_slots: usize,
}

impl BufferedKlineStore {
    /// 创建新的双缓冲存储
    #[instrument(target = "BufferedKlineStore", name="new_store", fields(total_slots), skip_all, err)]
    pub async fn new(
        symbol_registry: Arc<SymbolMetadataRegistry>,
        swap_interval_ms: u64,
    ) -> Result<Self> {
        let total_slots = symbol_registry.get_total_kline_slots();
        tracing::Span::current().record("total_slots", total_slots);

        info!(target: "buffered_kline_store", event_name = "存储初始化开始", total_slots = total_slots, swap_interval_ms = swap_interval_ms, "初始化双缓冲K线存储: total_slots={}, swap_interval_ms={}", total_slots, swap_interval_ms);
        
        // 创建两个相同大小的缓冲区
        let write_buffer = Self::create_buffer(total_slots);
        let read_buffer = Self::create_buffer(total_slots);
        
        let store = Self {
            symbol_registry,
            write_buffer: Arc::new(RwLock::new(write_buffer)),
            read_buffer: Arc::new(RwLock::new(read_buffer)),
            swap_interval_ms,
            swap_count: Arc::new(AtomicU64::new(0)),
            scheduler_running: Arc::new(AtomicBool::new(false)),
            stop_signal: Arc::new(Notify::new()),
            snapshot_ready_notify: Arc::new(Notify::new()),
            total_slots,
        };
        
        info!(target: "buffered_kline_store", event_name = "存储初始化完成", total_slots = total_slots, "双缓冲K线存储初始化完成: total_slots={}", total_slots);
        Ok(store)
    }
    
    /// 创建缓冲区
    fn create_buffer(size: usize) -> Vec<AtomicKlineData> {
        let mut buffer = Vec::with_capacity(size);
        for _ in 0..size {
            buffer.push(AtomicKlineData::new());
        }
        buffer
    }
    
    /// 启动调度器
    #[instrument(target = "BufferedKlineStore", fields(swap_interval_ms = self.swap_interval_ms), skip(self), err)]
    pub async fn start_scheduler(&self) -> Result<()> {
        if self.scheduler_running.load(Ordering::Relaxed) {
            warn!(target: "buffered_kline_store", event_name = "调度器已运行", "调度器已经在运行");
            return Ok(());
        }

        info!(target: "buffered_kline_store", event_name = "调度器启动", swap_interval_ms = self.swap_interval_ms, "启动双缓冲调度器: swap_interval_ms={}", self.swap_interval_ms);
        self.scheduler_running.store(true, Ordering::Relaxed);
        
        let write_buffer = self.write_buffer.clone();
        let read_buffer = self.read_buffer.clone();
        let swap_count = self.swap_count.clone();
        let scheduler_running = self.scheduler_running.clone();
        let stop_signal = self.stop_signal.clone();
        let snapshot_ready_notify = self.snapshot_ready_notify.clone();
        let swap_interval_ms = self.swap_interval_ms;
        
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_millis(swap_interval_ms));
            
            while scheduler_running.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // 执行缓冲区切换
                        let start_time = Instant::now();
                        
                        // 获取写锁进行原子切换
                        let mut write_guard = write_buffer.write().await;
                        let mut read_guard = read_buffer.write().await;
                        
                        // 交换缓冲区引用（这里实际上是交换Vec的内容）
                        std::mem::swap(&mut *write_guard, &mut *read_guard);
                        
                        drop(write_guard);
                        drop(read_guard);
                        
                        let swap_duration = start_time.elapsed();
                        let count = swap_count.fetch_add(1, Ordering::Relaxed) + 1;

                        // 记录缓冲区交换事件
                        let read_size = read_buffer.read().await.len();
                        let write_size = write_buffer.read().await.len();
                        let duration_ms = swap_duration.as_secs_f64() * 1000.0;

                        info!(
                            target: "buffered_kline_store",
                            event_name = "缓冲区交换完成",
                            is_high_freq = true,
                            swap_count = count,
                            duration_ms = duration_ms,
                            read_buffer_size = read_size,
                            write_buffer_size = write_size,
                            "缓冲区交换完成"
                        );

                        debug!(target: "buffered_kline_store", "缓冲区切换详情: swap_count={}, duration_ms={:.2}", count, duration_ms);

                        // 通知新快照就绪
                        snapshot_ready_notify.notify_waiters();
                    }
                    _ = stop_signal.notified() => {
                        info!(target: "buffered_kline_store", event_name = "调度器停止信号", "收到停止信号，调度器退出");
                        break;
                    }
                }
            }

            scheduler_running.store(false, Ordering::Relaxed);
            info!(target: "buffered_kline_store", event_name = "调度器已停止", "双缓冲调度器已停止");
        }.instrument(tracing::info_span!("buffer_swap_scheduler")));
        
        Ok(())
    }
    
    /// 停止调度器
    #[instrument(target = "BufferedKlineStore", name="stop_scheduler", skip(self), err)]
    pub async fn stop_scheduler(&self) -> Result<()> {
        if !self.scheduler_running.load(Ordering::Relaxed) {
            info!(target: "buffered_kline_store", event_name = "调度器未运行", "调度器未在运行，无需停止");
            return Ok(());
        }

        info!(target: "buffered_kline_store", event_name = "调度器停止开始", "停止双缓冲调度器");
        self.scheduler_running.store(false, Ordering::Relaxed);
        self.stop_signal.notify_waiters();

        // 等待调度器完全停止
        let start_wait = Instant::now();
        while self.scheduler_running.load(Ordering::Relaxed) {
            if start_wait.elapsed() > Duration::from_secs(5) {
                warn!(target: "buffered_kline_store", event_name = "调度器停止超时", "等待调度器停止超时(5s)");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        info!(target: "buffered_kline_store", event_name = "调度器停止确认", "双缓冲调度器已停止");
        Ok(())
    }
    
    /// 写入K线数据
    #[instrument(target = "BufferedKlineStore", name="write_kline", fields(symbol_index, period_index, flat_index), skip(self, kline_data), err)]
    pub async fn write_kline_data(
        &self,
        symbol_index: u32,
        period_index: u32,
        kline_data: &KlineData,
    ) -> Result<()> {
        // 计算扁平化索引
        let flat_index = self.symbol_registry.calculate_flat_index(symbol_index, period_index);
        tracing::Span::current().record("flat_index", flat_index);

        if flat_index >= self.total_slots {
            return Err(AppError::DataError(format!(
                "索引超出范围: {} >= {}",
                flat_index,
                self.total_slots
            )));
        }
        
        // 获取写缓冲区的读锁（允许并发写入）
        let write_buffer = self.write_buffer.read().await;
        
        // 原子地更新数据
        write_buffer[flat_index].load_from(kline_data);
        
        Ok(())
    }
    
    /// 读取K线数据
    #[instrument(target = "BufferedKlineStore", name="read_kline", fields(symbol_index, period_index, flat_index), skip(self), err)]
    pub async fn read_kline_data(
        &self,
        symbol_index: u32,
        period_index: u32,
    ) -> Result<KlineData> {
        // 计算扁平化索引
        let flat_index = self.symbol_registry.calculate_flat_index(symbol_index, period_index);
        tracing::Span::current().record("flat_index", flat_index);
        
        if flat_index >= self.total_slots {
            return Err(AppError::DataError(format!(
                "索引超出范围: {} >= {}",
                flat_index,
                self.total_slots
            )));
        }
        
        // 获取读缓冲区的读锁
        let read_buffer = self.read_buffer.read().await;
        
        // 原子地读取数据
        let kline_data = read_buffer[flat_index].to_kline_data();
        
        Ok(kline_data)
    }
    
    /// 批量读取所有K线数据快照
    pub async fn get_read_buffer_snapshot(&self) -> Result<Vec<KlineData>> {
        let read_buffer = self.read_buffer.read().await;
        let mut snapshot = Vec::with_capacity(self.total_slots);
        
        for atomic_kline in read_buffer.iter() {
            snapshot.push(atomic_kline.to_kline_data());
        }
        
        Ok(snapshot)
    }
    
    /// 批量读取指定品种的所有周期K线数据
    pub async fn get_symbol_klines(&self, symbol_index: u32) -> Result<Vec<KlineData>> {
        let periods_per_symbol = self.symbol_registry.get_periods_per_symbol();
        let mut klines = Vec::with_capacity(periods_per_symbol);
        
        for period_index in 0..periods_per_symbol as u32 {
            let kline_data = self.read_kline_data(symbol_index, period_index).await?;
            klines.push(kline_data);
        }
        
        Ok(klines)
    }
    
    /// 批量读取指定周期的所有品种K线数据
    pub async fn get_period_klines(&self, period_index: u32) -> Result<Vec<KlineData>> {
        let symbol_count = self.symbol_registry.get_symbol_count().await;
        let mut klines = Vec::with_capacity(symbol_count);
        
        for symbol_index in 0..symbol_count as u32 {
            let kline_data = self.read_kline_data(symbol_index, period_index).await?;
            klines.push(kline_data);
        }
        
        Ok(klines)
    }
    
    /// 等待新快照就绪
    pub async fn wait_for_snapshot(&self) {
        self.snapshot_ready_notify.notified().await;
    }
    
    /// 获取切换计数
    pub async fn get_swap_count(&self) -> u64 {
        self.swap_count.load(Ordering::Relaxed)
    }
    
    /// 获取调度器运行状态
    pub fn is_scheduler_running(&self) -> bool {
        self.scheduler_running.load(Ordering::Relaxed)
    }
    
    /// 获取存储统计信息
    pub async fn get_statistics(&self) -> BufferStatistics {
        let swap_count = self.swap_count.load(Ordering::Relaxed);
        let is_running = self.scheduler_running.load(Ordering::Relaxed);
        
        BufferStatistics {
            total_slots: self.total_slots,
            swap_count,
            is_scheduler_running: is_running,
            swap_interval_ms: self.swap_interval_ms,
        }
    }
}

/// 缓冲区统计信息
#[derive(Debug, Clone)]
pub struct BufferStatistics {
    /// 总存储槽数量
    pub total_slots: usize,
    /// 切换计数
    pub swap_count: u64,
    /// 调度器是否运行
    pub is_scheduler_running: bool,
    /// 切换间隔（毫秒）
    pub swap_interval_ms: u64,
}
