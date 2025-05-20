// 中心化调度器 - 定期交换双缓冲区
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use log::{info, error, debug};
use tokio::time;
use crate::klcommon::aggkline::double_buffered_store::DoubleBufferedKlineStore;

/// 中心化调度器
pub struct CentralScheduler {
    /// 双缓冲K线存储
    kline_store: Arc<DoubleBufferedKlineStore>,
    /// 交换间隔（毫秒）
    swap_interval_ms: u64,
    /// 运行标志
    running: Arc<AtomicBool>,
}

impl CentralScheduler {
    /// 创建新的中心化调度器
    pub fn new(kline_store: Arc<DoubleBufferedKlineStore>, swap_interval_ms: u64) -> Self {
        Self {
            kline_store,
            swap_interval_ms,
            running: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// 启动调度器
    pub fn start(&self) -> Arc<AtomicBool> {
        // 设置运行标志
        self.running.store(true, Ordering::SeqCst);
        
        // 克隆引用以在异步任务中使用
        let kline_store = self.kline_store.clone();
        let running = self.running.clone();
        let swap_interval_ms = self.swap_interval_ms;
        
        // 启动异步任务
        tokio::spawn(async move {
            info!("中心化调度器已启动，交换间隔: {}ms", swap_interval_ms);
            
            // 创建间隔定时器
            let mut interval = time::interval(Duration::from_millis(swap_interval_ms));
            
            // 循环执行交换操作
            while running.load(Ordering::SeqCst) {
                // 等待下一个时间点
                interval.tick().await;
                
                // 交换缓冲区
                kline_store.swap_buffers();
                debug!("中心化调度器已交换缓冲区");
            }
            
            info!("中心化调度器已停止");
        });
        
        // 返回运行标志，以便外部控制停止
        self.running.clone()
    }
    
    /// 停止调度器
    pub fn stop(&self) {
        info!("正在停止中心化调度器");
        self.running.store(false, Ordering::SeqCst);
    }
}

impl Drop for CentralScheduler {
    fn drop(&mut self) {
        // 确保在销毁时停止调度器
        self.stop();
    }
}
