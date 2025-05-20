// 双缓冲K线存储 - 实现高性能无锁读取的K线数据存储
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use log::{debug, info, error};

/// 用于 FlatKlineStore 的 KlineData 结构体
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]  // 使用 C 布局并 8 字节对齐，适合现代 CPU
pub struct KlineData {
    pub open_time_ms: i64,       // 开盘时间 (毫秒)
    pub open: f64,               // 开盘价
    pub high: f64,               // 最高价
    pub low: f64,                // 最低价
    pub close: f64,              // 收盘价
    pub volume: f64,             // 成交量
    pub quote_asset_volume: f64, // 成交金额
}

/// 扁平化存储所有品种的 K 线数据
pub struct FlatKlineStore {
    /// 单一连续数组，预分配较大容量
    /// 总长度为 品种总数 * 周期总数
    klines: Vec<KlineData>,
    /// 每个品种的周期数
    period_count: usize,
}

impl FlatKlineStore {
    /// 创建新的 FlatKlineStore
    pub fn new(capacity: usize, period_count: usize) -> Self {
        // 预分配容量
        let mut klines = Vec::with_capacity(capacity * period_count);
        
        // 用默认值填充
        klines.resize_with(capacity * period_count, KlineData::default);
        
        info!("创建FlatKlineStore，容量: {} 个品种，每个品种 {} 个周期", capacity, period_count);
        
        Self {
            klines,
            period_count,
        }
    }
    
    /// 获取指定位置的 K 线数据
    pub fn get(&self, symbol_index: usize, period_index: usize) -> Option<KlineData> {
        let offset = self.calculate_offset(symbol_index, period_index);
        self.klines.get(offset).copied()
    }
    
    /// 设置指定位置的 K 线数据
    pub fn set(&mut self, symbol_index: usize, period_index: usize, kline: KlineData) -> bool {
        let offset = self.calculate_offset(symbol_index, period_index);
        if offset < self.klines.len() {
            self.klines[offset] = kline;
            true
        } else {
            error!("设置K线数据失败: 偏移量 {} 超出范围 {}", offset, self.klines.len());
            false
        }
    }
    
    /// 计算偏移量
    #[inline]
    fn calculate_offset(&self, symbol_index: usize, period_index: usize) -> usize {
        symbol_index * self.period_count + period_index
    }
    
    /// 获取容量
    pub fn capacity(&self) -> usize {
        self.klines.capacity() / self.period_count
    }
    
    /// 获取周期数
    pub fn period_count(&self) -> usize {
        self.period_count
    }
    
    /// 获取所有 K 线数据的引用
    pub fn klines(&self) -> &[KlineData] {
        &self.klines
    }
    
    /// 获取所有 K 线数据的可变引用
    pub fn klines_mut(&mut self) -> &mut [KlineData] {
        &mut self.klines
    }
}

/// 双缓冲 K 线存储
pub struct DoubleBufferedKlineStore {
    /// 两个缓冲区
    buffers: [AtomicPtr<FlatKlineStore>; 2],
    /// 当前写缓冲区索引
    active_write_index: AtomicUsize,
}

impl DoubleBufferedKlineStore {
    /// 创建新的 DoubleBufferedKlineStore
    pub fn new(capacity: usize, period_count: usize) -> Self {
        // 创建两个 FlatKlineStore
        let store1 = Box::new(FlatKlineStore::new(capacity, period_count));
        let store2 = Box::new(FlatKlineStore::new(capacity, period_count));
        
        // 将 Box 转换为原始指针
        let ptr1 = Box::into_raw(store1);
        let ptr2 = Box::into_raw(store2);
        
        info!("创建DoubleBufferedKlineStore，容量: {} 个品种，每个品种 {} 个周期", capacity, period_count);
        
        Self {
            buffers: [
                AtomicPtr::new(ptr1),
                AtomicPtr::new(ptr2),
            ],
            active_write_index: AtomicUsize::new(0),
        }
    }
    
    /// 交换读写缓冲区
    pub fn swap_buffers(&self) {
        let current_idx = self.active_write_index.load(Ordering::Relaxed);
        let new_idx = 1 - current_idx;
        self.active_write_index.store(new_idx, Ordering::Release);
        debug!("交换缓冲区: {} -> {}", current_idx, new_idx);
    }
    
    /// 获取写缓冲区指针
    pub fn get_write_buffer_ptr(&self) -> *mut FlatKlineStore {
        let idx = self.active_write_index.load(Ordering::Acquire);
        self.buffers[idx].load(Ordering::Acquire)
    }
    
    /// 获取读缓冲区指针
    pub fn get_read_buffer_ptr(&self) -> *const FlatKlineStore {
        let idx = 1 - self.active_write_index.load(Ordering::Acquire);
        self.buffers[idx].load(Ordering::Acquire)
    }
    
    /// 写入 K 线数据
    pub fn write_kline(&self, symbol_index: usize, period_index: usize, kline: KlineData) -> bool {
        let write_ptr = self.get_write_buffer_ptr();
        
        // 安全检查
        if write_ptr.is_null() {
            error!("写入K线数据失败: 写缓冲区指针为空");
            return false;
        }
        
        // 安全地访问写缓冲区
        unsafe {
            (*write_ptr).set(symbol_index, period_index, kline)
        }
    }
    
    /// 读取 K 线数据
    pub fn read_kline(&self, symbol_index: usize, period_index: usize) -> Option<KlineData> {
        let read_ptr = self.get_read_buffer_ptr();
        
        // 安全检查
        if read_ptr.is_null() {
            error!("读取K线数据失败: 读缓冲区指针为空");
            return None;
        }
        
        // 安全地访问读缓冲区
        unsafe {
            (*read_ptr).get(symbol_index, period_index)
        }
    }
}

impl Drop for DoubleBufferedKlineStore {
    fn drop(&mut self) {
        // 安全地释放两个缓冲区
        for buffer in &self.buffers {
            let ptr = buffer.load(Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    // 将原始指针转换回 Box，让 Box 负责释放内存
                    let _ = Box::from_raw(ptr);
                }
            }
        }
        info!("DoubleBufferedKlineStore已释放");
    }
}
