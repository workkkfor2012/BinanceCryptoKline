// 全局品种周期注册表 - 管理交易品种到存储索引的映射
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use log::{info, error, debug, warn};
use crate::klcommon::{Database, Result, AppError};

/// 品种元数据
#[derive(Debug, Clone)]
pub struct SymbolMetadata {
    /// 品种名称
    pub symbol: String,
    /// 在 FlatKlineStore 中的索引
    pub index: usize,
    /// 首次上币时间（毫秒时间戳）
    pub first_listing_time: i64,
}

/// 周期元数据
#[derive(Debug, Clone)]
pub struct PeriodMetadata {
    /// 周期名称 (1m, 5m, 30m, 4h, 1d, 1w)
    pub period: String,
    /// 周期毫秒数
    pub period_ms: i64,
    /// 在 FlatKlineStore 中的索引
    pub index: usize,
}

/// 全局品种周期注册表
pub struct GlobalSymbolPeriodRegistry {
    /// 品种到元数据的映射
    symbol_to_metadata: RwLock<HashMap<String, SymbolMetadata>>,
    /// 周期到索引的映射
    period_to_index: RwLock<HashMap<String, usize>>,
    /// 索引到周期的映射
    index_to_period: RwLock<HashMap<usize, String>>,
    /// FlatKlineStore 的容量
    flat_store_capacity: usize,
    /// 每个品种的周期数
    period_count: usize,
    /// 数据库连接
    db: Arc<Database>,
}

impl GlobalSymbolPeriodRegistry {
    /// 创建新的全局品种周期注册表
    pub fn new(db: Arc<Database>, flat_store_capacity: usize, periods: &[(&str, i64)]) -> Self {
        let period_count = periods.len();
        
        // 初始化周期映射
        let mut period_to_index = HashMap::new();
        let mut index_to_period = HashMap::new();
        
        for (i, (period, _)) in periods.iter().enumerate() {
            period_to_index.insert(period.to_string(), i);
            index_to_period.insert(i, period.to_string());
        }
        
        info!("创建GlobalSymbolPeriodRegistry，容量: {} 个品种，{} 个周期", flat_store_capacity, period_count);
        
        Self {
            symbol_to_metadata: RwLock::new(HashMap::new()),
            period_to_index: RwLock::new(period_to_index),
            index_to_period: RwLock::new(index_to_period),
            flat_store_capacity,
            period_count,
            db,
        }
    }
    
    /// 初始化注册表
    pub async fn initialize(&self, symbols: &[String]) -> Result<()> {
        info!("初始化GlobalSymbolPeriodRegistry，{} 个品种", symbols.len());
        
        // 获取所有品种的首次上币时间
        let mut symbol_listing_times = Vec::new();
        
        for symbol in symbols {
            match self.get_first_listing_time(symbol).await {
                Ok(time) => {
                    symbol_listing_times.push((symbol.clone(), time));
                },
                Err(e) => {
                    error!("获取品种 {} 的首次上币时间失败: {}", symbol, e);
                    // 使用当前时间作为默认值
                    let current_time = chrono::Utc::now().timestamp_millis();
                    symbol_listing_times.push((symbol.clone(), current_time));
                }
            }
        }
        
        // 按照上币时间排序
        symbol_listing_times.sort_by_key(|(_, time)| *time);
        
        // 分配索引
        let mut symbol_to_metadata = self.symbol_to_metadata.write().unwrap();
        
        for (i, (symbol, time)) in symbol_listing_times.iter().enumerate() {
            if i >= self.flat_store_capacity {
                warn!("品种数量超过容量，忽略品种: {}", symbol);
                continue;
            }
            
            symbol_to_metadata.insert(symbol.clone(), SymbolMetadata {
                symbol: symbol.clone(),
                index: i,
                first_listing_time: *time,
            });
            
            debug!("分配索引: {} -> {}, 上币时间: {}", symbol, i, time);
        }
        
        info!("初始化完成，共 {} 个品种", symbol_to_metadata.len());
        
        Ok(())
    }
    
    /// 获取品种的首次上币时间（从日K线表中获取第一条记录的时间）
    async fn get_first_listing_time(&self, symbol: &str) -> Result<i64> {
        // 从日K线表中获取第一条记录的时间
        let interval = "1d";
        
        // 获取最早的K线时间戳
        match self.db.get_latest_kline_timestamp(symbol, interval)? {
            Some(timestamp) => {
                debug!("品种 {} 的首次上币时间: {}", symbol, timestamp);
                Ok(timestamp)
            },
            None => {
                // 如果没有找到记录，使用当前时间
                let current_time = chrono::Utc::now().timestamp_millis();
                warn!("品种 {} 没有找到日K线记录，使用当前时间: {}", symbol, current_time);
                Ok(current_time)
            }
        }
    }
    
    /// 获取品种的索引
    pub fn get_symbol_index(&self, symbol: &str) -> Option<usize> {
        let symbol_to_metadata = self.symbol_to_metadata.read().unwrap();
        symbol_to_metadata.get(symbol).map(|metadata| metadata.index)
    }
    
    /// 获取周期的索引
    pub fn get_period_index(&self, period: &str) -> Option<usize> {
        let period_to_index = self.period_to_index.read().unwrap();
        period_to_index.get(period).copied()
    }
    
    /// 获取索引对应的周期
    pub fn get_period_by_index(&self, index: usize) -> Option<String> {
        let index_to_period = self.index_to_period.read().unwrap();
        index_to_period.get(&index).cloned()
    }
    
    /// 添加新品种
    pub async fn add_symbol(&self, symbol: &str) -> Result<usize> {
        // 检查品种是否已存在
        {
            let symbol_to_metadata = self.symbol_to_metadata.read().unwrap();
            if let Some(metadata) = symbol_to_metadata.get(symbol) {
                return Ok(metadata.index);
            }
        }
        
        // 获取品种的首次上币时间
        let first_listing_time = match self.get_first_listing_time(symbol).await {
            Ok(time) => time,
            Err(e) => {
                error!("获取品种 {} 的首次上币时间失败: {}", symbol, e);
                // 使用当前时间作为默认值
                chrono::Utc::now().timestamp_millis()
            }
        };
        
        // 分配新索引
        let mut symbol_to_metadata = self.symbol_to_metadata.write().unwrap();
        
        // 检查是否超过容量
        if symbol_to_metadata.len() >= self.flat_store_capacity {
            return Err(AppError::DataError(format!(
                "品种数量超过容量: {} >= {}", 
                symbol_to_metadata.len(), 
                self.flat_store_capacity
            )));
        }
        
        // 分配新索引（当前最大索引 + 1）
        let new_index = symbol_to_metadata.values()
            .map(|metadata| metadata.index)
            .max()
            .unwrap_or(0) + 1;
        
        // 添加新品种
        symbol_to_metadata.insert(symbol.to_string(), SymbolMetadata {
            symbol: symbol.to_string(),
            index: new_index,
            first_listing_time,
        });
        
        info!("添加新品种: {} -> {}, 上币时间: {}", symbol, new_index, first_listing_time);
        
        Ok(new_index)
    }
    
    /// 获取周期数
    pub fn get_period_count(&self) -> usize {
        self.period_count
    }
    
    /// 获取所有已注册的品种
    pub fn get_all_symbols(&self) -> Vec<String> {
        let symbol_to_metadata = self.symbol_to_metadata.read().unwrap();
        symbol_to_metadata.keys().cloned().collect()
    }
    
    /// 获取所有已注册的周期
    pub fn get_all_periods(&self) -> Vec<String> {
        let period_to_index = self.period_to_index.read().unwrap();
        period_to_index.keys().cloned().collect()
    }
}
