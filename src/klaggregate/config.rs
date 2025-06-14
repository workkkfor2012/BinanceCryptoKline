//! K线聚合系统配置模块

use serde::{Deserialize, Serialize};
use crate::klaggregate::types::constants::*;
use crate::klcommon::Result;

/// K线聚合系统配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateConfig {
    /// 数据库配置
    pub database: DatabaseConfig,
    
    /// WebSocket配置
    pub websocket: WebSocketConfig,
    
    /// 缓冲区配置
    pub buffer: BufferConfig,
    
    /// 持久化配置
    pub persistence: PersistenceConfig,
    
    /// 支持的时间周期列表
    pub supported_intervals: Vec<String>,
    
    /// 最大支持的品种数量
    pub max_symbols: usize,
    
    /// 缓冲区切换间隔（毫秒）
    pub buffer_swap_interval_ms: u64,
    
    /// 持久化间隔（毫秒）
    pub persistence_interval_ms: u64,
}

/// 数据库配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// 数据库文件路径
    pub database_path: String,
    
    /// 连接池大小
    pub pool_size: u32,
    
    /// 连接超时（秒）
    pub connection_timeout_secs: u64,
    
    /// 是否启用WAL模式
    pub enable_wal: bool,
}

/// WebSocket配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    
    /// 代理地址
    pub proxy_host: String,
    
    /// 代理端口
    pub proxy_port: u16,
    
    /// WebSocket URL
    pub websocket_url: String,
    
    /// 连接超时（秒）
    pub connection_timeout_secs: u64,
    
    /// 重连间隔（秒）
    pub reconnect_interval_secs: u64,
    
    /// 最大重连次数
    pub max_reconnect_attempts: usize,
    
    /// 心跳间隔（秒）
    pub heartbeat_interval_secs: u64,
}

/// 缓冲区配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConfig {
    /// 初始缓冲区大小
    pub initial_capacity: usize,
    
    /// 是否启用内存预分配
    pub enable_preallocation: bool,
    
    /// 内存对齐大小
    pub alignment_size: usize,
}

/// 持久化配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// 批量写入大小
    pub batch_size: usize,
    
    /// 写入队列大小
    pub queue_size: usize,
    
    /// 写入超时（秒）
    pub write_timeout_secs: u64,
    
    /// 是否启用压缩日志
    pub enable_compressed_logging: bool,
}

impl Default for AggregateConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            websocket: WebSocketConfig::default(),
            buffer: BufferConfig::default(),
            persistence: PersistenceConfig::default(),
            supported_intervals: DEFAULT_INTERVALS.iter().map(|s| s.to_string()).collect(),
            max_symbols: DEFAULT_MAX_SYMBOLS,
            buffer_swap_interval_ms: DEFAULT_BUFFER_SWAP_INTERVAL_MS,
            persistence_interval_ms: DEFAULT_PERSISTENCE_INTERVAL_MS,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_path: "kline_data.db".to_string(),
            pool_size: 10,
            connection_timeout_secs: 30,
            enable_wal: true,
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_host: "127.0.0.1".to_string(),
            proxy_port: 1080,
            websocket_url: "wss://fstream.binance.com/ws".to_string(),
            connection_timeout_secs: 30,
            reconnect_interval_secs: 5,
            max_reconnect_attempts: 10,
            heartbeat_interval_secs: 30,
        }
    }
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            initial_capacity: DEFAULT_MAX_SYMBOLS * DEFAULT_INTERVALS.len(),
            enable_preallocation: true,
            alignment_size: CACHE_LINE_SIZE,
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            queue_size: 5000,
            write_timeout_secs: 30,
            enable_compressed_logging: true,
        }
    }
}

impl AggregateConfig {
    /// 从文件加载配置
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::klcommon::AppError::IoError(e))?;
        
        let config: Self = toml::from_str(&content)
            .map_err(|e| crate::klcommon::AppError::ConfigError(format!("解析配置文件失败: {}", e)))?;
        
        config.validate()?;
        Ok(config)
    }
    
    /// 保存配置到文件
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| crate::klcommon::AppError::ConfigError(format!("序列化配置失败: {}", e)))?;
        
        std::fs::write(path, content)
            .map_err(|e| crate::klcommon::AppError::IoError(e))?;
        
        Ok(())
    }
    
    /// 验证配置的有效性
    pub fn validate(&self) -> Result<()> {
        // 验证时间周期
        if self.supported_intervals.is_empty() {
            return Err(crate::klcommon::AppError::ConfigError(
                "支持的时间周期列表不能为空".to_string()
            ));
        }
        
        // 验证每个时间周期的格式
        for interval in &self.supported_intervals {
            if crate::klcommon::api::interval_to_milliseconds(interval) <= 0 {
                return Err(crate::klcommon::AppError::ConfigError(
                    format!("无效的时间周期: {}", interval)
                ));
            }
        }
        
        // 验证最大品种数
        if self.max_symbols == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "最大品种数必须大于0".to_string()
            ));
        }
        
        // 验证缓冲区切换间隔
        if self.buffer_swap_interval_ms == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "缓冲区切换间隔必须大于0".to_string()
            ));
        }
        
        // 验证持久化间隔
        if self.persistence_interval_ms == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "持久化间隔必须大于0".to_string()
            ));
        }
        
        // 验证数据库配置
        if self.database.pool_size == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "数据库连接池大小必须大于0".to_string()
            ));
        }
        
        // 验证WebSocket配置
        if self.websocket.proxy_port == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "代理端口必须大于0".to_string()
            ));
        }
        
        // 验证缓冲区配置
        if self.buffer.initial_capacity == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "缓冲区初始容量必须大于0".to_string()
            ));
        }
        
        // 验证持久化配置
        if self.persistence.batch_size == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "批量写入大小必须大于0".to_string()
            ));
        }
        
        if self.persistence.queue_size == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "写入队列大小必须大于0".to_string()
            ));
        }
        
        Ok(())
    }
    
    /// 获取总的K线存储槽数量
    pub fn get_total_kline_slots(&self) -> usize {
        self.max_symbols * self.supported_intervals.len()
    }
    
    /// 获取周期索引
    pub fn get_period_index(&self, interval: &str) -> Option<u32> {
        self.supported_intervals
            .iter()
            .position(|i| i == interval)
            .map(|pos| pos as u32)
    }
    
    /// 根据索引获取周期字符串
    pub fn get_interval_by_index(&self, index: u32) -> Option<&str> {
        self.supported_intervals
            .get(index as usize)
            .map(|s| s.as_str())
    }
    
    /// 创建默认配置文件
    pub fn create_default_config_file(path: &str) -> Result<()> {
        let config = Self::default();
        config.save_to_file(path)
    }
}
