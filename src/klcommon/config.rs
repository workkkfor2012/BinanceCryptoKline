//! K线聚合系统配置模块

use serde::{Deserialize, Serialize};
use crate::klcommon::Result;

/// 系统配置常量
pub mod constants {
    /// 默认缓冲区切换间隔（毫秒）
    pub const DEFAULT_BUFFER_SWAP_INTERVAL_MS: u64 = 1000;

    /// 默认持久化间隔（毫秒）
    pub const DEFAULT_PERSISTENCE_INTERVAL_MS: u64 = 5000;

    /// 默认支持的最大品种数
    pub const DEFAULT_MAX_SYMBOLS: usize = 10000;

    /// 默认支持的时间周期
    pub const DEFAULT_INTERVALS: &[&str] = &["1m", "5m", "30m", "1h", "4h", "1d", "1w"];

    /// CPU缓存行大小
    pub const CACHE_LINE_SIZE: usize = 64;
}

use constants::*;

/// 默认启用完全追踪功能
fn default_enable_full_tracing() -> bool {
    true
}

/// 默认启用日志系统
fn default_log_enabled() -> bool {
    true
}

/// 默认WebLog管道名称
fn default_weblog_pipe_name() -> Option<String> {
    Some("weblog_pipe".to_string())
}

/// 默认启用控制台输出
fn default_enable_console_output() -> bool {
    false
}

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

    /// 网关配置
    pub gateway: GatewayConfig,

    /// 日志配置
    pub logging: LoggingConfig,

    /// 支持的时间周期列表
    pub supported_intervals: Vec<String>,

    /// 最大支持的品种数量
    pub max_symbols: usize,

    /// 缓冲区切换间隔（毫秒）
    pub buffer_swap_interval_ms: u64,

    /// 注意：persistence_interval_ms 已移除
    /// 数据持久化频率现在由 gateway.pull_interval_ms 间接控制

    /// Actor心跳间隔（秒）
    pub actor_heartbeat_interval_s: Option<u64>,

    /// Watchdog检查间隔（秒）
    pub watchdog_check_interval_s: Option<u64>,

    /// Watchdog Actor超时阈值（秒）
    pub watchdog_actor_timeout_s: Option<u64>,

    /// Actor通道容量（有界通道的缓冲区大小）
    pub channel_capacity: Option<usize>,
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

/// 网关配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Gateway拉取Worker数据的间隔（毫秒）
    pub pull_interval_ms: u64,

    /// 单个Worker请求的超时时间（毫秒）
    pub pull_timeout_ms: u64,

    /// Worker连续超时的告警阈值
    pub timeout_alert_threshold: usize,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// 日志系统总开关
    #[serde(default = "default_log_enabled")]
    pub enabled: bool,

    /// 日志级别 (trace, debug, info, warn, error)
    pub log_level: String,

    /// 日志传输方式 (named_pipe, file, console)
    pub log_transport: String,

    /// 命名管道名称 (用于Log MCP)
    pub pipe_name: String,

    /// WebLog管道名称 (用于WebLog可视化)
    #[serde(default = "default_weblog_pipe_name")]
    pub weblog_pipe_name: Option<String>,

    /// 是否启用完全追踪功能（包括跨线程上下文传递）
    #[serde(default = "default_enable_full_tracing")]
    pub enable_full_tracing: bool,

    /// 是否启用控制台输出（用于调试）
    #[serde(default = "default_enable_console_output")]
    pub enable_console_output: bool,
}

// 移除 Default 实现，强制从配置文件读取所有配置
// impl Default for AggregateConfig 已被移除，配置必须从文件加载

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
            reconnect_interval_secs: 1,
            max_reconnect_attempts: 300, // 5分钟 × 60秒 ÷ 1秒 = 300次
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

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            pull_interval_ms: 100,  // 100ms间隔，高频拉取
            pull_timeout_ms: 50,    // 50ms超时，远小于拉取间隔
            timeout_alert_threshold: 5,  // 连续5次超时后告警
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_level: "trace".to_string(),
            log_transport: "named_pipe".to_string(),
            pipe_name: r"\\.\pipe\kline_log_pipe".to_string(),
            weblog_pipe_name: Some("weblog_pipe".to_string()),
            enable_full_tracing: true,
            enable_console_output: false,
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
        
        // 验证Gateway配置
        if self.gateway.pull_interval_ms == 0 {
            return Err(crate::klcommon::AppError::ConfigError(
                "Gateway拉取间隔必须大于0".to_string()
            ));
        }

        if self.gateway.pull_timeout_ms >= self.gateway.pull_interval_ms {
            return Err(crate::klcommon::AppError::ConfigError(
                "Gateway超时时间必须小于拉取间隔".to_string()
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

    // 移除 create_default_config_file 方法，不再支持创建默认配置文件
    // 所有配置必须手动在配置文件中指定
}
