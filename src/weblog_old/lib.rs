//! # WebLog - 命名管道JSON日志显示系统
//!
//! 专用于命名管道的高性能JSON日志可视化系统，提供：
//! - 实时JSON日志流显示
//! - Trace/Span可视化
//! - 结构化日志解析
//! - 性能指标分析
//! - 分布式追踪监控
//!
//! ## 核心特性
//!
//! ### 专用输入源
//! - **命名管道**: 仅支持Windows命名管道输入
//! - **JSON格式**: 仅支持JSON格式的tracing日志
//!
//! ### Web Server
//! 基于Axum的Web服务器，提供：
//! - WebSocket实时数据推送
//! - RESTful API接口
//! - 静态文件服务
//! - JSON tracing日志解析和处理
//!
//! ### 前端界面
//! - `index.html` - 模块监控界面
//! - 实时日志聚合显示
//! - 模块分类和统计
//!
//! ## 使用方法
//!
//! ### 启动（必需指定命名管道）
//! ```bash
//! cargo run --bin weblog -- --pipe-name "kline_log_pipe"
//! ```
//!
//! ## 配置
//!
//! 通过环境变量配置：
//! - `PIPE_NAME`: 命名管道名称
//! - `RUST_LOG`: 日志级别
//! - `WEB_PORT`: Web服务端口 (默认8080)

pub mod types;
pub mod log_parser;
pub mod trace_manager;
pub mod web_server;
pub mod high_freq_aggregator;
pub mod module_manager;

pub use types::*;
pub use log_parser::*;
pub use trace_manager::*;
pub use web_server::*;
pub use high_freq_aggregator::*;
pub use module_manager::*;

/// WebLog系统的主要错误类型
#[derive(thiserror::Error, Debug)]
pub enum WebLogError {
    #[error("Web服务器错误: {0}")]
    WebServer(#[from] axum::Error),
    
    #[error("JSON解析错误: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("命名管道错误: {0}")]
    NamedPipe(String),
    
    #[error("日志解析错误: {0}")]
    LogParsing(String),
}

pub type Result<T> = std::result::Result<T, WebLogError>;

/// WebLog系统配置
#[derive(Debug, Clone)]
pub struct WebLogConfig {
    /// Web服务端口
    pub web_port: u16,
    /// 日志传输方式
    pub log_transport: LogTransport,
    /// 命名管道名称
    pub pipe_name: Option<String>,
    /// 最大保留的Trace数量
    pub max_traces: usize,
    /// 最大保留的日志条目数量
    pub max_log_entries: usize,
}

#[derive(Debug, Clone)]
pub enum LogTransport {
    NamedPipe(String),
}

impl Default for WebLogConfig {
    fn default() -> Self {
        Self {
            web_port: 8080,
            log_transport: LogTransport::NamedPipe("kline_log_pipe".to_string()),
            pipe_name: Some("kline_log_pipe".to_string()),
            max_traces: 1000,
            max_log_entries: 10000,
        }
    }
}

impl WebLogConfig {
    /// 从环境变量创建配置
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        // 设置端口
        if let Ok(port_str) = std::env::var("WEB_PORT") {
            if let Ok(port) = port_str.parse() {
                config.web_port = port;
            }
        }
        
        // 设置命名管道名称
        let pipe_name = std::env::var("PIPE_NAME")
            .unwrap_or_else(|_| "kline_log_pipe".to_string());
        config.log_transport = LogTransport::NamedPipe(pipe_name.clone());
        config.pipe_name = Some(pipe_name);
        
        config
    }
}
