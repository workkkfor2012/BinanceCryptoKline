use thiserror::Error;
use std::net::AddrParseError;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("API error: {0}")]
    ApiError(String),

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("Time parsing error: {0}")]
    TimeParseError(#[from] chrono::ParseError),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Data error: {0}")]
    #[allow(dead_code)]
    DataError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    #[error("URL parsing error: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("WebSocket protocol error: {0}")]
    WebSocketProtocolError(String),

    #[error("HTTP request error: {0}")]
    HttpRequestError(#[from] http::Error),

    #[error("Address parse error: {0}")]
    AddrParseError(#[from] AddrParseError),

    #[error("SQLite error: {0}")]
    SqliteError(#[from] rusqlite::Error),

    #[error("Web server error: {0}")]
    WebServerError(String),

    #[error("Aggregation error: {0}")]
    AggregationError(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("Actor error: {0}")]
    ActorError(String),

    #[error("Unknown error: {0}")]
    #[allow(dead_code)]
    Unknown(String),
}

impl AppError {
    /// 获取错误类型的简洁摘要，用于追踪系统的错误分类
    ///
    /// 返回一个稳定的错误类别字符串，便于TraceDistiller进行错误聚合和分析
    /// ✨ 优化为业务导向的错误分类，便于AI诊断系统理解业务影响
    pub fn get_error_type_summary(&self) -> &'static str {
        match self {
            // ✨ K线数据获取相关的业务错误
            AppError::ApiError(_) => "kline_data_acquisition_failed",
            AppError::HttpError(_) => "market_data_connection_failed",
            AppError::HttpRequestError(_) => "market_data_request_malformed",

            // ✨ K线数据处理相关的业务错误
            AppError::JsonError(_) => "kline_data_parsing_failed",
            AppError::CsvError(_) => "kline_export_failed",
            AppError::ParseError(_) => "market_data_format_invalid",
            AppError::DataError(_) => "kline_data_validation_failed",

            // ✨ K线数据存储相关的业务错误
            AppError::DatabaseError(_) => "kline_data_persistence_failed",
            AppError::SqliteError(_) => "kline_storage_operation_failed",

            // ✨ 市场数据连接相关的业务错误
            AppError::WebSocketError(_) => "realtime_market_data_failed",
            AppError::WebSocketProtocolError(_) => "market_data_stream_failed",
            AppError::UrlParseError(_) => "market_endpoint_invalid",
            AppError::AddrParseError(_) => "market_server_address_invalid",

            // ✨ 系统资源相关的业务错误
            AppError::IoError(_) => "kline_file_operation_failed",
            AppError::ChannelError(_) => "kline_processing_pipeline_failed",

            // ✨ 时间处理相关的业务错误
            AppError::TimeParseError(_) => "kline_timestamp_invalid",

            // ✨ 配置相关的业务错误
            AppError::ConfigError(_) => "kline_service_configuration_invalid",

            // ✨ 业务逻辑相关的错误
            AppError::AggregationError(_) => "kline_aggregation_logic_failed",
            AppError::ActorError(_) => "kline_processing_actor_failed",
            AppError::WebServerError(_) => "kline_api_server_failed",

            // 未分类错误
            AppError::Unknown(_) => "kline_service_unknown_error",
        }
    }

    /// ✨ [新增] 获取业务影响级别，用于AI诊断系统评估错误的业务严重性
    pub fn get_business_impact_level(&self) -> &'static str {
        match self {
            // 高影响：直接影响核心K线数据业务
            AppError::DatabaseError(_) |
            AppError::SqliteError(_) => "high", // 数据持久化失败影响数据完整性

            AppError::ApiError(_) |
            AppError::HttpError(_) => "high", // 无法获取市场数据

            // 中等影响：影响数据质量或处理效率
            AppError::JsonError(_) |
            AppError::DataError(_) => "medium", // 数据解析失败可能导致数据丢失

            AppError::WebSocketError(_) |
            AppError::WebSocketProtocolError(_) => "medium", // 实时数据流中断

            // 低影响：不直接影响核心业务功能
            AppError::ConfigError(_) => "low", // 配置问题通常可以修复
            AppError::TimeParseError(_) => "low", // 时间解析问题影响有限
            AppError::UrlParseError(_) |
            AppError::AddrParseError(_) => "low", // 地址解析问题

            // 系统级影响：可能影响整个服务
            AppError::IoError(_) |
            AppError::ChannelError(_) => "medium", // 系统资源问题

            // 其他错误
            _ => "medium", // 默认中等影响
        }
    }

    /// 检查错误是否为可重试类型
    ///
    /// 用于决策系统判断是否应该重试失败的操作
    pub fn is_retryable(&self) -> bool {
        match self {
            // 网络相关错误通常可重试
            AppError::HttpError(_) |
            AppError::ApiError(_) |
            AppError::WebSocketError(_) => true,

            // 临时性系统资源错误可重试
            AppError::IoError(_) |
            AppError::ChannelError(_) => true,

            // 数据库锁争用等可重试
            AppError::DatabaseError(msg) => {
                // 检查是否为锁争用或临时性错误
                msg.contains("locked") || msg.contains("busy") || msg.contains("timeout")
            },
            AppError::SqliteError(_) => {
                // SQLite错误通常可重试（锁争用、忙等）
                true
            },

            // 解析错误、配置错误等不可重试
            AppError::JsonError(_) |
            AppError::CsvError(_) |
            AppError::ParseError(_) |
            AppError::DataError(_) |
            AppError::TimeParseError(_) |
            AppError::ConfigError(_) |
            AppError::UrlParseError(_) |
            AppError::AddrParseError(_) => false,

            // 业务逻辑错误需要具体分析
            AppError::AggregationError(_) |
            AppError::ActorError(_) |
            AppError::WebServerError(_) => false,

            // 未知错误保守处理，不重试
            AppError::Unknown(_) => false,

            AppError::HttpRequestError(_) => false,
            AppError::WebSocketProtocolError(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
