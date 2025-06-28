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
    pub fn get_error_type_summary(&self) -> &'static str {
        match self {
            // API相关错误
            AppError::ApiError(_) => "api_request_failed",
            AppError::HttpError(_) => "http_request_failed",
            AppError::HttpRequestError(_) => "http_request_malformed",

            // 数据处理错误
            AppError::JsonError(_) => "json_parsing_failed",
            AppError::CsvError(_) => "csv_parsing_failed",
            AppError::ParseError(_) => "data_parsing_failed",
            AppError::DataError(_) => "data_validation_failed",

            // 数据库相关错误
            AppError::DatabaseError(_) => "database_operation_failed",
            AppError::SqliteError(_) => "sqlite_operation_failed",

            // 网络和连接错误
            AppError::WebSocketError(_) => "websocket_connection_failed",
            AppError::WebSocketProtocolError(_) => "websocket_protocol_failed",
            AppError::UrlParseError(_) => "url_parsing_failed",
            AppError::AddrParseError(_) => "address_parsing_failed",

            // 系统资源错误
            AppError::IoError(_) => "io_operation_failed",
            AppError::ChannelError(_) => "channel_communication_failed",

            // 时间处理错误
            AppError::TimeParseError(_) => "time_parsing_failed",

            // 配置和初始化错误
            AppError::ConfigError(_) => "configuration_error",

            // 业务逻辑错误
            AppError::AggregationError(_) => "aggregation_logic_failed",
            AppError::ActorError(_) => "actor_system_failed",
            AppError::WebServerError(_) => "web_server_failed",

            // 未分类错误
            AppError::Unknown(_) => "unknown_error",
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
