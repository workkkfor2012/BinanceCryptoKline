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

pub type Result<T> = std::result::Result<T, AppError>;
