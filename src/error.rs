use thiserror::Error;

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

    #[error("Unknown error: {0}")]
    #[allow(dead_code)]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
