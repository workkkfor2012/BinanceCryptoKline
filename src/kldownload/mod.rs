// Export downloader related modules
pub mod api;
pub mod models;
pub mod config;
pub mod downloader;
pub mod storage;
pub mod error;
pub mod utils;
pub mod db;
pub mod aggregator;
pub mod websocket;

// Re-export commonly used modules for convenience
pub use api::BinanceApi;
pub use models::{DownloadTask, DownloadResult, Kline};
pub use config::Config;
pub use downloader::Downloader;
pub use error::{Result, AppError};
pub use aggregator::KlineAggregator;
pub use websocket::{ContinuousKlineClient, ContinuousKlineConfig};

