use chrono::{DateTime, NaiveDate, Utc};
use log::{debug, info};
use std::path::PathBuf;
use crate::error::{AppError, Result};

/// Supported kline intervals
pub const SUPPORTED_INTERVALS: [&str; 15] = [
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
];

/// Configuration for the downloader
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory to save downloaded data
    pub output_dir: PathBuf,
    /// Number of concurrent downloads
    pub concurrency: usize,
    /// Kline intervals to download
    pub intervals: Vec<String>,
    /// Start time for data download
    pub start_time: Option<i64>,
    /// End time for data download
    pub end_time: Option<i64>,
    /// Specific symbols to download
    pub symbols: Option<Vec<String>>,
    /// Base URL for Binance API
    /// API基础URL
    #[allow(dead_code)]
    pub api_base_url: String,
    /// Maximum number of klines per request
    #[allow(dead_code)]
    pub max_limit: i32,
    /// Maximum number of klines to keep per symbol and interval
    pub max_klines_per_symbol: i32,
    /// Use SQLite for storage
    pub use_sqlite: bool,
    /// SQLite database path
    pub db_path: PathBuf,
    /// Only update new klines (from last timestamp to now)
    pub update_only: bool,
}

impl Config {
    /// Create a new configuration
    pub fn new(
        output_dir: String,
        concurrency: usize,
        intervals: Option<String>,
        start_time: Option<String>,
        end_time: Option<String>,
        symbols: Option<String>,
        use_sqlite: bool,
        max_klines: Option<i32>,
        update_only: bool,
    ) -> Self {
        let output_dir = PathBuf::from(output_dir.clone());

        // Parse intervals
        let mut intervals = if let Some(interval_str) = intervals {
            interval_str.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| SUPPORTED_INTERVALS.contains(&s.as_str()))
                .collect::<Vec<String>>()
        } else {
            // Default to 1m and 5m
            vec!["1m".to_string(), "5m".to_string()]
        };

        if intervals.is_empty() {
            info!("No valid intervals provided, using defaults: 1m,5m");
            intervals = vec!["1m".to_string(), "5m".to_string()];
        } else {
            info!("Using intervals: {}", intervals.join(", "));
        }

        // Parse start time and end time
        let start_time = start_time
            .and_then(|s| parse_date_string(&s).ok())
            .map(|dt| dt.timestamp_millis());

        let end_time = end_time
            .and_then(|s| parse_date_string(&s).ok())
            .map(|dt| dt.timestamp_millis());

        // Parse symbols
        let symbols = symbols.map(|s| {
            s.split(',')
                .map(|s| s.trim().to_uppercase())
                .collect::<Vec<String>>()
        });

        // Set database path
        let db_path = PathBuf::from(format!("{}/klines.db", output_dir.display()));

        // 处理max_klines参数，0表示无限制
        let max_klines_per_symbol = match max_klines.unwrap_or(0) {
            0 => i32::MAX, // 如果是0，表示无限制，使用i32::MAX
            n => n,       // 否则使用用户指定的值
        };

        debug!("Configuration: output_dir={:?}, concurrency={}, intervals={:?}, start_time={:?}, end_time={:?}, symbols={:?}, use_sqlite={}, max_klines={}",
            output_dir, concurrency, intervals, start_time, end_time, symbols, use_sqlite, max_klines_per_symbol);

        Self {
            output_dir,
            concurrency,
            intervals,
            start_time,
            end_time,
            symbols,
            api_base_url: "".to_string(), // 不再使用这个字段，由api.rs中的自动切换机制管理
            max_limit: 1000,
            max_klines_per_symbol,
            use_sqlite,
            db_path,
            update_only,
        }
    }

    /// Ensure output directory exists
    pub fn ensure_output_dir(&self) -> Result<()> {
        if !self.output_dir.exists() {
            std::fs::create_dir_all(&self.output_dir)?;
        }
        Ok(())
    }

    /// Get output path for a specific symbol and interval
    pub fn get_output_path(&self, symbol: &str, interval: &str) -> PathBuf {
        let filename = format!("{}_{}_{}.csv",
            symbol.to_lowercase(),
            interval,
            chrono::Utc::now().format("%Y%m%d"));

        self.output_dir.join(filename)
    }
}

/// Parse a date string in YYYY-MM-DD format
fn parse_date_string(date_str: &str) -> Result<DateTime<Utc>> {
    let naive_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| AppError::TimeParseError(e))?;

    let naive_datetime = naive_date.and_hms_opt(0, 0, 0)
        .ok_or_else(|| AppError::ConfigError(format!("Invalid time: {}", date_str)))?;

    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc))
}
