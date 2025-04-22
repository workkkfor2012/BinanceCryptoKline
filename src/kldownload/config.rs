use super::error::{AppError, Result};
use chrono::{DateTime, NaiveDate, Utc};
use log::{debug, info};
use std::path::PathBuf;

/// Configuration for the downloader
pub struct Config {
    /// Output directory for downloaded data
    pub output_dir: PathBuf,
    /// Number of concurrent downloads
    pub concurrency: usize,
    /// Intervals to download (e.g., 1m, 5m, 15m, 1h, 4h, 1d)
    pub intervals: Vec<String>,
    /// Start time for download (optional)
    pub start_time: Option<DateTime<Utc>>,
    /// End time for download (optional)
    pub end_time: Option<DateTime<Utc>>,
    /// Symbols to download (optional, if None, all symbols will be downloaded)
    pub symbols: Option<Vec<String>>,
    /// Base URL for the API
    pub api_base_url: String,
    /// Maximum number of klines per request
    pub max_limit: usize,
    /// Maximum number of klines to keep per symbol
    pub max_klines_per_symbol: i32,
    /// Whether to use SQLite for storage
    pub use_sqlite: bool,
    /// Path to the SQLite database
    pub db_path: PathBuf,
    /// Whether to only update existing symbols
    pub update_only: bool,
}

impl Config {
    /// Create a new configuration
    pub fn new(
        output_dir: Option<String>,
        concurrency: Option<usize>,
        intervals: Option<Vec<String>>,
        start_date: Option<String>,
        end_date: Option<String>,
        symbols: Option<Vec<String>>,
        use_sqlite: Option<bool>,
        max_klines: Option<i32>,
        update_only: Option<bool>,
    ) -> Self {
        // Set output directory
        let output_dir = match output_dir {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from("./data"),
        };

        // Set concurrency
        let concurrency = concurrency.unwrap_or(10);

        // Set intervals
        let intervals = match intervals {
            Some(intervals) => intervals,
            None => vec![
                "1m".to_string(),
                "5m".to_string(),
                "30m".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "1w".to_string(),
            ],
        };
        info!("Using intervals: {}", intervals.join(", "));

        // Set start time
        let start_time = match start_date {
            Some(date) => match parse_date_string(&date) {
                Ok(time) => Some(time),
                Err(e) => {
                    debug!("Failed to parse start date: {}", e);
                    None
                }
            },
            None => None,
        };

        // Set end time
        let end_time = match end_date {
            Some(date) => match parse_date_string(&date) {
                Ok(time) => Some(time),
                Err(e) => {
                    debug!("Failed to parse end date: {}", e);
                    None
                }
            },
            None => None,
        };

        // Set use_sqlite
        let use_sqlite = use_sqlite.unwrap_or(true);

        // Set update_only
        let update_only = update_only.unwrap_or(false);

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
