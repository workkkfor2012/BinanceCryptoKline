use crate::kldata::{Config, Downloader};
use crate::klcommon::{Database, Result};
use log::{info, error};
use std::path::PathBuf;

/// Startup check module, used to check database and historical kline data
pub struct StartupCheck;

impl StartupCheck {
    /// Execute startup check flow
    pub async fn run() -> Result<bool> {
        info!("Executing startup check flow...");

        // Check if database file exists
        let db_path = PathBuf::from("./data/klines.db");
        let db_exists = db_path.exists();

        if !db_exists {
            info!("Database file does not exist, will download historical klines");
            // Execute download process
            Self::download_historical_klines().await?;
            return Ok(true); // Return true to indicate download was executed
        }

        // Check BTC 1-minute kline count
        let db = Database::new(&db_path)?;
        let btc_1m_count = db.get_kline_count("BTCUSDT", "1m")?;
        let btc_1w_count = db.get_kline_count("BTCUSDT", "1w")?;

        info!("BTC 1-minute kline count: {}", btc_1m_count);
        info!("BTC 1-week kline count: {}", btc_1w_count);

        // Determine if kline count meets requirements
        let min_1m_count = 900; // Use 90% of expected value (1000) as minimum requirement
        let min_1w_count = 1; // At least 1 weekly kline required
        if btc_1m_count < min_1m_count || btc_1w_count < min_1w_count {
            info!("BTC kline count does not meet requirements, will download historical klines");
            // Execute download process
            Self::download_historical_klines().await?;
            return Ok(true); // Return true to indicate download was executed
        }

        info!("Startup check completed, database and historical kline data are normal");
        Ok(false) // Return false to indicate no download was executed
    }

    /// Download historical kline data
    pub async fn download_historical_klines() -> Result<()> {
        info!("Starting to download historical kline data...");

        // Initialize downloader configuration
        let config = Config::new(
            Some("./data".to_string()),
            Some(15),                                  // Concurrency
            Some(vec!["1m".to_string(), "5m".to_string(), "30m".to_string(), "4h".to_string(), "1d".to_string(), "1w".to_string()]), // Kline intervals
            None,                                // Start time (auto calculate)
            None,                                // End time (current time)
            None,                                // Symbols (download all USDT-M perpetual contracts)
            Some(true),                          // Use SQLite file storage
            Some(0),                             // No limit on kline count
            Some(false),                         // Don't use update mode
        );

        // Create downloader instance
        let downloader = match Downloader::new(config) {
            Ok(d) => d,
            Err(e) => return Err(crate::klcommon::AppError::ApiError(format!("Failed to create downloader: {}", e))),
        };

        // Run download process
        match downloader.run().await {
            Ok(_) => {
                info!("Historical kline download completed, data saved to database");
                Ok(())
            },
            Err(e) => {
                error!("Historical kline download failed: {}", e);
                Err(crate::klcommon::AppError::ApiError(format!("Historical kline download failed: {}", e)))
            }
        }
    }
}
