use crate::kldata::KlineBackfiller;
use crate::klcommon::{Database, Result};
use tracing::{info, error};
use std::path::PathBuf;
use std::sync::Arc;

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

        // Create database connection
        let db_path = PathBuf::from("./data/klines.db");
        let db = Arc::new(Database::new(&db_path)?);

        // Define intervals to download
        let intervals = vec![
            "1m".to_string(),
            "5m".to_string(),
            "30m".to_string(),
            "4h".to_string(),
            "1d".to_string(),
            "1w".to_string()
        ];

        // Create backfiller instance
        let backfiller = KlineBackfiller::new(db, intervals);

        // Run backfill process
        match backfiller.run_once().await {
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
