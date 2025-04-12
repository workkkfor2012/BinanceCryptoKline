use crate::error::{AppError, Result};
use crate::models::Kline;
use log::{debug, info};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

pub type DbPool = Pool<SqliteConnectionManager>;

/// Database handler for kline data
pub struct Database {
    pool: DbPool,
}

impl Database {
    /// Create a new database connection
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref();

        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let manager = SqliteConnectionManager::file(db_path);
        let pool = Pool::new(manager)
            .map_err(|e| AppError::DatabaseError(format!("Failed to create connection pool: {}", e)))?;

        // Initialize database
        let db = Self { pool };
        db.init_db()?;

        info!("Database initialized at {}", db_path.display());
        Ok(db)
    }

    /// Initialize database tables
    fn init_db(&self) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Create symbols table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS symbols (
                symbol TEXT PRIMARY KEY,
                base_asset TEXT,
                quote_asset TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        ).map_err(|e| AppError::DatabaseError(format!("Failed to create symbols table: {}", e)))?;

        Ok(())
    }

    /// Ensure table exists for a specific symbol and interval
    fn ensure_symbol_table(&self, symbol: &str, interval: &str) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Create table name: lowercase symbol_interval (e.g., btc_1m)
        // 去掉交易对名称中的"USDT"后缀
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // 对数字开头的品种添加"k_"前缀
        let table_name = if symbol_lower.chars().next().unwrap_or('a').is_digit(10) {
            format!("k_{symbol_lower}_{interval_lower}")
        } else {
            format!("{symbol_lower}_{interval_lower}")
        };

        // Create table for this symbol and interval
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                open_time INTEGER PRIMARY KEY,
                open TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                close TEXT NOT NULL,
                volume TEXT NOT NULL,
                close_time INTEGER NOT NULL,
                quote_asset_volume TEXT NOT NULL,
                number_of_trades INTEGER NOT NULL,
                taker_buy_base_asset_volume TEXT NOT NULL,
                taker_buy_quote_asset_volume TEXT NOT NULL,
                ignore TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            table_name
        );

        conn.execute(&create_table_sql, [])
            .map_err(|e| AppError::DatabaseError(format!("Failed to create table {}: {}", table_name, e)))?;

        Ok(())
    }

    /// Save a symbol to the database
    pub fn save_symbol(&self, symbol: &str, base_asset: &str, quote_asset: &str, status: &str) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        conn.execute(
            "INSERT OR REPLACE INTO symbols (symbol, base_asset, quote_asset, status) VALUES (?, ?, ?, ?)",
            params![symbol, base_asset, quote_asset, status],
        ).map_err(|e| AppError::DatabaseError(format!("Failed to save symbol: {}", e)))?;

        Ok(())
    }

    /// Save klines to the database
    pub fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline]) -> Result<usize> {
        if klines.is_empty() {
            return Ok(0);
        }

        // Ensure table exists for this symbol and interval
        self.ensure_symbol_table(symbol, interval)?;

        // Create table name: lowercase symbol_interval (e.g., btc_1m)
        // 去掉交易对名称中的"USDT"后缀
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // 对数字开头的品种添加"k_"前缀
        let table_name = if symbol_lower.chars().next().unwrap_or('a').is_digit(10) {
            format!("k_{symbol_lower}_{interval_lower}")
        } else {
            format!("{symbol_lower}_{interval_lower}")
        };

        let mut conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Begin transaction
        let tx = conn.transaction()
            .map_err(|e| AppError::DatabaseError(format!("Failed to begin transaction: {}", e)))?;

        let mut count = 0;

        // Create SQL with dynamic table name
        let insert_sql = format!(
            "INSERT OR REPLACE INTO {} (
                open_time, open, high, low, close, volume,
                close_time, quote_asset_volume, number_of_trades,
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            table_name
        );

        for kline in klines {
            let result = tx.execute(
                &insert_sql,
                params![
                    kline.open_time,
                    kline.open,
                    kline.high,
                    kline.low,
                    kline.close,
                    kline.volume,
                    kline.close_time,
                    kline.quote_asset_volume,
                    kline.number_of_trades,
                    kline.taker_buy_base_asset_volume,
                    kline.taker_buy_quote_asset_volume,
                    kline.ignore,
                ],
            );

            match result {
                Ok(_) => count += 1,
                Err(e) => {
                    // Rollback transaction on error
                    let _ = tx.rollback();
                    return Err(AppError::DatabaseError(format!("Failed to save kline to {}: {}", table_name, e)));
                }
            }
        }

        // Commit transaction
        tx.commit()
            .map_err(|e| AppError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        debug!("Saved {} klines for {}/{} to table {}", count, symbol, interval, table_name);
        Ok(count)
    }

    /// Get the latest kline timestamp for a symbol and interval
    pub fn get_latest_kline_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>> {
        // Create table name: lowercase symbol_interval (e.g., btc_1m)
        // 去掉交易对名称中的"USDT"后缀
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // 对数字开头的品种添加"k_"前缀
        let table_name = if symbol_lower.chars().next().unwrap_or('a').is_digit(10) {
            format!("k_{symbol_lower}_{interval_lower}")
        } else {
            format!("{symbol_lower}_{interval_lower}")
        };

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Check if table exists
        let table_exists: bool = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            params![table_name],
            |row| row.get::<_, i64>(0).map(|count| count > 0),
        ).unwrap_or(false);

        if !table_exists {
            return Ok(None);
        }

        let query = format!("SELECT MAX(open_time) FROM {}", table_name);
        let result: Option<i64> = conn.query_row(
            &query,
            [],
            |row| row.get(0),
        ).optional()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get latest kline timestamp from {}: {}", table_name, e)))?;

        Ok(result)
    }

    /// Get the count of klines for a symbol and interval
    pub fn get_kline_count(&self, symbol: &str, interval: &str) -> Result<i64> {
        // Create table name: lowercase symbol_interval (e.g., btc_1m)
        // 去掉交易对名称中的"USDT"后缀
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // 对数字开头的品种添加"k_"前缀
        let table_name = if symbol_lower.chars().next().unwrap_or('a').is_digit(10) {
            format!("k_{symbol_lower}_{interval_lower}")
        } else {
            format!("{symbol_lower}_{interval_lower}")
        };

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Check if table exists
        let table_exists: bool = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            params![table_name],
            |row| row.get::<_, i64>(0).map(|count| count > 0),
        ).unwrap_or(false);

        if !table_exists {
            return Ok(0);
        }

        let query = format!("SELECT COUNT(*) FROM {}", table_name);
        let count: i64 = conn.query_row(
            &query,
            [],
            |row| row.get(0),
        ).map_err(|e| AppError::DatabaseError(format!("Failed to get kline count from {}: {}", table_name, e)))?;

        Ok(count)
    }

    /// 不再限制K线数量，保留所有数据
    pub fn trim_klines(&self, symbol: &str, interval: &str, max_count: i64) -> Result<usize> {
        // 不再限制K线数量，直接返回0
        debug!("K-line trimming disabled, keeping all data for {}/{}", symbol, interval);
        Ok(0)
    }
}
