use crate::klcommon::error::{AppError, Result};
use crate::klcommon::models::Kline;
use log::{debug, info};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;

// Global counters for tracking insert and update operations
// Format: (insert_count, update_count, last_log_time)
static DB_OPERATIONS: Lazy<(AtomicUsize, AtomicUsize, std::sync::Mutex<Instant>)> = Lazy::new(|| {
    (AtomicUsize::new(0), AtomicUsize::new(0), std::sync::Mutex::new(Instant::now()))
});

// Log interval in seconds
// Output database operation statistics every 10 seconds
const DB_LOG_INTERVAL: u64 = 10;

pub type DbPool = Pool<SqliteConnectionManager>;

/// Database handler for kline data
pub struct Database {
    pool: DbPool,
}

impl Database {
    /// Create a new database connection with WAL mode and optimized settings
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref();

        // Ensure parent directory exists for database
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        info!("Using SQLite database with WAL mode at {}", db_path.display());

        // Create database connection manager with WAL mode and performance optimizations
        let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
            conn.execute_batch("
                PRAGMA journal_mode = WAL;           -- Enable WAL mode
                PRAGMA synchronous = NORMAL;         -- Balance performance and safety
                PRAGMA cache_size = -50000;          -- About 200MB cache (negative means KB)
                PRAGMA mmap_size = 268435456;        -- 256MB memory mapping
                PRAGMA temp_store = MEMORY;          -- Store temp tables in memory
                PRAGMA wal_autocheckpoint = 1000;    -- Checkpoint every 1000 pages
                PRAGMA busy_timeout = 5000;          -- 5 second busy timeout
            ")
        });

        // Create connection pool with multiple concurrent connections
        let pool = Pool::builder()
            .max_size(10) // Maximum 10 concurrent connections, adjust as needed
            .build(manager)
            .map_err(|e| AppError::DatabaseError(format!("Failed to create connection pool: {}", e)))?;

        // Initialize database instance
        let db = Self { pool };

        // Initialize database tables
        db.init_db()?;

        info!("SQLite database with WAL mode initialized successfully");
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

        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

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

        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

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
        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

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
        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

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

    /// No longer limiting kline count, keep all data
    pub fn trim_klines(&self, symbol: &str, interval: &str, _max_count: i64) -> Result<usize> {
        // No longer limiting kline count, just return 0
        debug!("K-line trimming disabled, keeping all data for {}/{}", symbol, interval);
        Ok(0)
    }

    /// Get database connection
    pub fn get_connection(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))
    }

    /// Insert new kline data (called when is_closed=true)
    pub fn insert_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<()> {
        // Ensure table exists
        self.ensure_symbol_table(symbol, interval)?;

        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Directly insert new kline (closed kline)
        conn.execute(
            &format!("INSERT INTO {} (
                open_time, open, high, low, close, volume, close_time,
                quote_asset_volume, number_of_trades,
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", table_name),
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
                kline.ignore
            ],
        ).map_err(|e| AppError::DatabaseError(format!("Failed to insert kline: {}", e)))?;

        // Update insert counter and check if log output is needed
        DB_OPERATIONS.0.fetch_add(1, Ordering::Relaxed);
        self.check_and_log_db_operations();

        Ok(())
    }

    /// Update existing kline data (called when is_closed=false)
    pub fn update_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<()> {
        // Ensure table exists
        self.ensure_symbol_table(symbol, interval)?;

        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Try to update existing kline (unclosed kline)
        let result = conn.execute(
            &format!("UPDATE {} SET
                open = ?,
                high = ?,
                low = ?,
                close = ?,
                volume = ?,
                close_time = ?,
                quote_asset_volume = ?,
                number_of_trades = ?,
                taker_buy_base_asset_volume = ?,
                taker_buy_quote_asset_volume = ?,
                ignore = ?
                WHERE open_time = ?", table_name),
            params![
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
                kline.open_time
            ],
        ).map_err(|e| AppError::DatabaseError(format!("Failed to update kline: {}", e)))?;

        // Check if any records were updated
        if result == 0 {
            // If no records were updated, the record doesn't exist and needs to be inserted
            conn.execute(
                &format!("INSERT INTO {} (
                    open_time, open, high, low, close, volume, close_time,
                    quote_asset_volume, number_of_trades,
                    taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", table_name),
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
                    kline.ignore
                ],
            ).map_err(|e| AppError::DatabaseError(format!("Failed to insert kline after update failed: {}", e)))?;

            // Update insert counter
            DB_OPERATIONS.0.fetch_add(1, Ordering::Relaxed);
        } else {
            // Update update counter
            DB_OPERATIONS.1.fetch_add(1, Ordering::Relaxed);
        }

        // Check if log output is needed
        self.check_and_log_db_operations();

        Ok(())
    }

    /// Save a single kline (compatible with old version)
    pub fn save_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<()> {
        // Ensure table exists
        self.ensure_symbol_table(symbol, interval)?;

        // Create table name: k_symbol_interval (e.g., k_btc_1m)
        // Remove "USDT" suffix from symbol name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();

        // Consistently use k_ prefix
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Check if a kline with the same timestamp already exists
        let exists: bool = conn.query_row(
            &format!("SELECT 1 FROM {} WHERE open_time = ?", table_name),
            params![kline.open_time],
            |_| Ok(true),
        ).unwrap_or(false);

        if exists {
            // Update existing kline
            conn.execute(
                &format!("UPDATE {} SET
                    open = ?,
                    high = ?,
                    low = ?,
                    close = ?,
                    volume = ?,
                    close_time = ?,
                    quote_asset_volume = ?,
                    number_of_trades = ?,
                    taker_buy_base_asset_volume = ?,
                    taker_buy_quote_asset_volume = ?,
                    ignore = ?
                    WHERE open_time = ?", table_name),
                params![
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
                    kline.open_time
                ],
            ).map_err(|e| AppError::DatabaseError(format!("Failed to update kline: {}", e)))?;

            // Update counter and check if log output is needed
            DB_OPERATIONS.1.fetch_add(1, Ordering::Relaxed);
            self.check_and_log_db_operations();
        } else {
            // Insert new kline
            conn.execute(
                &format!("INSERT INTO {} (
                    open_time, open, high, low, close, volume, close_time,
                    quote_asset_volume, number_of_trades,
                    taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", table_name),
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
                    kline.ignore
                ],
            ).map_err(|e| AppError::DatabaseError(format!("Failed to insert kline: {}", e)))?;

            // Update counter and check if log output is needed
            DB_OPERATIONS.0.fetch_add(1, Ordering::Relaxed);
            self.check_and_log_db_operations();
        }

        Ok(())
    }

    /// Check and output database operation statistics
    fn check_and_log_db_operations(&self) {
        // Get last log time
        let mut last_log_time = DB_OPERATIONS.2.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_log_time);

        // If log interval has passed, output log and reset counters
        if elapsed >= Duration::from_secs(DB_LOG_INTERVAL) {
            let insert_count = DB_OPERATIONS.0.swap(0, Ordering::Relaxed);
            let update_count = DB_OPERATIONS.1.swap(0, Ordering::Relaxed);

            if insert_count > 0 || update_count > 0 {
                debug!("Database operations in the last {} seconds: {} inserts, {} updates",
                       DB_LOG_INTERVAL, insert_count, update_count);
            }

            // Update last log time
            *last_log_time = now;
        }
    }

    /// Get all symbols from the database
    pub fn get_all_symbols(&self) -> Result<Vec<String>> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Query all symbols from the symbols table
        let mut stmt = conn.prepare("SELECT symbol FROM symbols")?;
        let symbols = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut result = Vec::new();
        for symbol in symbols {
            result.push(symbol?);
        }

        // If no symbols in the symbols table, try to get them from the table names
        if result.is_empty() {
            // Query all table names that start with k_
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'k_%'"
            )?;
            let tables = stmt.query_map([], |row| row.get::<_, String>(0))?;

            let mut symbols = std::collections::HashSet::new();
            for table in tables {
                let table_name = table?;
                // Extract symbol from table name (format: k_symbol_interval)
                if let Some(symbol_interval) = table_name.strip_prefix("k_") {
                    if let Some(symbol) = symbol_interval.split('_').next() {
                        // Add USDT suffix back
                        let symbol = format!("{}{}", symbol.to_uppercase(), "USDT");
                        symbols.insert(symbol);
                    }
                }
            }

            result = symbols.into_iter().collect();
        }

        Ok(result)
    }

    /// Get the latest klines
    pub fn get_latest_klines(&self, symbol: &str, interval: &str, limit: usize) -> Result<Vec<Kline>> {
        // Ensure table exists
        self.ensure_symbol_table(symbol, interval)?;

        // Create table name
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // Query latest klines
        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} ORDER BY open_time DESC LIMIT ?",
            table_name
        ))?;

        let klines = stmt.query_map([limit], |row| {
            Ok(Kline {
                open_time: row.get(0)?,
                open: row.get(1)?,
                high: row.get(2)?,
                low: row.get(3)?,
                close: row.get(4)?,
                volume: row.get(5)?,
                close_time: row.get(6)?,
                quote_asset_volume: row.get(7)?,
                number_of_trades: row.get(8)?,
                taker_buy_base_asset_volume: row.get(9)?,
                taker_buy_quote_asset_volume: row.get(10)?,
                ignore: row.get(11)?,
            })
        })?;

        let mut result = Vec::new();
        for kline in klines {
            result.push(kline?);
        }

        // Sort results by time in ascending order
        result.sort_by_key(|k| k.open_time);

        Ok(result)
    }
}
