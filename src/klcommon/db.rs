use crate::klcommon::error::{AppError, Result};
use crate::klcommon::models::Kline;
use crate::klcommon::context::instrument_if_enabled;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension, Connection as RusqliteConnection};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use once_cell::sync::Lazy;
use tokio::task;
use tracing::{Span, debug, info};
use kline_macros::perf_profile;
use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};
use dashmap::DashSet;

// Global counters for tracking insert and update operations
// Format: (insert_count, update_count, last_log_time)
static DB_OPERATIONS: Lazy<(AtomicUsize, AtomicUsize, std::sync::Mutex<Instant>)> = Lazy::new(|| {
    (AtomicUsize::new(0), AtomicUsize::new(0), std::sync::Mutex::new(Instant::now()))
});

// Log interval in seconds
// Output database operation statistics every 10 seconds
const DB_LOG_INTERVAL: u64 = 10;

// ✨ 表创建状态缓存，避免重复的 CREATE TABLE IF NOT EXISTS 查询
static CREATED_TABLES: Lazy<DashSet<String>> = Lazy::new(DashSet::new);

// 数据库连接池类型
pub type DbPool = Pool<SqliteConnectionManager>;


// =====================================================================================
// ✨ [MODIFIED] 写入逻辑重构
// =====================================================================================

/// 写入任务结构体，表示一个待执行的数据库写入操作。
/// [MODIFIED] 统一了任务格式，以支持批量写入。
#[derive(Debug)]
struct WriteTask {
    #[allow(dead_code)]
    transaction_id: u64,
    // 任务现在直接携带批量写入所需的数据格式
    klines_to_save: Vec<(String, String, Kline)>,
    result_sender: oneshot::Sender<Result<usize>>,
    span: Span,
}

/// 数据库写入队列处理器
#[derive(Debug)]
struct DbWriteQueueProcessor {
    receiver: mpsc::Receiver<WriteTask>,
    pool: DbPool,
}

impl DbWriteQueueProcessor {
    /// 创建新的写入队列处理器
    fn new(receiver: mpsc::Receiver<WriteTask>, pool: DbPool) -> Self {
        Self { receiver, pool }
    }

    /// 启动写入队列处理线程
    fn start(mut self) {
        tokio::spawn(async move {
            while let Some(task) = self.receiver.recv().await {
                let pool_clone = self.pool.clone();
                let klines_count = task.klines_to_save.len();

                let processing_future = async move {
                    // [MODIFIED] 调用统一的、静态的批量写入实现
                    let db_result = Self::process_batch_write_task(pool_clone, task.klines_to_save).await;

                    // 根据结果转换回 usize 或 Error
                    let final_result = match db_result {
                        Ok(_) => Ok(klines_count),
                        Err(e) => Err(e),
                    };
                    let _ = task.result_sender.send(final_result);
                };

                instrument_if_enabled(processing_future, task.span).await;
            }
        });
    }

    /// [MODIFIED] 异步包装器，用于在 spawn_blocking 中执行实际的数据库操作。
    async fn process_batch_write_task(pool: DbPool, klines_to_save: Vec<(String, String, Kline)>) -> Result<()> {
        let parent_span = tracing::Span::current();

        let result = task::spawn_blocking(move || {
            parent_span.in_scope(|| {
                // 这个闭包是同步的
                let mut conn = pool.get()
                    .map_err(|e| AppError::DatabaseError(format!("获取数据库连接失败: {}", e)))?;
                // 调用包含所有核心逻辑的实现函数
                perform_batch_upsert(&mut conn, klines_to_save)
            })
        }).await;

        match result {
            Ok(db_result) => db_result,
            Err(join_error) => {
                let panic_error = AppError::DatabaseError(format!("K线数据持久化任务 panic: {:?}", join_error));
                Err(panic_error)
            }
        }
    }
}


/// [NEW] 所有K线写入操作的统一底层实现。
/// 这是一个同步函数，设计为在 `spawn_blocking` 或事务中调用。
#[perf_profile(skip_all, err)]
fn perform_batch_upsert(conn: &mut RusqliteConnection, klines_to_save: Vec<(String, String, Kline)>) -> Result<()> {
    if klines_to_save.is_empty() {
        return Ok(());
    }

    let kline_count = klines_to_save.len();

    // 1. 按表名（symbol, interval）对K线数据进行分组
    let mut grouped_klines: HashMap<(String, String), Vec<Kline>> = HashMap::new();
    for (symbol, interval, kline) in klines_to_save {
        grouped_klines.entry((symbol, interval)).or_default().push(kline);
    }

    // 2. 开启一个事务，以保证整个批量操作的原子性
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

    // 3. 循环处理每个分组
    for ((symbol, interval), klines) in grouped_klines {
        let table_name = get_table_name_static(&symbol, &interval);

        // 确保表存在 (在事务内部)
        ensure_symbol_table_in_tx(&tx, &table_name)?;

        // 准备该表的UPSERT语句
        let sql = format!(
            "INSERT INTO {} (open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(open_time) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
                 volume=excluded.volume, close_time=excluded.close_time, quote_asset_volume=excluded.quote_asset_volume,
                 number_of_trades=excluded.number_of_trades, taker_buy_base_asset_volume=excluded.taker_buy_base_asset_volume,
                 taker_buy_quote_asset_volume=excluded.taker_buy_quote_asset_volume, ignore=excluded.ignore",
            table_name
        );

        let mut stmt = tx.prepare_cached(&sql)?;

        // 循环执行该分组内所有K线的UPSERT
        for kline in klines {
            stmt.execute(params![
                kline.open_time, kline.open, kline.high, kline.low, kline.close, kline.volume,
                kline.close_time, kline.quote_asset_volume, kline.number_of_trades,
                kline.taker_buy_base_asset_volume, kline.taker_buy_quote_asset_volume, kline.ignore
            ])?;
        }
    }

    // 4. 提交事务
    tx.commit()?;
    
    // 记录统计信息
    DB_OPERATIONS.0.fetch_add(kline_count, Ordering::Relaxed);
    let (insert_count, update_count, last_log_time) = &*DB_OPERATIONS;
    let mut last_time = last_log_time.lock().unwrap();
    let now = Instant::now();

    if now.duration_since(*last_time).as_secs() >= DB_LOG_INTERVAL {
        info!(
            "DB Stats: {} writes in last {}s. Total Inserts/Updates: {}, Total Reads(placeholder): {}",
            kline_count,
            now.duration_since(*last_time).as_secs(),
            insert_count.load(Ordering::SeqCst),
            update_count.load(Ordering::SeqCst),
        );
        *last_time = now;
    }

    Ok(())
}

/// [NEW] 静态辅助函数：生成表名
fn get_table_name_static(symbol: &str, interval: &str) -> String {
    let symbol_lower = symbol.to_lowercase().replace("usdt", "");
    let interval_lower = interval.to_lowercase();
    format!("k_{symbol_lower}_{interval_lower}")
}

/// [NEW] 静态辅助函数：在事务内确保表存在
fn ensure_symbol_table_in_tx(tx: &rusqlite::Transaction, table_name: &str) -> Result<()> {
    if CREATED_TABLES.contains(table_name) {
        return Ok(());
    }
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            open_time INTEGER PRIMARY KEY, open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL, close TEXT NOT NULL,
            volume TEXT NOT NULL, close_time INTEGER NOT NULL, quote_asset_volume TEXT NOT NULL, number_of_trades INTEGER NOT NULL,
            taker_buy_base_asset_volume TEXT NOT NULL, taker_buy_quote_asset_volume TEXT NOT NULL, ignore TEXT NOT NULL
        )",
        table_name
    );
    tx.execute(&create_table_sql, [])?;
    CREATED_TABLES.insert(table_name.to_string());
    Ok(())
}


// =====================================================================================
// Database 结构体和主要方法
// =====================================================================================


/// Database handler for kline data
#[derive(Debug)]
pub struct Database {
    pool: DbPool,
    write_queue_sender: mpsc::Sender<WriteTask>,
}

impl Database {
    /// Create a new database connection with WAL mode and optimized settings
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref();

        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
            conn.execute_batch("
                PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA cache_size = -102400;
                PRAGMA mmap_size = 104857600; PRAGMA temp_store = MEMORY; PRAGMA wal_autocheckpoint = 1000;
                PRAGMA busy_timeout = 5000; PRAGMA page_size = 4096;
            ")
        });

        let pool = Pool::builder()
            .max_size(10)
            .build(manager)
            .map_err(|e| AppError::DatabaseError(format!("Failed to create connection pool: {}", e)))?;

        let (sender, receiver) = mpsc::channel(5000);

        // [MODIFIED] 直接创建处理器并启动，不再保留其状态
        let processor = DbWriteQueueProcessor::new(receiver, pool.clone());
        processor.start();

        let db = Self {
            pool,
            write_queue_sender: sender,
        };

        db.init_db()?;

        Ok(db)
    }

    /// Initialize database tables
    fn init_db(&self) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS symbols (
                symbol TEXT PRIMARY KEY, base_asset TEXT, quote_asset TEXT,
                status TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        ).map_err(|e| AppError::DatabaseError(format!("Failed to create symbols table: {}", e)))?;

        Ok(())
    }

    /// Ensure table exists for a specific symbol and interval
    pub fn ensure_symbol_table(&self, symbol: &str, interval: &str) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let table_name = get_table_name_static(symbol, interval);
        
        if CREATED_TABLES.contains(&table_name) {
            return Ok(());
        }

        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                open_time INTEGER PRIMARY KEY, open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL, close TEXT NOT NULL,
                volume TEXT NOT NULL, close_time INTEGER NOT NULL, quote_asset_volume TEXT NOT NULL, number_of_trades INTEGER NOT NULL,
                taker_buy_base_asset_volume TEXT NOT NULL, taker_buy_quote_asset_volume TEXT NOT NULL, ignore TEXT NOT NULL
            )",
            table_name
        );

        conn.execute(&create_table_sql, [])
            .map_err(|e| AppError::DatabaseError(format!("Failed to create table {}: {}", table_name, e)))?;
        
        CREATED_TABLES.insert(table_name);
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

    /// [MODIFIED] 批量 Upsert K线数据到数据库。
    /// 这个方法是同步的，设计为在 `tokio::task::spawn_blocking` 中调用。
    pub fn upsert_klines_batch(&self, klines_to_save: Vec<(String, String, Kline)>) -> Result<()> {
        if klines_to_save.is_empty() {
            return Ok(());
        }
        let mut conn = self.get_connection()?;
        // 调用统一的底层实现
        perform_batch_upsert(&mut conn, klines_to_save)
    }

    /// [MODIFIED] 使用写入队列异步保存K线数据
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval, kline_count = klines.len()))]
    pub async fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline], transaction_id: u64) -> Result<usize> {
        if klines.is_empty() {
            return Ok(0);
        }

        // 确保表存在，这是一个快速的内存检查
        self.ensure_symbol_table(symbol, interval)?;

        // [MODIFIED] 将数据打包成统一的批量格式
        let klines_to_save: Vec<(String, String, Kline)> = klines.iter()
            .map(|k| (symbol.to_string(), interval.to_string(), k.clone()))
            .collect();

        // [MODIFIED] 标记为未使用，以消除警告
        let _kline_count = klines_to_save.len();

        let (result_sender, result_receiver) = oneshot::channel();

        let task = WriteTask {
            transaction_id,
            klines_to_save, // 使用新的字段
            result_sender,
            span: Span::current(),
        };

        if let Err(e) = self.write_queue_sender.send(task).await {
            let queue_error = AppError::DatabaseError(format!("无法将写入任务添加到队列: {}", e));
            return Err(queue_error);
        }

        match result_receiver.await {
            Ok(result) => result,
            Err(e) => {
                let wait_error = AppError::DatabaseError(format!("等待写入操作结果时出错: {}", e));
                Err(wait_error)
            }
        }
    }


    // =====================================================================================
    // ✨ [REMOVED] 遗留的写入方法
    // `insert_kline`, `update_kline`, `save_kline` (单条版本) 均已移除。
    // 所有写入操作现在都通过 `save_klines` (异步) 或 `upsert_klines_batch` (同步) 进行，
    // 它们最终都使用统一、高效、事务安全的 `perform_batch_upsert` 实现。
    // =====================================================================================


    /// Get the latest kline timestamp for a symbol and interval
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval))]
    pub fn get_latest_kline_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>> {
        let table_name = get_table_name_static(symbol, interval);
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let table_exists: bool = conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            params![table_name],
            |_| Ok(true),
        ).optional()?.is_some();

        if !table_exists {
            return Ok(None);
        }
        
        let query = format!("SELECT MAX(open_time) FROM {}", table_name);
        conn.query_row(&query, [], |row| {
            // MAX() 函数在表为空时返回 NULL，我们需要正确处理这种情况
            let value: Option<i64> = row.get(0)?;
            Ok(value)
        })
        .map_err(|e| AppError::DatabaseError(format!("Failed to get latest kline timestamp from {}: {}", table_name, e)))
    }

    /// 批量获取多个品种的最早K线时间戳（性能优化）
    #[perf_profile(skip_all, err)]
    pub fn batch_get_earliest_kline_timestamps(&self, symbols: &[String], interval: &str) -> Result<Vec<(String, Option<i64>)>> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let mut results = Vec::with_capacity(symbols.len());

        for symbol in symbols {
            let table_name = get_table_name_static(symbol, interval);

            let table_exists: bool = conn.query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                params![table_name],
                |_| Ok(true),
            ).optional()?.is_some();

            if !table_exists {
                results.push((symbol.clone(), None));
                continue;
            }

            let query = format!("SELECT MIN(open_time) FROM {}", table_name);
            let timestamp: Option<i64> = conn.query_row(&query, [], |row| row.get(0))
                .optional()
                .map_err(|e| AppError::DatabaseError(format!("Failed to get earliest kline timestamp from {}: {}", table_name, e)))?;

            results.push((symbol.clone(), timestamp));
        }

        Ok(results)
    }

    /// Get the earliest kline timestamp for a symbol and interval
    #[perf_profile(skip_all, err)]
    pub fn get_earliest_kline_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>> {
        let table_name = get_table_name_static(symbol, interval);
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let table_exists: bool = conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            params![table_name],
            |_| Ok(true),
        ).optional()?.is_some();

        if !table_exists {
            return Ok(None);
        }
        
        let query = format!("SELECT MIN(open_time) FROM {}", table_name);
        conn.query_row(&query, [], |row| row.get(0))
            .optional()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get earliest kline timestamp from {}: {}", table_name, e)))
    }

    /// Get the count of klines for a symbol and interval
    #[perf_profile(skip_all, err)]
    pub fn get_kline_count(&self, symbol: &str, interval: &str) -> Result<i64> {
        let table_name = get_table_name_static(symbol, interval);

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let table_exists: bool = conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            params![table_name],
            |_| Ok(true),
        ).optional()?.is_some();

        if !table_exists {
            return Ok(0);
        }

        let query = format!("SELECT COUNT(*) FROM {}", table_name);
        conn.query_row(&query, [], |row| row.get(0))
            .map_err(|e| AppError::DatabaseError(format!("Failed to get kline count from {}: {}", table_name, e)))
    }
    
    /// No longer limiting kline count, keep all data
    #[perf_profile(skip_all, err)]
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


    /// Get all symbols from the database
    #[perf_profile(skip_all, err)]
    pub fn get_all_symbols(&self) -> Result<Vec<String>> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let mut stmt = conn.prepare("SELECT symbol FROM symbols")?;
        let mut symbols_from_table = Vec::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for symbol in rows {
            symbols_from_table.push(symbol?);
        }

        if !symbols_from_table.is_empty() {
            return Ok(symbols_from_table);
        }

        // Fallback: Infer symbols from table names if `symbols` table is empty
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'k_%'"
        )?;
        let mut symbols_from_names = std::collections::HashSet::new();
        let tables = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for table in tables {
            if let Ok(table_name) = table {
                if let Some(symbol_part) = table_name.split('_').nth(1) {
                    let symbol = format!("{}USDT", symbol_part.to_uppercase());
                    symbols_from_names.insert(symbol);
                }
            }
        }
        Ok(symbols_from_names.into_iter().collect())
    }

    /// Get the latest klines
    #[perf_profile(skip_all, err)]
    pub fn get_latest_klines(&self, symbol: &str, interval: &str, limit: usize) -> Result<Vec<Kline>> {
        self.ensure_symbol_table(symbol, interval)?;
        let table_name = get_table_name_static(symbol, interval);
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;
        
        let sql = format!(
            "SELECT open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, 
             taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
             FROM {} ORDER BY open_time DESC LIMIT ?", table_name);
        let mut stmt = conn.prepare(&sql)?;

        let klines_iter = stmt.query_map([limit], |row| {
            Ok(Kline {
                open_time: row.get(0)?, open: row.get(1)?, high: row.get(2)?, low: row.get(3)?, close: row.get(4)?,
                volume: row.get(5)?, close_time: row.get(6)?, quote_asset_volume: row.get(7)?,
                number_of_trades: row.get(8)?, taker_buy_base_asset_volume: row.get(9)?,
                taker_buy_quote_asset_volume: row.get(10)?, ignore: row.get(11)?,
            })
        })?;

        let mut result: Vec<Kline> = klines_iter.collect::<std::result::Result<_, _>>()?;
        result.sort_by_key(|k| k.open_time);
        Ok(result)
    }

    // ... [其他 get_* 方法保持不变] ...
    // 以下是保留的只读方法，它们是业务逻辑所必需的。

    /// 获取指定时间范围内的K线数据
    #[perf_profile(skip_all, err)]
    pub fn get_klines_in_range(&self, symbol: &str, interval: &str, start_time: i64, end_time: i64) -> Result<Vec<Kline>> {
        self.ensure_symbol_table(symbol, interval)?;
        let table_name = get_table_name_static(symbol, interval);

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} WHERE open_time >= ? AND open_time <= ? ORDER BY open_time",
            table_name
        ))?;

        let klines = stmt.query_map([start_time, end_time], |row| {
            Ok(Kline {
                open_time: row.get(0)?, open: row.get(1)?, high: row.get(2)?,
                low: row.get(3)?, close: row.get(4)?, volume: row.get(5)?,
                close_time: row.get(6)?, quote_asset_volume: row.get(7)?,
                number_of_trades: row.get(8)?, taker_buy_base_asset_volume: row.get(9)?,
                taker_buy_quote_asset_volume: row.get(10)?, ignore: row.get(11)?,
            })
        })?;

        let mut result = Vec::new();
        for kline in klines {
            result.push(kline?);
        }
        Ok(result)
    }

    /// 根据开始时间获取K线
    #[perf_profile(skip_all, err)]
    pub fn get_kline_by_time(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>> {
        self.ensure_symbol_table(symbol, interval)?;
        let table_name = get_table_name_static(symbol, interval);

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} WHERE open_time = ?",
            table_name
        ))?;

        stmt.query_row([open_time], |row| {
            Ok(Kline {
                open_time: row.get(0)?, open: row.get(1)?, high: row.get(2)?,
                low: row.get(3)?, close: row.get(4)?, volume: row.get(5)?,
                close_time: row.get(6)?, quote_asset_volume: row.get(7)?,
                number_of_trades: row.get(8)?, taker_buy_base_asset_volume: row.get(9)?,
                taker_buy_quote_asset_volume: row.get(10)?, ignore: row.get(11)?,
            })
        }).optional()
        .map_err(|e| AppError::DatabaseError(e.to_string()))
    }
}