use crate::klcommon::error::{AppError, Result};
use crate::klcommon::models::Kline;
use log::{debug, info, error};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use std::thread;
use crossbeam_channel::{bounded, Sender, Receiver};

// Global counters for tracking insert and update operations
// Format: (insert_count, update_count, last_log_time)
static DB_OPERATIONS: Lazy<(AtomicUsize, AtomicUsize, std::sync::Mutex<Instant>)> = Lazy::new(|| {
    (AtomicUsize::new(0), AtomicUsize::new(0), std::sync::Mutex::new(Instant::now()))
});

// Log interval in seconds
// Output database operation statistics every 10 seconds
const DB_LOG_INTERVAL: u64 = 10;

// 数据库连接池类型
pub type DbPool = Pool<SqliteConnectionManager>;

/// 写入任务结构体，表示一个待执行的数据库写入操作
#[derive(Debug)]
struct WriteTask {
    symbol: String,
    interval: String,
    klines: Vec<Kline>,
    result_sender: Sender<Result<usize>>,
}



/// 数据库写入队列处理器
#[derive(Debug)]
struct DbWriteQueueProcessor {
    receiver: Receiver<WriteTask>,
    pool: DbPool,
    is_running: Arc<Mutex<bool>>,
}

impl DbWriteQueueProcessor {
    /// 创建新的写入队列处理器
    fn new(receiver: Receiver<WriteTask>, pool: DbPool) -> Self {
        Self {
            receiver,
            pool,
            is_running: Arc::new(Mutex::new(true)),
        }
    }

    /// 启动写入队列处理线程
    fn start(self) -> Arc<Mutex<bool>> {
        let is_running = self.is_running.clone();

        thread::spawn(move || {
            info!("数据库写入队列处理器已启动");

            while *self.is_running.lock().unwrap() {
                // 尝试从队列中获取任务，最多等待100毫秒
                match self.receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(task) => {
                        // 处理写入任务
                        let result = self.process_write_task(task.symbol.as_str(), task.interval.as_str(), &task.klines);

                        // 发送结果
                        if let Err(e) = task.result_sender.send(result) {
                            error!("无法发送写入任务结果: {}", e);
                        }
                    },
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        // 超时，继续循环
                        continue;
                    },
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        // 发送端已关闭，退出循环
                        info!("数据库写入队列已关闭，处理器将退出");
                        break;
                    }
                }
            }

            info!("数据库写入队列处理器已停止");
        });

        is_running
    }

    /// 处理单个写入任务
    pub fn process_write_task(&self, symbol: &str, interval: &str, klines: &[Kline]) -> Result<usize> {
        if klines.is_empty() {
            return Ok(0);
        }

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        // 获取数据库连接
        let mut conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("获取数据库连接失败: {}", e)))?;

        // 确保表存在
        self.ensure_symbol_table(&conn, symbol, interval, &table_name)?;

        // 开始事务
        let tx = conn.transaction()
            .map_err(|e| AppError::DatabaseError(format!("开始事务失败: {}", e)))?;

        let mut count = 0;
        let mut _updated = 0;

        // 处理每个K线
        for kline in klines {
            // 检查是否存在
            let exists: bool = tx.query_row(
                &format!("SELECT 1 FROM {} WHERE open_time = ?", table_name),
                params![kline.open_time],
                |_| Ok(true)
            ).optional().map_err(|e| AppError::DatabaseError(format!("查询K线失败: {}", e)))?.is_some();

            if exists {
                // 更新现有K线
                let result = tx.execute(
                    &format!("UPDATE {} SET
                        open = ?, high = ?, low = ?, close = ?, volume = ?,
                        close_time = ?, quote_asset_volume = ?, number_of_trades = ?,
                        taker_buy_base_asset_volume = ?, taker_buy_quote_asset_volume = ?, ignore = ?
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
                        kline.open_time,
                    ],
                );

                match result {
                    Ok(_) => {
                        _updated += 1;
                        count += 1;
                    },
                    Err(e) => {
                        // 回滚事务
                        let _ = tx.rollback();
                        return Err(AppError::DatabaseError(format!("更新K线失败: {}", e)));
                    }
                }
            } else {
                // 插入新K线
                let result = tx.execute(
                    &format!("INSERT INTO {} (
                        open_time, open, high, low, close, volume,
                        close_time, quote_asset_volume, number_of_trades,
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
                        kline.ignore,
                    ],
                );

                match result {
                    Ok(_) => count += 1,
                    Err(e) => {
                        // 回滚事务
                        let _ = tx.rollback();
                        return Err(AppError::DatabaseError(format!("插入K线失败: {}", e)));
                    }
                }
            }
        }

        // 提交事务
        tx.commit()
            .map_err(|e| AppError::DatabaseError(format!("提交事务失败: {}", e)))?;

        //debug!("成功保存 {} 条K线数据到表 {} (更新: {})", count, table_name, updated);
        Ok(count)
    }

    /// 确保表存在
    fn ensure_symbol_table(&self, conn: &rusqlite::Connection, _symbol: &str, _interval: &str, table_name: &str) -> Result<()> {
        // 创建表
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
            .map_err(|e| AppError::DatabaseError(format!("创建表 {} 失败: {}", table_name, e)))?;

        Ok(())
    }
}

/// Database handler for kline data
#[derive(Debug)]
pub struct Database {
    pool: DbPool,
    write_queue_sender: Sender<WriteTask>,
    queue_processor_running: Arc<Mutex<bool>>,
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
                PRAGMA cache_size = -204800;         -- Set cache to 200MB (200 * 1024 KiB)
                PRAGMA mmap_size = 268435456;        -- 256MB memory mapping
                PRAGMA temp_store = MEMORY;          -- Store temp tables in memory
                PRAGMA wal_autocheckpoint = 1000;    -- Checkpoint every 1000 pages
                PRAGMA busy_timeout = 5000;          -- 5 second busy timeout
            ")
        });

        // Create connection pool with multiple concurrent connections
        let pool = Pool::builder()
            .max_size(10) // 减少到10个并发连接，减少锁竞争
            .build(manager)
            .map_err(|e| AppError::DatabaseError(format!("Failed to create connection pool: {}", e)))?;

        // 创建写入队列通道，设置合理的缓冲区大小
        let (sender, receiver) = bounded(1000); // 队列最多容纳1000个写入任务

        // 创建写入队列处理器
        let processor = DbWriteQueueProcessor::new(receiver, pool.clone());

        // 启动写入队列处理线程
        let queue_processor_running = processor.start();

        // Initialize database instance
        let db = Self {
            pool,
            write_queue_sender: sender,
            queue_processor_running,
        };

        // Initialize database tables
        db.init_db()?;

        info!("SQLite database with WAL mode and write queue initialized successfully");
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
    pub fn ensure_symbol_table(&self, symbol: &str, interval: &str) -> Result<()> {
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

    /// Save klines to the database using the write queue
    pub fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline]) -> Result<usize> {
        if klines.is_empty() {
            return Ok(0);
        }

        // 确保表存在（这一步仍然需要，因为可能会有其他地方需要查询这个表）
        self.ensure_symbol_table(symbol, interval)?;

        // 如果DbWriteQueue不可用，则使用原来的写入队列
        // 创建一个K线数据的副本，以便在队列中安全地传递
        let klines_copy: Vec<Kline> = klines.to_vec();

        // 创建一个通道，用于接收写入操作的结果
        let (result_sender, result_receiver) = bounded(1);

        // 创建写入任务
        let task = WriteTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            klines: klines_copy,
            result_sender,
        };

        // 将任务发送到写入队列
        match self.write_queue_sender.send(task) {
            Ok(_) => {
                //debug!("已将 {} 条K线数据的写入任务添加到队列: {}/{}", klines.len(), symbol, interval);
            },
            Err(e) => {
                return Err(AppError::DatabaseError(
                    format!("无法将写入任务添加到队列: {}", e)
                ));
            }
        }

        // 等待写入操作完成并获取结果
        match result_receiver.recv() {
            Ok(result) => result,
            Err(e) => {
                Err(AppError::DatabaseError(
                    format!("等待写入操作结果时出错: {}", e)
                ))
            }
        }
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

        // 首先检查表是否有数据
        let has_data: bool = conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)", table_name),
            [],
            |row| row.get(0),
        ).unwrap_or(false);

        if !has_data {
            // 表存在但没有数据
            return Ok(None);
        }

        // 表存在且有数据，获取最大时间戳
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

    /// 关闭写入队列处理器
    pub fn shutdown(&self) {
        info!("正在关闭数据库写入队列...");

        // 设置运行标志为false，通知处理器停止
        if let Ok(mut running) = self.queue_processor_running.lock() {
            *running = false;
        }

        // 等待所有剩余的任务处理完成
        // 这里我们不等待，因为处理器会在接收到关闭信号后自行退出

        info!("数据库写入队列已关闭");
    }
}

// 实现Drop特性，确保在Database被丢弃时关闭写入队列处理器
impl Drop for Database {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// 继续实现Database的方法
impl Database {
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

        // 检查是否需要输出日志
        let mut last_log_time = DB_OPERATIONS.2.lock().unwrap();
        let now = Instant::now();
        if now.duration_since(*last_log_time).as_secs() >= DB_LOG_INTERVAL {
            let insert_count = DB_OPERATIONS.0.load(Ordering::Relaxed);
            let update_count = DB_OPERATIONS.1.load(Ordering::Relaxed);
            debug!("数据库操作统计: 插入={}, 更新={}", insert_count, update_count);
            *last_log_time = now;
        }

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

        // 检查是否需要输出日志
        let mut last_log_time = DB_OPERATIONS.2.lock().unwrap();
        let now = Instant::now();
        if now.duration_since(*last_log_time).as_secs() >= DB_LOG_INTERVAL {
            let insert_count = DB_OPERATIONS.0.load(Ordering::Relaxed);
            let update_count = DB_OPERATIONS.1.load(Ordering::Relaxed);
            debug!("数据库操作统计: 插入={}, 更新={}", insert_count, update_count);
            *last_log_time = now;
        }

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

            // 检查是否需要输出日志
            let mut last_log_time = DB_OPERATIONS.2.lock().unwrap();
            let now = Instant::now();
            if now.duration_since(*last_log_time).as_secs() >= DB_LOG_INTERVAL {
                let insert_count = DB_OPERATIONS.0.load(Ordering::Relaxed);
                let update_count = DB_OPERATIONS.1.load(Ordering::Relaxed);
                debug!("数据库操作统计: 插入={}, 更新={}", insert_count, update_count);
                *last_log_time = now;
            }
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

            // 检查是否需要输出日志
            let mut last_log_time = DB_OPERATIONS.2.lock().unwrap();
            let now = Instant::now();
            if now.duration_since(*last_log_time).as_secs() >= DB_LOG_INTERVAL {
                let insert_count = DB_OPERATIONS.0.load(Ordering::Relaxed);
                let update_count = DB_OPERATIONS.1.load(Ordering::Relaxed);
                debug!("数据库操作统计: 插入={}, 更新={}", insert_count, update_count);
                *last_log_time = now;
            }
        }

        Ok(())
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

    /// 获取指定时间范围内的K线数据
    pub fn get_klines_in_range(&self, symbol: &str, interval: &str, start_time: i64, end_time: i64) -> Result<Vec<Kline>> {
        // 确保表存在
        self.ensure_symbol_table(symbol, interval)?;

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // 查询指定时间范围内的K线
        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} WHERE open_time >= ? AND open_time <= ? ORDER BY open_time",
            table_name
        ))?;

        let klines = stmt.query_map([start_time, end_time], |row| {
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

        Ok(result)
    }

    /// 根据开始时间获取K线
    pub fn get_kline_by_time(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>> {
        // 确保表存在
        self.ensure_symbol_table(symbol, interval)?;

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // 查询指定时间的K线
        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} WHERE open_time = ?",
            table_name
        ))?;

        let kline = stmt.query_row([open_time], |row| {
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
        }).optional()?;

        Ok(kline)
    }

    /// 获取指定时间之前的最后一根K线
    pub fn get_last_kline_before(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>> {
        // 确保表存在
        self.ensure_symbol_table(symbol, interval)?;

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // 查询指定时间之前的最后一根K线
        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} WHERE open_time < ? ORDER BY open_time DESC LIMIT 1",
            table_name
        ))?;

        let kline = stmt.query_row([open_time], |row| {
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
        }).optional()?;

        Ok(kline)
    }
}
