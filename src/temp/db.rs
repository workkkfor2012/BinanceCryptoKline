use crate::klcommon::error::{AppError, Result};
use crate::klcommon::models::Kline;
use crate::klcommon::context::instrument_if_enabled;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use tokio::task;
use tracing::{Span, debug, error};
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

/// 写入任务结构体，表示一个待执行的数据库写入操作
#[derive(Debug)]
struct WriteTask {
    transaction_id: u64,
    symbol: String,
    interval: String,
    klines: Vec<Kline>,
    result_sender: oneshot::Sender<Result<usize>>,
    // ✨ [修改] 直接使用标准的 tracing::Span
    span: Span,
}



/// 数据库写入队列处理器
#[derive(Debug)]
struct DbWriteQueueProcessor {
    // ✨ 2. [修改] 使用 mpsc::Receiver
    receiver: mpsc::Receiver<WriteTask>,
    pool: DbPool,
    is_running: Arc<Mutex<bool>>,
}

impl DbWriteQueueProcessor {
    /// 创建新的写入队列处理器
    fn new(receiver: mpsc::Receiver<WriteTask>, pool: DbPool) -> Self {
        Self {
            receiver,
            pool,
            is_running: Arc::new(Mutex::new(true)),
        }
    }

    /// 启动写入队列处理线程
    fn start(mut self) -> Arc<Mutex<bool>> {
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            while let Some(task) = self.receiver.recv().await {
                if !*self.is_running.lock().unwrap() {
                    break;
                }

                let pool_clone = self.pool.clone();

                let processing_future = async move {
                    let db_result = Self::process_write_task_wrapper(pool_clone, task.transaction_id, task.symbol, task.interval, task.klines).await;
                    let _ = task.result_sender.send(db_result);
                };

                // ✨ [修改] 使用零成本抽象的条件化追踪
                instrument_if_enabled(processing_future, task.span).await;
            }
        });

        is_running
    }

    /// 包裹函数，用于创建顶层Span并调用阻塞任务
    async fn process_write_task_wrapper(pool: DbPool, _transaction_id: u64, symbol: String, interval: String, klines: Vec<Kline>) -> Result<usize> {
        let parent_span = tracing::Span::current();

        let result = task::spawn_blocking(move || {
            parent_span.in_scope(|| Self::process_write_task_static(&pool, &symbol, &interval, &klines))
        }).await;

        match result {
            Ok(Ok(count)) => {
                Ok(count)
            },
            Ok(Err(e)) => {
                Err(e)
            },
            Err(join_error) => {
                let panic_error = AppError::DatabaseError(format!("K线数据持久化任务 panic: {:?}", join_error));
                Err(panic_error)
            }
        }
    }

    /// 静态版本的写入任务处理函数，用于在 spawn_blocking 中执行
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval, kline_count = klines.len()))]
    fn process_write_task_static(pool: &DbPool, symbol: &str, interval: &str, klines: &[Kline]) -> Result<usize> {
        let _start_time = std::time::Instant::now();

        if klines.is_empty() {
            return Ok(0);
        }

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        // 获取数据库连接
        let mut conn = pool.get()
            .map_err(|e| AppError::DatabaseError(format!("获取数据库连接失败: {}", e)))?;

        // 使用缓存避免重复的表创建检查
        if !CREATED_TABLES.contains(&table_name) {
            Self::ensure_symbol_table_static(&conn, symbol, interval, &table_name)?;
            CREATED_TABLES.insert(table_name.clone());
        }

        // 开始事务
        let tx = conn.transaction()
            .map_err(|e| AppError::DatabaseError(format!("开始事务失败: {}", e)))?;

        let mut count = 0;

        let upsert_sql = format!(
            "INSERT INTO {} (open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(open_time) DO UPDATE SET
                 open = excluded.open,
                 high = excluded.high,
                 low = excluded.low,
                 close = excluded.close,
                 volume = excluded.volume,
                 close_time = excluded.close_time,
                 quote_asset_volume = excluded.quote_asset_volume,
                 number_of_trades = excluded.number_of_trades,
                 taker_buy_base_asset_volume = excluded.taker_buy_base_asset_volume,
                 taker_buy_quote_asset_volume = excluded.taker_buy_quote_asset_volume,
                 ignore = excluded.ignore",
            table_name
        );

        // 准备UPSERT语句
        let mut stmt = tx.prepare(&upsert_sql)
            .map_err(|e| AppError::DatabaseError(format!("准备UPSERT语句失败: {}", e)))?;

        // 批量执行UPSERT操作
        for kline in klines.iter() {
            match stmt.execute(params![
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
            ]) {
                Ok(changed_rows) => {
                    count += changed_rows;
                },
                Err(e) => {
                    // 添加详细错误日志
                    error!(
                        table_name = %table_name,
                        error_chain = format!("{:#}", e),
                        message = "批量UPSERT操作中，单条K线执行失败"
                    );
                    drop(stmt);
                    let _ = tx.rollback();
                    let upsert_error = AppError::DatabaseError(format!("UPSERT K线失败: {}", e));
                    return Err(upsert_error);
                }
            }
        }

        // 释放stmt，然后提交事务
        drop(stmt);

        // 提交事务
        match tx.commit() {
            Ok(_) => {
                DB_OPERATIONS.0.fetch_add(count, Ordering::SeqCst);
            }
            Err(e) => {
                error!(
                    // log_type 会从父Span继承
                    table_name = %table_name,
                    kline_count = klines.len(),
                    error_chain = format!("{:#}", e),
                    message = "数据库事务提交失败"
                );
                let commit_error = AppError::DatabaseError(format!("提交事务失败: {}", e));
                return Err(commit_error);
            }
        }

        // 记录统计信息（每10秒输出一次）
        let (insert_count, update_count, last_log_time) = &*DB_OPERATIONS;
        let mut last_time = last_log_time.lock().unwrap();
        let now = Instant::now();

        if now.duration_since(*last_time).as_secs() >= DB_LOG_INTERVAL {
            let _total_inserts = insert_count.load(Ordering::SeqCst);
            let _total_updates = update_count.load(Ordering::SeqCst);
            *last_time = now;
        }

        Ok(count)
    }

    /// 处理单个写入任务
    #[allow(dead_code)]
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
                        count += 1;
                    },
                    Err(e) => {
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
                        let _ = tx.rollback();
                        return Err(AppError::DatabaseError(format!("插入K线失败: {}", e)));
                    }
                }
            }
        }

        // 提交事务
        tx.commit()
            .map_err(|e| AppError::DatabaseError(format!("提交事务失败: {}", e)))?;

        Ok(count)
    }

    /// 静态版本的确保表存在函数，用于在 spawn_blocking 中执行
    fn ensure_symbol_table_static(conn: &rusqlite::Connection, _symbol: &str, _interval: &str, table_name: &str) -> Result<()> {
        // ✨ [新增] 低频日志：记录表的首次创建动作
        // debug!(
        //     log_type = "low_freq",
        //     table_name = %table_name,
        //     message = "确保K线表存在 (CREATE TABLE IF NOT EXISTS)"
        // );
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
                ignore TEXT NOT NULL
            )",
            table_name
        );

        conn.execute(&create_table_sql, [])
            .map_err(|e| AppError::DatabaseError(format!("创建表 {} 失败: {}", table_name, e)))?;
        Ok(())
    }

    /// 确保表存在
    // #[instrument] 移除：高频调用函数，每次数据库操作都会调用，产生大量噪音
    #[allow(dead_code)]
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
                ignore TEXT NOT NULL
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
    // ✨ 5. [修改] 使用 mpsc::Sender
    write_queue_sender: mpsc::Sender<WriteTask>,
    #[allow(dead_code)]
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

        // Create database connection manager with WAL mode and performance optimizations
        let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
            conn.execute_batch("
                PRAGMA journal_mode = WAL;          -- 保留WAL模式以获得更好的安全性
                PRAGMA synchronous = NORMAL;        -- 平衡性能和安全性
                PRAGMA cache_size = -102400;        -- 设置缓存为100MB (负数表示KB)
                PRAGMA mmap_size = 104857600;       -- 100MB内存映射
                PRAGMA temp_store = MEMORY;         -- 临时表存储在内存中
                PRAGMA wal_autocheckpoint = 1000;   -- 每1000页检查点
                PRAGMA busy_timeout = 5000;        -- 10秒忙等待超时
                PRAGMA page_size = 4096;            -- 更大的页面大小
            ")
        });

        // Create connection pool with multiple concurrent connections
        let pool = Pool::builder()
            .max_size(10) // 减少到10个并发连接，减少锁竞争
            .build(manager)
            .map_err(|e| AppError::DatabaseError(format!("Failed to create connection pool: {}", e)))?;

        // 创建 Tokio 的 mpsc 写入队列通道
        let (sender, receiver) = mpsc::channel(5000); // 队列最多容纳5000个写入任务

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
                ignore TEXT NOT NULL
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

    /// 批量 Upsert K线数据到数据库。
    /// 这个方法是同步的，设计为在 `tokio::task::spawn_blocking` 中调用。
    /// 参数：Vec<(symbol, interval, kline)> - 已经转换好的Kline数据
    #[perf_profile(skip_all, err)]
    pub fn upsert_klines_batch(&self, klines_to_save: Vec<(String, String, Kline)>) -> Result<()> {
        if klines_to_save.is_empty() {
            return Ok(());
        }

        // 1. 按表名（symbol, interval）对K线数据进行分组
        let mut grouped_klines: HashMap<(String, String), Vec<Kline>> = HashMap::new();
        for (symbol, interval, kline) in klines_to_save {
            let key = (symbol, interval);
            grouped_klines.entry(key).or_default().push(kline);
        }

        let mut conn = self.get_connection()?;

        // 2. 开启一个事务，以保证整个批量操作的原子性
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        // 3. 循环处理每个分组
        for ((symbol, interval), klines) in grouped_klines {
            let table_name = self.get_table_name(&symbol, &interval);

            // 确保表存在 (在事务内部)
            self.ensure_symbol_table_in_tx(&tx, &table_name)?;

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
        Ok(())
    }

    /// Save klines to the database using the write queue (Async)
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval, kline_count = klines.len()))]
    pub async fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline], transaction_id: u64) -> Result<usize> {
        if klines.is_empty() {
            return Ok(0);
        }

        // 确保表存在（这一步仍然需要，因为可能会有其他地方需要查询这个表）
        self.ensure_symbol_table(symbol, interval)?;

        // 创建一个K线数据的副本，以便在队列中安全地传递
        let klines_copy: Vec<Kline> = klines.to_vec();

        // 创建一个 oneshot 通道，用于异步接收写入操作的结果
        let (result_sender, result_receiver) = oneshot::channel();

        // 创建写入任务，并捕获当前的 tracing 上下文
        let task = WriteTask {
            transaction_id,
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            klines: klines_copy,
            result_sender,
            // ✨ [修改] 直接捕获当前的 Span
            span: Span::current(),
        };

        // 使用异步的 send().await
        if let Err(e) = self.write_queue_sender.send(task).await {
            let queue_error = AppError::DatabaseError(format!("无法将写入任务添加到队列: {}", e));
            return Err(queue_error);
        }

        // 异步等待写入操作完成并获取结果
        match result_receiver.await {
            Ok(result) => result,
            Err(e) => {
                let wait_error = AppError::DatabaseError(format!("等待写入操作结果时出错: {}", e));
                Err(wait_error)
            }
        }
    }



    /// Get the latest kline timestamp for a symbol and interval
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval))]
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

    /// 批量获取多个品种的最早K线时间戳（性能优化）
    #[perf_profile(skip_all, err)]
    pub fn batch_get_earliest_kline_timestamps(&self, symbols: &[String], interval: &str) -> Result<Vec<(String, Option<i64>)>> {
        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        let mut results = Vec::with_capacity(symbols.len());

        for symbol in symbols {
            // Create table name: k_symbol_interval (e.g., k_btc_1m)
            // Remove "USDT" suffix from symbol name
            let symbol_lower = symbol.to_lowercase().replace("usdt", "");
            let interval_lower = interval.to_lowercase();
            let table_name = format!("k_{symbol_lower}_{interval_lower}");

            // Check if table exists
            let table_exists: bool = conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
                params![table_name],
                |row| row.get::<_, i64>(0).map(|count| count > 0),
            ).unwrap_or(false);

            if !table_exists {
                results.push((symbol.clone(), None));
                continue;
            }

            // Check if table has data
            let has_data: bool = conn.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)", table_name),
                [],
                |row| row.get(0),
            ).unwrap_or(false);

            if !has_data {
                results.push((symbol.clone(), None));
                continue;
            }

            // Get minimum timestamp
            let query = format!("SELECT MIN(open_time) FROM {}", table_name);
            let timestamp: Option<i64> = conn.query_row(
                &query,
                [],
                |row| row.get(0),
            ).optional()
                .map_err(|e| AppError::DatabaseError(format!("Failed to get earliest kline timestamp from {}: {}", table_name, e)))?;

            results.push((symbol.clone(), timestamp));
        }

        Ok(results)
    }

    /// Get the earliest kline timestamp for a symbol and interval
    #[perf_profile(skip_all, err)]
    pub fn get_earliest_kline_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>> {
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

        // 表存在且有数据，获取最小时间戳
        let query = format!("SELECT MIN(open_time) FROM {}", table_name);
        let result: Option<i64> = conn.query_row(
            &query,
            [],
            |row| row.get(0),
        ).optional()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get earliest kline timestamp from {}: {}", table_name, e)))?;

        Ok(result)
    }

    /// Get the count of klines for a symbol and interval
    #[perf_profile(skip_all, err)]
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

    // 注意：我们移除了异步方法，改为在KlineProcessor中使用tokio::task::spawn_blocking

    /// No longer limiting kline count, keep all data
    #[perf_profile(skip_all, err)]
    pub fn trim_klines(&self, symbol: &str, interval: &str, _max_count: i64) -> Result<usize> {
        // No longer limiting kline count, just return 0
        debug!("K-line trimming disabled, keeping all data for {}/{}", symbol, interval);
        Ok(0)
    }

    /// 辅助函数：生成表名
    fn get_table_name(&self, symbol: &str, interval: &str) -> String {
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        format!("k_{symbol_lower}_{interval_lower}")
    }

    /// 辅助函数：在事务内确保表存在，并更新字段
    fn ensure_symbol_table_in_tx(&self, tx: &rusqlite::Transaction, table_name: &str) -> Result<()> {
        // 使用静态缓存避免重复执行 CREATE TABLE
        if CREATED_TABLES.contains(table_name) {
            return Ok(());
        }

        // 更新后的 CREATE TABLE 语句，包含所有字段
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
                ignore TEXT NOT NULL
            )",
            table_name
        );
        tx.execute(&create_table_sql, [])?;
        // 将已创建的表名加入缓存
        CREATED_TABLES.insert(table_name.to_string());
        Ok(())
    }

    /// Get database connection
    // #[instrument] 移除：数据库连接获取是底层资源管理，对业务流程分析是噪音
    pub fn get_connection(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))
    }

    // The shutdown logic is now handled automatically by dropping the Sender.
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
                quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
                taker_buy_quote_asset_volume, ignore
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
    #[perf_profile(skip_all, err)]
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
                    quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume, ignore
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

    /// Save a single kline, now uses full-featured UPSERT and transaction.
    pub fn save_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<()> {
        let table_name = self.get_table_name(symbol, interval);
        let mut conn = self.get_connection()?;

        // 使用事务包装，保证原子性
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        // 确保表存在
        self.ensure_symbol_table_in_tx(&tx, &table_name)?;

        // 使用与批量方法相同的、包含所有字段的UPSERT语句
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

        tx.execute(&sql, params![
            kline.open_time, kline.open, kline.high, kline.low, kline.close, kline.volume,
            kline.close_time, kline.quote_asset_volume, kline.number_of_trades,
            kline.taker_buy_base_asset_volume, kline.taker_buy_quote_asset_volume, kline.ignore
        ])?;

        tx.commit()?;
        Ok(())
    }



    /// Get all symbols from the database
    #[perf_profile(skip_all, err)]
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
    #[perf_profile(skip_all, err)]
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
    #[perf_profile(skip_all, err)]
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
    #[perf_profile(skip_all, err)]
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
    #[perf_profile(skip_all, err)]
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

    /// 获取倒数第二根K线
    #[perf_profile(skip_all, err)]
    pub fn get_second_last_kline(&self, symbol: &str, interval: &str) -> Result<Option<Kline>> {
        // 确保表存在
        self.ensure_symbol_table(symbol, interval)?;

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        let conn = self.pool.get()
            .map_err(|e| AppError::DatabaseError(format!("Failed to get connection: {}", e)))?;

        // 检查表中的记录数量
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM {}", table_name),
            [],
            |row| row.get(0),
        ).map_err(|e| AppError::DatabaseError(format!("Failed to get count from {}: {}", table_name, e)))?;

        // 如果记录数少于2条，则无法获取倒数第二条
        if count < 2 {
            return Ok(None);
        }

        // 查询倒数第二根K线
        let mut stmt = conn.prepare(&format!(
            "SELECT open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
             taker_buy_quote_asset_volume, ignore
             FROM {} ORDER BY open_time DESC LIMIT 1 OFFSET 1",
            table_name
        ))?;

        let kline = stmt.query_row([], |row| {
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
