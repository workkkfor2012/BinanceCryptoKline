use crate::klcommon::error::{AppError, Result};
use crate::klcommon::models::Kline;
// ✨ 导入我们的新抽象
use crate::klcommon::context::{AppTraceContext, TraceContext};
use tracing::{debug, info, error, instrument};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use tokio::task;

// ✨ 1. [修改] 导入 Tokio 的 mpsc 和 oneshot
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
    transaction_id: u64,  // ✨ [新增] 事务ID，用于业务追踪
    symbol: String,
    interval: String,
    klines: Vec<Kline>,
    result_sender: oneshot::Sender<Result<usize>>,
    // ✨ 字段类型变为抽象的 AppTraceContext
    context: AppTraceContext,
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
    #[instrument(ret)]
    fn new(receiver: mpsc::Receiver<WriteTask>, pool: DbPool) -> Self {
        tracing::debug!(decision = "queue_processor_init", "数据库写入队列处理器初始化");
        Self {
            receiver,
            pool,
            is_running: Arc::new(Mutex::new(true)),
        }
    }

    /// 启动写入队列处理线程
    #[instrument(ret)]
    fn start(mut self) -> Arc<Mutex<bool>> { // ✨ 3. [修改] self 需要是 mut
        let is_running = self.is_running.clone();
        tracing::debug!(decision = "queue_processor_start", "启动数据库写入队列处理器");

        // ✨ 关键修复：使用 tokio::spawn 而不是 std::thread::spawn
        // 这样可以保持 tracing 上下文，避免 ensure_symbol_table 变成孤儿 Span
        tokio::spawn(async move {
            info!(log_type = "module", "数据库写入队列处理器已启动 (串行模式)"); // 提示已是串行

            // ✨ 4. [修改] 使用异步的 recv().await
            while let Some(task) = self.receiver.recv().await {
                if !*self.is_running.lock().unwrap() {
                    break;
                }

                // ✨ 关键修复：直接在父 Span 的上下文中 .await 任务 ✨
                // 1. 进入从 `WriteTask` 带来的 `parent_span`。
                // 2. 在这个上下文中，调用并 .await 数据库写入函数。
                // 3. 这样，`db_write_task` 就会被正确地记录为 `parent_span` 的子节点。
                let pool_clone = self.pool.clone();

                // ✨ 现在调用的是 Trait 方法，业务逻辑完全解耦！
                let processing_future = async move {
                    let db_result = Self::process_write_task_wrapper(pool_clone, task.transaction_id, task.symbol, task.interval, task.klines).await;
                    let _ = task.result_sender.send(db_result);
                };

                TraceContext::instrument(&task.context, processing_future).await;
            }

            info!(log_type = "module", "数据库写入队列处理器已停止");
        });

        is_running
    }

    /// ✨ [新增] 包裹函数，用于创建顶层Span并调用阻塞任务
    #[instrument(
        name = "db_write_task",
        skip_all,
        fields(symbol = %symbol, interval = %interval, kline_count = klines.len(), transaction_id = transaction_id)
    )]
    async fn process_write_task_wrapper(pool: DbPool, transaction_id: u64, symbol: String, interval: String, klines: Vec<Kline>) -> Result<usize> {
        // 埋点：DB写入开始
        tracing::info!(
            log_type = "transaction",
            transaction_id,
            event_name = "db_write_start",
            kline_count_to_write = klines.len(),
        );
        tracing::debug!(decision = "db_write_start", symbol = %symbol, interval = %interval, kline_count = klines.len(), "开始处理数据库写入任务");

        // 为日志记录克隆变量
        let symbol_for_log = symbol.clone();
        let interval_for_log = interval.clone();

        // ✨ [根本性修复] 在进入阻塞任务前，捕获当前的 span 上下文
        let parent_span = tracing::Span::current();

        let result = task::spawn_blocking(move || {
            // ✨ 在新的阻塞线程中，使用 `in_scope` 方法恢复父 span 的上下文，
            // ✨ 这样 process_write_task_static 就能正确地链接到其父节点
            parent_span.in_scope(|| Self::process_write_task_static(&pool, &symbol, &interval, &klines))
        }).await;

        match result {
            Ok(Ok(count)) => {
                // 埋点：DB写入成功
                tracing::info!(
                    log_type = "transaction",
                    transaction_id,
                    event_name = "db_write_success",
                    saved_kline_count = count,
                );
                tracing::debug!(decision = "db_write_complete", symbol = %symbol_for_log, interval = %interval_for_log, result = ?count, "数据库写入任务完成");
                Ok(count)
            },
            Ok(Err(e)) => {
                // 埋点：DB写入失败 (事务内错误)
                tracing::info!(
                    log_type = "transaction",
                    transaction_id,
                    event_name = "db_write_failure",
                    reason = "database_operation_error",
                    error.summary = e.get_error_type_summary(),
                    error.details = %e,
                );
                tracing::debug!(decision = "db_write_complete", symbol = %symbol_for_log, interval = %interval_for_log, result = ?e, "数据库写入任务完成");
                Err(e)
            },
            Err(join_error) => {
                let panic_error = AppError::DatabaseError(format!("数据库写入任务 panic: {:?}", join_error));
                // 埋点：DB写入失败 (任务Panic)
                tracing::info!(
                    log_type = "transaction",
                    transaction_id,
                    event_name = "db_write_failure",
                    reason = "task_panic",
                    error.summary = panic_error.get_error_type_summary(),
                    error.details = %panic_error,
                );
                tracing::error!(
                    message = "数据库写入任务panic",
                    symbol = %symbol_for_log,
                    interval = %interval_for_log,
                    error.summary = panic_error.get_error_type_summary(),
                    error.details = %panic_error
                );
                Err(panic_error)
            }
        }
    }

    /// 静态版本的写入任务处理函数，用于在 spawn_blocking 中执行
    #[instrument(skip_all, ret, err)]
    fn process_write_task_static(pool: &DbPool, symbol: &str, interval: &str, klines: &[Kline]) -> Result<usize> {
        let start_time = std::time::Instant::now();

        if klines.is_empty() {
            tracing::debug!(decision = "empty_klines", symbol = %symbol, interval = %interval, "K线数据为空，跳过处理");
            return Ok(0);
        }

        tracing::debug!(decision = "db_transaction_start", symbol = %symbol, interval = %interval, kline_count = klines.len(), "开始数据库事务处理");

        // 创建表名
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        let table_name = format!("k_{symbol_lower}_{interval_lower}");

        // 获取数据库连接
        let mut conn = pool.get()
            .map_err(|e| {
                let conn_error = AppError::DatabaseError(format!("获取数据库连接失败: {}", e));
                tracing::error!(
                    message = "获取数据库连接失败",
                    symbol = %symbol,
                    interval = %interval,
                    error.summary = conn_error.get_error_type_summary(),
                    error.details = %conn_error
                );
                conn_error
            })?;

        // ✨ 优化：使用缓存避免重复的表创建检查
        if !CREATED_TABLES.contains(&table_name) {
            Self::ensure_symbol_table_static(&conn, symbol, interval, &table_name)?;
            CREATED_TABLES.insert(table_name.clone());
            tracing::debug!(
                decision = "table_created_and_cached",
                table_name = %table_name,
                "表创建完成并加入缓存"
            );
        }

        // 开始事务
        let tx = conn.transaction()
            .map_err(|e| {
                let tx_error = AppError::DatabaseError(format!("开始事务失败: {}", e));
                tracing::error!(
                    message = "开始事务失败",
                    symbol = %symbol,
                    interval = %interval,
                    table_name = %table_name,
                    error.summary = tx_error.get_error_type_summary(),
                    error.details = %tx_error
                );
                tx_error
            })?;

        let mut count = 0;

        // ✨ [修复循环聚合] ✨
        // 1. 字段名从total_klines改为task_count以匹配distiller的期望
        // 2. 增加iterator_type字段，让日志更易读
        // 3. 添加concurrency字段标明这是串行处理
        // ✨ 性能优化：使用 UPSERT 语句替代 SELECT + INSERT/UPDATE 双重操作
        let upsert_sql = format!(
            "INSERT INTO {} (open_time, open, high, low, close, volume, close_time, quote_asset_volume)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(open_time) DO UPDATE SET
                 open = excluded.open,
                 high = excluded.high,
                 low = excluded.low,
                 close = excluded.close,
                 volume = excluded.volume,
                 close_time = excluded.close_time,
                 quote_asset_volume = excluded.quote_asset_volume",
            table_name
        );

        let kline_processing_loop_span = tracing::info_span!(
            "kline_upsert_loop",
            iterator_type = "kline",
            task_count = klines.len(),
            concurrency = 1,
            operation = "upsert"
        );

        let _enter = kline_processing_loop_span.enter();

        // 准备UPSERT语句
        let mut stmt = tx.prepare(&upsert_sql)
            .map_err(|e| {
                let prepare_error = AppError::DatabaseError(format!("准备UPSERT语句失败: {}", e));
                tracing::error!(
                    message = "准备UPSERT语句失败",
                    symbol = %symbol,
                    interval = %interval,
                    table_name = %table_name,
                    sql = %upsert_sql,
                    error.summary = prepare_error.get_error_type_summary(),
                    error.details = %prepare_error
                );
                prepare_error
            })?;

        // 批量执行UPSERT操作
        for (index, kline) in klines.iter().enumerate() {
            // ✨ 为每一次UPSERT操作创建一个子Span
            let iteration_span = tracing::info_span!(
                "db_kline_upsert",
                kline_index = index,
                open_time = kline.open_time
            );
            let _iteration_enter = iteration_span.enter();

            match stmt.execute(params![
                kline.open_time,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
                kline.close_time,
                kline.quote_asset_volume,
            ]) {
                Ok(changed_rows) => {
                    count += changed_rows; // 每次UPSERT操作影响一行
                    tracing::debug!(
                        decision = "kline_upsert_success",
                        changed_rows = changed_rows,
                        "K线UPSERT操作成功"
                    );
                },
                Err(e) => {
                    // 先释放stmt，然后回滚事务
                    drop(stmt);
                    let _ = tx.rollback();
                    let upsert_error = AppError::DatabaseError(format!("UPSERT K线失败: {}", e));
                    error!(log_type = "module", "❌ 数据库UPSERT失败，需要检查数据库状态: 表={}, K线索引={}, 错误={}", table_name, index, e);
                    tracing::error!(
                        message = "UPSERT K线失败",
                        symbol = %symbol,
                        interval = %interval,
                        kline_index = index,
                        table_name = %table_name,
                        open_time = kline.open_time,
                        error.summary = upsert_error.get_error_type_summary(),
                        error.details = %upsert_error
                    );
                    return Err(upsert_error);
                }
            }
        }

        // 释放stmt，然后提交事务
        drop(stmt);

        // 提交事务
        tx.commit()
            .map_err(|e| {
                let commit_error = AppError::DatabaseError(format!("提交事务失败: {}", e));
                tracing::error!(
                    message = "提交事务失败",
                    symbol = %symbol,
                    interval = %interval,
                    table_name = %table_name,
                    error.summary = commit_error.get_error_type_summary(),
                    error.details = %commit_error
                );
                commit_error
            })?;

        let duration_ms = start_time.elapsed().as_millis();

        // ✨ [性能断言] 检查数据库写入性能 - 考虑批处理大小的影响
        if !klines.is_empty() {
            let avg_time_per_kline_ms = duration_ms as f64 / klines.len() as f64;
            let batch_size = klines.len();

            // 根据批处理大小动态调整性能阈值
            // 小批量（<50）允许更高的平均耗时，因为固定开销被分摊到少量记录上
            let max_avg_write_time_ms = if batch_size < 50 {
                100.0 // 小批量允许100ms平均耗时
            } else if batch_size < 200 {
                20.0  // 中等批量允许20ms平均耗时
            } else {
                10.0  // 大批量要求10ms平均耗时
            };

            crate::soft_assert!(
                avg_time_per_kline_ms <= max_avg_write_time_ms,
                message = "数据库单条K线平均写入耗时过长。",
                expected_max_avg_ms = max_avg_write_time_ms,
                actual_avg_ms = avg_time_per_kline_ms,
                batch_size = batch_size,
                symbol = symbol.to_string(),
                condition = "avg_time_per_kline_ms <= MAX_AVG_WRITE_TIME_MS",
            );
        }

        // ✨ 更新全局统计信息 - UPSERT操作统一计为插入操作
        DB_OPERATIONS.0.fetch_add(count, Ordering::SeqCst);

        // 记录统计信息（每10秒输出一次）
        let (insert_count, update_count, last_log_time) = &*DB_OPERATIONS;
        let mut last_time = last_log_time.lock().unwrap();
        let now = Instant::now();

        if now.duration_since(*last_time).as_secs() >= DB_LOG_INTERVAL {
            let total_inserts = insert_count.load(Ordering::SeqCst);
            let total_updates = update_count.load(Ordering::SeqCst);
            info!(log_type = "module",
                "📊 数据库操作统计: 插入={}, 更新={}, 总计={}",
                total_inserts, total_updates, total_inserts + total_updates
            );
            *last_time = now;
        }

        tracing::debug!(
            decision = "db_transaction_complete",
            symbol = %symbol,
            interval = %interval,
            total_processed = count,
            operation = "upsert",
            "数据库UPSERT事务成功完成"
        );
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
                        close_time = ?, quote_asset_volume = ?
                    WHERE open_time = ?", table_name),
                    params![
                        kline.open,
                        kline.high,
                        kline.low,
                        kline.close,
                        kline.volume,
                        kline.close_time,
                        kline.quote_asset_volume,
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
                        close_time, quote_asset_volume
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", table_name),
                    params![
                        kline.open_time,
                        kline.open,
                        kline.high,
                        kline.low,
                        kline.close,
                        kline.volume,
                        kline.close_time,
                        kline.quote_asset_volume,
                    ],
                );

                match result {
                    Ok(_) => count += 1,
                    Err(e) => {
                        let _ = tx.rollback();
                        // ✨ [错误记录]: 在返回错误前，记录更详细的上下文
                        tracing::error!(
                            message = %format!("数据库写入失败: {}", e),
                            error.type = "DatabaseError",
                            table_name = %table_name,
                            failed_at_kline_index = count // 当前处理的K线索引
                        );
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

    /// 静态版本的确保表存在函数，用于在 spawn_blocking 中执行
    #[instrument(skip(conn), fields(table_name = %table_name), ret, err)]
    fn ensure_symbol_table_static(conn: &rusqlite::Connection, _symbol: &str, _interval: &str, table_name: &str) -> Result<()> {
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
                quote_asset_volume TEXT NOT NULL
            )",
            table_name
        );

        conn.execute(&create_table_sql, [])
            .map_err(|e| {
                let table_error = AppError::DatabaseError(format!("创建表 {} 失败: {}", table_name, e));
                tracing::error!(
                    message = "创建表失败",
                    table_name = %table_name,
                    error.summary = table_error.get_error_type_summary(),
                    error.details = %table_error
                );
                table_error
            })?;

        tracing::debug!(decision = "table_created", table_name = %table_name, "数据库表创建或确认存在");
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
                quote_asset_volume TEXT NOT NULL
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
    #[instrument(skip(db_path), fields(db_path = %db_path.as_ref().display()), ret, err)]
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref();
        tracing::debug!(decision = "db_init_start", db_path = %db_path.display(), "开始初始化数据库");

        // Ensure parent directory exists for database
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        info!(log_type = "module", "Using SQLite database with optimized performance settings at {}", db_path.display());

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
            .map_err(|e| {
                let pool_error = AppError::DatabaseError(format!("Failed to create connection pool: {}", e));
                tracing::error!(
                    message = "创建连接池失败",
                    db_path = %db_path.display(),
                    error.summary = pool_error.get_error_type_summary(),
                    error.details = %pool_error
                );
                pool_error
            })?;

        // ✨ 6. [修改] 创建 Tokio 的 mpsc 写入队列通道
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
        match db.init_db() {
            Ok(_) => {
                info!(log_type = "module", "✅ SQLite数据库初始化成功，已启用写入队列和性能优化");
                tracing::debug!(decision = "db_init_success", "数据库初始化成功");
            },
            Err(e) => {
                error!(log_type = "module", "❌ 数据库初始化失败，程序无法继续: {}", e);
                tracing::error!(
                    message = "数据库初始化失败",
                    db_path = %db_path.display(),
                    error.summary = e.get_error_type_summary(),
                    error.details = %e
                );
                return Err(e);
            }
        }

        Ok(db)
    }

    /// Initialize database tables
    #[instrument(ret, err)]
    fn init_db(&self) -> Result<()> {
        tracing::debug!(decision = "init_db_start", "开始初始化数据库表");

        let conn = self.pool.get()
            .map_err(|e| {
                let conn_error = AppError::DatabaseError(format!("Failed to get connection: {}", e));
                tracing::error!(
                    message = "获取数据库连接失败",
                    error.summary = conn_error.get_error_type_summary(),
                    error.details = %conn_error
                );
                conn_error
            })?;

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
        ).map_err(|e| {
            let table_error = AppError::DatabaseError(format!("Failed to create symbols table: {}", e));
            tracing::error!(
                message = "创建symbols表失败",
                error.summary = table_error.get_error_type_summary(),
                error.details = %table_error
            );
            table_error
        })?;

        tracing::debug!(decision = "init_db_complete", "数据库表初始化完成");
        Ok(())
    }

    /// Ensure table exists for a specific symbol and interval
    #[instrument(skip(self, symbol, interval), fields(symbol = %symbol, interval = %interval), ret, err)]
    pub fn ensure_symbol_table(&self, symbol: &str, interval: &str) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| {
                let conn_error = AppError::DatabaseError(format!("Failed to get connection: {}", e));
                tracing::error!(
                    message = "获取数据库连接失败",
                    symbol = %symbol,
                    interval = %interval,
                    error.summary = conn_error.get_error_type_summary(),
                    error.details = %conn_error
                );
                conn_error
            })?;

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
                quote_asset_volume TEXT NOT NULL
            )",
            table_name
        );

        conn.execute(&create_table_sql, [])
            .map_err(|e| {
                let table_error = AppError::DatabaseError(format!("Failed to create table {}: {}", table_name, e));
                tracing::error!(
                    message = "创建表失败",
                    symbol = %symbol,
                    interval = %interval,
                    table_name = %table_name,
                    error.summary = table_error.get_error_type_summary(),
                    error.details = %table_error
                );
                table_error
            })?;

        tracing::debug!(decision = "table_ensured", symbol = %symbol, interval = %interval, table_name = %table_name, "数据库表创建或确认存在");
        Ok(())
    }

    /// Save a symbol to the database
    #[instrument(skip(self, symbol, base_asset, quote_asset, status), fields(symbol = %symbol, base_asset = %base_asset, quote_asset = %quote_asset, status = %status), ret, err)]
    pub fn save_symbol(&self, symbol: &str, base_asset: &str, quote_asset: &str, status: &str) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| {
                let conn_error = AppError::DatabaseError(format!("Failed to get connection: {}", e));
                tracing::error!(
                    message = "获取数据库连接失败",
                    symbol = %symbol,
                    error.summary = conn_error.get_error_type_summary(),
                    error.details = %conn_error
                );
                conn_error
            })?;

        conn.execute(
            "INSERT OR REPLACE INTO symbols (symbol, base_asset, quote_asset, status) VALUES (?, ?, ?, ?)",
            params![symbol, base_asset, quote_asset, status],
        ).map_err(|e| {
            let save_error = AppError::DatabaseError(format!("Failed to save symbol: {}", e));
            tracing::error!(
                message = "保存符号失败",
                symbol = %symbol,
                error.summary = save_error.get_error_type_summary(),
                error.details = %save_error
            );
            save_error
        })?;

        tracing::debug!(decision = "symbol_saved", symbol = %symbol, "符号保存成功");
        Ok(())
    }

    /// Save klines to the database using the write queue (Async)
    #[instrument(skip(self, klines), fields(symbol = %symbol, interval = %interval, kline_count = klines.len(), transaction_id = transaction_id), ret, err)]
    pub async fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline], transaction_id: u64) -> Result<usize> {
        if klines.is_empty() {
            tracing::debug!(decision = "empty_klines", symbol = %symbol, interval = %interval, "K线数据为空，跳过保存");
            return Ok(0);
        }

        tracing::debug!(decision = "save_klines_start", symbol = %symbol, interval = %interval, kline_count = klines.len(), "开始保存K线数据到写入队列");

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
            // ✨ 调用 AppTraceContext::new()，它会根据编译特性选择正确的实现
            context: AppTraceContext::new(),
        };

        // ✨ 7. [修改] 使用异步的 send().await
        if let Err(e) = self.write_queue_sender.send(task).await {
            let queue_error = AppError::DatabaseError(format!("无法将写入任务添加到队列: {}", e));
            tracing::error!(
                message = "写入任务队列发送失败",
                symbol = %symbol,
                interval = %interval,
                error.summary = queue_error.get_error_type_summary(),
                error.details = %queue_error
            );
            return Err(queue_error);
        }

        tracing::debug!(decision = "queue_task_sent", symbol = %symbol, interval = %interval, "写入任务已发送到队列，等待处理结果");

        // 异步等待写入操作完成并获取结果
        match result_receiver.await {
            Ok(result) => {
                tracing::debug!(decision = "save_result", symbol = %symbol, interval = %interval, result = ?result, "写入操作完成");
                result
            },
            Err(e) => {
                let wait_error = AppError::DatabaseError(format!("等待写入操作结果时出错: {}", e));
                tracing::error!(
                    message = "等待写入操作结果失败",
                    symbol = %symbol,
                    interval = %interval,
                    error.summary = wait_error.get_error_type_summary(),
                    error.details = %wait_error
                );
                Err(wait_error)
            }
        }
    }



    /// Get the latest kline timestamp for a symbol and interval
    // #[instrument] 移除：重复的数据库查询，在大循环中成为噪音，更高层的业务逻辑已隐含其耗时
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
    #[instrument(target = "Database", skip_all, err)]
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
    #[instrument(target = "Database", skip_all, err)]
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
    #[instrument(target = "Database", skip_all, err)]
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
    #[instrument(target = "Database", skip_all, err)]
    pub fn trim_klines(&self, symbol: &str, interval: &str, _max_count: i64) -> Result<usize> {
        // No longer limiting kline count, just return 0
        debug!("K-line trimming disabled, keeping all data for {}/{}", symbol, interval);
        Ok(0)
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
                quote_asset_volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", table_name),
            params![
                kline.open_time,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
                kline.close_time,
                kline.quote_asset_volume
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
    #[instrument(target = "Database", skip_all, err)]
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
                quote_asset_volume = ?
                WHERE open_time = ?", table_name),
            params![
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
                kline.close_time,
                kline.quote_asset_volume,
                kline.open_time
            ],
        ).map_err(|e| AppError::DatabaseError(format!("Failed to update kline: {}", e)))?;

        // Check if any records were updated
        if result == 0 {
            // If no records were updated, the record doesn't exist and needs to be inserted
            conn.execute(
                &format!("INSERT INTO {} (
                    open_time, open, high, low, close, volume, close_time,
                    quote_asset_volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", table_name),
                params![
                    kline.open_time,
                    kline.open,
                    kline.high,
                    kline.low,
                    kline.close,
                    kline.volume,
                    kline.close_time,
                    kline.quote_asset_volume
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
                    quote_asset_volume = ?
                    WHERE open_time = ?", table_name),
                params![
                    kline.open,
                    kline.high,
                    kline.low,
                    kline.close,
                    kline.volume,
                    kline.close_time,
                    kline.quote_asset_volume,
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
                    quote_asset_volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", table_name),
                params![
                    kline.open_time,
                    kline.open,
                    kline.high,
                    kline.low,
                    kline.close,
                    kline.volume,
                    kline.close_time,
                    kline.quote_asset_volume
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
    #[instrument(target = "Database", skip_all, err)]
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
    #[instrument(target = "Database", skip_all, err)]
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
             quote_asset_volume
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
                number_of_trades: 0, // 默认值
                taker_buy_base_asset_volume: "0".to_string(), // 默认值
                taker_buy_quote_asset_volume: "0".to_string(), // 默认值
                ignore: "0".to_string(), // 默认值
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
    #[instrument(target = "Database", skip_all, err)]
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
             quote_asset_volume
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
                number_of_trades: 0, // 默认值
                taker_buy_base_asset_volume: "0".to_string(), // 默认值
                taker_buy_quote_asset_volume: "0".to_string(), // 默认值
                ignore: "0".to_string(), // 默认值
            })
        })?;

        let mut result = Vec::new();
        for kline in klines {
            result.push(kline?);
        }

        Ok(result)
    }

    /// 根据开始时间获取K线
    #[instrument(target = "Database", skip_all, err)]
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
             quote_asset_volume
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
                number_of_trades: 0, // 默认值
                taker_buy_base_asset_volume: "0".to_string(), // 默认值
                taker_buy_quote_asset_volume: "0".to_string(), // 默认值
                ignore: "0".to_string(), // 默认值
            })
        }).optional()?;

        Ok(kline)
    }

    /// 获取指定时间之前的最后一根K线
    #[instrument(target = "Database", skip_all, err)]
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
             quote_asset_volume
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
                number_of_trades: 0, // 默认值
                taker_buy_base_asset_volume: "0".to_string(), // 默认值
                taker_buy_quote_asset_volume: "0".to_string(), // 默认值
                ignore: "0".to_string(), // 默认值
            })
        }).optional()?;

        Ok(kline)
    }

    /// 获取倒数第二根K线
    #[instrument(target = "Database", skip_all, err)]
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
             quote_asset_volume
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
                number_of_trades: 0, // 默认值
                taker_buy_base_asset_volume: "0".to_string(), // 默认值
                taker_buy_quote_asset_volume: "0".to_string(), // 默认值
                ignore: "0".to_string(), // 默认值
            })
        }).optional()?;

        Ok(kline)
    }
}
