// SQLite存储 - 将K线数据存储到SQLite数据库
use crate::klcommon::aggkline::models::KlineBar;
use crate::klcommon::{AppError, Result};
use log::{info, error, debug};
use tokio::sync::mpsc;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::time::{Duration, Instant};
use std::path::Path;

/// SQLite存储
pub struct SqliteStorage {
    /// 数据库连接池
    pool: SqlitePool,
}

impl SqliteStorage {
    /// 创建新的SQLite存储
    pub async fn new(database_url: &str) -> Result<Self> {
        info!("初始化SQLite存储: {}", database_url);
        
        // 确保数据库目录存在
        if let Some(parent) = Path::new(database_url).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        
        // 创建连接池
        let pool = SqlitePoolOptions::new()
            .max_connections(5) // 根据需要调整
            .connect(database_url)
            .await
            .map_err(|e| AppError::DatabaseError(format!("连接数据库失败: {}", e)))?;
        
        // 设置数据库参数
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await
            .map_err(|e| AppError::DatabaseError(format!("设置WAL模式失败: {}", e)))?;
        
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&pool)
            .await
            .map_err(|e| AppError::DatabaseError(format!("设置synchronous模式失败: {}", e)))?;
        
        info!("SQLite存储初始化成功");
        
        Ok(Self { pool })
    }
    
    /// 初始化数据库表
    pub async fn init_tables(&self) -> Result<()> {
        info!("初始化数据库表");
        
        // 创建K线表
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS klines (
                symbol TEXT NOT NULL,
                period_ms INTEGER NOT NULL,
                open_time_ms INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                turnover REAL NOT NULL,
                number_of_trades INTEGER NOT NULL,
                taker_buy_volume REAL NOT NULL,
                taker_buy_quote_volume REAL NOT NULL,
                PRIMARY KEY (symbol, period_ms, open_time_ms)
            ) WITHOUT ROWID;
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AppError::DatabaseError(format!("创建K线表失败: {}", e)))?;
        
        // 创建索引
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS idx_klines_symbol_period_time ON klines (symbol, period_ms, open_time_ms DESC);"#
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AppError::DatabaseError(format!("创建索引失败: {}", e)))?;
        
        info!("数据库表初始化成功");
        Ok(())
    }
    
    /// 插入或替换单条K线
    pub async fn insert_or_replace_kline(&self, kline: &KlineBar) -> Result<()> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO klines (
                symbol, period_ms, open_time_ms, open, high, low, close, 
                volume, turnover, number_of_trades, taker_buy_volume, taker_buy_quote_volume
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&kline.symbol)
        .bind(kline.period_ms)
        .bind(kline.open_time_ms)
        .bind(kline.open)
        .bind(kline.high)
        .bind(kline.low)
        .bind(kline.close)
        .bind(kline.volume)
        .bind(kline.turnover)
        .bind(kline.number_of_trades)
        .bind(kline.taker_buy_volume)
        .bind(kline.taker_buy_quote_volume)
        .execute(&self.pool)
        .await
        .map_err(|e| AppError::DatabaseError(format!("插入K线失败: {}", e)))?;
        
        Ok(())
    }
    
    /// 运行存储任务
    pub async fn run_storage_task(self, mut completed_kline_receiver: mpsc::Receiver<KlineBar>) -> Result<()> {
        info!("[SqliteStorageTask] 启动");
        
        // 批量写入缓冲区
        let mut kline_batch = Vec::with_capacity(100);
        
        // 批量写入间隔
        let mut batch_interval = tokio::time::interval(Duration::from_secs(1));
        
        // 统计信息
        let mut total_klines = 0;
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(30);
        
        loop {
            tokio::select! {
                Some(kline) = completed_kline_receiver.recv() => {
                    kline_batch.push(kline);
                    total_klines += 1;
                    
                    // 批量大小达到阈值，立即写入
                    if kline_batch.len() >= 100 {
                        if let Err(e) = self.write_batch(&mut kline_batch).await {
                            error!("[SqliteStorageTask] 批量写入失败: {}", e);
                        }
                    }
                }
                _ = batch_interval.tick() => {
                    // 定时写入未满的批次
                    if !kline_batch.is_empty() {
                        if let Err(e) = self.write_batch(&mut kline_batch).await {
                            error!("[SqliteStorageTask] 定时批量写入失败: {}", e);
                        }
                    }
                    
                    // 输出统计信息
                    if last_stats_time.elapsed() >= stats_interval {
                        info!("[SqliteStorageTask] 已存储 {} 条K线", total_klines);
                        last_stats_time = Instant::now();
                    }
                }
                else => {
                    // 通道关闭且没有更多的tick
                    if !kline_batch.is_empty() {
                        info!("[SqliteStorageTask] 通道关闭，写入剩余 {} 条K线", kline_batch.len());
                        if let Err(e) = self.write_batch(&mut kline_batch).await {
                            error!("[SqliteStorageTask] 最终批量写入失败: {}", e);
                        }
                    }
                    info!("[SqliteStorageTask] 完成的K线通道已关闭，退出");
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// 批量写入K线
    async fn write_batch(&self, kline_batch: &mut Vec<KlineBar>) -> Result<()> {
        if kline_batch.is_empty() {
            return Ok(());
        }
        
        // 开始事务
        let mut tx = self.pool.begin().await
            .map_err(|e| AppError::DatabaseError(format!("开始事务失败: {}", e)))?;
        
        // 在事务内执行批量插入
        for kline in kline_batch.iter() {
            sqlx::query(
                r#"
                INSERT OR REPLACE INTO klines (
                    symbol, period_ms, open_time_ms, open, high, low, close, 
                    volume, turnover, number_of_trades, taker_buy_volume, taker_buy_quote_volume
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&kline.symbol)
            .bind(kline.period_ms)
            .bind(kline.open_time_ms)
            .bind(kline.open)
            .bind(kline.high)
            .bind(kline.low)
            .bind(kline.close)
            .bind(kline.volume)
            .bind(kline.turnover)
            .bind(kline.number_of_trades)
            .bind(kline.taker_buy_volume)
            .bind(kline.taker_buy_quote_volume)
            .execute(&mut *tx)
            .await
            .map_err(|e| AppError::DatabaseError(format!("批量插入K线失败: {}", e)))?;
        }
        
        // 提交事务
        tx.commit().await
            .map_err(|e| AppError::DatabaseError(format!("提交事务失败: {}", e)))?;
        
        // 清空批次
        let batch_size = kline_batch.len();
        kline_batch.clear();
        
        debug!("[SqliteStorageTask] 成功写入 {} 条K线", batch_size);
        
        Ok(())
    }
}
