//! K线数据持久化模块
//! 
//! 负责将内存中的K线数据定期持久化到数据库。

use crate::klaggregate::{AggregateConfig, BufferedKlineStore, SymbolMetadataRegistry, KlineData};
use crate::klcommon::{Result, AppError, Database};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::time::{interval, Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{info, debug, warn, error, instrument, event, Level, Instrument};

/// K线数据持久化器
pub struct KlineDataPersistence {
    /// 配置
    config: AggregateConfig,
    
    /// 双缓冲存储引用
    buffered_store: Arc<BufferedKlineStore>,
    
    /// 符号元数据注册表
    symbol_registry: Arc<SymbolMetadataRegistry>,
    
    /// 数据库连接
    database: Arc<Database>,
    
    /// 运行状态
    is_running: Arc<AtomicBool>,

    /// 持久化计数
    persistence_count: Arc<AtomicU64>,

    /// 成功计数
    success_count: Arc<AtomicU64>,

    /// 错误计数
    error_count: Arc<AtomicU64>,
    
    /// 并发控制信号量
    semaphore: Arc<Semaphore>,
}

impl KlineDataPersistence {
    /// 创建新的K线数据持久化器
    #[instrument(target = "KlineDataPersistence", name="new_persistence", fields(batch_size = config.persistence.batch_size), skip_all, err)]
    pub async fn new(
        config: AggregateConfig,
        buffered_store: Arc<BufferedKlineStore>,
        symbol_registry: Arc<SymbolMetadataRegistry>,
    ) -> Result<Self> {
        info!(target: "KlineDataPersistence", log_type = "module", event_name = "持久化器初始化", batch_size = config.persistence.batch_size, "🔧 创建K线数据持久化器: batch_size={}", config.persistence.batch_size);
        
        // 创建数据库连接
        let database = Arc::new(Database::new(&config.database.database_path)?);
        
        // 创建并发控制信号量（限制同时进行的持久化任务数量）
        let semaphore = Arc::new(Semaphore::new(config.persistence.batch_size.min(10)));
        
        Ok(Self {
            config,
            buffered_store,
            symbol_registry,
            database,
            is_running: Arc::new(AtomicBool::new(false)),
            persistence_count: Arc::new(AtomicU64::new(0)),
            success_count: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
            semaphore,
        })
    }
    
    /// 启动持久化服务
    #[instrument(target = "KlineDataPersistence", name="start_persistence", fields(persistence_interval_ms = self.config.persistence_interval_ms), skip(self), err)]
    pub async fn start(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            warn!(target: "KlineDataPersistence", event_name = "持久化器已运行", "K线数据持久化器已经在运行");
            return Ok(());
        }

        info!(target: "KlineDataPersistence", log_type = "module", event_name = "持久化器启动", persistence_interval_ms = self.config.persistence_interval_ms, "🚀 启动K线数据持久化器: persistence_interval_ms={}", self.config.persistence_interval_ms);
        self.is_running.store(true, Ordering::Relaxed);

        // 启动定时持久化任务
        self.start_persistence_task().await;

        // 启动统计输出任务
        self.start_statistics_task().await;

        info!(target: "KlineDataPersistence", log_type = "module", event_name = "持久化器启动完成", "✅ K线数据持久化器启动完成");
        Ok(())
    }
    
    /// 停止持久化服务
    #[instrument(target = "KlineDataPersistence", name="stop_persistence", skip(self), err)]
    pub async fn stop(&self) -> Result<()> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        info!(target: "KlineDataPersistence", log_type = "module", event_name = "持久化器停止", "🛑 停止K线数据持久化器");
        self.is_running.store(false, Ordering::Relaxed);

        // 等待所有持久化任务完成
        let batch_size = self.config.persistence.batch_size as u32;
        let _permits = self.semaphore.acquire_many(batch_size).await
            .map_err(|e| AppError::DataError(format!("等待持久化任务完成失败: {}", e)))?;

        info!(target: "KlineDataPersistence", log_type = "module", event_name = "持久化器停止完成", "✅ K线数据持久化器已停止");
        Ok(())
    }
    
    /// 启动定时持久化任务
    async fn start_persistence_task(&self) {
        let is_running = self.is_running.clone();
        let buffered_store = self.buffered_store.clone();
        let symbol_registry = self.symbol_registry.clone();
        let database = self.database.clone();
        let persistence_count = self.persistence_count.clone();
        let success_count = self.success_count.clone();
        let error_count = self.error_count.clone();
        let semaphore = self.semaphore.clone();
        let batch_size = self.config.persistence.batch_size;
        let persistence_interval_ms = self.config.persistence_interval_ms;
        
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_millis(persistence_interval_ms));
            
            while is_running.load(Ordering::Relaxed) {
                interval_timer.tick().await;
                
                // 获取信号量许可
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        debug!(target: "KlineDataPersistence", event_name = "持久化任务跳过", "持久化任务繁忙，跳过本次执行");
                        continue;
                    }
                };
                
                let buffered_store = buffered_store.clone();
                let symbol_registry = symbol_registry.clone();
                let database = database.clone();
                let persistence_count = persistence_count.clone();
                let success_count = success_count.clone();
                let error_count = error_count.clone();
                
                // 异步执行持久化任务
                let count = persistence_count.load(Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit; // 确保permit在任务结束时释放
                    
                    let start_time = Instant::now();
                    persistence_count.fetch_add(1, Ordering::Relaxed);
                    
                    match Self::execute_persistence_task(
                        &buffered_store,
                        &symbol_registry,
                        &database,
                        batch_size,
                    ).await {
                        Ok(persisted_count) => {
                            success_count.fetch_add(1, Ordering::Relaxed);
                            let duration = start_time.elapsed();
                            
                            if persisted_count > 0 {
                                debug!(target: "KlineDataPersistence", "持久化完成: persisted_count={}, duration_ms={:.2}", persisted_count, duration.as_secs_f64() * 1000.0);
                            }
                        }
                        Err(e) => {
                            error_count.fetch_add(1, Ordering::Relaxed);
                            error!(target: "KlineDataPersistence", event_name = "持久化执行失败", error = %e, "持久化失败");
                        }
                    }
                }.instrument(tracing::info_span!("single_persistence_job", job_id = count)));
            }
        }.instrument(tracing::info_span!("persistence_scheduler_task")));
    }
    
    /// 执行持久化任务
    #[instrument(
        target = "KlineDataPersistence",
        name = "execute_persistence_task",
        fields(batch_size = %batch_size, persisted_count = 0),
        skip_all,
        err
    )]
    async fn execute_persistence_task(
        buffered_store: &Arc<BufferedKlineStore>,
        symbol_registry: &Arc<SymbolMetadataRegistry>,
        database: &Arc<Database>,
        batch_size: usize,
    ) -> Result<usize> {
        // 获取K线数据快照
        let kline_snapshot = buffered_store.get_read_buffer_snapshot().await?;
        
        let mut persisted_count = 0;
        let mut batch = Vec::new();
        
        for kline_data in kline_snapshot {
            // 跳过空的K线数据
            if kline_data.is_empty() {
                continue;
            }
            
            batch.push(kline_data);
            
            // 当批次达到指定大小时，执行持久化
            if batch.len() >= batch_size {
                persisted_count += Self::persist_kline_batch(
                    &batch,
                    symbol_registry,
                    database,
                ).await?;
                batch.clear();
            }
        }
        
        // 持久化剩余的K线数据
        if !batch.is_empty() {
            persisted_count += Self::persist_kline_batch(
                &batch,
                symbol_registry,
                database,
            ).await?;
        }
        
        tracing::Span::current().record("persisted_count", persisted_count);
        Ok(persisted_count)
    }
    
    /// 持久化K线批次
    #[instrument(
        target = "KlineDataPersistence",
        name = "persist_kline_batch",
        fields(
            batch_size = %batch.len(),
            total_records = 0,
            updated_records = 0,
            inserted_records = 0,
            persisted_count = 0
        ),
        skip_all,
        err
    )]
    async fn persist_kline_batch(
        batch: &[KlineData],
        symbol_registry: &Arc<SymbolMetadataRegistry>,
        database: &Arc<Database>,
    ) -> Result<usize> {
        let mut persisted_count = 0;
        
        for kline_data in batch {
            // 获取品种名称和周期字符串
            let symbol = match symbol_registry.get_symbol_by_index(kline_data.symbol_index).await {
                Some(symbol) => symbol,
                None => {
                    warn!(target: "KlineDataPersistence", event_name = "品种索引未找到", symbol_index = kline_data.symbol_index, "未找到索引对应的品种");
                    continue;
                }
            };

            let interval = match symbol_registry.get_interval_by_index(kline_data.period_index).await {
                Some(interval) => interval,
                None => {
                    warn!(target: "KlineDataPersistence", event_name = "周期索引未找到", period_index = kline_data.period_index, "未找到索引对应的周期");
                    continue;
                }
            };
            
            // 转换为数据库格式
            let kline = kline_data.to_kline(&symbol, &interval);
            
            // 执行UPSERT操作
            match Self::upsert_kline(database, &symbol, &interval, &kline).await {
                Ok(()) => {
                    persisted_count += 1;
                }
                Err(e) => {
                    error!(target: "KlineDataPersistence", event_name = "K线持久化失败", symbol = %symbol, interval = %interval, error = %e, "持久化K线失败");
                }
            }
        }

        // 记录批量持久化完成事件
        let total_records = batch.len() as u64;
        let updated_records = persisted_count as u64; // 简化：假设所有成功的都是更新
        let inserted_records = 0u64; // 简化：在实际实现中需要区分插入和更新
        let failed_count = batch.len() - persisted_count;

        event!(
            Level::INFO,
            target = "KlineDataPersistence",
            event_name = "批量持久化完成",
            total_records = total_records,
            updated_records = updated_records,
            inserted_records = inserted_records,
            success_count = persisted_count,
            failed_count = failed_count,
            "批量持久化完成"
        );

        tracing::Span::current().record("persisted_count", persisted_count);

        Ok(persisted_count)
    }
    
    /// 执行K线UPSERT操作
    #[instrument(target = "KlineDataPersistence", name="upsert_kline_db", fields(symbol = %symbol, interval = %interval), skip_all, err)]
    async fn upsert_kline(
        database: &Arc<Database>,
        symbol: &str,
        interval: &str,
        kline: &crate::klcommon::Kline,
    ) -> Result<()> {
        let database = database.clone();
        let symbol = symbol.to_string();
        let interval = interval.to_string();
        let kline = kline.clone();
        
        // 在阻塞任务中执行数据库操作
        tokio::task::spawn_blocking(move || {
            database.save_kline(&symbol, &interval, &kline)
        })
        .await
        .map_err(|e| AppError::DataError(format!("数据库任务执行失败: {}", e)))??;
        
        Ok(())
    }
    
    /// 启动统计输出任务
    async fn start_statistics_task(&self) {
        let is_running = self.is_running.clone();
        let persistence_count = self.persistence_count.clone();
        let success_count = self.success_count.clone();
        let error_count = self.error_count.clone();
        
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(60)); // 每分钟输出一次统计
            let mut last_persistence_count = 0;
            let mut last_success_count = 0;
            let mut last_error_count = 0;
            
            while is_running.load(Ordering::Relaxed) {
                interval_timer.tick().await;
                
                let current_persistence = persistence_count.load(Ordering::Relaxed);
                let current_success = success_count.load(Ordering::Relaxed);
                let current_error = error_count.load(Ordering::Relaxed);
                
                let persistence_rate = current_persistence - last_persistence_count;
                let success_rate = current_success - last_success_count;
                let error_rate = current_error - last_error_count;
                
                if persistence_rate > 0 || error_rate > 0 {
                    info!(
                        target: "KlineDataPersistence",
                        event_name = "持久化统计报告",
                        total_persistence = current_persistence,
                        persistence_rate = persistence_rate,
                        total_success = current_success,
                        success_rate = success_rate,
                        total_errors = current_error,
                        error_rate = error_rate,
                        "持久化统计报告"
                    );
                }
                
                last_persistence_count = current_persistence;
                last_success_count = current_success;
                last_error_count = current_error;
            }
        }.instrument(tracing::info_span!("persistence_statistics_task")));
    }
    
    /// 获取状态字符串
    pub async fn get_status(&self) -> String {
        if self.is_running.load(Ordering::Relaxed) {
            format!(
                "运行中 (执行: {}, 成功: {}, 错误: {})",
                self.persistence_count.load(Ordering::Relaxed),
                self.success_count.load(Ordering::Relaxed),
                self.error_count.load(Ordering::Relaxed)
            )
        } else {
            "已停止".to_string()
        }
    }
    
    /// 获取统计信息
    pub async fn get_statistics(&self) -> PersistenceStatistics {
        PersistenceStatistics {
            is_running: self.is_running.load(Ordering::Relaxed),
            persistence_count: self.persistence_count.load(Ordering::Relaxed),
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            available_permits: self.semaphore.available_permits(),
        }
    }
}

/// 持久化统计信息
#[derive(Debug, Clone)]
pub struct PersistenceStatistics {
    /// 是否运行中
    pub is_running: bool,
    /// 持久化执行次数
    pub persistence_count: u64,
    /// 成功次数
    pub success_count: u64,
    /// 错误次数
    pub error_count: u64,
    /// 可用的并发许可数
    pub available_permits: usize,
}
