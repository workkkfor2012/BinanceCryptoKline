use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, AppError};
use crate::klcommon::api::get_aligned_time;
use crate::klcommon::models::Kline as DbKline;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, Ordering};
use once_cell::sync::Lazy;
use futures::{stream, StreamExt};
use tracing::{info, warn}; // 确保导入了 info 和 warn 宏
use kline_macros::perf_profile; // 导入性能分析宏
use chrono::TimeZone; // 添加TimeZone导入
// [新增] 连接池管理相关导入
use reqwest::Client;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicUsize as AtomicUsizeOrdering, Ordering as AtomicOrdering};

// 全局统计变量，用于跟踪补齐K线的数量和日志显示
// 格式: (补齐K线总数, 最后日志时间, 交易对统计Map)
static BACKFILL_STATS: Lazy<(AtomicUsize, std::sync::Mutex<Instant>, std::sync::Mutex<HashMap<String, usize>>)> = Lazy::new(|| {
    (AtomicUsize::new(0), std::sync::Mutex::new(Instant::now()), std::sync::Mutex::new(HashMap::new()))
});

// 全局API请求计数器
// 格式: (发送的请求数, 成功的请求数, 失败的请求数)
static API_REQUEST_STATS: Lazy<(AtomicUsize, AtomicUsize, AtomicUsize)> = Lazy::new(|| {
    (AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0))
});



// 日志间隔，每30秒输出一次摘要
const BACKFILL_LOG_INTERVAL: u64 = 30;
const CONCURRENCY: usize = 20; // 第一次补齐并发数
const SECOND_ROUND_CONCURRENCY: usize = 200; // 第二次补齐并发数

// [新增] 连接池相关常量
const POOL_TARGET_SIZE: usize = 200; // 目标池大小

// 任务执行结果的枚举，方便模式匹配
#[derive(Debug)]
enum TaskResult {
    Success(usize), // 成功，并返回写入的K线数量
    Failure {
        task: DownloadTask,
        error: AppError,
    },
}

// ✨ [新增] 任务重试跟踪结构
#[derive(Debug, Clone)]
struct TaskRetryTracker {
    task: DownloadTask,
    error: AppError,
    retry_count: u32,
}

impl TaskRetryTracker {
    fn new(task: DownloadTask, error: AppError) -> Self {
        Self {
            task,
            error,
            retry_count: 1,
        }
    }

    fn increment_retry(&mut self, new_error: AppError) {
        self.retry_count += 1;
        self.error = new_error;
    }

    fn should_retry(&self, max_retries_per_task: u32) -> bool {
        self.retry_count <= max_retries_per_task && self.error.is_retryable()
    }
}

/// K线数据补齐模块
#[derive(Debug)]
pub struct KlineBackfiller {
    db: Arc<Database>,
    // [修改] 不再持有 api 实例，而是持有客户端池
    client_pool: Arc<RwLock<Vec<Arc<Client>>>>,
    pool_next_index: Arc<AtomicUsizeOrdering>, // 用于轮询，使用Arc包装以支持Clone
    pool_maintenance_stop: Arc<std::sync::atomic::AtomicBool>, // 停止后台维护任务的标志
    intervals: Vec<String>,
    test_mode: bool,
    test_symbols: Vec<String>,
}

impl Clone for KlineBackfiller {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            client_pool: self.client_pool.clone(),
            pool_next_index: self.pool_next_index.clone(),
            pool_maintenance_stop: self.pool_maintenance_stop.clone(),
            intervals: self.intervals.clone(),
            test_mode: self.test_mode,
            test_symbols: self.test_symbols.clone(),
        }
    }
}

impl KlineBackfiller {
    /// 创建新的K线补齐器实例
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        let backfiller = Self {
            db,
            client_pool: Arc::new(RwLock::new(Vec::with_capacity(POOL_TARGET_SIZE))),
            pool_next_index: Arc::new(AtomicUsizeOrdering::new(0)),
            pool_maintenance_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            intervals,
            test_mode: false,
            test_symbols: vec![],
        };

        // [新增] 启动后台池填充和维护任务
        backfiller.start_pool_maintenance_task();

        backfiller
    }

    /// 创建测试模式的K线补齐器实例
    pub fn new_test_mode(db: Arc<Database>, intervals: Vec<String>, test_symbols: Vec<String>) -> Self {
        let backfiller = Self {
            db,
            client_pool: Arc::new(RwLock::new(Vec::with_capacity(POOL_TARGET_SIZE))),
            pool_next_index: Arc::new(AtomicUsizeOrdering::new(0)),
            pool_maintenance_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            intervals,
            test_mode: true,
            test_symbols,
        };

        // [新增] 启动后台池填充和维护任务
        backfiller.start_pool_maintenance_task();

        backfiller
    }

    /// [新增] 启动后台任务来填充和维护客户端池
    fn start_pool_maintenance_task(&self) {
        let pool_clone = self.client_pool.clone();
        let stop_flag = self.pool_maintenance_stop.clone();
        tokio::spawn(async move {
            info!(target: "client_pool", "客户端池维护任务启动，目标大小: {}", POOL_TARGET_SIZE);
            loop {
                // 检查停止标志
                if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    info!(target: "client_pool", "收到停止信号，客户端池维护任务退出");
                    break;
                }
                let current_size;
                {
                    // 使用单独的块来限制读锁的范围
                    let pool_guard = pool_clone.read().await;
                    current_size = pool_guard.len();
                }

                if current_size >= POOL_TARGET_SIZE {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                // [修改] 创建客户端时，传入 true 以使用高并发代理 (10808端口)
                if let Ok(new_client) = BinanceApi::create_new_client(true) {
                    // [修复] 移除不必要的健康检查，新建的client本身就是健康的
                    let mut pool_guard = pool_clone.write().await;
                    if pool_guard.len() < POOL_TARGET_SIZE {
                        pool_guard.push(Arc::new(new_client));
                        info!(target: "client_pool", "成功添加新客户端到池中，当前大小: {}", pool_guard.len());
                    }
                } else {
                    // 创建客户端失败时稍作等待
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
    }

    /// [新增] 从池中获取一个客户端用于请求
    async fn get_client_from_pool(&self) -> Option<Arc<Client>> {
        let pool_guard = self.client_pool.read().await;
        if pool_guard.is_empty() {
            return None;
        }
        let index = self.pool_next_index.fetch_add(1, AtomicOrdering::Relaxed) % pool_guard.len();
        pool_guard.get(index).cloned()
    }

    /// [新增] 当客户端请求失败时，从池中移除它
    async fn remove_client_from_pool(&self, client_to_remove: &Arc<Client>) {
        let mut pool_guard = self.client_pool.write().await;
        let initial_len = pool_guard.len();
        pool_guard.retain(|c| !Arc::ptr_eq(c, client_to_remove));
        if pool_guard.len() < initial_len {
            warn!(target: "client_pool", "检测到失败，已从池中移除一个客户端，当前大小: {}", pool_guard.len());
        }
    }

    /// [新增] 清理连接池，释放所有连接（私有方法）
    async fn cleanup_client_pool(&self) {
        // 首先停止后台维护任务
        self.pool_maintenance_stop.store(true, std::sync::atomic::Ordering::Relaxed);

        // 然后清理连接池
        let mut pool_guard = self.client_pool.write().await;
        let pool_size = pool_guard.len();
        pool_guard.clear();
        if pool_size > 0 {
            info!(target: "client_pool", "连接池已清理，释放了 {} 个连接，后台维护任务已停止", pool_size);
        }
    }

    /// [新增] 公共方法：完成所有补齐任务后清理连接池
    pub async fn cleanup_after_all_backfill_rounds(&self) {
        info!(log_type = "low_freq", message = "所有补齐轮次完成，开始清理连接池...");
        self.cleanup_client_pool().await;
    }

    /// 更新补齐K线的统计信息并每30秒输出一次摘要日志
    fn update_backfill_stats(symbol: &str, interval: &str, count: usize) {
        // 更新总计数器
        BACKFILL_STATS.0.fetch_add(count, Ordering::Relaxed);

        // 更新按交易对和周期的计数器
        let key = format!("{}/{}", symbol, interval);
        let mut symbol_map = BACKFILL_STATS.2.lock().unwrap();
        let entry = symbol_map.entry(key).or_insert(0);
        *entry += count;

        // 检查是否需要输出日志
        let mut last_log_time = BACKFILL_STATS.1.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_log_time);

        // 如果日志间隔已过，输出日志并重置计数器
        if elapsed >= Duration::from_secs(BACKFILL_LOG_INTERVAL) {
            let total_count = BACKFILL_STATS.0.swap(0, Ordering::Relaxed);

            if total_count > 0 {
                // ✨ [埋点] 增加 Progress 日志
                let sent = API_REQUEST_STATS.0.load(Ordering::Relaxed);
                let success = API_REQUEST_STATS.1.load(Ordering::Relaxed);
                let failed = API_REQUEST_STATS.2.load(Ordering::Relaxed);

                let mut top_activity: Vec<String> = symbol_map.iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect();
                top_activity.sort_by_key(|b| std::cmp::Reverse(b.len())); // 简单排序

                info!(
                    log_type = "low_freq",
                    message = "K线数据补齐进度摘要",
                    elapsed_seconds = elapsed.as_secs(),
                    total_klines_backfilled = total_count,
                    api_requests_sent = sent,
                    api_requests_success = success,
                    api_requests_failed = failed,
                    top_symbols_activity = top_activity.join(", "),
                );
            }

            // 清空交易对计数器
            symbol_map.clear();

            // 更新最后日志时间
            *last_log_time = now;
        }
    }

    /// 运行一次性补齐流程
    #[perf_profile]
    pub async fn run_once(&self) -> Result<()> {
        self.run_once_with_round(1).await
    }

    /// 运行一次性补齐流程（可指定轮次以调整并发数）
    #[perf_profile]
    pub async fn run_once_with_round(&self, round: u32) -> Result<()> {
        let start_time = Instant::now();

        info!(
            log_type = "low_freq",
            message = "K线数据补齐任务启动",
            mode = if self.test_mode { "test" } else { "production" },
            intervals = self.intervals.join(","),
        );

        // ✨ [优化] 在开头获取一次完整的交易所信息，避免重复网络请求
        let exchange_info = if self.test_mode {
            None // 测试模式下不需要获取交易所信息
        } else {
            // [修改] 使用统一的重试方法获取交易所信息（每次重试都创建新连接）
            const MAX_RETRIES: usize = 20;
            const RETRY_INTERVAL: u64 = 2;

            Some(BinanceApi::retry_get_exchange_info(
                MAX_RETRIES,
                RETRY_INTERVAL,
                "backfill_run"
            ).await?)
        };

        let all_symbols = self.get_symbols_with_exchange_info(exchange_info.as_ref()).await?;

        info!(
            log_type = "low_freq",
            message = "已从API获取交易对列表",
            symbol_count = all_symbols.len(),
        );

        self.ensure_all_tables(&all_symbols)?;
        let tasks = self.create_all_download_tasks(&all_symbols).await?;

        info!(
            log_type = "low_freq",
            message = "下载任务创建完成",
            total_tasks_generated = tasks.len(),
        );

        if tasks.is_empty() {
            info!(log_type = "low_freq", message = "没有需要执行的补齐任务。");
            return Ok(());
        }

        // 根据轮次选择并发数
        let concurrency = match round {
            1 => CONCURRENCY,
            2 => SECOND_ROUND_CONCURRENCY,
            _ => SECOND_ROUND_CONCURRENCY, // 默认使用高并发
        };
        info!(
            log_type = "low_freq",
            message = "开始执行补齐任务",
            round = round,
            concurrency = concurrency,
            total_tasks = tasks.len(),
        );

        let first_round_start = Instant::now();
        let failed_tasks = self.execute_tasks_with_concurrency(tasks.clone(), "initial_download_loop", concurrency).await;
        let first_round_elapsed = first_round_start.elapsed();
        
        // 第一轮K线补齐完成统计
        info!(
            log_type = "low_freq",
            message = "第一轮K线补齐完成统计",
            total_tasks = tasks.len(),
            success_count = tasks.len() - failed_tasks.len(),
            failed_count = failed_tasks.len(),
            elapsed_seconds = first_round_elapsed.as_secs_f64(),
            success_rate = format!("{:.1}%", ((tasks.len() - failed_tasks.len()) as f64 / tasks.len() as f64) * 100.0),
        );

        // --- 新增：基于轮次的重试策略 ---
        const MAX_RETRIES: u32 = 3; // 最多3轮重试
        const MAX_RETRIES_PER_TASK: u32 = 5; // 单个任务最多重试5次

        // ✨ [优化] 第一轮不重试，直接返回失败任务给第二轮处理
        if round == 1 {
            info!(
                log_type = "low_freq",
                message = "第一轮补齐策略：快速批量下载，失败任务留给第二轮处理",
                failed_count = failed_tasks.len(),
                note = "第一轮专注于速度，不进行重试"
            );

            // 直接报告最终失败，不进行重试
            if !failed_tasks.is_empty() {
                self.report_final_failures(failed_tasks);
            }

            return Ok(());
        }

        // --- 第二轮及以后：完整的重试逻辑 ---
        info!(
            log_type = "low_freq",
            message = "第二轮补齐策略：精确补齐，启用完整重试机制",
            failed_count = failed_tasks.len(),
        );

        // 执行重试逻辑，将失败的任务转换为跟踪器
        let mut task_trackers: Vec<TaskRetryTracker> = failed_tasks
            .into_iter()
            .map(|(task, error)| TaskRetryTracker::new(task, error))
            .collect();

        // --- 改进的智能重试循环 ---
        for i in 1..=MAX_RETRIES {
            if task_trackers.is_empty() {
                info!(log_type = "low_freq", message = "所有任务已成功，提前结束重试循环。");
                break;
            }

            // ✨ [新增] 智能过滤：基于重试次数和错误类型过滤
            let current_trackers = std::mem::take(&mut task_trackers);
            let (retryable_trackers, exhausted_trackers): (Vec<_>, Vec<_>) = current_trackers
                .into_iter()
                .partition(|tracker| tracker.should_retry(MAX_RETRIES_PER_TASK));

            if !exhausted_trackers.is_empty() {
                warn!(
                    log_type = "low_freq",
                    message = "发现已达到重试上限或不可重试的任务",
                    exhausted_count = exhausted_trackers.len(),
                );
                // 将已耗尽重试次数的任务加入最终失败列表
                let final_failures: Vec<(DownloadTask, AppError)> = exhausted_trackers
                    .into_iter()
                    .map(|tracker| (tracker.task, tracker.error))
                    .collect();
                self.report_final_failures(final_failures);
            }

            if retryable_trackers.is_empty() {
                info!(log_type = "low_freq", message = "没有可重试的任务，结束重试循环。");
                break;
            }

            // ✨ [新增] 指数退避：每轮重试前等待递增的时间
            let backoff_seconds = 2_u64.pow(i - 1).min(30); // 2^(i-1)秒，最大30秒
            if i > 1 {
                info!(
                    log_type = "low_freq",
                    message = "执行指数退避等待",
                    retry_round = i,
                    backoff_seconds = backoff_seconds,
                );
                tokio::time::sleep(Duration::from_secs(backoff_seconds)).await;
            }

            info!(
                log_type = "low_freq",
                message = "开始进行重试轮次",
                retry_round = i,
                tasks_to_retry = retryable_trackers.len(),
                backoff_applied = backoff_seconds,
            );

            // 从可重试的跟踪器中提取任务列表
            let retry_tasks: Vec<DownloadTask> = retryable_trackers.iter().map(|tracker| tracker.task.clone()).collect();
            task_trackers = retryable_trackers; // 更新为只包含可重试的跟踪器

            let retry_start = Instant::now();
            // 重新执行失败的任务
            let retry_failures = self.execute_tasks_with_concurrency(retry_tasks.clone(), &format!("retry_download_loop_{}", i), concurrency).await;
            let retry_elapsed = retry_start.elapsed();

            // ✨ [新增] 更新跟踪器：增加重试次数，更新错误信息
            let mut updated_trackers = Vec::new();
            let retry_failure_map: std::collections::HashMap<String, AppError> = retry_failures
                .into_iter()
                .map(|(task, error)| (format!("{}_{}", task.symbol, task.interval), error))
                .collect();

            for mut tracker in task_trackers {
                let task_key = format!("{}_{}", tracker.task.symbol, tracker.task.interval);
                if let Some(new_error) = retry_failure_map.get(&task_key) {
                    // 任务仍然失败，更新跟踪器
                    tracker.increment_retry(new_error.clone());
                    updated_trackers.push(tracker);
                }
                // 如果任务不在失败列表中，说明成功了，不需要加入updated_trackers
            }

            task_trackers = updated_trackers;

            info!(
                log_type = "low_freq",
                message = "重试任务完成统计",
                retry_round = i,
                retry_tasks_count = retry_tasks.len(),
                retry_success_count = retry_tasks.len() - task_trackers.len(),
                retry_failed_count = task_trackers.len(),
                retry_elapsed_seconds = retry_elapsed.as_secs_f64(),
                retry_success_rate = if retry_tasks.is_empty() { "N/A".to_string() } else { format!("{:.1}%", ((retry_tasks.len() - task_trackers.len()) as f64 / retry_tasks.len() as f64) * 100.0) },
            );
        }

        // 循环结束后，仍然失败的任务被视为最终失败
        let final_failed_tasks_count = task_trackers.len();
        if !task_trackers.is_empty() {
            let final_failures: Vec<(DownloadTask, AppError)> = task_trackers
                .into_iter()
                .map(|tracker| (tracker.task, tracker.error))
                .collect();
            self.report_final_failures(final_failures);
        }

        info!(
            log_type = "low_freq",
            message = "K线数据补齐任务完成",
            total_duration_s = start_time.elapsed().as_secs_f64(),
            final_total_klines_backfilled = BACKFILL_STATS.0.load(Ordering::Relaxed),
            final_api_requests_sent = API_REQUEST_STATS.0.load(Ordering::Relaxed),
            final_api_requests_success = API_REQUEST_STATS.1.load(Ordering::Relaxed),
            final_api_requests_failed = API_REQUEST_STATS.2.load(Ordering::Relaxed),
            final_failed_tasks_count = final_failed_tasks_count,
        );



        // ✨ [新增] 清理已下架品种的数据
        info!(log_type = "low_freq", message = "开始清理已下架品种的数据...");
        match self.cleanup_delisted_symbols_with_exchange_info(exchange_info.as_ref()).await {
            Ok(cleaned_count) => {
                if cleaned_count > 0 {
                    info!(log_type = "low_freq", message = "已下架品种数据清理完成", cleaned_tables = cleaned_count);
                } else {
                    info!(log_type = "low_freq", message = "没有发现需要清理的已下架品种数据");
                }
            }
            Err(e) => {
                warn!(log_type = "low_freq", message = "清理已下架品种数据时出现问题", error = %e);
            }
        }

        // ✨ [暂时注释] 检查所有品种的所有周期是否都补齐到最新 - 功能稳定，暂时关闭以提升性能
        // info!(log_type = "low_freq", message = "开始验证所有品种的所有周期是否都补齐到最新...");
        // match self.verify_backfill_completeness().await {
        //     Ok(()) => {
        //         info!(log_type = "low_freq", message = "补齐完整性验证通过，所有品种的所有周期都已补齐到最新");
        //     }
        //     Err(e) => {
        //         warn!(log_type = "low_freq", message = "补齐完整性验证发现问题", error = %e);
        //         // 注意：这里不返回错误，只是警告，因为可能存在一些品种暂时无法获取最新数据的情况
        //     }
        // }

        // 注意：不在这里清理连接池，因为可能还有后续的补齐轮次需要使用连接池

        Ok(())
    }



    /// 执行一批下载任务，并返回失败的任务列表（可指定并发数）
    async fn execute_tasks_with_concurrency(&self, tasks: Vec<DownloadTask>, _loop_name: &str, concurrency: usize) -> Vec<(DownloadTask, AppError)> {
        let mut success_count = 0;
        let mut failed_tasks = Vec::new();
        let total_tasks = tasks.len();

        // API请求速率监控
        let start_time = std::time::Instant::now();
        let request_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let success_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // 启动速率监控任务
        let counter_clone = request_counter.clone();
        let success_clone = success_counter.clone();
        let monitor_task = tokio::spawn(async move {
            let mut last_requests = 0;
            let mut last_success = 0;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;
                let current_requests = counter_clone.load(std::sync::atomic::Ordering::Relaxed);
                let current_success = success_clone.load(std::sync::atomic::Ordering::Relaxed);

                let req_rate = (current_requests - last_requests) as f64 / 5.0;
                let success_rate = (current_success - last_success) as f64 / 5.0;

                info!(
                    log_type = "low_freq",
                    message = "API请求速率监控",
                    并发数 = concurrency,
                    总请求数 = current_requests,
                    成功请求数 = current_success,
                    请求速率_每秒 = format!("{:.1}", req_rate),
                    成功速率_每秒 = format!("{:.1}", success_rate),
                    成功率 = format!("{:.1}%", if current_requests > 0 { (current_success as f64 / current_requests as f64) * 100.0 } else { 0.0 }),
                );

                last_requests = current_requests;
                last_success = current_success;

                if current_requests >= total_tasks {
                    break;
                }
            }
        });

        let results = stream::iter(tasks)
            .map(|task| {
                // [修改] 克隆 self 以在闭包中使用
                let backfiller_clone = self.clone();
                let req_counter = request_counter.clone();
                let succ_counter = success_counter.clone();
                async move {
                    // 记录请求开始
                    req_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    // [修改] 调用 &self 的方法
                    let result = backfiller_clone.process_single_task_instrumented(task).await;

                    // 记录成功请求
                    if matches!(result, TaskResult::Success(_)) {
                        succ_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    result
                }
            })
            .buffer_unordered(concurrency);

        results
            .for_each(|result| {
                match result {
                    TaskResult::Success(count) => {
                        if count > 0 { success_count += 1; }
                    }
                    TaskResult::Failure { task, error } => {
                        failed_tasks.push((task, error));
                    }
                }
                async {}
            })
            .await;

        // 停止监控任务
        monitor_task.abort();

        let total_duration = start_time.elapsed();
        let final_requests = request_counter.load(std::sync::atomic::Ordering::Relaxed);
        let final_success = success_counter.load(std::sync::atomic::Ordering::Relaxed);

        info!(
            log_type = "low_freq",
            message = "API请求批次完成统计",
            并发数 = concurrency,
            总任务数 = total_tasks,
            实际请求数 = final_requests,
            成功请求数 = final_success,
            总耗时_秒 = total_duration.as_secs_f64(),
            平均请求速率_每秒 = format!("{:.1}", final_requests as f64 / total_duration.as_secs_f64()),
            理论最大速率_每秒 = format!("{:.1}", concurrency as f64),
            实际并发利用率 = format!("{:.1}%", (final_requests as f64 / total_duration.as_secs_f64()) / concurrency as f64 * 100.0),
        );

        failed_tasks
    }

    /// [重构] 对单个任务处理进行 instrument 的包装函数
    /// 这是高频业务的核心入口点。
    #[tracing::instrument(
        // ✨ [修改] 遵循规范更新Span定义
        name = "kline_backfill_transaction",
        skip_all,
        fields(
            tx_id = %format!("kline_backfill:{}-{}", task.symbol, task.interval),
            log_type = "high_freq",
            symbol = %task.symbol,
            interval = %task.interval,
        )
    )]
    async fn process_single_task_instrumented(&self, task: DownloadTask) -> TaskResult {
        self.process_single_task_with_perf(task).await
    }

    /// [重构] 带性能分析的单个任务处理函数
    #[perf_profile(skip_all, fields(symbol = %task.symbol, interval = %task.interval))]
    async fn process_single_task_with_perf(&self, task: DownloadTask) -> TaskResult {
        let client = match self.get_client_from_pool().await {
            Some(c) => c,
            None => {
                warn!(target: "client_pool", "客户端池为空，任务等待...");
                tokio::time::sleep(Duration::from_secs(1)).await;
                return TaskResult::Failure { task, error: AppError::ApiError("客户端池为空".into()) };
            }
        };

        match BinanceApi::download_continuous_klines(&client, &task).await {
            Ok(klines) => {
                if klines.is_empty() {
                    return TaskResult::Success(0);
                }

                let count = klines.len();
                if let Err(e) = self.db.save_klines_async(&task.symbol, &task.interval, &klines).await {
                     warn!(error_chain = format!("{:#}", e), message = "提交K线到写入队列失败");
                     return TaskResult::Failure { task, error: e };
                }

                Self::update_backfill_stats(&task.symbol, &task.interval, count);
                TaskResult::Success(count)
            },
            Err(e) => {
                // ✨ [修改] HTTP错误和包含"error"的响应都剔除客户端，确保连接池的健康性
                let should_remove_client = if let AppError::HttpError(ref re) = e {
                    if let Some(status) = re.status() {
                        let status_code = status.as_u16();
                        // 打印状态码用于调试
                        warn!(
                            log_type = "low_freq",
                            message = "HTTP错误状态码调试",
                            status_code = status_code,
                            error_message = %e,
                        );
                    } else {
                        warn!(
                            log_type = "low_freq",
                            message = "HTTP错误但无状态码",
                            error_message = %e,
                        );
                    }
                    true // 所有HTTP错误都移除客户端
                } else {
                    // 检查错误消息是否包含"error"关键词
                    let error_msg = e.to_string().to_lowercase();
                    if error_msg.contains("error") {
                        warn!(
                            log_type = "low_freq",
                            message = "错误消息包含error关键词，移除客户端",
                            error_message = %e,
                        );
                        true
                    } else {
                        false
                    }
                };

                if should_remove_client {
                    self.remove_client_from_pool(&client).await;
                }
                TaskResult::Failure { task, error: e }
            }
        }
    }

    // 旧的process_single_task方法已被重构为process_single_task_with_perf



    /// 报告最终无法完成的任务
    fn report_final_failures(&self, final_failures: Vec<(DownloadTask, AppError)>) {
        let mut error_summary: HashMap<String, usize> = HashMap::new();

        for (_task, error) in final_failures.iter() { // ✨ [修改] 移除 .take(10) 以统计所有失败
            let error_msg = error.to_string();
            let error_type = error_msg.split(':').next().unwrap_or("Unknown Error").trim();
            *error_summary.entry(error_type.to_string()).or_default() += 1;
        }

        // ✨ [新增] 低频日志：报告最终无法恢复的错误
        if !final_failures.is_empty() {
            warn!(
                log_type = "low_freq",
                message = "补齐任务最终失败摘要",
                total_final_failures = final_failures.len(),
                error_summary = ?error_summary,
            );
        }
    }

    /// 报告最终的运行摘要
    #[allow(dead_code)]
    fn report_summary(&self, _start_time: Instant) {
        // 清理后的简化版本
    }

    #[perf_profile]
    async fn get_symbols_with_exchange_info(&self, exchange_info: Option<&crate::klcommon::models::ExchangeInfo>) -> Result<Vec<String>> {
        if self.test_mode {
            return Ok(self.test_symbols.clone());
        }

        // [修改] 如果已经有交易所信息，直接使用；否则使用临时客户端获取
        let (trading_symbols, delisted_symbols) = if let Some(info) = exchange_info {
            // ✨ [优化] 使用已获取的交易所信息，避免重复网络请求
            self.parse_symbols_from_exchange_info(info)
        } else {
            // 兜底：如果没有交易所信息，直接获取（每次重试都创建新连接）
            BinanceApi::get_trading_usdt_perpetual_symbols().await?
        };

        // 处理已下架的品种
        if !delisted_symbols.is_empty() {
            // 先检查数据库中是否存在这些品种的数据，只有存在时才执行删除并记录日志
            let deleted_count = self.delete_delisted_symbols_data(&delisted_symbols).await?;
            if deleted_count > 0 {
                info!(
                    log_type = "low_freq",
                    message = "发现已下架品种，已执行数据删除",
                    delisted_count = delisted_symbols.len(),
                    deleted_tables = deleted_count,
                    delisted_symbols = delisted_symbols.join(", "),
                );
            }
        }

        if trading_symbols.is_empty() {
            let empty_error = AppError::ApiError("没有获取到交易对，无法继续。".to_string());
            return Err(empty_error);
        }

        Ok(trading_symbols)
    }

    /// 从交易所信息中解析出交易中和已下架的品种列表
    fn parse_symbols_from_exchange_info(&self, exchange_info: &crate::klcommon::models::ExchangeInfo) -> (Vec<String>, Vec<String>) {
        let mut trading_symbols = Vec::new();
        let mut delisted_symbols = Vec::new();

        for symbol in &exchange_info.symbols {
            let is_usdt = symbol.symbol.ends_with("USDT");
            let is_perpetual = symbol.contract_type == "PERPETUAL";

            // 只处理USDT永续合约
            if !is_usdt || !is_perpetual {
                continue;
            }

            // 根据状态进行不同处理
            match symbol.status.as_str() {
                "TRADING" => {
                    // 正常交易状态，加入交易列表
                    trading_symbols.push(symbol.symbol.clone());
                },
                "CLOSE" | "SETTLING" => {
                    // 已下架状态（包括CLOSE和SETTLING），加入删除列表
                    delisted_symbols.push(symbol.symbol.clone());
                },
                _ => {
                    // 其他未知状态，记录日志
                    info!(
                        log_type = "low_freq",
                        message = "发现未知状态的品种",
                        symbol = %symbol.symbol,
                        status = %symbol.status,
                        note = "非TRADING、CLOSE或SETTLING状态，需要关注",
                    );
                }
            }
        }

        (trading_symbols, delisted_symbols)
    }

    /// 获取数据库中已存在的K线表
    #[perf_profile]
    fn get_existing_kline_tables(&self) -> Result<Vec<(String, String)>> {
        let conn = self.db.get_connection()?;
        let mut tables = Vec::new();

        // 查询所有以k_开头的表
        let query = "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'k_%'";
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        // 解析表名，提取品种和周期
        for row in rows {
            let table_name = row?;
            if let Some((symbol, interval)) = self.parse_table_name(&table_name) {
                // 只处理指定的周期
                if self.intervals.contains(&interval) {
                    tables.push((symbol, interval));
                }
            }
        }

        Ok(tables)
    }

    /// 解析表名，提取品种和周期
    fn parse_table_name(&self, table_name: &str) -> Option<(String, String)> {
        let parts: Vec<&str> = table_name.split('_').collect();

        if parts.len() >= 3 {
            let symbol = format!("{}USDT", parts[1].to_uppercase()); // 添加USDT后缀
            let interval = parts[2].to_string();
            return Some((symbol, interval));
        }

        None
    }

    /// 预先创建所有需要的表
    #[perf_profile]
    fn ensure_all_tables(&self, symbols: &[String]) -> Result<()> {
        // --- 新增逻辑 (2.1) ---
        // 逻辑：添加计数器以统计新创建和已存在的表的数量。
        let mut created_count = 0;
        let mut existing_count = 0;

        // 获取已存在的表
        let existing_tables = self.get_existing_kline_tables()?;

        let mut existing_map = HashMap::new();
        for (symbol, interval) in existing_tables {
            existing_map.insert((symbol, interval), true);
        }

        // 为每个交易对和周期创建表
        for symbol in symbols {
            for interval in &self.intervals {
                // 检查表是否已存在
                if existing_map.contains_key(&(symbol.clone(), interval.clone())) {
                    // --- 修改逻辑 (2.2) ---
                    existing_count += 1;
                    continue;
                }

                self.db.ensure_symbol_table(symbol, interval)?;
                // --- 修改逻辑 (2.3) ---
                created_count += 1;
            }
        }

        info!(
            log_type = "low_freq",
            message = "数据库表结构准备完成",
            tables_created_now = created_count,
            tables_already_exist = existing_count,
            total_tables_ensured = created_count + existing_count,
        );

        Ok(())
    }

    /// 创建所有下载任务的主函数
    #[perf_profile]
    async fn create_all_download_tasks(&self, all_symbols: &[String]) -> Result<Vec<DownloadTask>> {
        let mut tasks = Vec::new();

        // 获取数据库中已存在的表信息
        let existing_tables = self.get_existing_kline_tables()?;
        let mut existing_symbol_intervals = HashMap::new();

        for (symbol, interval) in &existing_tables {
            existing_symbol_intervals
                .entry(symbol.clone())
                .or_insert_with(Vec::new)
                .push(interval.clone());
        }

        // 为新品种创建完整下载任务（先计算，避免借用冲突）
        let new_symbols: Vec<String> = all_symbols.iter()
            .filter(|symbol| !existing_symbol_intervals.contains_key(*symbol))
            .cloned()
            .collect();

        // 为已存在的品种创建补齐任务
        for (symbol, intervals) in existing_symbol_intervals {
            if !all_symbols.contains(&symbol) {
                continue;
            }
            for interval in intervals {
                let batch_tasks = self.create_task_for_existing_symbol(&symbol, &interval).await?;
                tasks.extend(batch_tasks);
            }
        }

        for symbol in new_symbols {
            for interval in &self.intervals {
                let batch_tasks = self.create_task_for_new_symbol(&symbol, interval).await?;
                tasks.extend(batch_tasks);
            }
        }

        Ok(tasks)
    }

    /// 为已存在的交易对创建补齐任务（支持分批下载）
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval))]
    async fn create_task_for_existing_symbol(&self, symbol: &str, interval: &str) -> Result<Vec<DownloadTask>> {
        let current_time = chrono::Utc::now().timestamp_millis();

        if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(symbol, interval)? {
            // 有数据的情况：创建补齐任务
            let interval_ms = crate::klcommon::api::interval_to_milliseconds(interval);

            // ✨ [修改] 关键修正：将下一次请求的 startTime 设置为本地最后一根K线的 open_time
            // 这将重新获取最后一根K线，以确保其数据的最终性和完整性，防止因数据未最终确定而导致的累积误差。
            // 原有逻辑: let start_time = last_timestamp + interval_ms;
            let start_time = last_timestamp;

            let aligned_start_time = get_aligned_time(start_time, interval);
            // 对于结束时间，使用当前时间而不对齐，确保能获取到最新数据
            let aligned_end_time = current_time;

            if aligned_start_time < aligned_end_time {
                // 计算需要多少个周期的数据
                let periods_needed = (aligned_end_time - aligned_start_time) / interval_ms;

                if periods_needed <= 1000 {
                    // 数据量不大，单次下载
                    Ok(vec![DownloadTask {
                        symbol: symbol.to_string(),
                        interval: interval.to_string(),
                        start_time: Some(aligned_start_time),
                        end_time: Some(aligned_end_time),
                        limit: 1000,
                    }])
                } else {
                    // 数据量大，需要分批下载
                    info!(
                        log_type = "low_freq",
                        message = "检测到大时间跨度，将分批下载",
                        symbol = symbol,
                        interval = interval,
                        periods_needed = periods_needed,
                        start_time = self.timestamp_to_datetime(aligned_start_time),
                        end_time = self.timestamp_to_datetime(aligned_end_time),
                    );

                    Ok(self.create_batched_download_tasks(symbol, interval, aligned_start_time, aligned_end_time, interval_ms))
                }
            } else {
                Ok(vec![]) // 不需要补齐
            }
        } else {
            // 表存在但无数据：创建完整下载任务
            let start_time = self.get_historical_start_time_for_full_download(interval);
            let aligned_start_time = get_aligned_time(start_time, interval);
            // 对于结束时间，使用当前时间而不对齐，确保能获取到最新数据
            let aligned_end_time = current_time;

            // 对于完整下载，也需要检查是否需要分批
            let interval_ms = crate::klcommon::api::interval_to_milliseconds(interval);
            let periods_needed = (aligned_end_time - aligned_start_time) / interval_ms;

            if periods_needed <= 1000 {
                Ok(vec![DownloadTask {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    start_time: Some(aligned_start_time),
                    end_time: Some(aligned_end_time),
                    limit: 1000,
                }])
            } else {
                info!(
                    log_type = "low_freq",
                    message = "新品种需要分批下载历史数据",
                    symbol = symbol,
                    interval = interval,
                    periods_needed = periods_needed,
                    start_time = self.timestamp_to_datetime(aligned_start_time),
                    end_time = self.timestamp_to_datetime(aligned_end_time),
                );

                Ok(self.create_batched_download_tasks(symbol, interval, aligned_start_time, aligned_end_time, interval_ms))
            }
        }
    }

    /// 创建分批下载任务
    fn create_batched_download_tasks(
        &self,
        symbol: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
        interval_ms: i64,
    ) -> Vec<DownloadTask> {
        let mut tasks = Vec::new();
        let max_periods_per_batch = 1000; // 每批最多1000个周期
        let batch_duration_ms = max_periods_per_batch * interval_ms;

        let mut current_start = start_time;
        let mut batch_number = 1;

        while current_start < end_time {
            // 计算当前批次的结束时间，确保不重叠
            // 使用 batch_duration_ms - 1 来定义一个完全包含在本批次内的、不重叠的闭合区间
            let current_end = std::cmp::min(current_start + batch_duration_ms - 1, end_time);

            // 安全检查：处理最后一次循环中可能出现的边界情况
            if current_end < current_start {
                break;
            }

            // 检查是否是最后一个批次
            let is_last_batch = current_end >= end_time || (current_start + batch_duration_ms) >= end_time;

            if is_last_batch {
                // 最后一个批次：不设置end_time，让API返回到当前最新时间的所有数据
                info!(
                    log_type = "low_freq",
                    message = "创建最后一个分批下载任务（无结束时间限制）",
                    symbol = symbol,
                    interval = interval,
                    batch_number = batch_number,
                    batch_start = self.timestamp_to_datetime(current_start),
                    note = "将获取到当前最新时间的所有数据",
                );

                tasks.push(DownloadTask {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    start_time: Some(current_start),
                    end_time: None, // 关键修改：最后一个批次不设置结束时间
                    limit: 1000,
                });
            } else {
                // 中间批次：设置明确的时间范围
                info!(
                    log_type = "low_freq",
                    message = "创建分批下载任务",
                    symbol = symbol,
                    interval = interval,
                    batch_number = batch_number,
                    batch_start = self.timestamp_to_datetime(current_start),
                    batch_end = self.timestamp_to_datetime(current_end),
                    estimated_periods = (current_end - current_start + 1) / interval_ms, // +1 因为是闭合区间
                    time_range_note = "使用不重叠的闭合区间避免重复下载",
                );

                tasks.push(DownloadTask {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    start_time: Some(current_start),
                    end_time: Some(current_end),
                    limit: 1000,
                });
            }

            // 下一个批次的起点直接增加一个完整的 batch_duration_ms
            // 这样 [t0, t0 + D - 1] 和 [t0 + D, t0 + 2D - 1] 这两个区间就是连续且不重叠的
            current_start = current_start + batch_duration_ms;
            batch_number += 1;
        }

        info!(
            log_type = "low_freq",
            message = "分批下载任务创建完成",
            symbol = symbol,
            interval = interval,
            total_batches = tasks.len(),
            last_batch_open_ended = true,
            time_span_start = self.timestamp_to_datetime(start_time),
            note = "最后一个批次将获取到当前最新时间的数据",
        );

        tasks
    }

    /// 为新品种创建完整下载任务
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval))]
    async fn create_task_for_new_symbol(&self, symbol: &str, interval: &str) -> Result<Vec<DownloadTask>> {
        let current_time = chrono::Utc::now().timestamp_millis();
        let start_time = self.get_historical_start_time_for_full_download(interval);

        let aligned_start_time = get_aligned_time(start_time, interval);
        // 对于结束时间，使用当前时间而不对齐，确保能获取到最新数据
        let aligned_end_time = current_time;

        // 检查是否需要分批下载
        let interval_ms = crate::klcommon::api::interval_to_milliseconds(interval);
        let periods_needed = (aligned_end_time - aligned_start_time) / interval_ms;

        if periods_needed <= 1000 {
            Ok(vec![DownloadTask {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time: Some(aligned_start_time),
                end_time: Some(aligned_end_time),
                limit: 1000,
            }])
        } else {
            Ok(self.create_batched_download_tasks(symbol, interval, aligned_start_time, aligned_end_time, interval_ms))
        }
    }

    /// 根据周期计算历史数据的起始时间
    fn calculate_historical_start_time(&self, current_time: i64, interval: &str) -> i64 {
        match interval {
            "1m" => current_time - 1000 * 60 * 1000, // 1000分钟
            "5m" => current_time - 5000 * 60 * 1000, // 5000分钟
            "30m" => current_time - 30000 * 60 * 1000, // 30000分钟
            "1h" => current_time - 1000 * 60 * 60 * 1000, // 1000小时
            "4h" => current_time - 4 * 1000 * 60 * 60 * 1000, // 4000小时
            "1d" => current_time - 1000 * 24 * 60 * 60 * 1000, // 1000天
            "1w" => current_time - 200 * 7 * 24 * 60 * 60 * 1000, // 200周
            _ => current_time - 1000 * 60 * 1000, // 默认1000分钟
        }
    }

    /// 根据周期为全量下载确定历史起点。
    /// 对于日线和周线，使用固定的"创世"日期；其他周期使用动态计算。
    fn get_historical_start_time_for_full_download(&self, interval: &str) -> i64 {
        match interval {
            "1d" | "1w" => {
                // 这是 2019-09-08 00:00:00 UTC 的毫秒时间戳。
                // 币安U本位永续合约大约在这个时间上线。
                1567900800000
            }
            _ => {
                // 对于其他周期，保持原有的动态计算逻辑
                let current_time = chrono::Utc::now().timestamp_millis();
                self.calculate_historical_start_time(current_time, interval)
            }
        }
    }





    /// 将毫秒时间戳转换为可读的日期时间格式
    fn timestamp_to_datetime(&self, timestamp_ms: i64) -> String {
        let dt = chrono::Utc.timestamp_millis(timestamp_ms);
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    /// 从数据库中获取所有品种和周期的最新K线数据，填充到latest_klines_map中
    fn populate_latest_klines_from_db(
        &self,
        symbols: &[String],
        latest_klines_map: Arc<Mutex<HashMap<(String, String), DbKline>>>
    ) -> Result<()> {
        let mut populated_count = 0;
        let mut missing_count = 0;

        for symbol in symbols {
            for interval in &self.intervals {
                // 获取该品种和周期的最新K线数据（获取1条最新的）
                match self.db.get_latest_klines(symbol, interval, 1) {
                    Ok(klines) if !klines.is_empty() => {
                        let latest_kline = &klines[0];
                        // 转换为DbKline格式
                        let db_kline = DbKline {
                            open_time: latest_kline.open_time,
                            open: latest_kline.open.clone(),
                            high: latest_kline.high.clone(),
                            low: latest_kline.low.clone(),
                            close: latest_kline.close.clone(),
                            volume: latest_kline.volume.clone(),
                            close_time: latest_kline.close_time,
                            quote_asset_volume: latest_kline.quote_asset_volume.clone(),
                            number_of_trades: latest_kline.number_of_trades,
                            taker_buy_base_asset_volume: latest_kline.taker_buy_base_asset_volume.clone(),
                            taker_buy_quote_asset_volume: latest_kline.taker_buy_quote_asset_volume.clone(),
                            ignore: latest_kline.ignore.clone(),
                        };

                        let mut guard = latest_klines_map.lock()
                            .map_err(|_| AppError::DataError("Mutex被污染".to_string()))?;
                        guard.insert((symbol.clone(), interval.clone()), db_kline);
                        populated_count += 1;
                    }
                    Ok(_) => {
                        // 表存在但没有数据
                        missing_count += 1;
                        warn!(
                            log_type = "low_freq",
                            message = "数据库中缺少K线数据",
                            symbol = symbol,
                            interval = interval,
                        );
                    }
                    Err(e) => {
                        // 表不存在或查询失败
                        missing_count += 1;
                        warn!(
                            log_type = "low_freq",
                            message = "获取最新K线数据失败",
                            symbol = symbol,
                            interval = interval,
                            error = %e,
                        );
                    }
                }
            }
        }

        info!(
            log_type = "low_freq",
            message = "从数据库获取最新K线数据完成",
            populated_count = populated_count,
            missing_count = missing_count,
            total_expected = symbols.len() * self.intervals.len(),
        );

        Ok(())
    }

    /// 清理已下架品种的数据
    async fn cleanup_delisted_symbols(&self) -> Result<usize> {
        info!(log_type = "low_freq", message = "开始检查并清理已下架品种的数据...");

        // ✨ [修改] 使用统一的重试方法获取完整的交易所信息
        const MAX_RETRIES: usize = 3;
        const RETRY_INTERVAL: u64 = 2;

        let exchange_info = {
            match BinanceApi::retry_get_exchange_info(
                MAX_RETRIES,
                RETRY_INTERVAL,
                "cleanup_delisted_symbols"
            ).await {
                Ok(info) => info,
                Err(e) => {
                    warn!(
                        log_type = "low_freq",
                        message = "无法获取交易所信息以进行清理，已达到最大重试次数，跳过本次清理",
                        max_retries = MAX_RETRIES,
                        error_chain = format!("{:#}", e),
                    );
                    return Ok(0); // 无法获取信息则不进行任何操作
                }
            }
        };

        // ✨ [修改] 从完整的品种信息中，精确地找出所有状态为 "CLOSE" 或 "SETTLING" 的品种
        let delisted_symbols: std::collections::HashSet<String> = exchange_info.symbols
            .iter()
            .filter(|s| s.status == "CLOSE" || s.status == "SETTLING")
            .map(|s| s.symbol.clone())
            .collect();

        if delisted_symbols.is_empty() {
            info!(log_type = "low_freq", message = "没有发现任何明确已下架(status=CLOSE或SETTLING)的品种");
            return Ok(0);
        }

        info!(
            log_type = "low_freq",
            message = "确认到已下架(status=CLOSE或SETTLING)的品种",
            delisted_count = delisted_symbols.len(),
            delisted_symbols = delisted_symbols.iter().cloned().collect::<Vec<_>>().join(", "),
        );

        // 获取数据库中所有已存在的K线表
        let all_existing_tables = self.get_existing_kline_tables()?;
        let mut cleaned_tables = 0;
        let conn = self.db.get_connection()?;

        // 遍历数据库中的表，如果对应的品种在下架列表中，则删除
        for (symbol_in_db, interval) in all_existing_tables {
            if delisted_symbols.contains(&symbol_in_db) {
                let table_name = format!("k_{}_{}", symbol_in_db.to_lowercase().replace("usdt", ""), interval);

                // 检查表是否存在（双重确认）
                let table_exists_query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
                let mut stmt = conn.prepare(table_exists_query)?;
                let exists = stmt.exists([&table_name])?;

                if exists {
                    let drop_query = format!("DROP TABLE IF EXISTS {}", table_name);
                    conn.execute(&drop_query, [])?;
                    cleaned_tables += 1;
                }
            }
        }

        if cleaned_tables > 0 {
            info!(
                log_type = "low_freq",
                message = "已下架品种数据清理完成",
                delisted_symbols_found = delisted_symbols.len(),
                cleaned_tables_count = cleaned_tables,
            );
        } else {
            info!(
                log_type = "low_freq",
                message = "已确认下架的品种在本地数据库中没有需要清理的数据表",
                delisted_symbols_count = delisted_symbols.len(),
            );
        }

        Ok(cleaned_tables)
    }

    /// 清理已下架品种的数据（使用已获取的交易所信息，避免重复网络请求）
    async fn cleanup_delisted_symbols_with_exchange_info(&self, exchange_info: Option<&crate::klcommon::models::ExchangeInfo>) -> Result<usize> {
        info!(log_type = "low_freq", message = "开始检查并清理已下架品种的数据...");

        // ✨ [优化] 使用已获取的交易所信息，避免重复网络请求
        let exchange_info = match exchange_info {
            Some(info) => info,
            None => {
                // 兜底：如果没有交易所信息，调用原来的函数
                return self.cleanup_delisted_symbols().await;
            }
        };

        // ✨ [修改] 从完整的品种信息中，精确地找出所有状态为 "CLOSE" 或 "SETTLING" 的品种
        let delisted_symbols: std::collections::HashSet<String> = exchange_info.symbols
            .iter()
            .filter(|s| s.status == "CLOSE" || s.status == "SETTLING")
            .map(|s| s.symbol.clone())
            .collect();

        if delisted_symbols.is_empty() {
            info!(log_type = "low_freq", message = "没有发现任何明确已下架(status=CLOSE或SETTLING)的品种");
            return Ok(0);
        }

        info!(
            log_type = "low_freq",
            message = "确认到已下架(status=CLOSE或SETTLING)的品种",
            delisted_count = delisted_symbols.len(),
            delisted_symbols = delisted_symbols.iter().cloned().collect::<Vec<_>>().join(", "),
        );

        // 获取数据库中所有已存在的K线表
        let all_existing_tables = self.get_existing_kline_tables()?;
        let mut cleaned_tables = 0;
        let conn = self.db.get_connection()?;

        // 遍历数据库中的表，如果对应的品种在下架列表中，则删除
        for (symbol_in_db, interval) in all_existing_tables {
            if delisted_symbols.contains(&symbol_in_db) {
                let table_name = format!("k_{}_{}", symbol_in_db.to_lowercase().replace("usdt", ""), interval);

                // 检查表是否存在（双重确认）
                let table_exists_query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
                let mut stmt = conn.prepare(table_exists_query)?;
                let exists = stmt.exists([&table_name])?;

                if exists {
                    let drop_query = format!("DROP TABLE IF EXISTS {}", table_name);
                    conn.execute(&drop_query, [])?;
                    cleaned_tables += 1;
                }
            }
        }

        if cleaned_tables > 0 {
            info!(
                log_type = "low_freq",
                message = "已下架品种数据清理完成",
                delisted_symbols_found = delisted_symbols.len(),
                cleaned_tables_count = cleaned_tables,
            );
        } else {
            info!(
                log_type = "low_freq",
                message = "已确认下架的品种在本地数据库中没有需要清理的数据表",
                delisted_symbols_count = delisted_symbols.len(),
            );
        }

        Ok(cleaned_tables)
    }

    /// 立即删除已下架品种的数据
    async fn delete_delisted_symbols_data(&self, delisted_symbols: &[String]) -> Result<usize> {
        let mut cleaned_tables = 0;
        let conn = self.db.get_connection()?;

        for symbol in delisted_symbols {
            for interval in &self.intervals {
                let table_name = format!("k_{}_{}", symbol.to_lowercase().replace("usdt", ""), interval);

                // 检查表是否存在
                let table_exists_query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
                let mut stmt = conn.prepare(table_exists_query)?;
                let exists = stmt.exists([&table_name])?;

                if exists {
                    let drop_query = format!("DROP TABLE IF EXISTS {}", table_name);
                    conn.execute(&drop_query, [])?;
                    cleaned_tables += 1;
                }
            }
        }

        Ok(cleaned_tables)
    }

    /// 从数据库加载所有品种和周期的最新K线数据
    #[perf_profile]
    pub async fn load_latest_klines_from_db(&self) -> Result<HashMap<(String, String), DbKline>> {
        info!(log_type = "low_freq", "开始从数据库加载所有最新K线状态...");
        let all_symbols = self.get_symbols_with_exchange_info(None).await?;
        let latest_klines_map = Arc::new(Mutex::new(HashMap::new()));

        self.populate_latest_klines_from_db(&all_symbols, latest_klines_map.clone())?;

        let final_map = Arc::try_unwrap(latest_klines_map)
            .map_err(|_| AppError::DataError("无法获取最新K线数据的独占访问权".to_string()))?
            .into_inner()
            .map_err(|_| AppError::DataError("Mutex被污染".to_string()))?;

        info!(log_type = "low_freq", "已从数据库加载 {} 条初始K线", final_map.len());
        Ok(final_map)
    }
}
