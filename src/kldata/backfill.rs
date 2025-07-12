use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, AppError};
use crate::klcommon::api::get_aligned_time;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use once_cell::sync::Lazy;
use futures::{stream, StreamExt};
use tracing::{info, warn}; // 确保导入了 info 和 warn 宏
use kline_macros::perf_profile; // 导入性能分析宏

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

// ✨ [新增] 用于生成唯一事务ID的全局原子计数器
static NEXT_TRANSACTION_ID: AtomicU64 = AtomicU64::new(1);

// 日志间隔，每30秒输出一次摘要
const BACKFILL_LOG_INTERVAL: u64 = 30;
const CONCURRENCY: usize = 50; // 并发数

// 任务执行结果的枚举，方便模式匹配
#[derive(Debug)]
enum TaskResult {
    Success(usize), // 成功，并返回写入的K线数量
    Failure {
        task: DownloadTask,
        error: AppError,
    },
}

/// K线数据补齐模块
#[derive(Debug)]
pub struct KlineBackfiller {
    db: Arc<Database>,
    api: BinanceApi,
    intervals: Vec<String>,
    test_mode: bool,
    test_symbols: Vec<String>,
}

impl KlineBackfiller {
    /// 创建新的K线补齐器实例
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        let api = BinanceApi::new();
        Self {
            db,
            api,
            intervals,
            test_mode: false,
            test_symbols: vec![],
        }
    }

    /// 创建测试模式的K线补齐器实例
    pub fn new_test_mode(db: Arc<Database>, intervals: Vec<String>, test_symbols: Vec<String>) -> Self {
        let api = BinanceApi::new();
        Self {
            db,
            api,
            intervals,
            test_mode: true,
            test_symbols,
        }
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
        let start_time = Instant::now();

        info!(
            log_type = "low_freq",
            message = "K线数据补齐任务启动",
            mode = if self.test_mode { "test" } else { "production" },
            intervals = self.intervals.join(","),
        );

        let all_symbols = self.get_symbols().await?;

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
            info!(log_type = "low_freq", message = "没有需要执行的补齐任务，程序正常结束。");
            return Ok(());
        }

        let failed_tasks = self.execute_tasks(tasks, "initial_download_loop").await;
        let mut final_failed_tasks_count = 0;

        if !failed_tasks.is_empty() {
            // 第一次重试
            let retry_tasks_1 = self.prepare_retry_tasks_with_round(&failed_tasks, Some(1));
            if !retry_tasks_1.is_empty() {
                let failed_tasks_after_retry_1 = self.execute_tasks(retry_tasks_1, "retry_download_loop_1").await;

                if !failed_tasks_after_retry_1.is_empty() {
                    // 第二次重试
                    let retry_tasks_2 = self.prepare_retry_tasks_with_round(&failed_tasks_after_retry_1, Some(2));
                    if !retry_tasks_2.is_empty() {
                        let final_failed_tasks = self.execute_tasks(retry_tasks_2, "retry_download_loop_2").await;
                        final_failed_tasks_count = final_failed_tasks.len();
                        if !final_failed_tasks.is_empty() {
                            self.report_final_failures(final_failed_tasks);
                        }
                    } else {
                        // 第二次重试没有可重试的任务，直接报告失败
                        final_failed_tasks_count = failed_tasks_after_retry_1.len();
                        self.report_final_failures(failed_tasks_after_retry_1);
                    }
                }
            } else {
                // 第一次重试没有可重试的任务，直接报告失败
                final_failed_tasks_count = failed_tasks.len();
                self.report_final_failures(failed_tasks);
            }
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

        Ok(())
    }

    /// 执行一批下载任务，并返回失败的任务列表
    async fn execute_tasks(&self, tasks: Vec<DownloadTask>, _loop_name: &str) -> Vec<(DownloadTask, AppError)> {
        let mut success_count = 0;
        let mut failed_tasks = Vec::new();

        let results = stream::iter(tasks)
            .map(|task| {
                let api = self.api.clone();
                let db = self.db.clone();
                async move {
                    // --- 修改点: 调用新的 instrumented 包装函数 ---
                    Self::process_single_task_instrumented(api, db, task).await
                }
            })
            .buffer_unordered(CONCURRENCY);

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

        failed_tasks
    }

    /// [新增] 对单个任务处理进行 instrument 的包装函数
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
    async fn process_single_task_instrumented(api: BinanceApi, db: Arc<Database>, task: DownloadTask) -> TaskResult {
        Self::process_single_task_with_perf(api, db, task).await
    }

    /// [新增] 带性能分析的单个任务处理函数
    #[perf_profile(skip_all, fields(symbol = %task.symbol, interval = %task.interval))]
    async fn process_single_task_with_perf(api: BinanceApi, db: Arc<Database>, task: DownloadTask) -> TaskResult {
        Self::process_single_task(api, db, task).await
    }

    /// 处理单个下载任务的核心逻辑
    async fn process_single_task(api: BinanceApi, db: Arc<Database>, task: DownloadTask) -> TaskResult {
        API_REQUEST_STATS.0.fetch_add(1, Ordering::SeqCst);

        let result: Result<usize> = async {
            let klines = match api.download_continuous_klines(&task).await {
                Ok(klines) => {
                    API_REQUEST_STATS.1.fetch_add(1, Ordering::SeqCst);
                    klines
                },
                Err(e) => {
                    warn!(
                        // log_type 会从父Span "high_freq" 继承
                        // ✨ [修改] 遵循深化错误记录规则
                        error_chain = format!("{:#}", e),
                        message = "从API下载K线失败"
                    );
                    return Err(e);
                }
            };

            if klines.is_empty() {
                return Ok(0);
            }

            match db.save_klines(&task.symbol, &task.interval, &klines, task.transaction_id).await {
                Ok(count) => {
                    Self::update_backfill_stats(&task.symbol, &task.interval, count);
                    Ok(count)
                },
                Err(e) => {
                    warn!(
                        // log_type 会从父Span "high_freq" 继承
                        // ✨ [修改] 遵循深化错误记录规则
                        error_chain = format!("{:#}", e),
                        message = "保存K线到数据库失败"
                    );
                    return Err(e);
                }
            }
        }.await;

        match result {
            Ok(count) => TaskResult::Success(count),
            Err(e) => {
                API_REQUEST_STATS.2.fetch_add(1, Ordering::SeqCst);
                TaskResult::Failure { task, error: e }
            }
        }
    }

    /// 从失败任务中筛选出需要重试的任务（带重试轮次信息）
    fn prepare_retry_tasks_with_round(&self, failed_tasks: &[(DownloadTask, AppError)], retry_round: Option<u32>) -> Vec<DownloadTask> {
        let retry_keywords = [
            "HTTP error", "timeout", "429", "Too Many Requests", "handshake", "connection", "network"
        ];

        let retry_tasks: Vec<DownloadTask> = failed_tasks.iter()
            .filter(|(_, error)| {
                let error_msg = error.to_string();
                retry_keywords.iter().any(|keyword| error_msg.contains(keyword))
            })
            .map(|(task, _)| task.clone())
            .collect();

        // ✨ [新增] 低频日志：记录关键决策的结果
        let message = match retry_round {
            Some(round) => format!("第{}次重试任务筛选完成", round),
            None => "重试任务筛选完成".to_string(),
        };

        info!(
            log_type = "low_freq",
            message = message,
            retry_round = retry_round.unwrap_or(0),
            total_failed_tasks = failed_tasks.len(),
            retryable_tasks_count = retry_tasks.len(),
        );

        retry_tasks
    }

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
    async fn get_symbols(&self) -> Result<Vec<String>> {
        if self.test_mode {
            return Ok(self.test_symbols.clone());
        }
        let symbols = self.api.get_trading_usdt_perpetual_symbols().await?;

        if symbols.is_empty() {
            let empty_error = AppError::ApiError("没有获取到交易对，无法继续。".to_string());
            return Err(empty_error);
        }

        Ok(symbols)
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
                if let Some(task) = self.create_task_for_existing_symbol(&symbol, &interval).await? {
                    tasks.push(task);
                }
            }
        }

        for symbol in new_symbols {
            for interval in &self.intervals {
                let task = self.create_task_for_new_symbol(&symbol, interval).await?;
                tasks.push(task);
            }
        }

        Ok(tasks)
    }

    /// 为已存在的交易对创建补齐任务
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval))]
    async fn create_task_for_existing_symbol(&self, symbol: &str, interval: &str) -> Result<Option<DownloadTask>> {
        let current_time = chrono::Utc::now().timestamp_millis();

        if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(symbol, interval)? {
            // 有数据的情况：创建补齐任务
            let interval_ms = crate::klcommon::api::interval_to_milliseconds(interval);
            let start_time = last_timestamp + interval_ms;

            let aligned_start_time = get_aligned_time(start_time, interval);
            let aligned_end_time = get_aligned_time(current_time, interval);

            if aligned_start_time < aligned_end_time {
                let transaction_id = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);

                Ok(Some(DownloadTask {
                    transaction_id,
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    start_time: Some(aligned_start_time),
                    end_time: Some(aligned_end_time),
                    limit: 1000,
                }))
            } else {
                Ok(None) // 不需要补齐
            }
        } else {
            // 表存在但无数据：创建完整下载任务
            let start_time = self.calculate_historical_start_time(current_time, interval);
            let aligned_start_time = get_aligned_time(start_time, interval);
            let aligned_end_time = get_aligned_time(current_time, interval);

            let transaction_id = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);

            Ok(Some(DownloadTask {
                transaction_id,
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time: Some(aligned_start_time),
                end_time: Some(aligned_end_time),
                limit: 1000,
            }))
        }
    }

    /// 为新品种创建完整下载任务
    #[perf_profile(skip_all, fields(symbol = %symbol, interval = %interval))]
    async fn create_task_for_new_symbol(&self, symbol: &str, interval: &str) -> Result<DownloadTask> {
        let current_time = chrono::Utc::now().timestamp_millis();
        let start_time = self.calculate_historical_start_time(current_time, interval);

        let aligned_start_time = get_aligned_time(start_time, interval);
        let aligned_end_time = get_aligned_time(current_time, interval);

        let transaction_id = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);

        Ok(DownloadTask {
            transaction_id,
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time: Some(aligned_start_time),
            end_time: Some(aligned_end_time),
            limit: 1000,
        })
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
}
