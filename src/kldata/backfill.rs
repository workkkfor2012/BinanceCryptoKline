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
const SECOND_ROUND_CONCURRENCY: usize = 60; // 第二次补齐并发数（3倍），因为下载的K线数量少

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
            Some(self.api.get_exchange_info().await?)
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

        // 根据轮次选择并发数：第一轮使用标准并发，第二轮及以后使用高并发
        let concurrency = if round == 1 { CONCURRENCY } else { SECOND_ROUND_CONCURRENCY };
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

        // --- 新增：定义重试参数 ---
        const MAX_RETRIES: u32 = 3; // 初始轮次 + 最多3轮重试

        // --- 重构后的任务执行与重试逻辑 ---
        // 执行第一轮下载，并将失败的任务收集起来
        let mut failed_tasks_with_errors = failed_tasks;

        // --- 新的、统一的重试循环 ---
        for i in 1..=MAX_RETRIES {
            if failed_tasks_with_errors.is_empty() {
                info!(log_type = "low_freq", message = "所有任务已成功，提前结束重试循环。");
                break; // 没有失败的任务，退出循环
            }

            info!(
                log_type = "low_freq",
                message = "开始进行重试轮次",
                retry_round = i,
                tasks_to_retry = failed_tasks_with_errors.len(),
                note = "等待本轮执行完成，以实现天然退避。",
            );

            // 从失败任务元组中提取出 DownloadTask 列表用于重试
            let retry_tasks: Vec<DownloadTask> = failed_tasks_with_errors.iter().map(|(task, _)| task.clone()).collect();

            let retry_start = Instant::now();
            // 重新执行失败的任务，并用新的失败列表覆盖旧的
            failed_tasks_with_errors = self.execute_tasks_with_concurrency(retry_tasks.clone(), &format!("retry_download_loop_{}", i), concurrency).await;
            let retry_elapsed = retry_start.elapsed();

            info!(
                log_type = "low_freq",
                message = "重试任务完成统计",
                retry_round = i,
                retry_tasks_count = retry_tasks.len(),
                retry_success_count = retry_tasks.len() - failed_tasks_with_errors.len(),
                retry_failed_count = failed_tasks_with_errors.len(),
                retry_elapsed_seconds = retry_elapsed.as_secs_f64(),
                retry_success_rate = if retry_tasks.is_empty() { "N/A".to_string() } else { format!("{:.1}%", ((retry_tasks.len() - failed_tasks_with_errors.len()) as f64 / retry_tasks.len() as f64) * 100.0) },
            );
        }

        // 循环结束后，仍然失败的任务被视为最终失败
        let final_failed_tasks_count = failed_tasks_with_errors.len();
        if !failed_tasks_with_errors.is_empty() {
            self.report_final_failures(failed_tasks_with_errors);
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

        // ✨ [新增验证] 检查所有品种的所有周期是否都补齐到最新
        info!(log_type = "low_freq", message = "开始验证所有品种的所有周期是否都补齐到最新...");
        match self.verify_backfill_completeness().await {
            Ok(()) => {
                info!(log_type = "low_freq", message = "补齐完整性验证通过，所有品种的所有周期都已补齐到最新");
            }
            Err(e) => {
                warn!(log_type = "low_freq", message = "补齐完整性验证发现问题", error = %e);
                // 注意：这里不返回错误，只是警告，因为可能存在一些品种暂时无法获取最新数据的情况
            }
        }

        Ok(())
    }

    /// 执行一批下载任务，并返回失败的任务列表
    async fn execute_tasks(&self, tasks: Vec<DownloadTask>, _loop_name: &str) -> Vec<(DownloadTask, AppError)> {
        self.execute_tasks_with_concurrency(tasks, _loop_name, CONCURRENCY).await
    }

    /// 执行一批下载任务，并返回失败的任务列表（可指定并发数）
    async fn execute_tasks_with_concurrency(&self, tasks: Vec<DownloadTask>, _loop_name: &str, concurrency: usize) -> Vec<(DownloadTask, AppError)> {
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



            match db.save_klines(&task.symbol, &task.interval, &klines).await {
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

        // 如果已经有交易所信息，直接使用；否则调用API获取
        let (trading_symbols, delisted_symbols) = if let Some(info) = exchange_info {
            // ✨ [优化] 使用已获取的交易所信息，避免重复网络请求
            self.parse_symbols_from_exchange_info(info)
        } else {
            // 兜底：如果没有交易所信息，仍然调用API
            self.api.get_trading_usdt_perpetual_symbols().await?
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

    /// 验证补齐完整性：检查所有品种的所有周期是否都补齐到最新
    /// 使用BTCUSDT作为标准，检查其他品种是否与BTCUSDT的最新时间戳一致
    async fn verify_backfill_completeness(&self) -> Result<()> {
        info!(log_type = "low_freq", message = "开始验证补齐完整性，使用BTCUSDT作为标准...");

        // 获取当前活跃的交易对列表
        let (active_symbols, _delisted_symbols) = self.api.get_trading_usdt_perpetual_symbols().await?;

        // 获取数据库中所有已存在的K线表
        let all_existing_tables = self.get_existing_kline_tables()?;

        // 只验证当前活跃的交易对
        let existing_tables: Vec<(String, String)> = all_existing_tables
            .into_iter()
            .filter(|(symbol, _interval)| active_symbols.contains(symbol))
            .collect();

        info!(
            log_type = "low_freq",
            message = "过滤后的验证范围",
            active_symbols_count = active_symbols.len(),
            tables_to_verify = existing_tables.len(),
        );

        if existing_tables.is_empty() {
            return Err(AppError::InitializationError("数据库中没有找到任何K线表".to_string()));
        }

        // 按周期分组存储最后一根K线的时间戳
        let mut interval_timestamps = std::collections::HashMap::new();

        // 遍历所有表，获取最后一根K线的时间戳
        for (symbol, interval) in &existing_tables {
            if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(symbol, interval)? {
                interval_timestamps
                    .entry(interval.clone())
                    .or_insert_with(std::collections::HashMap::new)
                    .insert(symbol.clone(), last_timestamp);
            }
        }

        // 使用BTCUSDT作为标准检查所有品种的时间戳
        let reference_symbol = "BTCUSDT";
        let mut verification_errors = Vec::new();
        let mut total_symbols_checked = 0;
        let mut inconsistent_symbols_count = 0;

        // 获取所有周期并排序
        let mut intervals: Vec<String> = interval_timestamps.keys().cloned().collect();
        intervals.sort();

        for interval in &intervals {
            if let Some(symbols_data) = interval_timestamps.get(interval) {
                // 检查BTCUSDT是否存在于此周期
                if let Some(&btc_timestamp) = symbols_data.get(reference_symbol) {
                    let btc_datetime = self.timestamp_to_datetime(btc_timestamp);

                    // 统计与BTCUSDT时间戳不一致的品种
                    let mut inconsistent_symbols = Vec::new();

                    for (symbol, &timestamp) in symbols_data {
                        if symbol == reference_symbol {
                            continue;
                        }
                        total_symbols_checked += 1;

                        if timestamp != btc_timestamp {
                            inconsistent_symbols.push((symbol.clone(), timestamp));
                            inconsistent_symbols_count += 1;
                        }
                    }

                    if !inconsistent_symbols.is_empty() {
                        let error_msg = format!(
                            "周期 {} 中有 {} 个品种的时间戳与 {} 不一致 (标准时间: {})",
                            interval,
                            inconsistent_symbols.len(),
                            reference_symbol,
                            btc_datetime
                        );
                        verification_errors.push(error_msg);

                        // 为每个不一致的品种单独输出日志
                        for (symbol, timestamp) in &inconsistent_symbols {
                            let timestamp_str = self.timestamp_to_datetime(*timestamp);
                            let time_diff_hours = (btc_timestamp - timestamp) / (1000 * 60 * 60);
                            let time_diff_desc = if time_diff_hours > 0 {
                                format!("落后{}小时", time_diff_hours)
                            } else if time_diff_hours < 0 {
                                format!("超前{}小时", -time_diff_hours)
                            } else {
                                "时间相同但毫秒不同".to_string()
                            };

                            warn!(
                                log_type = "low_freq",
                                message = "品种时间戳不一致",
                                symbol = symbol,
                                interval = interval,
                                symbol_timestamp = timestamp_str,
                                reference_symbol = reference_symbol,
                                reference_timestamp = btc_datetime,
                                time_difference = time_diff_desc,
                            );
                        }
                    } else {
                        info!(
                            log_type = "low_freq",
                            message = "周期验证通过",
                            interval = interval,
                            symbols_count = symbols_data.len() - 1, // 减去BTCUSDT本身
                            reference_timestamp = btc_datetime,
                        );
                    }
                } else {
                    let error_msg = format!("周期 {} 中没有找到标准品种 {} 的数据", interval, reference_symbol);
                    verification_errors.push(error_msg);
                    warn!(
                        log_type = "low_freq",
                        message = "标准品种缺失",
                        interval = interval,
                        reference_symbol = reference_symbol,
                    );
                }
            }
        }

        // 输出验证结果摘要
        info!(
            log_type = "low_freq",
            message = "补齐完整性验证结果摘要",
            total_intervals_checked = intervals.len(),
            total_symbols_checked = total_symbols_checked,
            inconsistent_symbols_count = inconsistent_symbols_count,
            verification_errors_count = verification_errors.len(),
            consistency_rate = if total_symbols_checked > 0 {
                format!("{:.1}%", ((total_symbols_checked - inconsistent_symbols_count) as f64 / total_symbols_checked as f64) * 100.0)
            } else {
                "N/A".to_string()
            },
        );

        if !verification_errors.is_empty() {
            let combined_error = format!(
                "补齐完整性验证发现 {} 个问题：{}",
                verification_errors.len(),
                verification_errors.join("; ")
            );
            return Err(AppError::InitializationError(combined_error));
        }

        Ok(())
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

        // ✨ [修改] 关键修正：直接获取完整的交易所信息，而不是依赖过滤后的活跃列表
        let exchange_info = match self.api.get_exchange_info().await {
            Ok(info) => info,
            Err(e) => {
                warn!(
                    log_type = "low_freq",
                    message = "无法获取交易所信息以进行清理，跳过本次清理",
                    error = %e
                );
                return Ok(0); // 无法获取信息则不进行任何操作
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
