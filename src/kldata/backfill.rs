use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, AppError};
use crate::klcommon::api::get_aligned_time;
use tracing::{info, warn, error, instrument, Instrument};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use once_cell::sync::Lazy;
use futures::{stream, StreamExt};

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
    #[instrument]
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        let api = BinanceApi::new();
        tracing::debug!(decision = "backfiller_mode", mode = "production", interval_count = intervals.len(), "创建生产模式补齐器");
        Self {
            db,
            api,
            intervals,
            test_mode: false,
            test_symbols: vec![],
        }
    }

    /// 创建测试模式的K线补齐器实例
    #[instrument]
    pub fn new_test_mode(db: Arc<Database>, intervals: Vec<String>, test_symbols: Vec<String>) -> Self {
        let api = BinanceApi::new();
        tracing::debug!(decision = "backfiller_mode", mode = "test", interval_count = intervals.len(), test_symbol_count = test_symbols.len(), "创建测试模式补齐器");
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
                // 构建简洁的摘要信息，不包含详细的交易对信息
                let summary = format!("补齐K线摘要 ({}秒): 总计 {} 条K线",
                    BACKFILL_LOG_INTERVAL, total_count);

                // 输出日志
                info!(log_type = "module", "{}", summary);
            }

            // 清空交易对计数器
            symbol_map.clear();

            // 更新最后日志时间
            *last_log_time = now;
        } else {
            // 如果不需要输出日志，则输出调试日志

        }
    }

    /// 运行一次性补齐流程
    #[instrument(name = "backfill_run_once", ret, err)]
    pub async fn run_once(&self) -> Result<()> {
        // ✨ [检查点] 流程开始
        tracing::info!(log_type = "checkpoint", message = "Backfill process started.");

        info!(log_type = "module", "开始一次性补齐K线数据...");
        let start_time = Instant::now();

        // 步骤 1 & 2: 获取交易对并准备表
        let all_symbols = self.get_symbols().await?;
        tracing::debug!(decision = "symbols_obtained", symbol_count = all_symbols.len(), test_mode = self.test_mode, "获取交易对列表完成");

        // ✨ [检查点] 交易对获取完成
        tracing::info!(log_type = "checkpoint", message = "Symbol acquisition complete.", count = all_symbols.len());

        info!(log_type = "module", "🗄️ 开始准备数据库表结构1...");
        self.ensure_all_tables(&all_symbols)?;
        info!(log_type = "module", "✅ 数据库表结构准备完成");
        tracing::debug!(decision = "tables_prepared", symbol_count = all_symbols.len(), interval_count = self.intervals.len(), "数据库表准备完成");

        // 步骤 3: 创建任务
        info!(log_type = "module", "📋 开始创建下载任务...");
        let tasks = self.create_all_download_tasks(&all_symbols).await?;
        if tasks.is_empty() {
            info!(log_type = "module", "✅ 所有数据都是最新的，无需补齐");
            tracing::debug!(decision = "no_backfill_needed", "所有数据都是最新的，无需补齐");
            return Ok(());
        }
        info!(log_type = "module", "📋 已创建 {} 个下载任务", tasks.len());
        tracing::debug!(decision = "tasks_created", task_count = tasks.len(), "下载任务创建完成");

        // ✨ [检查点] 任务创建完成
        tracing::info!(log_type = "checkpoint", message = "Download task creation complete.", count = tasks.len());

        // 步骤 4: 执行第一轮下载
        info!(log_type = "module", "开始第一轮下载，共 {} 个任务...", tasks.len());
        let failed_tasks = self.execute_tasks(tasks, "initial_download_loop").await;

        // ✨ [检查点] 初始任务执行完成
        tracing::info!(
            log_type = "checkpoint",
            message = "Initial task execution complete.",
            failed_count = failed_tasks.len()
        );

        // 步骤 5: 如果有失败，执行重试
        if !failed_tasks.is_empty() {
            tracing::debug!(decision = "retry_needed", failed_count = failed_tasks.len(), "检测到失败任务，准备重试");
            let retry_tasks = self.prepare_retry_tasks(&failed_tasks);
            if !retry_tasks.is_empty() {
                // ✨ [检查点] 重试阶段开始
                tracing::info!(log_type = "checkpoint", message = "Retry phase started.", count = retry_tasks.len());

                info!(log_type = "module", "开始重试 {} 个失败任务...", retry_tasks.len());
                tracing::debug!(decision = "retry_execution", retry_count = retry_tasks.len(), "开始执行重试任务");
                let final_failed_tasks = self.execute_tasks(retry_tasks, "retry_download_loop").await;
                if !final_failed_tasks.is_empty() {
                    tracing::debug!(decision = "final_failures", final_failed_count = final_failed_tasks.len(), "重试后仍有失败任务");
                    self.report_final_failures(final_failed_tasks);
                } else {
                    tracing::debug!(decision = "retry_success", "所有重试任务都成功");
                }
            } else {
                tracing::debug!(decision = "no_retry", reason = "no_retryable_tasks", "没有可重试的任务");
            }
        } else {
            tracing::debug!(decision = "no_retry", reason = "no_failures", "所有任务都成功，无需重试");
        }

        tracing::debug!(decision = "backfill_complete", "补齐流程完成");

        // ✨ [检查点] 流程结束
        tracing::info!(log_type = "checkpoint", message = "Backfill process finished.");

        self.report_summary(start_time);
        Ok(())
    }

    /// 执行一批下载任务，并返回失败的任务列表
    #[instrument(skip(self, tasks), fields(task_count = tasks.len(), loop_name = %loop_name, concurrency = CONCURRENCY), ret)]
    async fn execute_tasks(&self, tasks: Vec<DownloadTask>, loop_name: &str) -> Vec<(DownloadTask, AppError)> {
        let task_count = tasks.len();
        let start_time = Instant::now();

        let results = stream::iter(tasks)
            .map(|task| {
                let api = self.api.clone();
                let db = self.db.clone();
                // 将任务处理逻辑封装在一个 future 中
                async move {
                    Self::process_single_task(api, db, task).await
                }
            })
            .buffer_unordered(CONCURRENCY); // 以指定并发数执行

        let mut success_count = 0;
        let mut failed_tasks = Vec::new();

        // 为并发循环创建专用的Span - 必须以_loop结尾供TraceDistiller识别
        let processing_span = match loop_name {
            "initial_download_loop" => tracing::info_span!(
                "initial_download_loop",
                iterator_type = "download_task",
                task_count = task_count,
                concurrency = CONCURRENCY
            ),
            "retry_download_loop" => tracing::info_span!(
                "retry_download_loop",
                iterator_type = "retry_task",
                task_count = task_count,
                concurrency = CONCURRENCY
            ),
            _ => tracing::info_span!(
                "download_loop",
                iterator_type = "download_task",
                task_count = task_count,
                concurrency = CONCURRENCY
            ),
        };

        tracing::debug!(decision = "concurrent_execution_start", loop_name = %loop_name, task_count = task_count, concurrency = CONCURRENCY, "开始并发执行任务");

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
            .instrument(processing_span)
            .await;

        let elapsed = start_time.elapsed();
        info!(
            log_type = "module",
            "[{}] 完成。成功: {}, 失败: {}, 耗时: {:.2?}",
            loop_name, success_count, failed_tasks.len(), elapsed
        );

        tracing::debug!(
            decision = "concurrent_execution_complete",
            loop_name = %loop_name,
            success_count = success_count,
            failed_count = failed_tasks.len(),
            total_tasks = task_count,
            elapsed_ms = elapsed.as_millis(),
            "并发执行完成"
        );

        // ✨ [业务逻辑断言] 检查失败率
        let failure_rate = if task_count > 0 { failed_tasks.len() as f32 / task_count as f32 } else { 0.0 };
        const MAX_FAILURE_RATE: f32 = 0.1; // 失败率不应超过10%

        crate::soft_assert!(
            failure_rate <= MAX_FAILURE_RATE,
            message = "任务失败率过高。",
            loop_name = loop_name.to_string(),
            failure_rate = failure_rate,
            failed_count = failed_tasks.len(),
            total_count = task_count,
        );

        failed_tasks
    }

    /// 处理单个下载任务的核心逻辑
    #[instrument(name = "download_kline_task", skip_all, fields(symbol = %task.symbol, interval = %task.interval, limit = task.limit, start_time = ?task.start_time, end_time = ?task.end_time, transaction_id = task.transaction_id), ret)]
    async fn process_single_task(api: BinanceApi, db: Arc<Database>, task: DownloadTask) -> TaskResult {
        // 埋点：任务处理开始
        tracing::info!(
            log_type = "transaction",
            transaction_id = task.transaction_id,
            event_name = "processing_start",
            symbol = %task.symbol,
            interval = %task.interval,
        );

        API_REQUEST_STATS.0.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(decision = "download_start", symbol = %task.symbol, interval = %task.interval, "开始下载K线任务");

        let result: Result<usize> = async {
            let klines = api.download_continuous_klines(&task).await?;
            API_REQUEST_STATS.1.fetch_add(1, Ordering::SeqCst);
            tracing::debug!(decision = "download_success", symbol = %task.symbol, interval = %task.interval, kline_count = klines.len(), "API下载成功");

            if klines.is_empty() {
                warn!("{}/{}: API返回空结果，跳过", task.symbol, task.interval);
                tracing::debug!(decision = "empty_response", symbol = %task.symbol, interval = %task.interval, "API返回空结果，跳过处理");
                return Ok(0); // 空结果不是错误，但也没有写入
            }

            // ✨ 优化：小批量数据延迟写入，避免频繁的小事务
            const MIN_BATCH_SIZE: usize = 50; // 最小批量大小阈值

            if klines.len() < MIN_BATCH_SIZE {
                tracing::debug!(
                    decision = "small_batch_detected",
                    symbol = %task.symbol,
                    interval = %task.interval,
                    kline_count = klines.len(),
                    min_batch_size = MIN_BATCH_SIZE,
                    "检测到小批量数据，直接写入（可能导致性能警告）"
                );

        
            }

            tracing::debug!(decision = "save_klines", symbol = %task.symbol, interval = %task.interval, kline_count = klines.len(), "开始保存K线数据");
            let count = db.save_klines(&task.symbol, &task.interval, &klines, task.transaction_id).await?;
            Self::update_backfill_stats(&task.symbol, &task.interval, count);
            tracing::debug!(decision = "save_success", symbol = %task.symbol, interval = %task.interval, saved_count = count, "K线数据保存成功");
            Ok(count)
        }.await;

        match result {
            Ok(count) => {
                // 埋点：任务处理成功
                tracing::info!(
                    log_type = "transaction",
                    transaction_id = task.transaction_id,
                    event_name = "processing_success",
                    saved_kline_count = count,
                );
                tracing::debug!(decision = "task_success", symbol = %task.symbol, interval = %task.interval, saved_count = count, "任务成功完成");
                TaskResult::Success(count)
            },
            Err(e) => {
                // 埋点：任务处理失败
                tracing::info!(
                    log_type = "transaction",
                    transaction_id = task.transaction_id,
                    event_name = "processing_failure",
                    error.summary = e.get_error_type_summary(),
                    error.details = %e,
                );
                API_REQUEST_STATS.2.fetch_add(1, Ordering::SeqCst);
                error!("{}/{}: 任务失败: {}", task.symbol, task.interval, e);
                tracing::error!(
                    message = "下载任务失败",
                    symbol = %task.symbol,
                    interval = %task.interval,
                    error.summary = e.get_error_type_summary(),
                    error.details = %e
                );
                TaskResult::Failure { task, error: e }
            }
        }
    }

    /// 从失败任务中筛选出需要重试的任务
    #[instrument(skip(self, failed_tasks), fields(failed_count = failed_tasks.len()), ret)]
    fn prepare_retry_tasks(&self, failed_tasks: &[(DownloadTask, AppError)]) -> Vec<DownloadTask> {
        let retry_keywords = [
            "HTTP error", "timeout", "429", "Too Many Requests", "handshake", "connection", "network"
        ];
        info!("将重试包含以下关键词的错11误: {:?}", retry_keywords);

        let retry_tasks: Vec<DownloadTask> = failed_tasks.iter()
            .filter(|(_, error)| {
                let error_msg = error.to_string();
                retry_keywords.iter().any(|keyword| error_msg.contains(keyword))
            })
            .map(|(task, _)| task.clone())
            .collect();

        // 报告不可重试的错误
        let non_retry_count = failed_tasks.len() - retry_tasks.len();
        if non_retry_count > 0 {
            warn!(log_type = "module", "⚠️ {} 个任务因不可重试的错误（如数据库错误、数据解析错误）被永久放弃", non_retry_count);
            tracing::warn!(decision = "non_retryable_failures", non_retry_count = non_retry_count, total_failed = failed_tasks.len(), "发现不可重试的失败任务");
        }

        tracing::debug!(decision = "retry_tasks_prepared", retry_count = retry_tasks.len(), non_retry_count = non_retry_count, "重试任务准备完成");
        retry_tasks
    }

    /// 报告最终无法完成的任务
    #[instrument(skip(self, final_failures), fields(final_failure_count = final_failures.len()))]
    fn report_final_failures(&self, final_failures: Vec<(DownloadTask, AppError)>) {
        error!(log_type = "module", "❌ 重试后仍有 {} 个任务最终失败11，需要人工检查", final_failures.len());
        let mut error_summary: HashMap<String, usize> = HashMap::new();

        for (task, error) in final_failures.iter().take(10) { // 只打印前10个的详情
            let error_msg = error.to_string();
            let error_type = error_msg.split(':').next().unwrap_or("Unknown Error").trim();
            *error_summary.entry(error_type.to_string()).or_default() += 1;
            error!("  - {}/{}: {}", task.symbol, task.interval, error_msg);
        }

        if final_failures.len() > 10 {
            error!("  ... 以及其他 {} 个失败任务。", final_failures.len() - 10);
        }

        error!(log_type = "module", "最终失败任务摘要 - 需要人工干预:");
        for (err_type, count) in error_summary {
            error!(log_type = "module", "  - {}: {} 次", err_type, count);
        }
    }

    /// 报告最终的运行摘要
    fn report_summary(&self, start_time: Instant) {
        let elapsed = start_time.elapsed();
        let total_requests = API_REQUEST_STATS.0.load(Ordering::SeqCst);
        let successful_requests = API_REQUEST_STATS.1.load(Ordering::SeqCst);
        let failed_requests = API_REQUEST_STATS.2.load(Ordering::SeqCst);
        let total_klines = BACKFILL_STATS.0.load(Ordering::Relaxed);

        info!(log_type = "module", "================ K线补齐运行摘要 ================");
        info!(log_type = "module", "✅ K线补齐全部完成，总耗时: {:.2?}", elapsed);
        info!(log_type = "module", "📊 总计补齐K线: {} 条", total_klines);
        info!(log_type = "module", "🌐 API请求统计: 总计 {}, 成功 {}, 失败 {}", total_requests, successful_requests, failed_requests);
        info!(log_type = "module", "==============================================");
    }

    #[instrument(skip(self), ret, err)]
    async fn get_symbols(&self) -> Result<Vec<String>> {
        if self.test_mode {
            info!(log_type = "module", "🔧 测试模式，使用1预设交易对: {:?}", self.test_symbols);
            tracing::debug!(decision = "symbol_source", source = "test_mode", symbols = ?self.test_symbols, "使用测试模式预设交易对");
            return Ok(self.test_symbols.clone());
        }
        info!(log_type = "module", "📡 获取所有正在交易的U本位永续合约交易对...");
        tracing::debug!(decision = "symbol_source", source = "api", "从API获取交易对列表");
        let symbols = self.api.get_trading_usdt_perpetual_symbols().await?;
        info!(log_type = "module", "✅ 获取到 {} 个交易对", symbols.len());

        if symbols.is_empty() {
            let empty_error = AppError::ApiError("没有获取到交易对，无法继续。".to_string());
            tracing::error!(
                message = "API返回空交易对列表",
                error.summary = empty_error.get_error_type_summary(),
                error.details = %empty_error
            );
            return Err(empty_error);
        }

        // ✨ [业务逻辑断言] 检查交易对数量
        let symbol_count = symbols.len();
        const MIN_EXPECTED_SYMBOLS: usize = 400;

        crate::soft_assert!(
            symbol_count >= MIN_EXPECTED_SYMBOLS,
            message = "获取到的交易对数量远低于预期。",
            expected_min = MIN_EXPECTED_SYMBOLS,
            actual_count = symbol_count,
        );

        Ok(symbols)
    }

    /// 获取数据库中已存在的K线表
    #[instrument(ret, err)]
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
    #[instrument(skip(self, symbols), fields(total_symbols = symbols.len(), total_intervals = self.intervals.len()), ret, err)]
    fn ensure_all_tables(&self, symbols: &[String]) -> Result<()> {
        let total_expected = symbols.len() * self.intervals.len();
        info!(log_type = "module", "🗄️ 开始预先创建数据库表，共 {} 个交易对 × {} 个周期 = {} 个表",
              symbols.len(), self.intervals.len(), total_expected);

        let mut created_count = 0;
        let mut existing_count = 0;

        // 获取已存在的表
        let existing_tables = self.get_existing_kline_tables()?;

        // ✨ [状态快照 - 操作前]
        tracing::info!(
            log_type = "snapshot",
            name = "db_tables_before_creation",
            state = "before",
            existing_count = existing_tables.len(),
            expected_total = total_expected
        );

        let mut existing_map = HashMap::new();
        for (symbol, interval) in existing_tables {
            existing_map.insert((symbol, interval), true);
        }

        // ✨ [关键修复] 为表创建循环创建一个专用的、符合约定的 Span
        let table_creation_loop_span = tracing::info_span!(
            "table_creation_loop",      // 名字必须以 _loop 结尾！
            iterator_type = "table_config",
            task_count = total_expected,
            concurrency = 1             // 这是一个串行循环
        );
        // 进入这个 Span 的上下文，后续所有操作都将成为它的子节点
        let _enter = table_creation_loop_span.enter();

        // 为每个交易对和周期创建表
        for symbol in symbols {
            for interval in &self.intervals {
                // 检查表是否已存在
                if existing_map.contains_key(&(symbol.clone(), interval.clone())) {
                    existing_count += 1;
                    continue;
                }

                // 创建表的操作现在是 "table_creation_loop" 的子节点
                self.db.ensure_symbol_table(symbol, interval)?;
                created_count += 1;

                // 每创建100个表输出一次日志 (调整频率避免刷屏)
                if created_count % 100 == 0 {
                    //debug!("已创建 {} 个表，跳过 {} 个已存在的表", created_count, existing_count);
                }
            }
        }

        info!(log_type = "module", "✅ 数据库表创建完成，新创建 {} 个表，跳过 {} 个已存在的表", created_count, existing_count);

        // ✨ [状态快照 - 操作后]
        tracing::info!(
            log_type = "snapshot",
            name = "db_tables_after_creation",
            state = "after",
            created_count = created_count,
            skipped_count = existing_count,
            final_total = created_count + existing_count
        );

        Ok(())
    }

    /// 创建所有下载任务的主函数
    #[instrument(skip(self, all_symbols), fields(all_symbols_count = all_symbols.len(), total_intervals = self.intervals.len()), ret, err)]
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

        tracing::debug!(decision = "task_creation_analysis", existing_symbols = existing_symbol_intervals.len(), new_symbols = new_symbols.len(), "任务创建分析完成");

        // ✨ [关键修复] 为已存在品种的补齐任务创建循环创建专用 Span
        let existing_task_creation_loop_span = tracing::info_span!(
            "existing_task_creation_loop",
            iterator_type = "existing_symbol_interval",
            task_count = existing_symbol_intervals.len(),
            concurrency = 1
        );
        let _enter_existing = existing_task_creation_loop_span.enter();

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

        // 退出已存在品种任务创建的 span
        drop(_enter_existing);

        // ✨ [关键修复] 为新品种的完整下载任务创建循环创建专用 Span
        let new_task_creation_loop_span = tracing::info_span!(
            "new_task_creation_loop",
            iterator_type = "new_symbol_interval",
            task_count = new_symbols.len() * self.intervals.len(),
            concurrency = 1
        );
        let _enter_new = new_task_creation_loop_span.enter();

        for symbol in new_symbols {
            for interval in &self.intervals {
                let task = self.create_task_for_new_symbol(&symbol, interval).await?;
                tasks.push(task);
                tracing::debug!(decision = "full_download_task_created_for_new_symbol", symbol = %symbol, interval = %interval, "为新交易对创建完整下载任务");
            }
        }

        // 退出新品种任务创建的 span
        drop(_enter_new);

        tracing::debug!(decision = "task_creation_complete", total_tasks = tasks.len(), "所有下载任务创建完成");
        Ok(tasks)
    }

    /// 为已存在的交易对创建补齐任务
    #[instrument(skip(self), fields(symbol = %symbol, interval = %interval), ret, err)]
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

                // 埋点：任务创建事件
                tracing::info!(
                    log_type = "transaction",
                    transaction_id,
                    event_name = "task_created",
                    reason = "backfill_for_existing_symbol",
                    symbol = %symbol,
                    interval = %interval,
                );

                tracing::debug!(decision = "backfill_task_created", symbol = %symbol, interval = %interval, start_time = aligned_start_time, end_time = aligned_end_time, "为现有交易对创建补齐任务");
                Ok(Some(DownloadTask {
                    transaction_id,
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    start_time: Some(aligned_start_time),
                    end_time: Some(aligned_end_time),
                    limit: 1000,
                }))
            } else {
                tracing::debug!(decision = "no_backfill_needed_for_symbol", symbol = %symbol, interval = %interval, "交易对数据已是最新，无需补齐");
                Ok(None) // 不需要补齐
            }
        } else {
            // 表存在但无数据：创建完整下载任务
            let start_time = self.calculate_historical_start_time(current_time, interval);
            let aligned_start_time = get_aligned_time(start_time, interval);
            let aligned_end_time = get_aligned_time(current_time, interval);

            let transaction_id = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);

            // 埋点：任务创建事件
            tracing::info!(
                log_type = "transaction",
                transaction_id,
                event_name = "task_created",
                reason = "full_download_for_empty_table",
                symbol = %symbol,
                interval = %interval,
            );

            tracing::debug!(decision = "full_download_task_created_for_empty_table", symbol = %symbol, interval = %interval, start_time = aligned_start_time, end_time = aligned_end_time, "表存在但无数据，创建完整下载任务");
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
    #[instrument(skip(self), fields(symbol = %symbol, interval = %interval), ret, err)]
    async fn create_task_for_new_symbol(&self, symbol: &str, interval: &str) -> Result<DownloadTask> {
        let current_time = chrono::Utc::now().timestamp_millis();
        let start_time = self.calculate_historical_start_time(current_time, interval);

        let aligned_start_time = get_aligned_time(start_time, interval);
        let aligned_end_time = get_aligned_time(current_time, interval);

        let transaction_id = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);

        // 埋点：任务创建事件
        tracing::info!(
            log_type = "transaction",
            transaction_id,
            event_name = "task_created",
            reason = "full_download_for_new_symbol",
            symbol = %symbol,
            interval = %interval,
        );

        tracing::debug!(decision = "full_download_task_created_for_new_symbol", symbol = %symbol, interval = %interval, start_time = aligned_start_time, end_time = aligned_end_time, "为新交易对创建完整下载任务");
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
