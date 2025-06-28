use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, AppError};
use crate::klcommon::api::get_aligned_time;
use tracing::{info, warn, error, instrument, Instrument};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, Ordering};
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
                // 构建简洁的摘要信息，不包含详细的交易对信息
                let summary = format!("补齐K线摘要 ({}秒): 总计 {} 条K线",
                    BACKFILL_LOG_INTERVAL, total_count);

                // 输出日志
                info!(target: "backfill", log_type = "module", "{}", summary);
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
    #[instrument(name = "backfill_run_once", target = "backfill", skip_all)]
    pub async fn run_once(&self) -> Result<()> {
        info!(target: "backfill", log_type = "module", "开始一次性补齐K线数据...");
        let start_time = Instant::now();

        // 步骤 1 & 2: 获取交易对并准备表
        let all_symbols = self.get_symbols().await?;
        info!(target: "backfill", log_type = "module", "🗄️ 开始准备数据库表结构1...");
      
        
        self.ensure_all_tables(&all_symbols)?;
        info!(target: "backfill", log_type = "module", "✅ 数据库表结构准备完成");

        // 步骤 3: 创建任务
        info!(target: "backfill", log_type = "module", "📋 开始创建下载任务...");
        let tasks = self.create_all_download_tasks(&all_symbols).await?;
        if tasks.is_empty() {
            info!(target: "backfill", log_type = "module", "✅ 所有数据都是最新的，无需补齐");
            return Ok(());
        }
        info!(target: "backfill", log_type = "module", "📋 已创建 {} 个下载任务", tasks.len());

        // 步骤 4: 执行第一轮下载
        info!(target: "backfill", log_type = "module", "开始第一轮下载，共 {} 个任务...", tasks.len());
        let failed_tasks = self.execute_tasks(tasks, "initial_download_loop").await;

        // 步骤 5: 如果有失败，执行重试
        if !failed_tasks.is_empty() {
            tracing::debug!(decision = "retry_needed", failed_count = failed_tasks.len(), "检测到失败任务，准备重试");
            let retry_tasks = self.prepare_retry_tasks(&failed_tasks);
            if !retry_tasks.is_empty() {
                info!(target: "backfill", log_type = "module", "开始重试 {} 个失败任务...", retry_tasks.len());
                tracing::debug!(decision = "retry_execution", retry_count = retry_tasks.len(), "开始执行重试任务");
                let final_failed_tasks = self.execute_tasks(retry_tasks, "retry_download_loop").await;
                if !final_failed_tasks.is_empty() {
                    tracing::debug!(decision = "final_failures", final_failed_count = final_failed_tasks.len(), "重试后仍有失败任务");
                    self.report_final_failures(final_failed_tasks);
                }
            } else {
                tracing::debug!(decision = "no_retry", reason = "no_retryable_tasks", "没有可重试的任务");
            }
        } else {
            tracing::debug!(decision = "no_retry", reason = "no_failures", "所有任务都成功，无需重试");
        }

        self.report_summary(start_time);
        Ok(())
    }

    /// 执行一批下载任务，并返回失败的任务列表
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

        let processing_span = match loop_name {
            "initial_download_loop" => tracing::info_span!(
                "initial_download_loop",
                target = "backfill",
                task_count = task_count,
                concurrency = CONCURRENCY
            ),
            "retry_download_loop" => tracing::info_span!(
                "retry_download_loop",
                target = "backfill",
                task_count = task_count,
                concurrency = CONCURRENCY
            ),
            _ => tracing::info_span!(
                "download_loop",
                target = "backfill",
                task_count = task_count,
                concurrency = CONCURRENCY
            ),
        };

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
            target:"backfill", log_type = "module",
            "[{}] 完成。成功: {}, 失败: {}, 耗时: {:.2?}",
            loop_name, success_count, failed_tasks.len(), elapsed
        );

        failed_tasks
    }

    /// 处理单个下载任务的核心逻辑
    async fn process_single_task(api: BinanceApi, db: Arc<Database>, task: DownloadTask) -> TaskResult {
        API_REQUEST_STATS.0.fetch_add(1, Ordering::SeqCst);
        let task_span = tracing::info_span!(
            "download_kline_task",
            symbol = %task.symbol,
            interval = %task.interval
        );

        let result = async {
            let klines = api.download_continuous_klines(&task).await?;
            API_REQUEST_STATS.1.fetch_add(1, Ordering::SeqCst);

            if klines.is_empty() {
                warn!(target: "backfill", "{}/{}: API返回空结果，跳过", task.symbol, task.interval);
                tracing::debug!(decision = "empty_response", symbol = %task.symbol, interval = %task.interval, "API返回空结果，跳过处理");
                return Ok(0); // 空结果不是错误，但也没有写入
            }

            tracing::debug!(decision = "save_klines", symbol = %task.symbol, interval = %task.interval, kline_count = klines.len(), "开始保存K线数据");
            let count = db.save_klines(&task.symbol, &task.interval, &klines).await?;
            Self::update_backfill_stats(&task.symbol, &task.interval, count);
            Ok(count)
        }.instrument(task_span).await;

        match result {
            Ok(count) => {
                tracing::debug!(decision = "task_success", symbol = %task.symbol, interval = %task.interval, saved_count = count, "任务成功完成");
                TaskResult::Success(count)
            },
            Err(e) => {
                API_REQUEST_STATS.2.fetch_add(1, Ordering::SeqCst);
                error!(target: "backfill", "{}/{}: 任务失败: {}", task.symbol, task.interval, e);
                tracing::error!(message = "下载任务失败", symbol = %task.symbol, interval = %task.interval, error.details = %e);
                TaskResult::Failure { task, error: e }
            }
        }
    }

    /// 从失败任务中筛选出需要重试的任务
    fn prepare_retry_tasks(&self, failed_tasks: &[(DownloadTask, AppError)]) -> Vec<DownloadTask> {
        let retry_keywords = [
            "HTTP error", "timeout", "429", "Too Many Requests", "handshake", "connection", "network"
        ];
        info!(target: "backfill", "将重试包含以下关键词的错误: {:?}", retry_keywords);

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
            warn!(target: "backfill", log_type = "module", "⚠️ {} 个任务因不可重试的错误（如数据库错误、数据解析错误）被永久放弃", non_retry_count);
        }

        retry_tasks
    }

    /// 报告最终无法完成的任务
    fn report_final_failures(&self, final_failures: Vec<(DownloadTask, AppError)>) {
        error!(target: "backfill", log_type = "module", "❌ 重试后仍有 {} 个任务最终失败，需要人工检查", final_failures.len());
        let mut error_summary: HashMap<String, usize> = HashMap::new();

        for (task, error) in final_failures.iter().take(10) { // 只打印前10个的详情
            let error_msg = error.to_string();
            let error_type = error_msg.split(':').next().unwrap_or("Unknown Error").trim();
            *error_summary.entry(error_type.to_string()).or_default() += 1;
            error!(target: "backfill", "  - {}/{}: {}", task.symbol, task.interval, error_msg);
        }

        if final_failures.len() > 10 {
            error!(target: "backfill", "  ... 以及其他 {} 个失败任务。", final_failures.len() - 10);
        }

        error!(target: "backfill", log_type = "module", "最终失败任务摘要 - 需要人工干预:");
        for (err_type, count) in error_summary {
            error!(target: "backfill", log_type = "module", "  - {}: {} 次", err_type, count);
        }
    }

    /// 报告最终的运行摘要
    fn report_summary(&self, start_time: Instant) {
        let elapsed = start_time.elapsed();
        let total_requests = API_REQUEST_STATS.0.load(Ordering::SeqCst);
        let successful_requests = API_REQUEST_STATS.1.load(Ordering::SeqCst);
        let failed_requests = API_REQUEST_STATS.2.load(Ordering::SeqCst);
        let total_klines = BACKFILL_STATS.0.load(Ordering::Relaxed);

        info!(target: "backfill", log_type = "module", "================ K线补齐运行摘要 ================");
        info!(target: "backfill", log_type = "module", "✅ K线补齐全部完成，总耗时: {:.2?}", elapsed);
        info!(target: "backfill", log_type = "module", "📊 总计补齐K线: {} 条", total_klines);
        info!(target: "backfill", log_type = "module", "🌐 API请求统计: 总计 {}, 成功 {}, 失败 {}", total_requests, successful_requests, failed_requests);
        info!(target: "backfill", log_type = "module", "==============================================");
    }

    #[instrument(skip(self), ret, err)]
    async fn get_symbols(&self) -> Result<Vec<String>> {
        if self.test_mode {
            info!(target: "backfill", log_type = "module", "🔧 测试模式，使用1预设交易对: {:?}", self.test_symbols);
            tracing::debug!(decision = "symbol_source", source = "test_mode", symbols = ?self.test_symbols, "使用测试模式预设交易对");
            return Ok(self.test_symbols.clone());
        }
        info!(target: "backfill", log_type = "module", "📡 获取所有正在交易的U本位永续合约交易对...");
        tracing::debug!(decision = "symbol_source", source = "api", "从API获取交易对列表");
        let symbols = self.api.get_trading_usdt_perpetual_symbols().await?;
        info!(target: "backfill", log_type = "module", "✅ 获取到 {} 个交易对", symbols.len());
        if symbols.is_empty() {
            tracing::error!(message = "API返回空交易对列表", "获取交易对失败，无法继续");
            return Err(AppError::ApiError("没有获取到交易对，无法继续。".to_string()));
        }
        Ok(symbols)
    }

    /// 获取数据库中已存在的K线表
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
    fn ensure_all_tables(&self, symbols: &[String]) -> Result<()> {
        let total_tables = symbols.len() * self.intervals.len();
        info!(target: "backfill", log_type = "module", "🗄️ 开始预先创建数据库表，共 {} 个交易对 × {} 个周期 = {} 个表",
              symbols.len(), self.intervals.len(), total_tables);

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
                    existing_count += 1;
                    continue;
                }

                // 创建表
                self.db.ensure_symbol_table(symbol, interval)?;
                created_count += 1;

                // 每创建10个表输出一次日志
                if created_count % 10 == 0 {
                    //debug!("已创建 {} 个表，跳过 {} 个已存在的表", created_count, existing_count);
                }
            }
        }

        info!(target: "backfill", log_type = "module", "✅ 数据库表创建完成，新创建 {} 个表，跳过 {} 个已存在的表", created_count, existing_count);
        Ok(())
    }

    /// 创建所有下载任务的主函数
    /// 注意：移除了#[instrument]注解，因为已被外部的task_creation_loop span追踪
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
    // #[instrument] 移除：高频调用函数，在任务创建阶段会被大量调用产生噪音
    async fn create_task_for_existing_symbol(&self, symbol: &str, interval: &str) -> Result<Option<DownloadTask>> {
        let current_time = chrono::Utc::now().timestamp_millis();

        if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(symbol, interval)? {
            // 有数据的情况：创建补齐任务
            let interval_ms = crate::klcommon::api::interval_to_milliseconds(interval);
            let start_time = last_timestamp + interval_ms;

            let aligned_start_time = get_aligned_time(start_time, interval);
            let aligned_end_time = get_aligned_time(current_time, interval);

            if aligned_start_time < aligned_end_time {
                Ok(Some(DownloadTask {
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

            Ok(Some(DownloadTask {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time: Some(aligned_start_time),
                end_time: Some(aligned_end_time),
                limit: 1000,
            }))
        }
    }

    /// 为新品种创建完整下载任务
    // #[instrument] 移除：高频调用函数，在任务创建阶段会被大量调用产生噪音
    async fn create_task_for_new_symbol(&self, symbol: &str, interval: &str) -> Result<DownloadTask> {
        let current_time = chrono::Utc::now().timestamp_millis();
        let start_time = self.calculate_historical_start_time(current_time, interval);

        let aligned_start_time = get_aligned_time(start_time, interval);
        let aligned_end_time = get_aligned_time(current_time, interval);

        Ok(DownloadTask {
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
