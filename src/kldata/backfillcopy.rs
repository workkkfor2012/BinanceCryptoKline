use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, AppError};
use crate::klcommon::api::get_aligned_time;
use tracing::{debug, info, warn, error, instrument, Instrument};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, Ordering};
use once_cell::sync::Lazy;
// 时间戳相关导入已移至 timestamp_checker.rs

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
                info!(log_type = "low_freq", target = "backfill", "{}", summary);
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
        info!(log_type = "low_freq", target = "backfill", "开始一次性补齐K线数据...");
        let start_time = Instant::now();

        // 1. 获取交易对列表
        let all_symbols = if self.test_mode {
            info!(log_type = "module", target = "backfill", "🔧 测试模式已启用，限制交易对为: {:?}", self.test_symbols);
            self.test_symbols.clone()
        } else {
            info!(log_type = "module", target = "backfill", "📡 获取所有正在交易的U本位永续合约交易对...");
            match self.api.get_trading_usdt_perpetual_symbols().await {
                Ok((trading_symbols, _delisted_symbols)) => {
                    info!(log_type = "module", target = "backfill", "✅ 获取到 {} 个交易对", trading_symbols.len());
                    trading_symbols
                },
                Err(e) => {
                    // 获取交易对失败是严重错误，直接返回错误并结束程序
                    error!(log_type = "module", target = "backfill", "❌ 获取交易对信息失败: {}", e);
                    return Err(AppError::ApiError(format!("获取交易对信息失败: {}", e)));
                }
            }
        };

        // 如果没有获取到交易对，直接返回错误
        if all_symbols.is_empty() {
            error!(log_type = "module", target = "backfill", "没有获取到交易对，补齐流程结束");
            return Err(AppError::ApiError("没有获取到交易对，无法继续补齐流程".to_string()));
        }

        // 2. 创建所有必要的表
        self.ensure_all_tables(&all_symbols)?;

        // 3. 创建所有下载任务 - 声明为循环以便TraceDistiller聚合
        let task_creation_loop_span = tracing::info_span!(
            "task_creation_loop",
            target = "backfill",
            iterator_type = "SymbolInterval",
            task_count = all_symbols.len() * self.intervals.len()
        );

        let tasks = async {
            self.create_all_download_tasks(&all_symbols).await
        }.instrument(task_creation_loop_span).await?;

        info!(log_type = "module", target = "backfill", "创建了 {} 个下载任务（包括补齐任务和新品种完整下载任务）", tasks.len());

        if tasks.is_empty() {
            info!(log_type = "module", target = "backfill", "没有需要补齐或下载的K线数据，所有数据都是最新的");
            return Ok(());
        }

        // 在async块外部声明这些变量，以便在async块结束后仍能访问
        let semaphore = Arc::new(tokio::sync::Semaphore::new(50)); // 增加到50个并发，充分利用网络带宽，写入操作由DbWriteQueue序列化处理
        let mut handles = Vec::new();
        // 存储失败任务及其失败原因 - 使用更简单的结构
        let failed_tasks = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        // 存储失败原因的统计
        let error_reasons = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::<String, usize>::new()));
        // 创建一个计数器来跟踪添加到失败列表的任务数量
        let failed_tasks_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // ✨【必须的修改】✨ 1. 包裹第一轮下载循环
        // 我们用一个新的 span 来框住整个批量下载的逻辑。
        // 这将成为所有 download_kline_task 的父 span。
        let initial_download_loop_span = tracing::info_span!(
            "initial_download_loop", // 约定的 `_loop` 后缀
            target = "backfill",
            task_count = tasks.len(),
            concurrency = 50, // 明确指出并发数
            iterator_type = "DownloadTask"
        );

        // 使用 .instrument() 将这个 span 附加到接下来的异步块上
        let (_success_count, error_count) = async {

            for (task_index, task) in tasks.into_iter().enumerate() {
                let api_clone = self.api.clone();
                let semaphore_clone = semaphore.clone();
                let db_clone = self.db.clone();
                let failed_tasks_clone = failed_tasks.clone();
                let error_reasons_clone = error_reasons.clone();
                let failed_tasks_counter_clone = failed_tasks_counter.clone();
                let task_clone = task.clone();

                let _symbol = task.symbol.clone();
                let _interval = task.interval.clone();

                // ✨【关键修复】✨ 先定义future，将span创建移动到获取许可之后
                let download_future = async move {
                    // 获取信号量许可
                    let _permit = semaphore_clone.acquire().await.unwrap();

                    let symbol = task.symbol.clone();
                    let interval = task.interval.clone();

                    // ✨【关键修复】✨ 在这里创建span，它只包裹真正的下载和保存工作
                    let task_span = tracing::info_span!(
                        "download_kline_task",
                        symbol = %symbol,
                        interval = %interval,
                        target = "backfill",
                        task_index = task_index
                    );

                async move {
                    //debug!(target: "backfill", log_type = "module", "Starting task...");
                    
                    // 记录API请求
                    let _request_id = API_REQUEST_STATS.0.fetch_add(1, Ordering::SeqCst);

                    //debug!(target: "backfill", log_type = "module", "Calling API...");
                    match api_clone.download_continuous_klines(&task).await {
                        Ok(klines) => {
                            //debug!(target: "backfill", log_type = "module", "API call successful, received {} klines.", klines.len());
                            API_REQUEST_STATS.1.fetch_add(1, Ordering::SeqCst);

                            if klines.is_empty() {
                                let error_msg = format!("{}/{}: API returned empty result", symbol, interval);
                                warn!(target: "backfill", log_type = "module", "{}", error_msg);
                                // This is not a critical error, so we don't add it to the failed_tasks list for retry.
                                // We simply return Ok to let the task complete.
                                return Ok(());
                            }

                            let mut sorted_klines = klines;
                            sorted_klines.sort_by_key(|k| k.open_time);

                            //debug!(target: "backfill", log_type = "module", "Saving to database...");
                            match db_clone.save_klines(&symbol, &interval, &sorted_klines).await {
                                Ok(count) => {
                                    //debug!(target: "backfill", log_type = "module", "Database save successful, {} klines saved. Task complete.", count);
                                    Self::update_backfill_stats(&symbol, &interval, count);
                                    Ok(())
                                }
                                Err(db_err) => {
                                    let error_msg = format!("Database save failed: {}", db_err);
                                    error!(target: "backfill", log_type = "module", "{}/{}: {}", symbol, interval, error_msg);
                                    
                                    // Add to failed tasks for retry
                                    let mut tasks = failed_tasks_clone.lock().await;
                                    tasks.push((task_clone.clone(), error_msg.clone()));
                                    failed_tasks_counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                    
                                    Err(AppError::DatabaseError(error_msg))
                                }
                            }
                        }
                        Err(e) => {
                            debug!(target: "backfill", log_type = "module", "API call failed.");
                            API_REQUEST_STATS.2.fetch_add(1, Ordering::SeqCst);

                            let error_msg = format!("API download failed: {}", e);
                            error!(target: "backfill", log_type = "module", "{}/{}: {}", symbol, interval, error_msg);

                            // Add to failed tasks for retry
                            let mut tasks = failed_tasks_clone.lock().await;
                            tasks.push((task_clone, error_msg.clone()));
                            failed_tasks_counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                            Err(AppError::ApiError(error_msg))
                        }
                    }
                }.instrument(task_span).await
                };
               
                // ✨【最终修复】✨ 在 spawn 之前，用父 span 的上下文来"包裹"这个 future
                // `tracing::Span::current()` 获取到的是当前的 `initial_download_loop` span
                let instrumented_future = download_future.instrument(tracing::Span::current());
                let handle = tokio::spawn(instrumented_future); // spawn 被包裹后的 future
                handles.push(handle);
            }
            debug!(target: "backfill", log_type = "module", "等待所有任务完成");
            // 等待所有任务完成
            let mut success_count = 0;
            let mut error_count = 0;
            let total_handles = handles.len();

            info!(target: "backfill", log_type = "module",
                "开始等待任务完成循环 - 总任务数: {}, 并发数: 50, 预期成功率: >95%",
                total_handles
            );
           

            for (i, handle) in handles.into_iter().enumerate() {
                match handle.await {
                    Ok(result) => {
                        match result {
                            Ok(_) => {
                                success_count += 1;
                                if i % 100 == 0 {
                                    debug!(target = "backfill", "已完成 {} 个任务，成功: {}, 失败: {}", i+1, success_count, error_count);
                                }
                            },
                            Err(_) => {
                                error_count += 1;
                                if error_count % 10 == 0 {
                                    debug!(target = "backfill", "已完成 {} 个任务，成功: {}, 失败: {}", i+1, success_count, error_count);
                                }
                            },
                        }
                    }
                    Err(join_err) => { // Task panicked
                        error!(target = "backfill", "任务 #{} 执行因panic而失败: {}", i + 1, join_err);
                        error_count += 1;
                        // 注意：此处panic的任务目前不会被添加到 failed_tasks 列表，因为原始task对象不易获取
                        // 可以在最外层执行 backfill 时增加对 panic 的捕获和记录，如果需要更全面的失败任务列表
                    }
                }
            }
            info!(target = "backfill",log_type = "module",  "  任务统计for循环的结束");
            let elapsed = start_time.elapsed();
            let total_seconds = elapsed.as_secs();
            let minutes = total_seconds / 60;
            let seconds = total_seconds % 60;

            // ✨【建议】✨ 将结果记录到span中，这样on_close时可以读取到
            tracing::Span::current().record("success_count", success_count);
            tracing::Span::current().record("error_count", error_count);
            tracing::Span::current().record("elapsed_seconds", total_seconds);

            info!(target = "backfill",
                "第一轮K线补齐完成，成功: {}，失败: {}，耗时: {}分{}秒",
                success_count, error_count, minutes, seconds
            );

            // 返回统计结果
            (success_count, error_count)

        }.instrument(initial_download_loop_span).await; // <-- 在这里 await instrument 过的 future

        // 检查失败任务列表大小和计数器
        let failed_tasks_size = failed_tasks.lock().await.len();
        let failed_tasks_count = failed_tasks_counter.load(std::sync::atomic::Ordering::SeqCst);
        debug!(target = "backfill", "任务完成后，失败任务列表大小: {}, 计数器值: {}, 统计的错误数: {}",
               failed_tasks_size, failed_tasks_count, error_count);

        // 如果存在不一致，记录警告
        if failed_tasks_size != error_count || failed_tasks_count != error_count {
            warn!(target = "backfill", "失败任务统计不一致: 列表大小={}, 计数器值={}, 统计错误数={}",
                  failed_tasks_size, failed_tasks_count, error_count);
        }

        // 打印失败原因统计
        let reasons = error_reasons.lock().await;

        if !reasons.is_empty() {
            info!(target: "backfill", "失败原因统计:");

            // 将原因按出现次数排序
            let mut reason_counts: Vec<(String, usize)> = reasons.iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            reason_counts.sort_by(|a, b| b.1.cmp(&a.1));

            for (reason, count) in reason_counts {
                let percentage = (count as f64 / error_count as f64) * 100.0;
                info!(target: "backfill", "  - {}: {} 次 ({:.1}%)", reason, count, percentage);
            }
        }

        // 获取失败的任务列表和错误信息
        let failed_tasks_with_errors = failed_tasks.lock().await;

        // 打印所有失败任务的详细错误信息
        info!(target: "backfill", "所有失败任务的详细错误信息 (总计 {} 个):", failed_tasks_with_errors.len());
        info!(target: "backfill", "失败任务计数器值: {}, 统计的错误数: {}",
              failed_tasks_counter.load(std::sync::atomic::Ordering::SeqCst), error_count);

        // 调试信息：检查失败任务列表是否为空
        if failed_tasks_with_errors.is_empty() {
            error!(target: "backfill", "失败任务列表为空，但统计显示有 {} 个失败任务", error_count);

            // 如果列表为空但计数器不为零，说明有并发问题
            if failed_tasks_counter.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                error!(target: "backfill", "检测到并发问题: 计数器值为 {}, 但列表为空",
                       failed_tasks_counter.load(std::sync::atomic::Ordering::SeqCst));
            }
        } else {
            debug!(target: "backfill", "失败任务列表中有 {} 个任务", failed_tasks_with_errors.len());

            // 打印前5个失败任务的信息用于调试
            for (i, (task, error_msg)) in failed_tasks_with_errors.iter().take(5).enumerate() {
                debug!(target: "backfill", "调试 - 失败任务 #{}: {}/{} - 错误: {}",
                       i+1, task.symbol, task.interval, error_msg);
            }
        }

        // 按错误类型分组统计
        let _error_type_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        // 记录每种错误类型的前5个示例
        let _error_examples: std::collections::HashMap<String, Vec<(String, String, String)>> = std::collections::HashMap::new();

        // 只在日志中记录失败任务的总数
        info!(target: "backfill", "失败任务详细信息 (总计 {} 个):", failed_tasks_with_errors.len());
        info!(target: "backfill", "{}", "=".repeat(100));

        // 按错误类型分组统计
        let mut error_type_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        // 记录每种错误类型的前5个示例
        let mut error_examples: std::collections::HashMap<String, Vec<(String, String, String)>> = std::collections::HashMap::new();

        for (i, (task, error_msg)) in failed_tasks_with_errors.iter().enumerate() {
            // 提取错误类型
            let error_type = if error_msg.contains("HTTP error") {
                "HTTP error"
            } else if error_msg.contains("timeout") {
                "请求超时"
            } else if error_msg.contains("429 Too Many Requests") {
                "429 Too Many Requests"
            } else if error_msg.contains("空结果") {
                "空结果"
            } else if error_msg.contains("unexpected EOF during handshake") {
                "握手中断"
            } else {
                // 提取错误类型的第一部分
                let parts: Vec<&str> = error_msg.split(':').collect();
                if parts.len() > 1 {
                    parts[0].trim()
                } else {
                    "其他错误"
                }
            };

            // 更新错误类型计数
            *error_type_counts.entry(error_type.to_string()).or_insert(0) += 1;

            // 为每种错误类型保存最多5个示例
            if let Some(examples) = error_examples.get_mut(error_type) {
                if examples.len() < 5 {
                    examples.push((task.symbol.clone(), task.interval.clone(), error_msg.clone()));
                }
            } else {
                error_examples.insert(error_type.to_string(), vec![(task.symbol.clone(), task.interval.clone(), error_msg.clone())]);
            }

            // 构建URL参数用于日志记录
            let mut url_params = format!(
                "symbol={}&interval={}&limit={}",
                task.symbol, task.interval, task.limit
            );

            // 添加可选的起始时间
            if let Some(start_time) = task.start_time {
                url_params.push_str(&format!("&startTime={}", start_time));
            }

            // 添加可选的结束时间
            if let Some(end_time) = task.end_time {
                url_params.push_str(&format!("&endTime={}", end_time));
            }

            // 构建完整URL
            let fapi_url = format!("https://fapi.binance.com/fapi/v1/klines?{}", url_params);

            // 不再写入错误日志文件，只在控制台打印详细信息

            // 每10个错误在控制台打印一个详细示例
            if i < 10 || i % 100 == 0 {
                info!(target: "backfill", "失败任务 #{}: {}/{} - {}", i+1, task.symbol, task.interval, error_type);
                info!(target: "backfill", "  错误信息: {}", error_msg);
                info!(target: "backfill", "  请求URL: {}", fapi_url);
            }

            // 每100个错误打印一次进度
            if (i + 1) % 100 == 0 || i == failed_tasks_with_errors.len() - 1 {
                info!(target: "backfill", "已处理 {}/{} 个失败任务", i + 1, failed_tasks_with_errors.len());
            };
        }

        info!(target: "backfill", "所有失败任务的详细信息已记录到主日志文件");

        // 打印错误类型统计
        info!(target: "backfill", "失败任务按错误类型统计:");
        let mut sorted_error_types: Vec<(String, usize)> = error_type_counts.into_iter().collect();
        sorted_error_types.sort_by(|a, b| b.1.cmp(&a.1)); // 按数量降序排序

        for (error_type, count) in &sorted_error_types {
            let percentage = (count * 100) as f64 / failed_tasks_with_errors.len() as f64;
            info!(target: "backfill", "  - {}: {} 个任务 ({:.1}%)", error_type, count, percentage);

            // 打印该错误类型的示例
            if let Some(examples) = error_examples.get(error_type) {
                info!(target: "backfill", "    示例:");
                for (idx, (symbol, interval, error)) in examples.iter().enumerate() {
                    info!(target: "backfill", "    {}) {}/{}: {}", idx + 1, symbol, interval, error);
                }
            }
        }

        // 定义需要重试的错误关键词，这样做更健壮，不容易出错
        let retry_keywords = vec![
            "HTTP error",
            "timeout",           // 直接检查关键词，捕获各种超时错误
            "429",              // 直接检查状态码
            "Too Many Requests",
            "handshake",        // 捕获 "unexpected EOF during handshake"
            "connection",       // 捕获连接相关错误
            "network",          // 捕获网络相关错误
        ];

        info!(target: "backfill", "将重试包含以下关键词的错误: {:?}", retry_keywords);

        // 获取需要重试的任务列表（根据错误关键词过滤）
        let retry_tasks: Vec<DownloadTask> = failed_tasks_with_errors
            .iter()
            .filter(|(_, error_msg)| {
                // 只要错误信息包含任意一个重试关键词，就进行重试
                retry_keywords.iter().any(|keyword| error_msg.contains(keyword))
            })
            .map(|(task, _)| task.clone())
            .collect();

        // 如果有需要重试的任务，进行重试
        if !retry_tasks.is_empty() {
            info!(target: "backfill", "开始重试 {} 个失败的下载任务...", retry_tasks.len());

            // ✨【必须的修改】✨ 2. 包裹第二轮重试循环
            let retry_loop_span = tracing::info_span!(
                "retry_download_loop", // 同样使用 `_loop` 后缀
                target = "backfill",
                task_count = retry_tasks.len(),
                concurrency = 50,
                iterator_type = "RetryTask"
            );

            async {
                let retry_start_time = Instant::now();
                let mut retry_handles = Vec::new();
                let retry_failed_tasks = Arc::new(tokio::sync::Mutex::new(Vec::new()));
                let retry_error_reasons = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
                let retry_semaphore = Arc::new(tokio::sync::Semaphore::new(50)); // 重试使用50个并发，充分利用网络带宽，写入操作由DbWriteQueue序列化处理
                let total_retry_tasks = retry_tasks.len();

                info!(target: "backfill", log_type = "module",
                    "开始重试任务创建循环 - 重试任务数: {}, 并发数: 50, 预期成功率: >80%",
                    total_retry_tasks
                );

                for (retry_index, task) in retry_tasks.into_iter().enumerate() {
                    let api_clone = self.api.clone();
                    let semaphore_clone = retry_semaphore.clone();
                    let db_clone = self.db.clone();
                    let retry_failed_tasks_clone = retry_failed_tasks.clone();
                    let retry_error_reasons_clone = retry_error_reasons.clone();
                    let task_clone = task.clone();

                    let _symbol = task.symbol.clone();
                    let _interval = task.interval.clone();

                    // ✨【关键修复】✨ 先定义future，将span创建移动到获取许可之后
                    let retry_future = async move {
                        // 获取信号量许可
                        let _permit = semaphore_clone.acquire().await.unwrap();

                        let symbol = task.symbol.clone();
                        let interval = task.interval.clone();

                        // ✨【关键修复】✨ 在这里创建span，它只包裹真正的重试工作
                        let retry_task_span = tracing::info_span!(
                            "retry_download_task", // 不再需要 "_sample" 后缀
                            symbol = %symbol,
                            interval = %interval,
                            target = "backfill",
                            retry_index = retry_index
                        );

                    async move {
                        // 记录API请求
                        let request_id = API_REQUEST_STATS.0.fetch_add(1, Ordering::SeqCst);
                        // 不再记录开始请求的日志

                        // 构建URL参数
                        let mut url_params = format!(
                            "symbol={}&interval={}&limit={}",
                            task.symbol, task.interval, task.limit
                        );

                        // 添加可选的起始时间
                        if let Some(start_time) = task.start_time {
                            url_params.push_str(&format!("&startTime={}", start_time));
                        }

                        // 添加可选的结束时间
                        if let Some(end_time) = task.end_time {
                            url_params.push_str(&format!("&endTime={}", end_time));
                        }

                        // 构建完整URL
                        let fapi_url = format!("https://fapi.binance.com/fapi/v1/klines?{}", url_params);
                        // 不再记录URL日志，只在失败时记录

                        // 下载任务
                        match api_clone.download_continuous_klines(&task).await {
                            Ok(klines) => {
                                // 更新成功请求计数
                                API_REQUEST_STATS.1.fetch_add(1, Ordering::SeqCst);
                                // 不再记录成功请求的日志

                                if klines.is_empty() {
                                    // 记录空结果错误
                                    let error_msg = "空结果".to_string();
                                    error!(target: "backfill", "{}/{}: 重试下载失败: {}", symbol, interval, error_msg);

                                    // 更新错误统计
                                    {
                                        let mut reasons = retry_error_reasons_clone.lock().await;
                                        *reasons.entry(error_msg.clone()).or_insert(0) += 1;
                                    }

                                    // 将失败的任务和原因添加到失败列表中
                                    retry_failed_tasks_clone.lock().await.push((task_clone, error_msg));
                                    return Err(AppError::DataError("空结果".to_string()));
                                }

                                // 按时间排序
                                let mut sorted_klines = klines.clone();
                                sorted_klines.sort_by_key(|k| k.open_time);

                                // 保存到数据库 (异步)
                                let count = db_clone.save_klines(&symbol, &interval, &sorted_klines).await?;

                                // 更新统计信息
                                Self::update_backfill_stats(&symbol, &interval, count);

                                Ok(())
                            }
                            Err(e) => {
                                // 更新失败请求计数
                                API_REQUEST_STATS.2.fetch_add(1, Ordering::SeqCst);

                                let error_msg = format!("{}", e);
                                error!(target: "backfill", "重试API请求 #{}: {}/{} - 请求失败: {}",
                                       request_id, symbol, interval, error_msg);
                                error!(target: "backfill", "{}/{}: 重试下载失败: {}", symbol, interval, error_msg);
                                error!(target: "backfill", "失败的URL: {}", fapi_url);

                                // 更新错误统计
                                {
                                    let mut reasons = retry_error_reasons_clone.lock().await;
                                    let reason_key = if error_msg.contains("429 Too Many Requests") {
                                        "429 Too Many Requests".to_string()
                                    } else if error_msg.contains("timeout") {
                                        "请求超时".to_string()
                                    } else if error_msg.contains("unexpected EOF during handshake") {
                                        "握手中断".to_string()
                                    } else if error_msg.contains("HTTP error") {
                                        "HTTP error".to_string()
                                    } else if error_msg.contains("empty response") {
                                        "空响应".to_string()
                                    } else {
                                        // 提取错误类型
                                        let parts: Vec<&str> = error_msg.split(':').collect();
                                        if parts.len() > 1 {
                                            parts[0].trim().to_string()
                                        } else {
                                            error_msg.clone()
                                        }
                                    };
                                    *reasons.entry(reason_key).or_insert(0) += 1;
                                }

                                // 将失败的任务和原因添加到失败列表中
                                retry_failed_tasks_clone.lock().await.push((task_clone, format!("URL={}, 错误: {}", fapi_url, error_msg)));
                                Err(e)
                            }
                        }
                    }.instrument(retry_task_span).await // instrument 并 await
                };

                    // ✨【最终修复】✨ 对重试任务也应用相同的 instrument 逻辑
                    let instrumented_retry_future = retry_future.instrument(tracing::Span::current());
                    let handle = tokio::spawn(instrumented_retry_future);
                    retry_handles.push(handle);
                }

                // 等待所有重试任务完成
                let mut retry_success_count = 0;
                let mut retry_error_count = 0;
                let total_retry_handles = retry_handles.len();

                info!(target: "backfill", log_type = "module",
                    "开始等待重试任务完成循环 - 总重试任务数: {}, 并发数: 50, 预期成功率: >80%",
                    total_retry_handles
                );

                for handle in retry_handles {
                    match handle.await {
                        Ok(result) => {
                            match result {
                                Ok(_) => retry_success_count += 1,
                                Err(_) => retry_error_count += 1,
                            }
                        }
                        Err(e) => {
                            error!(target: "backfill", "重试任务执行失败: {}", e);
                            retry_error_count += 1;
                        }
                    }
                }

                let retry_elapsed = retry_start_time.elapsed();
                let final_failed_tasks = retry_failed_tasks.lock().await.len();
                let total_seconds = retry_elapsed.as_secs();
                let minutes = total_seconds / 60;
                let seconds = total_seconds % 60;

                // ✨【建议】✨ 记录统计结果到span，包括更多有用的信息
                tracing::Span::current().record("success_count", retry_success_count);
                tracing::Span::current().record("error_count", retry_error_count);
                tracing::Span::current().record("final_failed_count", final_failed_tasks);
                tracing::Span::current().record("elapsed_seconds", total_seconds);

                info!(target: "backfill",
                    "重试下载完成，成功: {}，失败: {}，最终失败: {}，耗时: {}分{}秒",
                    retry_success_count, retry_error_count, final_failed_tasks, minutes, seconds
                );

            // 打印重试失败原因统计
            let retry_reasons = retry_error_reasons.lock().await;
            if !retry_reasons.is_empty() {
                info!(target: "backfill", "重试失败原因统计:");

                // 将原因按出现次数排序
                let mut reason_counts: Vec<(String, usize)> = retry_reasons.iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                reason_counts.sort_by(|a, b| b.1.cmp(&a.1));

                for (reason, count) in reason_counts {
                    let percentage = (count as f64 / retry_error_count as f64) * 100.0;
                    info!(target: "backfill", "  - {}: {} 次 ({:.1}%)", reason, count, percentage);
                }
            }

            // 如果有最终失败的任务，打印所有失败任务的详细信息
            if final_failed_tasks > 0 {
                let final_failed = retry_failed_tasks.lock().await;
                info!(target: "backfill", "所有最终失败任务的详细信息（共 {} 个）:", final_failed.len());

                // 按错误类型分组
                let mut error_type_groups = std::collections::HashMap::new();

                // 只在日志中记录重试失败任务的总数
                info!(target: "backfill", "重试失败任务详细信息 (总计 {} 个):", final_failed.len());
                info!(target: "backfill", "{}", "=".repeat(100));

                for (i, (task, reason)) in final_failed.iter().enumerate() {
                    // 提取错误类型
                    let error_type = if reason.contains("429 Too Many Requests") {
                        "429 Too Many Requests"
                    } else if reason.contains("timeout") {
                        "请求超时"
                    } else if reason.contains("unexpected EOF during handshake") {
                        "握手中断"
                    } else if reason.contains("HTTP error") {
                        "HTTP error"
                    } else if reason.contains("空结果") {
                        "空结果"
                    } else {
                        "其他错误"
                    };

                    error_type_groups
                        .entry(error_type.to_string())
                        .or_insert_with(Vec::new)
                        .push((task.clone(), reason.clone()));

                    // 构建URL参数用于日志记录
                    let mut url_params = format!(
                        "symbol={}&interval={}&limit={}",
                        task.symbol, task.interval, task.limit
                    );

                    // 添加可选的起始时间
                    if let Some(start_time) = task.start_time {
                        url_params.push_str(&format!("&startTime={}", start_time));
                    }

                    // 添加可选的结束时间
                    if let Some(end_time) = task.end_time {
                        url_params.push_str(&format!("&endTime={}", end_time));
                    }

                    // 构建完整URL
                    let fapi_url = format!("https://fapi.binance.com/fapi/v1/klines?{}", url_params);

                    // 不再写入错误日志文件，只在控制台打印详细信息

                    // 每10个错误在控制台打印一个详细示例
                    if i < 10 || i % 100 == 0 {
                        info!(target: "backfill", "重试失败任务 #{}: {}/{} - {}", i+1, task.symbol, task.interval, error_type);
                        info!(target: "backfill", "  错误信息: {}", reason);
                        info!(target: "backfill", "  请求URL: {}", fapi_url);
                    }
                }

                info!(target: "backfill", "所有重试失败任务的详细信息已记录到主日志文件");

                // 按错误类型显示
                for (error_type, tasks) in &error_type_groups {
                    info!(target: "backfill", "\n错误类型: {} - {} 个任务", error_type, tasks.len());

                    // 只显示每种类型的前5个示例
                    for (i, (task, reason)) in tasks.iter().take(5).enumerate() {
                        info!(target: "backfill", "  {}. {}/{}: {}", i+1, task.symbol, task.interval, reason);
                    }

                    // 如果有更多，显示剩余数量
                    if tasks.len() > 5 {
                        info!(target: "backfill", "  ... 以及其他 {} 个任务", tasks.len() - 5);
                    }
                }
            }

            }.instrument(retry_loop_span).await; // <-- await instrument 过的 future
        }

        let total_elapsed = start_time.elapsed();
        let total_seconds = total_elapsed.as_secs();
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;

        // 获取API请求统计
        let total_requests = API_REQUEST_STATS.0.load(Ordering::SeqCst);
        let successful_requests = API_REQUEST_STATS.1.load(Ordering::SeqCst);
        let failed_requests = API_REQUEST_STATS.2.load(Ordering::SeqCst);

        info!(target: "backfill", "API请求统计: 总计发送 {} 个请求，成功 {} 个，失败 {} 个",
            total_requests, successful_requests, failed_requests
        );

        // 获取总计补齐的K线数量
        let total_klines = BACKFILL_STATS.0.load(Ordering::Relaxed);

        info!(target: "backfill",
            "K线补齐全部完成，总计: {} 条K线，总耗时: {}小时{}分{}秒",
            total_klines, hours, minutes, seconds
        );

        // 时间戳检查功能已移至 timestamp_checker.rs，测试已通过，此处屏蔽
        // let timestamp_checker = crate::kldata::TimestampChecker::new(self.db.clone(), self.intervals.clone());
        // timestamp_checker.check_last_kline_consistency().await?;
        info!(target: "backfill", "时间戳检查功能已移至 timestamp_checker.rs，测试已通过，此处屏蔽");

        Ok(())
    }

    // 时间戳检查相关方法已移至 timestamp_checker.rs

    // 时间戳转换方法已移至 timestamp_checker.rs

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
        info!(target: "backfill", "开始预先创建所有需要的表，共 {} 个交易对，每个 {} 个周期",
              symbols.len(), self.intervals.len());

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

        info!(target: "backfill", "表创建完成，新创建 {} 个表，跳过 {} 个已存在的表", created_count, existing_count);
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
    #[instrument(skip(self), ret, err)]
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
    #[instrument(skip(self), ret, err)]
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
