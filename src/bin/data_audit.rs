use clap::Parser;
use chrono::{NaiveDateTime, TimeZone, Utc, Timelike};
use futures::{stream, StreamExt};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::{Duration, Instant};
use anyhow::Result;
use serde_json::Value;
use reqwest::Client;
use tokio::sync::RwLock;

use kline_server::klcommon::{
    api::BinanceApi,
    db::Database,
    models::Kline,
};

// 高并发配置常量
const POOL_TARGET_SIZE: usize = 100; // 稽核工具使用较小的连接池
const AUDIT_CONCURRENCY: usize = 50; // 稽核并发数
const MAX_RETRIES: u32 = 3; // 最多重试次数

// 任务执行结果枚举
#[derive(Debug)]
enum AuditTaskResult {
    Success((Vec<MismatchDetail>, AuditStats)),
    Failure {
        symbol: String,
        interval: String,
        error: anyhow::Error,
    },
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Continuous K-line data correctness audit service.", long_about = None)]
struct Args {
    /// Comma-separated list of symbols to audit (e.g., "BTCUSDT,ETHUSDT"), or "ALL" for all symbols.
    #[arg(short, long, value_delimiter = ',', default_value = "BTCUSDT")]
    symbols: Vec<String>,

    /// Path to the local SQLite database file.
    #[arg(short, long)]
    db_path: String,

    /// Comma-separated list of intervals to check (fixed: 1m,5m,30m).
    #[arg(short, long, value_delimiter = ',', default_value = "1m,5m,30m")]
    intervals: Vec<String>,

    /// The duration of past data to audit in each cycle, in seconds (default: 1800 for 30 minutes).
    #[arg(long, default_value_t = 1800)]
    audit_duration_seconds: u64,

    /// Run only once instead of continuously (for testing purposes).
    #[arg(long, default_value_t = false)]
    run_once: bool,
}

#[derive(Serialize, Debug, Clone)]
struct ReportKline {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl ReportKline {
    fn from_kline(kline: &Kline) -> Self {
        Self {
            open: kline.open.parse().unwrap_or(0.0),
            high: kline.high.parse().unwrap_or(0.0),
            low: kline.low.parse().unwrap_or(0.0),
            close: kline.close.parse().unwrap_or(0.0),
            volume: kline.volume.parse().unwrap_or(0.0),
        }
    }
}

#[derive(Serialize, Debug)]
enum MismatchType {
    MissingInLocal, // 本地缺失
    ExtraInLocal,   // 本地多出
    FieldMismatch,  // 字段不匹配
}

#[derive(Serialize, Debug)]
struct MismatchDetail {
    symbol: String,
    interval: String,
    mismatch_type: MismatchType,
    open_time: i64,
    local_kline: Option<ReportKline>,
    binance_kline: Option<ReportKline>,
}

#[derive(Debug, Default)]
struct AuditStats {
    total_checked: usize,
    missing_in_local: usize,
    extra_in_local: usize,
    field_mismatches: usize,
}

impl AuditStats {
    fn merge(&mut self, other: AuditStats) {
        self.total_checked += other.total_checked;
        self.missing_in_local += other.missing_in_local;
        self.extra_in_local += other.extra_in_local;
        self.field_mismatches += other.field_mismatches;
    }

    fn increment_missing(&mut self) {
        self.missing_in_local += 1;
    }

    fn increment_extra(&mut self) {
        self.extra_in_local += 1;
    }

    fn increment_mismatch(&mut self) {
        self.field_mismatches += 1;
    }
}

#[derive(Serialize)]
struct SummaryReport {
    audit_timestamp: String,
    total_tasks: usize,
    total_checked: usize,
    missing_in_local: usize,
    extra_in_local: usize,
    field_mismatches: usize,
    success_rate: f64,
}

/// 高并发数据稽核器
#[derive(Clone)]
struct ConcurrentAuditor {
    client_pool: Arc<RwLock<Vec<Arc<Client>>>>,
    pool_next_index: Arc<AtomicUsize>,
    pool_maintenance_stop: Arc<std::sync::atomic::AtomicBool>,
}

impl ConcurrentAuditor {
    /// 创建新的并发稽核器
    fn new() -> Self {
        let auditor = Self {
            client_pool: Arc::new(RwLock::new(Vec::with_capacity(POOL_TARGET_SIZE))),
            pool_next_index: Arc::new(AtomicUsize::new(0)),
            pool_maintenance_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        // 启动连接池维护任务
        auditor.start_pool_maintenance_task();
        auditor
    }

    /// 启动后台任务来填充和维护客户端池
    fn start_pool_maintenance_task(&self) {
        let pool_clone = self.client_pool.clone();
        let stop_flag = self.pool_maintenance_stop.clone();

        tokio::spawn(async move {
            println!("🔧 客户端池维护任务启动，目标大小: {}", POOL_TARGET_SIZE);

            loop {
                if stop_flag.load(Ordering::Relaxed) {
                    println!("🛑 客户端池维护任务退出");
                    break;
                }

                let current_size = {
                    let pool_guard = pool_clone.read().await;
                    pool_guard.len()
                };

                if current_size >= POOL_TARGET_SIZE {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                if let Ok(new_client) = BinanceApi::create_new_client() {
                    let mut pool_guard = pool_clone.write().await;
                    if pool_guard.len() < POOL_TARGET_SIZE {
                        pool_guard.push(Arc::new(new_client));
                        if pool_guard.len() % 20 == 0 {
                            println!("📈 客户端池大小: {}/{}", pool_guard.len(), POOL_TARGET_SIZE);
                        }
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
    }

    /// 从池中获取一个客户端
    async fn get_client_from_pool(&self) -> Option<Arc<Client>> {
        let pool_guard = self.client_pool.read().await;
        if pool_guard.is_empty() {
            return None;
        }
        let index = self.pool_next_index.fetch_add(1, Ordering::Relaxed) % pool_guard.len();
        pool_guard.get(index).cloned()
    }

    /// 当客户端请求失败时，从池中移除它
    async fn remove_client_from_pool(&self, client_to_remove: &Arc<Client>) {
        let mut pool_guard = self.client_pool.write().await;
        let initial_len = pool_guard.len();
        pool_guard.retain(|c| !Arc::ptr_eq(c, client_to_remove));
        if pool_guard.len() < initial_len {
            println!("⚠️  已从池中移除失败的客户端，当前大小: {}", pool_guard.len());
        }
    }

    /// 清理连接池
    async fn cleanup(&self) {
        self.pool_maintenance_stop.store(true, Ordering::Relaxed);
        let mut pool_guard = self.client_pool.write().await;
        let pool_size = pool_guard.len();
        pool_guard.clear();
        if pool_size > 0 {
            println!("🧹 连接池已清理，释放了 {} 个连接", pool_size);
        }
    }
}

/// 稽核任务结构
#[derive(Debug, Clone)]
struct AuditTask {
    symbol: String,
    interval: String,
    db_path: String,
    start_timestamp: i64,
    end_timestamp: i64,
}

/// 高并发执行稽核任务
async fn execute_audit_tasks_concurrently(
    auditor: &ConcurrentAuditor,
    tasks: Vec<AuditTask>,
) -> Vec<Result<(Vec<MismatchDetail>, AuditStats), anyhow::Error>> {
    let total_tasks = tasks.len();
    let start_time = Instant::now();

    // 统计计数器
    let request_counter = Arc::new(AtomicUsize::new(0));
    let success_counter = Arc::new(AtomicUsize::new(0));

    // 启动监控任务
    let counter_clone = request_counter.clone();
    let success_clone = success_counter.clone();
    let monitor_task = tokio::spawn(async move {
        let mut last_requests = 0;
        let mut last_success = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;
            let current_requests = counter_clone.load(Ordering::Relaxed);
            let current_success = success_clone.load(Ordering::Relaxed);

            let req_rate = (current_requests - last_requests) as f64 / 10.0;
            let success_rate = (current_success - last_success) as f64 / 10.0;

            println!(
                "📊 稽核进度: {}/{} | 速率: {:.1}/s | 成功率: {:.1}%",
                current_requests,
                total_tasks,
                req_rate,
                if current_requests > 0 { (current_success as f64 / current_requests as f64) * 100.0 } else { 0.0 }
            );

            last_requests = current_requests;
            last_success = current_success;

            if current_requests >= total_tasks {
                break;
            }
        }
    });

    // 流式并发处理
    let results: Vec<_> = stream::iter(tasks)
        .map(|task| {
            let auditor_clone = auditor.clone();
            let req_counter = request_counter.clone();
            let succ_counter = success_counter.clone();

            async move {
                req_counter.fetch_add(1, Ordering::Relaxed);

                let result = execute_single_audit_task(&auditor_clone, task).await;

                if result.is_ok() {
                    succ_counter.fetch_add(1, Ordering::Relaxed);
                }

                result
            }
        })
        .buffer_unordered(AUDIT_CONCURRENCY)
        .collect()
        .await;

    // 停止监控任务
    monitor_task.abort();

    let total_duration = start_time.elapsed();
    let final_requests = request_counter.load(Ordering::Relaxed);
    let final_success = success_counter.load(Ordering::Relaxed);

    println!(
        "✅ 稽核任务完成统计: 总任务: {} | 成功: {} | 耗时: {:.1}s | 平均速率: {:.1}/s",
        total_tasks,
        final_success,
        total_duration.as_secs_f64(),
        final_requests as f64 / total_duration.as_secs_f64()
    );

    results
}

/// 等待到下一个分钟的第40秒
async fn wait_for_next_40_seconds() {
    let now = Utc::now();
    let current_second = now.second();

    let wait_seconds = if current_second < 40 {
        // 当前分钟的第40秒
        40 - current_second
    } else {
        // 下一分钟的第40秒
        60 - current_second + 40
    };

    println!("⏰ 等待 {} 秒到下一个执行时间点（每分钟第40秒）...", wait_seconds);
    tokio::time::sleep(Duration::from_secs(wait_seconds as u64)).await;
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // 创建日志目录
    fs::create_dir_all("logs/audit")?;

    if args.run_once {
        println!("🔍 开始单次数据正确性稽核...");
        run_single_audit_cycle(Arc::new(args)).await?;
    } else {
        println!("� 启动持续数据正确性稽核服务...");
        println!("📊 服务参数:");
        println!("   - 交易对: {:?}", args.symbols);
        println!("   - 检查周期: 1分钟、5分钟、30分钟（固定）");
        println!("   - 执行时机: 每分钟第40秒");
        println!("   - 每次稽核时长: {} 秒", args.audit_duration_seconds);
        println!("   - 数据库路径: {}", args.db_path);
        println!("");
        println!("💡 提示: 按 Ctrl+C 可优雅关闭服务");
        println!("");

        // 设置 Ctrl+C 优雅关闭信号
        let shutdown_signal = tokio::signal::ctrl_c();
        tokio::pin!(shutdown_signal);

        // 创建高并发稽核器（在循环外创建，以复用连接池）
        let auditor = ConcurrentAuditor::new();

        // 等待连接池初始化
        println!("⏳ 等待连接池初始化...");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 等待到下一个分钟的第40秒
        wait_for_next_40_seconds().await;

        // 创建每分钟触发的定时器
        let mut ticker = tokio::time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                // biased 确保优先检查关闭信号
                biased;
                _ = &mut shutdown_signal => {
                    println!("\n� 接收到关闭信号，正在关闭...");
                    break;
                },
                _ = ticker.tick() => {
                    // 检查当前是否是第40秒（允许1秒误差）
                    let now = Utc::now();
                    let current_second = now.second();

                    if current_second >= 39 && current_second <= 41 {
                        // 克隆需要在异步任务中使用的参数
                        let args_clone = Arc::new(args.clone());
                        let auditor_clone = auditor.clone();

                        // 在独立的 task 中运行单次稽核，防止稽核过程阻塞主循环
                        tokio::spawn(async move {
                            if let Err(e) = run_single_audit_cycle_with_auditor(args_clone, auditor_clone).await {
                                eprintln!("❌ 稽核周期执行失败: {}", e);
                            }
                        });
                    }
                }
            }
        }

        // 清理资源并退出
        println!("🧹 开始清理资源...");
        auditor.cleanup().await;
        println!("✅ 服务已优雅关闭。");
    }

    Ok(())
}

/// 运行单次稽核周期（创建临时连接池，用于单次运行模式）
async fn run_single_audit_cycle(args: Arc<Args>) -> Result<()> {
    let auditor = ConcurrentAuditor::new();

    // 等待连接池初始化
    println!("⏳ 等待连接池初始化...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let result = run_single_audit_cycle_with_auditor(args, auditor.clone()).await;

    // 单次运行模式才清理连接池
    auditor.cleanup().await;

    result
}

/// 运行单次稽核周期（使用现有的连接池）
async fn run_single_audit_cycle_with_auditor(args: Arc<Args>, auditor: ConcurrentAuditor) -> Result<()> {
    let cycle_start_time = Utc::now();
    println!("\n\n===== 启动新的稽核周期: {} =====", cycle_start_time.to_rfc3339());

    // 1. 动态计算本次稽核的时间范围
    let (start_time, end_time) = if args.run_once {
        // 单次运行模式：使用当前时间作为基准，稽核过去指定时长的数据
        let audit_duration = chrono::Duration::seconds(args.audit_duration_seconds as i64);
        let end_time = cycle_start_time;
        let start_time = end_time - audit_duration;
        (start_time, end_time)
    } else {
        // 持续运行模式：稽核过去指定时长的数据
        let audit_duration = chrono::Duration::seconds(args.audit_duration_seconds as i64);
        let end_time = cycle_start_time;
        let start_time = end_time - audit_duration;
        (start_time, end_time)
    };

    println!("📅 稽核时间范围: {} 到 {}",
        start_time.format("%Y-%m-%d %H:%M:%S UTC"),
        end_time.format("%Y-%m-%d %H:%M:%S UTC")
    );

    // 2. 确定要稽核的交易对列表
    let symbols_to_audit = if args.symbols.len() == 1 && args.symbols[0].to_uppercase() == "ALL" {
        println!("🔄 获取数据库中所有交易对...");
        let db = Database::new(&args.db_path)?;
        let all_symbols = db.get_all_symbols()?;
        println!("✅ 找到 {} 个交易对", all_symbols.len());
        all_symbols
    } else {
        args.symbols.clone()
    };

    println!("🎯 目标交易对: {:?}", symbols_to_audit);

    // 3. 创建稽核任务列表
    let mut audit_tasks = Vec::new();
    for symbol in &symbols_to_audit {
        for interval in &args.intervals {
            audit_tasks.push(AuditTask {
                symbol: symbol.clone(),
                interval: interval.clone(),
                db_path: args.db_path.clone(),
                start_timestamp: start_time.timestamp_millis(),
                end_timestamp: end_time.timestamp_millis(),
            });
        }
    }

    let total_tasks = audit_tasks.len();
    println!("🚀 开始执行 {} 个稽核任务，并发数: {}...", total_tasks, AUDIT_CONCURRENCY);

    // 4. 使用流式并发处理
    let results = execute_audit_tasks_concurrently(&auditor, audit_tasks).await;

    // 5. 创建报告文件并统一写入（基于周期开始时间生成唯一文件名）
    let timestamp_str = cycle_start_time.format("%Y%m%d_%H%M%S");
    let report_filename = format!("logs/audit/{}_audit_report.jsonl", timestamp_str);
    let mut report_file = File::create(&report_filename)?;

    let mut total_stats = AuditStats::default();
    let mut successful_tasks = 0;

    for result in results {
        match result {
            Ok((mismatches, stats)) => {
                for detail in mismatches {
                    let json_line = serde_json::to_string(&detail)?;
                    writeln!(report_file, "{}", json_line)?;
                }
                total_stats.merge(stats);
                successful_tasks += 1;
            },
            Err(e) => eprintln!("❌ 稽核任务失败: {}", e),
        }
    }

    // 6. 写入最终摘要
    write_summary_report(&mut report_file, &total_stats, total_tasks, successful_tasks)?;

    println!("✅ 稽核周期完成！报告已保存到: {}", report_filename);
    println!("📈 稽核摘要:");
    println!("   - 总任务数: {}", total_tasks);
    println!("   - 成功任务数: {}", successful_tasks);
    println!("   - 检查的K线数: {}", total_stats.total_checked);
    println!("   - 本地缺失: {}", total_stats.missing_in_local);
    println!("   - 本地多出: {}", total_stats.extra_in_local);
    println!("   - 字段不匹配: {}", total_stats.field_mismatches);

    let success_rate = if total_stats.total_checked > 0 {
        ((total_stats.total_checked - total_stats.missing_in_local - total_stats.extra_in_local - total_stats.field_mismatches) as f64 / total_stats.total_checked as f64) * 100.0
    } else {
        100.0
    };
    println!("   - 成功率: {:.2}%", success_rate);

    Ok(())
}

fn write_summary_report(
    report_file: &mut File,
    stats: &AuditStats,
    total_tasks: usize,
    _successful_tasks: usize,
) -> Result<()> {
    let success_rate = if stats.total_checked > 0 {
        ((stats.total_checked - stats.missing_in_local - stats.extra_in_local - stats.field_mismatches) as f64 / stats.total_checked as f64) * 100.0
    } else {
        100.0
    };

    let summary = SummaryReport {
        audit_timestamp: Utc::now().to_rfc3339(),
        total_tasks,
        total_checked: stats.total_checked,
        missing_in_local: stats.missing_in_local,
        extra_in_local: stats.extra_in_local,
        field_mismatches: stats.field_mismatches,
        success_rate,
    };

    let json_line = serde_json::to_string(&summary)?;
    writeln!(report_file, "{}", json_line)?;
    Ok(())
}

/// 执行单个稽核任务（使用连接池）
async fn execute_single_audit_task(
    auditor: &ConcurrentAuditor,
    task: AuditTask,
) -> Result<(Vec<MismatchDetail>, AuditStats), anyhow::Error> {
    // 使用任务中的时间戳，但仍然应用安全的时间控制（方案B：保留精确控制）
    let start_time = task.start_timestamp;
    let mut end_time = task.end_timestamp;

    // 计算安全的结束时间：避免稽核当前正在发生的那一分钟
    let now_utc = Utc::now();
    let last_minute_start_ts = (now_utc.timestamp() / 60 - 1) * 60 * 1000;
    let safe_end_time = last_minute_start_ts;

    // 如果请求的结束时间超过了安全时间，则调整为安全时间
    if end_time > safe_end_time {
        end_time = safe_end_time;
    }

    // 如果调整后的时间范围无效，返回空结果
    if start_time >= end_time {
        return Ok((Vec::new(), AuditStats::default()));
    }

    // 使用连接池获取币安数据
    let binance_data = fetch_binance_klines_with_pool(auditor, &task.symbol, &task.interval, start_time, end_time).await?;
    let local_data = fetch_local_klines(&task.db_path, &task.symbol, &task.interval, start_time, end_time).await?;

    // 对比并收集结果
    let (mismatches, stats) = compare_and_collect(&binance_data, &local_data, &task.symbol, &task.interval)?;

    Ok((mismatches, stats))
}

/// 使用连接池从币安API获取K线数据
async fn fetch_binance_klines_with_pool(
    auditor: &ConcurrentAuditor,
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<Kline>, anyhow::Error> {
    let client = auditor.get_client_from_pool().await
        .ok_or_else(|| anyhow::anyhow!("客户端池为空"))?;

    // 构建URL参数
    let url_params = format!(
        "symbol={}&interval={}&startTime={}&endTime={}&limit=1000",
        symbol, interval, start_time, end_time
    );

    // 构建完整URL
    let fapi_url = format!("https://fapi.binance.com/fapi/v1/klines?{}", url_params);

    // 发送请求
    let response = client.get(&fapi_url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .send()
        .await;

    match response {
        Ok(resp) => {
            if !resp.status().is_success() {
                let status = resp.status();

                // 检查是否需要移除客户端
                if status.as_u16() == 429 || status.as_u16() == 418 || status.as_u16() == 451 {
                    auditor.remove_client_from_pool(&client).await;
                }

                let text = resp.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("币安API请求失败: {} - {}", status, text));
            }

            let response_text = resp.text().await?;
            let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)?;

            let klines = raw_klines
                .iter()
                .filter_map(|raw| Kline::from_raw_kline(raw))
                .collect::<Vec<Kline>>();

            Ok(klines)
        }
        Err(e) => {
            // 连接错误，移除客户端
            if e.is_connect() || e.is_timeout() {
                auditor.remove_client_from_pool(&client).await;
            }
            Err(anyhow::anyhow!("网络请求失败: {}", e))
        }
    }
}

/// 稽核单个交易对和周期的数据（保留原函数作为兼容）
async fn audit_symbol_interval(
    symbol: String,
    interval: String,
    db_path: String,
    start_date: String,
    end_date: String,
) -> Result<(Vec<MismatchDetail>, AuditStats)> {
    // a. 计算安全的稽核时间范围
    let start_time = parse_date_to_timestamp(&start_date)?;
    let mut end_time = parse_date_to_timestamp(&end_date)? + 24 * 60 * 60 * 1000 - 1; // 结束日期的最后一毫秒

    let now_utc = Utc::now();
    let last_minute_start_ts = (now_utc.timestamp() / 60 - 1) * 60 * 1000;
    let safe_end_time = last_minute_start_ts;

    if end_time > safe_end_time {
        end_time = safe_end_time;
    }

    if start_time >= end_time {
        return Ok((Vec::new(), AuditStats::default()));
    }

    println!("🔍 稽核 {}/{}: {} - {}",
        symbol, interval,
        format_timestamp(start_time),
        format_timestamp(end_time)
    );

    // b. 拉取数据
    let binance_data = fetch_binance_klines(&symbol, &interval, start_time, end_time).await;
    let local_data = fetch_local_klines(&db_path, &symbol, &interval, start_time, end_time).await;

    // c. 对比并收集结果
    match (binance_data, local_data) {
        (Ok(b_klines), Ok(l_klines)) => {
            let (mismatches, stats) = compare_and_collect(&b_klines, &l_klines, &symbol, &interval)?;
            println!("✅ {}/{}: 检查了 {} 根K线，发现 {} 个差异",
                symbol, interval, stats.total_checked, mismatches.len());
            Ok((mismatches, stats))
        },
        (Err(e), _) => {
            eprintln!("❌ {}/{}: 币安数据获取失败: {}", symbol, interval, e);
            Err(anyhow::anyhow!("Binance fetch error for {}-{}: {}", symbol, interval, e))
        },
        (_, Err(e)) => {
            eprintln!("❌ {}/{}: 本地数据获取失败: {}", symbol, interval, e);
            Err(anyhow::anyhow!("Local fetch error for {}-{}: {}", symbol, interval, e))
        },
    }
}

/// 从币安API获取K线数据
async fn fetch_binance_klines(
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<Kline>> {
    let client = BinanceApi::create_new_client()?;

    // 构建URL参数
    let url_params = format!(
        "symbol={}&interval={}&startTime={}&endTime={}&limit=1000",
        symbol, interval, start_time, end_time
    );

    // 构建完整URL
    let fapi_url = format!("https://fapi.binance.com/fapi/v1/klines?{}", url_params);

    // 发送请求
    let response = client.get(&fapi_url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await?;
        return Err(anyhow::anyhow!("币安API请求失败: {} - {}", status, text));
    }

    let response_text = response.text().await?;
    let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)?;

    let klines = raw_klines
        .iter()
        .filter_map(|raw| Kline::from_raw_kline(raw))
        .collect::<Vec<Kline>>();

    Ok(klines)
}

/// 从本地数据库获取K线数据
async fn fetch_local_klines(
    db_path: &str,
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<Kline>> {
    let db = Database::new(db_path)?;
    let klines = db.get_klines_in_range(symbol, interval, start_time, end_time)?;
    Ok(klines)
}

/// 对比币安数据和本地数据，收集差异
fn compare_and_collect(
    binance_klines: &[Kline],
    local_klines: &[Kline],
    symbol: &str,
    interval: &str,
) -> Result<(Vec<MismatchDetail>, AuditStats)> {
    let mut mismatches = Vec::new();
    let mut stats = AuditStats::default();

    // 将K线数据转换为HashMap以便快速查找
    let mut binance_map = std::collections::HashMap::new();
    for kline in binance_klines {
        binance_map.insert(kline.open_time, kline);
    }

    let mut local_map = std::collections::HashMap::new();
    for kline in local_klines {
        local_map.insert(kline.open_time, kline);
    }

    // 获取所有时间戳的并集
    let mut all_timestamps = std::collections::HashSet::new();
    for kline in binance_klines {
        all_timestamps.insert(kline.open_time);
    }
    for kline in local_klines {
        all_timestamps.insert(kline.open_time);
    }

    stats.total_checked = all_timestamps.len();

    // 检查每个时间戳
    for &timestamp in &all_timestamps {
        let binance_kline = binance_map.get(&timestamp);
        let local_kline = local_map.get(&timestamp);

        match (binance_kline, local_kline) {
            (Some(b_kline), Some(l_kline)) => {
                // 两边都有，检查字段是否匹配
                if !klines_match(b_kline, l_kline) {
                    let detail = MismatchDetail {
                        symbol: symbol.to_string(),
                        interval: interval.to_string(),
                        mismatch_type: MismatchType::FieldMismatch,
                        open_time: timestamp,
                        local_kline: Some(ReportKline::from_kline(l_kline)),
                        binance_kline: Some(ReportKline::from_kline(b_kline)),
                    };
                    mismatches.push(detail);
                    stats.increment_mismatch();
                }
            },
            (Some(b_kline), None) => {
                // 币安有，本地没有
                let detail = MismatchDetail {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    mismatch_type: MismatchType::MissingInLocal,
                    open_time: timestamp,
                    local_kline: None,
                    binance_kline: Some(ReportKline::from_kline(b_kline)),
                };
                mismatches.push(detail);
                stats.increment_missing();
            },
            (None, Some(l_kline)) => {
                // 本地有，币安没有
                let detail = MismatchDetail {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    mismatch_type: MismatchType::ExtraInLocal,
                    open_time: timestamp,
                    local_kline: Some(ReportKline::from_kline(l_kline)),
                    binance_kline: None,
                };
                mismatches.push(detail);
                stats.increment_extra();
            },
            (None, None) => {
                // 不应该发生
                unreachable!("时间戳在集合中但两边都没有数据");
            }
        }
    }

    Ok((mismatches, stats))
}

/// 检查两个K线是否匹配（允许小的浮点数误差）
fn klines_match(binance_kline: &Kline, local_kline: &Kline) -> bool {
    const EPSILON: f64 = 1e-8;

    // 解析字符串为浮点数进行比较
    let b_open: f64 = binance_kline.open.parse().unwrap_or(0.0);
    let l_open: f64 = local_kline.open.parse().unwrap_or(0.0);
    let b_high: f64 = binance_kline.high.parse().unwrap_or(0.0);
    let l_high: f64 = local_kline.high.parse().unwrap_or(0.0);
    let b_low: f64 = binance_kline.low.parse().unwrap_or(0.0);
    let l_low: f64 = local_kline.low.parse().unwrap_or(0.0);
    let b_close: f64 = binance_kline.close.parse().unwrap_or(0.0);
    let l_close: f64 = local_kline.close.parse().unwrap_or(0.0);
    let b_volume: f64 = binance_kline.volume.parse().unwrap_or(0.0);
    let l_volume: f64 = local_kline.volume.parse().unwrap_or(0.0);

    // 检查基本时间戳
    if binance_kline.open_time != local_kline.open_time {
        return false;
    }

    // 检查价格字段（允许小的浮点数误差）
    if (b_open - l_open).abs() > EPSILON ||
       (b_high - l_high).abs() > EPSILON ||
       (b_low - l_low).abs() > EPSILON ||
       (b_close - l_close).abs() > EPSILON ||
       (b_volume - l_volume).abs() > EPSILON {
        return false;
    }

    // 检查交易笔数
    if binance_kline.number_of_trades != local_kline.number_of_trades {
        return false;
    }

    true
}

/// 解析日期字符串为时间戳（毫秒）
fn parse_date_to_timestamp(date_str: &str) -> Result<i64> {
    let naive_date = NaiveDateTime::parse_from_str(&format!("{} 00:00:00", date_str), "%Y-%m-%d %H:%M:%S")?;
    Ok(naive_date.timestamp() * 1000)
}

/// 格式化时间戳为可读字符串
fn format_timestamp(timestamp: i64) -> String {
    let datetime = chrono::Utc.timestamp_millis(timestamp);
    datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}
