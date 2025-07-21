use kline_server::klcommon::{Database, Result, api::interval_to_milliseconds, BinanceApi, DownloadTask};
use std::sync::Arc;
use std::collections::{HashMap, BTreeSet};
use chrono::{Utc, TimeZone};
use tracing::{info, warn, error, debug};
use clap::{Arg, Command};
use std::fs::File;
use std::io::Write;
use futures::{stream, StreamExt};
use tokio::sync::Semaphore;

/// K线数据空洞检测器
pub struct GapDetector {
    db: Arc<Database>,
    api: BinanceApi,
    intervals: Vec<String>,
}

/// 数据空洞信息
#[derive(Debug, Clone)]
pub struct DataGap {
    pub symbol: String,
    pub interval: String,
    pub start_time: i64,
    pub end_time: i64,
    pub missing_periods: i64,
    pub duration_hours: f64,
}

/// 修复结果
#[derive(Debug)]
pub struct RepairResult {
    pub symbol: String,
    pub interval: String,
    pub success: bool,
    pub klines_added: usize,
    pub error: Option<String>,
}

impl GapDetector {
    /// 创建新的空洞检测器实例
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        let api = BinanceApi::new();
        Self { db, api, intervals }
    }

    /// 检测所有品种和时间间隔的数据空洞
    pub async fn detect_all_gaps(&self) -> Result<Vec<DataGap>> {
        info!("🔍 开始检测数据库中的K线数据空洞...");
        
        let mut all_gaps = Vec::new();
        
        // 获取数据库中所有已存在的K线表
        let existing_tables = self.get_existing_kline_tables()?;
        info!("📊 发现 {} 个K线数据表", existing_tables.len());
        
        // 按间隔分组处理
        let mut tables_by_interval: HashMap<String, Vec<String>> = HashMap::new();
        for (symbol, interval) in existing_tables {
            tables_by_interval
                .entry(interval)
                .or_insert_with(Vec::new)
                .push(symbol);
        }
        
        // 检测每个时间间隔的数据空洞
        for (interval, symbols) in tables_by_interval {
            info!("🔍 检测 {} 时间间隔的数据空洞...", interval);
            
            let gaps = self.detect_gaps_for_interval(&interval, &symbols).await?;
            info!("📈 {} 时间间隔发现 {} 个数据空洞", interval, gaps.len());
            
            all_gaps.extend(gaps);
        }
        
        // 按严重程度排序（缺失周期数降序）
        all_gaps.sort_by(|a, b| b.missing_periods.cmp(&a.missing_periods));
        
        info!("✅ 空洞检测完成，总共发现 {} 个数据空洞", all_gaps.len());
        
        Ok(all_gaps)
    }

    /// 检测特定时间间隔的数据空洞
    async fn detect_gaps_for_interval(&self, interval: &str, symbols: &[String]) -> Result<Vec<DataGap>> {
        let mut gaps = Vec::new();
        let interval_ms = interval_to_milliseconds(interval);
        
        for symbol in symbols {
            debug!("🔍 检测 {}/{} 的数据空洞", symbol, interval);
            
            // 获取该品种的所有K线时间戳
            let timestamps = self.get_all_timestamps(symbol, interval).await?;
            
            if timestamps.is_empty() {
                warn!("⚠️  {}/{} 没有数据", symbol, interval);
                continue;
            }
            
            // 检测时间戳序列中的空洞
            let symbol_gaps = self.find_gaps_in_timestamps(&timestamps, interval_ms, symbol, interval);
            gaps.extend(symbol_gaps);
        }
        
        Ok(gaps)
    }

    /// 获取指定品种和时间间隔的所有时间戳
    async fn get_all_timestamps(&self, symbol: &str, interval: &str) -> Result<BTreeSet<i64>> {
        let table_name = self.get_table_name(symbol, interval);
        let conn = self.db.get_connection()?;
        
        let query = format!("SELECT open_time FROM {} ORDER BY open_time", table_name);
        let mut stmt = conn.prepare(&query)?;
        
        let mut timestamps = BTreeSet::new();
        let rows = stmt.query_map([], |row| {
            let timestamp: i64 = row.get(0)?;
            Ok(timestamp)
        })?;
        
        for row in rows {
            timestamps.insert(row?);
        }
        
        Ok(timestamps)
    }

    /// 在时间戳序列中查找空洞
    fn find_gaps_in_timestamps(&self, timestamps: &BTreeSet<i64>, interval_ms: i64, symbol: &str, interval: &str) -> Vec<DataGap> {
        let mut gaps = Vec::new();
        let timestamps_vec: Vec<i64> = timestamps.iter().cloned().collect();
        
        if timestamps_vec.len() < 2 {
            return gaps;
        }
        
        for i in 0..timestamps_vec.len() - 1 {
            let current_time = timestamps_vec[i];
            let next_time = timestamps_vec[i + 1];
            let expected_next_time = current_time + interval_ms;
            
            // 如果下一个时间戳不是期望的时间，说明有空洞
            if next_time > expected_next_time {
                let gap_start = expected_next_time;
                let gap_end = next_time - interval_ms;
                let missing_periods = (next_time - expected_next_time) / interval_ms;
                let duration_hours = (next_time - expected_next_time) as f64 / (1000.0 * 3600.0);
                
                // 只记录缺失超过1个周期的空洞
                if missing_periods > 0 {
                    gaps.push(DataGap {
                        symbol: symbol.to_string(),
                        interval: interval.to_string(),
                        start_time: gap_start,
                        end_time: gap_end,
                        missing_periods,
                        duration_hours,
                    });
                }
            }
        }
        
        gaps
    }

    /// 生成空洞报告
    pub fn generate_report(&self, gaps: &[DataGap]) {
        if gaps.is_empty() {
            info!("🎉 恭喜！没有发现任何数据空洞");
            return;
        }

        info!("📋 数据空洞详细报告");
        info!("{}", "=".repeat(80));

        // 按时间间隔分组统计
        let mut gaps_by_interval: HashMap<String, Vec<&DataGap>> = HashMap::new();
        for gap in gaps {
            gaps_by_interval
                .entry(gap.interval.clone())
                .or_insert_with(Vec::new)
                .push(gap);
        }

        // 输出每个时间间隔的统计信息
        for (interval, interval_gaps) in &gaps_by_interval {
            info!("\n📊 {} 时间间隔统计:", interval);
            info!("  空洞数量: {}", interval_gaps.len());

            let total_missing: i64 = interval_gaps.iter().map(|g| g.missing_periods).sum();
            let total_duration: f64 = interval_gaps.iter().map(|g| g.duration_hours).sum();

            info!("  缺失周期总数: {}", total_missing);
            info!("  缺失时间总计: {:.2} 小时", total_duration);

            // 找出最严重的空洞
            if let Some(worst_gap) = interval_gaps.iter().max_by_key(|g| g.missing_periods) {
                info!("  最严重空洞: {} (缺失 {} 个周期, {:.2} 小时)",
                      worst_gap.symbol, worst_gap.missing_periods, worst_gap.duration_hours);
            }
        }

        // 输出详细的空洞列表（前20个最严重的）
        info!("\n🔍 最严重的数据空洞 (前20个):");
        info!("{}", "-".repeat(80));
        info!("{:<12} {:<8} {:<20} {:<20} {:<8} {:<8}",
              "品种", "间隔", "开始时间", "结束时间", "缺失周期", "时长(h)");
        info!("{}", "-".repeat(80));

        for gap in gaps.iter().take(20) {
            let start_dt = self.timestamp_to_datetime(gap.start_time);
            let end_dt = self.timestamp_to_datetime(gap.end_time);

            info!("{:<12} {:<8} {:<20} {:<20} {:<8} {:<8.2}",
                  gap.symbol, gap.interval, start_dt, end_dt,
                  gap.missing_periods, gap.duration_hours);
        }

        if gaps.len() > 20 {
            info!("... 还有 {} 个空洞未显示", gaps.len() - 20);
        }
    }

    /// 自动修复数据空洞
    pub async fn auto_repair_gaps(&self, gaps: &[DataGap], max_gaps_to_repair: usize) -> Result<Vec<RepairResult>> {
        if gaps.is_empty() {
            info!("🎉 没有需要修复的数据空洞");
            return Ok(vec![]);
        }

        info!("🔧 开始自动修复数据空洞...");
        info!("📊 总空洞数: {}, 将修复前 {} 个", gaps.len(), max_gaps_to_repair);

        // 按严重程度排序，优先修复严重的空洞
        let mut sorted_gaps = gaps.to_vec();
        sorted_gaps.sort_by(|a, b| b.missing_periods.cmp(&a.missing_periods));

        // 限制修复数量
        let gaps_to_repair = &sorted_gaps[..std::cmp::min(max_gaps_to_repair, sorted_gaps.len())];

        // 创建修复任务
        let mut repair_tasks = Vec::new();
        for gap in gaps_to_repair {
            let task = DownloadTask {
                symbol: gap.symbol.clone(),
                interval: gap.interval.clone(),
                start_time: Some(gap.start_time),
                end_time: Some(gap.end_time),
                limit: 1000,
            };
            repair_tasks.push(task);
        }

        info!("📋 创建了 {} 个修复任务", repair_tasks.len());

        // 执行修复任务
        let results = self.execute_repair_tasks(repair_tasks).await;

        // 统计修复结果
        let success_count = results.iter().filter(|r| r.success).count();
        let total_klines_added: usize = results.iter().map(|r| r.klines_added).sum();

        info!("✅ 修复完成统计:");
        info!("  成功修复: {} 个空洞", success_count);
        info!("  失败修复: {} 个空洞", results.len() - success_count);
        info!("  新增K线: {} 条", total_klines_added);

        Ok(results)
    }

    /// 执行修复任务
    async fn execute_repair_tasks(&self, tasks: Vec<DownloadTask>) -> Vec<RepairResult> {
        let semaphore = Arc::new(Semaphore::new(5)); // 限制并发数
        let mut results = Vec::new();

        let task_results = stream::iter(tasks)
            .map(|task| {
                let api = self.api.clone();
                let db = self.db.clone();
                let semaphore = semaphore.clone();

                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    self.repair_single_gap(api, db, task).await
                }
            })
            .buffer_unordered(5)
            .collect::<Vec<_>>()
            .await;

        results.extend(task_results);
        results
    }

    /// 修复单个空洞
    async fn repair_single_gap(&self, api: BinanceApi, db: Arc<Database>, task: DownloadTask) -> RepairResult {
        info!("🔧 修复空洞: {} {}", task.symbol, task.interval);

        match api.download_continuous_klines(&task).await {
            Ok(klines) => {
                if klines.is_empty() {
                    warn!("⚠️ 没有获取到数据: {} {}", task.symbol, task.interval);
                    return RepairResult {
                        symbol: task.symbol,
                        interval: task.interval,
                        success: false,
                        klines_added: 0,
                        error: Some("没有获取到数据".to_string()),
                    };
                }

                match db.save_klines(&task.symbol, &task.interval, &klines).await {
                    Ok(count) => {
                        info!("✅ 修复成功: {} {} (新增 {} 条K线)", task.symbol, task.interval, count);
                        RepairResult {
                            symbol: task.symbol,
                            interval: task.interval,
                            success: true,
                            klines_added: count,
                            error: None,
                        }
                    }
                    Err(e) => {
                        error!("❌ 保存数据失败: {} {} - {}", task.symbol, task.interval, e);
                        RepairResult {
                            symbol: task.symbol,
                            interval: task.interval,
                            success: false,
                            klines_added: 0,
                            error: Some(format!("保存数据失败: {}", e)),
                        }
                    }
                }
            }
            Err(e) => {
                error!("❌ 下载数据失败: {} {} - {}", task.symbol, task.interval, e);
                RepairResult {
                    symbol: task.symbol,
                    interval: task.interval,
                    success: false,
                    klines_added: 0,
                    error: Some(format!("下载数据失败: {}", e)),
                }
            }
        }
    }

    /// 生成修复建议和批量修复脚本
    pub fn generate_fix_suggestions(&self, gaps: &[DataGap]) -> Result<()> {
        if gaps.is_empty() {
            return Ok(());
        }

        info!("\n💡 修复建议");
        info!("{}", "=".repeat(80));

        // 按严重程度分类
        let critical_gaps: Vec<&DataGap> = gaps.iter().filter(|g| g.missing_periods > 100).collect();
        let major_gaps: Vec<&DataGap> = gaps.iter().filter(|g| g.missing_periods > 10 && g.missing_periods <= 100).collect();
        let minor_gaps: Vec<&DataGap> = gaps.iter().filter(|g| g.missing_periods <= 10).collect();

        info!("🔴 严重空洞 (>100个周期): {} 个", critical_gaps.len());
        info!("🟡 重要空洞 (10-100个周期): {} 个", major_gaps.len());
        info!("🟢 轻微空洞 (≤10个周期): {} 个", minor_gaps.len());

        // 生成修复脚本
        self.generate_repair_script(gaps)?;

        Ok(())
    }

    /// 生成修复脚本
    fn generate_repair_script(&self, gaps: &[DataGap]) -> Result<()> {
        let script_path = "repair_gaps.ps1";
        let mut file = File::create(script_path)?;

        writeln!(file, "# K线数据空洞修复脚本")?;
        writeln!(file, "# 自动生成于: {}", Utc::now().format("%Y-%m-%d %H:%M:%S"))?;
        writeln!(file, "")?;
        writeln!(file, "param(")?;
        writeln!(file, "    [switch]$DryRun,")?;
        writeln!(file, "    [switch]$Verbose")?;
        writeln!(file, ")")?;
        writeln!(file, "")?;
        writeln!(file, "if ($DryRun) {{")?;
        writeln!(file, "    Write-Host '🔍 干运行模式 - 仅显示将要执行的命令' -ForegroundColor Yellow")?;
        writeln!(file, "}}")?;
        writeln!(file, "")?;

        // 按时间间隔分组生成修复命令
        let mut gaps_by_interval: HashMap<String, Vec<&DataGap>> = HashMap::new();
        for gap in gaps {
            gaps_by_interval
                .entry(gap.interval.clone())
                .or_insert_with(Vec::new)
                .push(gap);
        }

        for (interval, interval_gaps) in gaps_by_interval {
            writeln!(file, "# 修复 {} 时间间隔的空洞", interval)?;
            writeln!(file, "Write-Host '🔧 修复 {} 时间间隔的 {} 个空洞...' -ForegroundColor Green", interval, interval_gaps.len())?;
            writeln!(file, "")?;

            for gap in interval_gaps {
                let start_dt = self.timestamp_to_datetime(gap.start_time);
                let end_dt = self.timestamp_to_datetime(gap.end_time);

                writeln!(file, "# 修复 {} {} 空洞: {} 到 {} (缺失 {} 个周期)",
                         gap.symbol, gap.interval, start_dt, end_dt, gap.missing_periods)?;

                writeln!(file, "$command = 'cargo run --bin gap_detector -- --database data\\klines.db --repair --symbol {} --interval {} --start-time {} --end-time {}'",
                         gap.symbol, gap.interval, gap.start_time, gap.end_time)?;

                writeln!(file, "if ($DryRun) {{")?;
                writeln!(file, "    Write-Host \"  将执行: $command\" -ForegroundColor Cyan")?;
                writeln!(file, "}} else {{")?;
                writeln!(file, "    Write-Host \"  执行: {} {} {} 到 {}\" -ForegroundColor Blue",
                         gap.symbol, gap.interval, start_dt, end_dt)?;
                writeln!(file, "    Invoke-Expression $command")?;
                writeln!(file, "    if ($LASTEXITCODE -ne 0) {{")?;
                writeln!(file, "        Write-Host \"    ❌ 修复失败\" -ForegroundColor Red")?;
                writeln!(file, "    }} else {{")?;
                writeln!(file, "        Write-Host \"    ✅ 修复成功\" -ForegroundColor Green")?;
                writeln!(file, "    }}")?;
                writeln!(file, "}}")?;
                writeln!(file, "")?;
            }
        }

        writeln!(file, "Write-Host '✅ 修复脚本执行完成' -ForegroundColor Green")?;

        info!("📝 已生成修复脚本: {}", script_path);
        info!("💡 使用方法:");
        info!("   .\\{} -DryRun     # 预览将要执行的命令", script_path);
        info!("   .\\{}            # 执行实际修复", script_path);

        Ok(())
    }

    /// 将毫秒时间戳转换为可读的日期时间格式
    fn timestamp_to_datetime(&self, timestamp_ms: i64) -> String {
        let dt = Utc.timestamp_millis_opt(timestamp_ms).unwrap();
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    /// 获取表名
    fn get_table_name(&self, symbol: &str, interval: &str) -> String {
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        format!("k_{symbol_lower}_{interval_lower}")
    }

    /// 获取数据库中已存在的K线表
    fn get_existing_kline_tables(&self) -> Result<Vec<(String, String)>> {
        let conn = self.db.get_connection()?;
        let mut tables = Vec::new();

        let query = "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'k_%'";
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        for row in rows {
            let table_name = row?;
            if let Some((symbol, interval)) = self.parse_table_name(&table_name) {
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
            let symbol = format!("{}USDT", parts[1].to_uppercase());
            let interval = parts[2].to_string();
            return Some((symbol, interval));
        }

        None
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 解析命令行参数
    let matches = Command::new("gap_detector")
        .about("K线数据空洞检测器")
        .arg(Arg::new("db_path")
            .short('d')
            .long("database")
            .value_name("PATH")
            .help("数据库文件路径")
            .default_value("data/klines.db"))
        .arg(Arg::new("intervals")
            .short('i')
            .long("intervals")
            .value_name("INTERVALS")
            .help("要检测的时间间隔，用逗号分隔")
            .default_value("1m,5m,30m,1h,4h,1d"))
        .arg(Arg::new("repair")
            .long("repair")
            .help("自动修复检测到的空洞")
            .action(clap::ArgAction::SetTrue))
        .arg(Arg::new("max_repair")
            .long("max-repair")
            .value_name("COUNT")
            .help("最大修复空洞数量")
            .default_value("50"))
        .arg(Arg::new("symbol")
            .long("symbol")
            .value_name("SYMBOL")
            .help("只检测指定品种"))
        .arg(Arg::new("interval")
            .long("interval")
            .value_name("INTERVAL")
            .help("只检测指定时间间隔"))
        .arg(Arg::new("start_time")
            .long("start-time")
            .value_name("TIMESTAMP")
            .help("检测起始时间戳"))
        .arg(Arg::new("end_time")
            .long("end-time")
            .value_name("TIMESTAMP")
            .help("检测结束时间戳"))
        .get_matches();

    let db_path = matches.get_one::<String>("db_path").unwrap();
    let intervals_str = matches.get_one::<String>("intervals").unwrap();
    let intervals: Vec<String> = intervals_str.split(',').map(|s| s.trim().to_string()).collect();
    let repair_mode = matches.get_flag("repair");
    let max_repair: usize = matches.get_one::<String>("max_repair").unwrap().parse().unwrap_or(50);

    info!("🚀 启动K线数据空洞检测器");
    info!("📁 数据库路径: {}", db_path);
    info!("⏰ 检测间隔: {:?}", intervals);
    if repair_mode {
        info!("🔧 修复模式: 启用 (最大修复 {} 个空洞)", max_repair);
    }

    // 创建数据库连接
    let db = Arc::new(Database::new(db_path)?);

    // 创建空洞检测器
    let detector = GapDetector::new(db, intervals);

    // 执行空洞检测
    let gaps = detector.detect_all_gaps().await?;

    // 生成报告
    detector.generate_report(&gaps);

    // 生成修复建议
    detector.generate_fix_suggestions(&gaps)?;

    // 如果启用修复模式，执行自动修复
    if repair_mode && !gaps.is_empty() {
        info!("\n🔧 开始自动修复模式...");
        let repair_results = detector.auto_repair_gaps(&gaps, max_repair).await?;

        // 输出修复结果摘要
        info!("\n📊 修复结果摘要:");
        for result in &repair_results {
            if result.success {
                info!("✅ {} {}: 成功修复，新增 {} 条K线",
                      result.symbol, result.interval, result.klines_added);
            } else {
                warn!("❌ {} {}: 修复失败 - {}",
                      result.symbol, result.interval,
                      result.error.as_ref().unwrap_or(&"未知错误".to_string()));
            }
        }

        // 重新检测以验证修复效果
        info!("\n🔍 重新检测以验证修复效果...");
        let remaining_gaps = detector.detect_all_gaps().await?;

        if remaining_gaps.len() < gaps.len() {
            info!("✅ 修复成功！空洞数量从 {} 减少到 {}", gaps.len(), remaining_gaps.len());
        } else {
            warn!("⚠️ 修复效果有限，仍有 {} 个空洞", remaining_gaps.len());
        }
    }

    Ok(())
}
