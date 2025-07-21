use kline_server::klcommon::{Database, Result, api::interval_to_milliseconds};
use std::sync::Arc;
use std::collections::HashMap;
use tracing::info;
use rusqlite::OptionalExtension;

/// 快速空洞检测器 - 用于快速检查数据完整性
pub struct QuickGapChecker {
    db: Arc<Database>,
}

/// 简化的空洞信息
#[derive(Debug, Clone)]
pub struct SimpleGap {
    pub symbol: String,
    pub interval: String,
    pub missing_periods: i64,
    pub duration_hours: f64,
}

impl QuickGapChecker {
    /// 创建新的快速空洞检测器实例
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// 快速检测所有时间间隔的数据空洞
    pub async fn quick_check(&self) -> Result<Vec<SimpleGap>> {
        info!("🔍 开始快速数据完整性检查...");

        // 检测所有标准时间间隔
        let intervals = vec!["1m".to_string(), "5m".to_string(), "30m".to_string(),
                           "1h".to_string(), "4h".to_string(), "1d".to_string()];
        let mut all_gaps = Vec::new();
        
        // 获取数据库中所有已存在的K线表
        let existing_tables = self.get_existing_kline_tables(&intervals)?;
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
        
        info!("✅ 快速检查完成，总共发现 {} 个数据空洞", all_gaps.len());
        
        Ok(all_gaps)
    }

    /// 检测特定时间间隔的数据空洞
    async fn detect_gaps_for_interval(&self, interval: &str, symbols: &[String]) -> Result<Vec<SimpleGap>> {
        let mut gaps = Vec::new();
        let interval_ms = interval_to_milliseconds(interval);
        
        for symbol in symbols {
            // 获取该品种的时间戳范围
            if let (Some(first_time), Some(last_time)) = (
                self.get_first_timestamp(symbol, interval).await?,
                self.get_last_timestamp(symbol, interval).await?
            ) {
                // 计算期望的总周期数
                let expected_periods = (last_time - first_time) / interval_ms + 1;
                
                // 获取实际的K线数量
                let actual_count = self.get_kline_count(symbol, interval).await?;
                
                // 如果实际数量少于期望数量，说明有空洞
                if actual_count < expected_periods {
                    let missing_periods = expected_periods - actual_count;
                    let duration_hours = (missing_periods * interval_ms) as f64 / (1000.0 * 3600.0);
                    
                    gaps.push(SimpleGap {
                        symbol: symbol.clone(),
                        interval: interval.to_string(),
                        missing_periods,
                        duration_hours,
                    });
                }
            }
        }
        
        Ok(gaps)
    }

    /// 获取第一个时间戳
    async fn get_first_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>> {
        let table_name = self.get_table_name(symbol, interval);
        let conn = self.db.get_connection()?;
        
        let query = format!("SELECT MIN(open_time) FROM {}", table_name);
        let result: Option<i64> = conn.query_row(&query, [], |row| row.get(0))
            .optional()?;
        
        Ok(result)
    }

    /// 获取最后一个时间戳
    async fn get_last_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>> {
        let table_name = self.get_table_name(symbol, interval);
        let conn = self.db.get_connection()?;
        
        let query = format!("SELECT MAX(open_time) FROM {}", table_name);
        let result: Option<i64> = conn.query_row(&query, [], |row| row.get(0))
            .optional()?;
        
        Ok(result)
    }

    /// 获取K线数量
    async fn get_kline_count(&self, symbol: &str, interval: &str) -> Result<i64> {
        let table_name = self.get_table_name(symbol, interval);
        let conn = self.db.get_connection()?;
        
        let query = format!("SELECT COUNT(*) FROM {}", table_name);
        let count: i64 = conn.query_row(&query, [], |row| row.get(0))?;
        
        Ok(count)
    }

    /// 生成快速报告
    pub fn generate_quick_report(&self, gaps: &[SimpleGap]) {
        if gaps.is_empty() {
            info!("🎉 恭喜！没有发现明显的数据空洞");
            return;
        }
        
        info!("📋 快速数据完整性报告");
        info!("{}", "=".repeat(60));

        // 总体统计
        let total_missing: i64 = gaps.iter().map(|g| g.missing_periods).sum();
        let total_duration: f64 = gaps.iter().map(|g| g.duration_hours).sum();

        info!("📊 总体统计:");
        info!("  有空洞的品种总数: {}", gaps.len());
        info!("  缺失周期总计: {}", total_missing);
        info!("  缺失时间总计: {:.2} 小时 ({:.2} 天)", total_duration, total_duration / 24.0);

        // 按时间间隔分组统计
        let mut gaps_by_interval: HashMap<String, Vec<&SimpleGap>> = HashMap::new();
        for gap in gaps {
            gaps_by_interval
                .entry(gap.interval.clone())
                .or_insert_with(Vec::new)
                .push(gap);
        }
        
        // 按时间间隔排序输出统计信息
        let mut sorted_intervals: Vec<String> = gaps_by_interval.keys().cloned().collect();
        sorted_intervals.sort_by(|a, b| {
            // 自定义排序：1m, 5m, 30m, 1h, 4h, 1d
            let order = ["1m", "5m", "30m", "1h", "4h", "1d"];
            let a_pos = order.iter().position(|&x| x == a).unwrap_or(999);
            let b_pos = order.iter().position(|&x| x == b).unwrap_or(999);
            a_pos.cmp(&b_pos)
        });

        for interval in &sorted_intervals {
            let interval_gaps = gaps_by_interval.get(interval).unwrap();
            info!("\n📊 {} 时间间隔:", interval);
            info!("  有空洞的品种数: {}", interval_gaps.len());
            
            let total_missing: i64 = interval_gaps.iter().map(|g| g.missing_periods).sum();
            let total_duration: f64 = interval_gaps.iter().map(|g| g.duration_hours).sum();
            
            info!("  缺失周期总数: {}", total_missing);
            info!("  缺失时间总计: {:.2} 小时", total_duration);
            
            // 找出最严重的空洞
            if let Some(worst_gap) = interval_gaps.iter().max_by_key(|g| g.missing_periods) {
                info!("  最严重空洞: {} (缺失 {} 个周期)", 
                      worst_gap.symbol, worst_gap.missing_periods);
            }
        }
        
        // 输出最严重的空洞列表（前10个）
        info!("\n🔍 最严重的数据空洞 (前10个):");
        info!("{}", "-".repeat(50));
        info!("{:<12} {:<8} {:<12} {:<10}", "品种", "间隔", "缺失周期", "时长(h)");
        info!("{}", "-".repeat(50));
        
        for gap in gaps.iter().take(10) {
            info!("{:<12} {:<8} {:<12} {:<10.2}", 
                  gap.symbol, gap.interval, gap.missing_periods, gap.duration_hours);
        }
        
        if gaps.len() > 10 {
            info!("... 还有 {} 个空洞未显示", gaps.len() - 10);
        }

        info!("\n💡 建议:");
        info!("  - 使用完整的空洞检测器进行详细分析: .\\start_gap_detector.ps1");
        info!("  - 运行历史数据回填服务补充缺失数据: .\\start_kldata_service.ps1");
    }

    /// 获取表名
    fn get_table_name(&self, symbol: &str, interval: &str) -> String {
        let symbol_lower = symbol.to_lowercase().replace("usdt", "");
        let interval_lower = interval.to_lowercase();
        format!("k_{symbol_lower}_{interval_lower}")
    }

    /// 获取数据库中已存在的K线表
    fn get_existing_kline_tables(&self, intervals: &[String]) -> Result<Vec<(String, String)>> {
        let conn = self.db.get_connection()?;
        let mut tables = Vec::new();

        let query = "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'k_%'";
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        for row in rows {
            let table_name = row?;
            if let Some((symbol, interval)) = self.parse_table_name(&table_name) {
                if intervals.contains(&interval) {
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

    let db_path = "data/klines.db";
    
    info!("🚀 启动快速数据完整性检查器");
    info!("📁 数据库路径: {}", db_path);
    info!("⚡ 检测范围: 所有时间间隔 (1m,5m,30m,1h,4h,1d)");

    // 创建数据库连接
    let db = Arc::new(Database::new(db_path)?);
    
    // 创建快速检测器
    let checker = QuickGapChecker::new(db);
    
    // 执行快速检查
    let gaps = checker.quick_check().await?;
    
    // 生成报告
    checker.generate_quick_report(&gaps);

    Ok(())
}
