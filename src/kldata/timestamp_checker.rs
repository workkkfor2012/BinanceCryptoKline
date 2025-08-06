use crate::klcommon::{Database, Result};
use log::info;
use std::sync::Arc;
use chrono::TimeZone;

/// K线时间戳检查器
pub struct TimestampChecker {
    db: Arc<Database>,
    intervals: Vec<String>,
}

impl TimestampChecker {
    /// 创建新的时间戳检查器实例
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        Self { db, intervals }
    }

    /// 检查所有品种的所有周期的最后一根K线时间戳是否一致，使用BTC作为标准
    pub async fn check_last_kline_consistency(&self) -> Result<()> {
        info!("开始检查所有品种的所有周期的最后一根K线时间戳是否一致...");

        // 获取数据库中所有已存在的K线表
        let existing_tables = self.get_existing_kline_tables()?;

        // 按周期分组存储最后一根K线的时间戳
        let mut interval_timestamps = std::collections::HashMap::new();

        // 遍历所有表，获取最后一根K线的时间戳
        for (symbol, interval) in &existing_tables {
            // 获取最后一根K线的时间戳
            if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(symbol, interval)? {
                interval_timestamps
                    .entry(interval.clone())
                    .or_insert_with(std::collections::HashMap::new)
                    .insert(symbol.clone(), last_timestamp);
            }
        }

        // 使用BTC作为标准检查所有品种的时间戳
        info!("\n使用BTC各周期的最后时间戳作为标准检查其他品种:");
        info!("{}", "=".repeat(80));

        // 选择BTC作为参考品种
        let reference_symbol = "BTCUSDT";

        info!("\n使用 {} 作为参考品种检查所有周期:", reference_symbol);

        // 获取所有周期
        let mut intervals: Vec<String> = interval_timestamps.keys().cloned().collect();
        intervals.sort();

        // 检查BTC在各个周期中是否都有数据
        let has_all_intervals = intervals.iter().all(|interval| {
            interval_timestamps.get(interval)
                .map(|symbols| symbols.contains_key(reference_symbol))
                .unwrap_or(false)
        });

        if !has_all_intervals {
            info!("  {} 在某些周期中没有数据，无法进行完整检查", reference_symbol);
            // 继续执行，只检查BTC存在的周期
        }

        // 获取BTC在各个周期的最后一根K线时间戳作为标准
        let mut btc_timestamps = std::collections::HashMap::new();
        for interval in &intervals {
            if let Some(symbols) = interval_timestamps.get(interval) {
                if let Some(&ts) = symbols.get(reference_symbol) {
                    btc_timestamps.insert(interval.clone(), ts);
                }
            }
        }

        // 输出BTC各周期的最后一根K线时间戳
        info!("\nBTC各周期最后一根K线时间戳 (标准):");

        let mut sorted_intervals: Vec<String> = btc_timestamps.keys().cloned().collect();
        sorted_intervals.sort();

        for interval in &sorted_intervals {
            if let Some(&ts) = btc_timestamps.get(interval) {
                info!("  {}: {}", interval, self.timestamp_to_datetime(ts));
            }
        }

        // 检查每个周期内其他品种与BTC的时间戳是否一致
        info!("\n检查每个周期内其他品种与BTC的时间戳是否一致:");

        for interval in &sorted_intervals {
            if let Some(symbols_data) = interval_timestamps.get(interval) {
                let btc_timestamp = btc_timestamps.get(interval).unwrap();

                // 统计与BTC时间戳一致和不一致的品种数量
                let mut consistent_symbols = Vec::new();
                let mut inconsistent_symbols = Vec::new();

                for (symbol, &timestamp) in symbols_data {
                    if symbol == reference_symbol {
                        continue;
                    }

                    if timestamp == *btc_timestamp {
                        consistent_symbols.push(symbol.clone());
                    } else {
                        inconsistent_symbols.push((symbol.clone(), timestamp));
                    }
                }

                // 输出结果
                info!("\n周期: {}", interval);
                info!("BTC时间戳: {}", self.timestamp_to_datetime(*btc_timestamp));
                info!("总品种数: {}", symbols_data.len());
                info!("与BTC一致的品种数: {}", consistent_symbols.len());
                info!("与BTC不一致的品种数: {}", inconsistent_symbols.len());

                // 如果有不一致的品种，显示详细信息
                if !inconsistent_symbols.is_empty() {
                    info!("\n不一致的品种:");

                    // 按时间戳分组
                    let mut timestamp_groups = std::collections::HashMap::new();
                    for (symbol, timestamp) in inconsistent_symbols {
                        timestamp_groups
                            .entry(timestamp)
                            .or_insert_with(Vec::new)
                            .push(symbol);
                    }

                    // 显示每个不同时间戳的品种
                    for (timestamp, symbols) in timestamp_groups {
                        info!("  时间戳 {}:", self.timestamp_to_datetime(timestamp));

                        // 最多显示10个品种
                        let mut sorted_symbols = symbols.clone();
                        sorted_symbols.sort();

                        for symbol in sorted_symbols.iter().take(10) {
                            info!("    - {}", symbol);
                        }

                        if symbols.len() > 10 {
                            info!("    - ... 以及其他 {} 个品种", symbols.len() - 10);
                        }
                    }
                }
            }
        }

        // 总结
        info!("\n\n总结:");
        info!("{}", "=".repeat(80));
        info!("已完成使用BTC各周期的最后时间戳作为标准检查其他品种的时间戳");

        Ok(())
    }



    /// 将毫秒时间戳转换为可读的日期时间格式
    fn timestamp_to_datetime(&self, timestamp_ms: i64) -> String {
        let dt = chrono::Utc.timestamp_millis(timestamp_ms);
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
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
}
