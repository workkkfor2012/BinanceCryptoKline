use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, Kline};
use log::{info, warn, error};
use std::sync::Arc;
use tokio::sync::Semaphore;
use chrono::TimeZone;
use tracing::Instrument;

/// K线时间戳检查器
pub struct TimestampChecker {
    db: Arc<Database>,
    api: BinanceApi,
    intervals: Vec<String>,
}

impl TimestampChecker {
    /// 创建新的时间戳检查器实例
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        let api = BinanceApi::new();
        Self { db, api, intervals }
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

        // 专门为1分钟周期中时间戳不一致的品种再次下载最新K线
        self.fix_inconsistent_1m_klines().await?;

        Ok(())
    }

    /// 列出1分钟周期中时间戳不一致的品种并下载最新K线，使用BTC作为标准
    async fn fix_inconsistent_1m_klines(&self) -> Result<()> {
        info!("开始检查1分钟周期中时间戳不一致的品种...");

        // 获取数据库中所有已存在的K线表
        let existing_tables = self.get_existing_kline_tables()?;

        // 按周期分组存储最后一根K线的时间戳
        let mut interval_timestamps = std::collections::HashMap::new();

        // 遍历所有表，获取最后一根K线的时间戳
        for (symbol, interval) in &existing_tables {
            // 只处理1分钟周期
            if interval != "1m" {
                continue;
            }

            // 获取最后一根K线的时间戳
            if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(symbol, interval)? {
                interval_timestamps
                    .entry(interval.clone())
                    .or_insert_with(std::collections::HashMap::new)
                    .insert(symbol.clone(), last_timestamp);
            }
        }

        // 获取1分钟周期的时间戳数据
        if let Some(symbols_data) = interval_timestamps.get("1m") {
            // 找出BTC的时间戳作为参考
            let reference_symbol = "BTCUSDT";
            if let Some(&btc_timestamp) = symbols_data.get(reference_symbol) {
                info!("{} 1分钟周期的最后一根K线时间戳: {}", reference_symbol, self.timestamp_to_datetime(btc_timestamp));

                // 找出与BTC时间戳不一致的品种
                let mut inconsistent_symbols = Vec::new();
                for (symbol, &timestamp) in symbols_data.iter() {
                    if symbol != reference_symbol && timestamp != btc_timestamp {
                        inconsistent_symbols.push((symbol.clone(), timestamp));
                    }
                }

                if inconsistent_symbols.is_empty() {
                    info!("没有发现与{}时间戳不一致的1分钟周期品种", reference_symbol);
                    return Ok(());
                }

                // 按时间戳分组
                let mut timestamp_groups = std::collections::HashMap::new();
                for (symbol, timestamp) in &inconsistent_symbols {
                    timestamp_groups
                        .entry(*timestamp)
                        .or_insert_with(Vec::new)
                        .push(symbol.clone());
                }

                // 计算与BTC时间戳的差异（以分钟为单位）
                let btc_dt = chrono::Utc.timestamp_millis(btc_timestamp);
                let btc_formatted = btc_dt.format("%Y-%m-%d %H:%M:%S").to_string();

                // 输出不一致的品种
                info!("发现 {} 个与BTCUSDT时间戳不一致的1分钟周期品种", inconsistent_symbols.len());
                info!("BTCUSDT时间戳: {} ({})", btc_formatted, btc_timestamp);
                info!("按时间戳分组的不一致品种:");

                for (timestamp, symbols) in &timestamp_groups {
                    let ts_dt = chrono::Utc.timestamp_millis(*timestamp);
                    let ts_formatted = ts_dt.format("%Y-%m-%d %H:%M:%S").to_string();

                    // 计算时间差（分钟）
                    let diff_minutes = (btc_timestamp - timestamp) / (60 * 1000);
                    let diff_text = if diff_minutes > 0 {
                        format!("落后{}分钟", diff_minutes)
                    } else {
                        format!("超前{}分钟", -diff_minutes)
                    };

                    info!("  时间戳 {} ({}): {} 个品种, 与BTC {}",
                          ts_formatted, timestamp, symbols.len(), diff_text);

                    // 显示所有品种
                    let mut sorted_symbols = symbols.clone();
                    sorted_symbols.sort();

                    for symbol in sorted_symbols.iter() {
                        info!("    - {}", symbol);
                    }
                }

                // 创建下载任务
                let mut tasks = Vec::new();

                for (symbol, _) in inconsistent_symbols {
                    // 创建下载任务，只获取最新的一根K线
                    let task = DownloadTask {
                        symbol: symbol.clone(),
                        interval: "1m".to_string(),
                        start_time: None, // 不指定起始时间，币安会默认获取最新的一根K线
                        end_time: None,   // 不指定结束时间
                        limit: 1,         // 只获取最新的一根K线
                    };

                    tasks.push(task);
                }

                info!("创建了 {} 个下载任务，准备下载不一致品种的最新1分钟K线", tasks.len());

                // 执行下载任务
                let semaphore = Arc::new(Semaphore::new(50)); // 使用50个并发，匹配连接池大小
                let mut handles = Vec::new();

                for task in tasks {
                    let api_clone = self.api.clone();
                    let semaphore_clone = semaphore.clone();
                    let symbol = task.symbol.clone();
                    let symbol_for_span = symbol.clone();

                    let handle = tokio::spawn(async move {
                        // 获取信号量许可
                        let _permit = semaphore_clone.acquire().await.unwrap();

                        // 构建简化的URL参数，只包含symbol、interval和limit
                        let url_params = format!(
                            "symbol={}&interval={}&limit={}",
                            task.symbol, task.interval, task.limit
                        );

                        // 构建完整URL
                        let fapi_url = format!("https://fapi.binance.com/fapi/v1/klines?{}", url_params);

                        // 打印请求URL
                        info!("{}/1m: 请求URL: {}", symbol, fapi_url);

                        // 下载任务
                        match api_clone.download_continuous_klines(&task).await {
                            Ok(klines) => {
                                if klines.is_empty() {
                                    info!("{}/1m: 没有获取到最新K线数据", symbol);
                                    return Ok((symbol, None));
                                }

                                // 获取最新的一根K线
                                let latest_kline = klines.iter().max_by_key(|k| k.open_time).cloned();

                                if let Some(kline) = latest_kline {
                                    let dt = chrono::Utc.timestamp_millis(kline.open_time);
                                    let time_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                                    info!("{}/1m: 最新K线时间戳: {}", symbol, time_str);
                                    return Ok((symbol, Some(kline)));
                                } else {
                                    info!("{}/1m: 没有获取到有效的K线数据", symbol);
                                    return Ok((symbol, None));
                                }
                            }
                            Err(e) => {
                                error!("{}/1m: 下载最新K线失败: {}", symbol, e);
                                Err(e)
                            }
                        }
                    }.instrument(tracing::info_span!("timestamp_check_task", symbol = %symbol_for_span, target = "timestamp_checker")));

                    handles.push(handle);
                }

                // 等待所有任务完成
                let mut results: Vec<(String, Option<Kline>)> = Vec::new();

                for handle in handles {
                    match handle.await {
                        Ok(result) => {
                            match result {
                                Ok(data) => results.push(data),
                                Err(_) => {}
                            }
                        }
                        Err(e) => {
                            error!("任务执行失败: {}", e);
                        }
                    }
                }

                // 输出结果摘要
                let success_count = results.iter().filter(|(_, kline)| kline.is_some()).count();
                let error_count = results.iter().filter(|(_, kline)| kline.is_none()).count();

                info!(
                    "1分钟周期K线下载完成，成功: {}，失败: {}",
                    success_count, error_count
                );

                // 按时间戳分组显示结果
                let mut timestamp_results = std::collections::HashMap::new();

                for (symbol, kline_opt) in results {
                    if let Some(kline) = kline_opt {
                        timestamp_results
                            .entry(kline.open_time)
                            .or_insert_with(Vec::new)
                            .push(symbol);
                    }
                }

                info!("下载的最新K线时间戳分布:");

                for (timestamp, symbols) in timestamp_results {
                    info!("  时间戳 {}: {} 个品种", self.timestamp_to_datetime(timestamp), symbols.len());

                    // 显示所有品种
                    let mut sorted_symbols = symbols.clone();
                    sorted_symbols.sort();

                    for symbol in sorted_symbols.iter() {
                        info!("    - {}", symbol);
                    }
                }
            } else {
                warn!("数据库中没有找到BTCUSDT的1分钟周期K线数据");
            }
        }

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
