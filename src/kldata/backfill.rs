use crate::klcommon::{Database, Result, Kline};
use crate::klcommon::models::DownloadTask;
use crate::kldata::downloader::BinanceApi;
use log::{info, warn, error};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Instant;

/// K线数据补齐模块
pub struct KlineBackfiller {
    db: Arc<Database>,
    api: BinanceApi,
    intervals: Vec<String>,
}

impl KlineBackfiller {
    /// 创建新的K线补齐器实例
    pub fn new(db: Arc<Database>, intervals: Vec<String>) -> Self {
        let api = BinanceApi::new();
        Self { db, api, intervals }
    }

    /// 运行一次性补齐流程
    pub async fn run_once(&self) -> Result<()> {
        info!("开始一次性补齐K线数据...");
        let start_time = Instant::now();

        // 1. 获取所有U本位永续合约交易对
        let api = BinanceApi::new();
        let all_symbols = match api.get_exchange_info().await {
            Ok(exchange_info) => {
                // 从交易所获取所有U本位永续合约交易对
                let mut symbols = Vec::new();
                for symbol_info in &exchange_info.symbols {
                    if symbol_info.symbol.ends_with("USDT") && symbol_info.status == "TRADING" {
                        symbols.push(symbol_info.symbol.clone());
                    }
                }
                info!("从交易所获取到 {} 个U本位永续合约交易对", symbols.len());
                symbols
            },
            Err(e) => {
                warn!("获取交易所信息失败: {}，使用默认交易对列表", e);
                vec![
                    "BTCUSDT".to_string(),
                    "ETHUSDT".to_string(),
                    "BNBUSDT".to_string(),
                    "ADAUSDT".to_string(),
                    "DOGEUSDT".to_string(),
                    "XRPUSDT".to_string(),
                    "SOLUSDT".to_string(),
                    "DOTUSDT".to_string(),
                ]
            }
        };

        // 2. 获取数据库中所有已存在的K线表
        let existing_tables = self.get_existing_kline_tables()?;
        info!("数据库中找到 {} 个已存在的K线表", existing_tables.len());

        // 3. 按品种和周期组织已存在的表
        let mut existing_symbol_intervals = HashMap::new();
        for (symbol, interval) in &existing_tables {
            existing_symbol_intervals
                .entry(symbol.clone())
                .or_insert_with(Vec::new)
                .push(interval.clone());
        }

        info!("需要补齐 {} 个已存在品种的K线数据", existing_symbol_intervals.len());

        // 4. 找出新增的品种（在交易所列表中但不在数据库中）
        let mut new_symbols = Vec::new();
        for symbol in &all_symbols {
            if !existing_symbol_intervals.contains_key(symbol) {
                new_symbols.push(symbol.clone());
            }
        }
        info!("发现 {} 个新品种需要下载完整数据", new_symbols.len());

        // 5. 创建下载任务
        let mut tasks = Vec::new();
        let current_time = chrono::Utc::now().timestamp_millis();

        // 5.1 为已存在的品种创建补齐任务
        for (symbol, intervals) in existing_symbol_intervals {
            for interval in intervals {
                // 获取最后一根K线的时间戳
                if let Some(last_timestamp) = self.db.get_latest_kline_timestamp(&symbol, &interval)? {
                    // 计算从最后时间戳到当前时间需要补齐的数据
                    let start_time = last_timestamp + 60000; // 从最后一根K线后一分钟开始

                    // 只有当最后K线时间早于当前时间时才需要补齐
                    if start_time < current_time {
                        let start_time_str = chrono::DateTime::<chrono::Utc>::from_timestamp(start_time / 1000, 0)
                            .map(|dt| dt.to_rfc3339())
                            .unwrap_or_else(|| start_time.to_string());

                        let current_time_str = chrono::DateTime::<chrono::Utc>::from_timestamp(current_time / 1000, 0)
                            .map(|dt| dt.to_rfc3339())
                            .unwrap_or_else(|| current_time.to_string());

                        info!("补齐 {}/{} 从 {} 到 {}",
                              symbol, interval,
                              start_time_str,
                              current_time_str);

                        // 创建下载任务
                        let task = DownloadTask {
                            symbol: symbol.clone(),
                            interval: interval.clone(),
                            start_time: Some(start_time),
                            end_time: Some(current_time),
                            limit: 1000,
                        };

                        tasks.push(task);
                    } else {
                        info!("{}/{} 已经是最新的，无需补齐", symbol, interval);
                    }
                } else {
                    warn!("无法确定 {}/{} 的最后K线时间戳", symbol, interval);
                }
            }
        }

        // 5.2 为新品种创建完整下载任务
        for symbol in new_symbols {
            for interval in &self.intervals {
                info!("为新品种 {}/{} 创建完整下载任务", symbol, interval);

                // 计算起始时间（根据周期不同设置不同的历史长度）
                let start_time = match interval.as_str() {
                    "1m" => current_time - 1000 * 60 * 1000, // 1000分钟
                    "5m" => current_time - 5000 * 60 * 1000, // 5000分钟
                    "30m" => current_time - 30000 * 60 * 1000, // 30000分钟
                    "4h" => current_time - 4 * 1000 * 60 * 60 * 1000, // 4000小时
                    "1d" => current_time - 1000 * 24 * 60 * 60 * 1000, // 1000天
                    "1w" => current_time - 200 * 7 * 24 * 60 * 60 * 1000, // 200周
                    _ => current_time - 1000 * 60 * 1000, // 默认1000分钟
                };

                // 创建下载任务
                let task = DownloadTask {
                    symbol: symbol.clone(),
                    interval: interval.clone(),
                    start_time: Some(start_time),
                    end_time: Some(current_time),
                    limit: 1000,
                };

                tasks.push(task);
            }
        }

        info!("创建了 {} 个下载任务（包括补齐任务和新品种完整下载任务）", tasks.len());

        if tasks.is_empty() {
            info!("没有需要补齐或下载的K线数据，所有数据都是最新的");
            return Ok(());
        }

        // 4. 执行下载任务
        let semaphore = Arc::new(tokio::sync::Semaphore::new(15)); // 使用15个并发
        let mut handles = Vec::new();

        for task in tasks {
            let api_clone = self.api.clone();
            let semaphore_clone = semaphore.clone();
            let db_clone = self.db.clone();

            let handle = tokio::spawn(async move {
                // 获取信号量许可
                let _permit = semaphore_clone.acquire().await.unwrap();

                let symbol = task.symbol.clone();
                let interval = task.interval.clone();

                // 下载任务
                match api_clone.download_klines(&task).await {
                    Ok(klines) => {
                        if klines.is_empty() {
                            info!("{}/{}: 没有新的K线数据需要补齐", symbol, interval);
                            return Ok(());
                        }

                        // 按时间排序
                        let mut sorted_klines = klines.clone();
                        sorted_klines.sort_by_key(|k| k.open_time);

                        // 保存到数据库
                        let count = db_clone.save_klines(&symbol, &interval, &sorted_klines)?;
                        info!("{}/{}: 成功补齐 {} 条K线数据", symbol, interval, count);

                        Ok(())
                    }
                    Err(e) => {
                        error!("{}/{}: 补齐K线数据失败: {}", symbol, interval, e);
                        Err(e)
                    }
                }
            });

            handles.push(handle);
        }

        // 等待所有任务完成
        let mut success_count = 0;
        let mut error_count = 0;

        for handle in handles {
            match handle.await {
                Ok(result) => {
                    match result {
                        Ok(_) => success_count += 1,
                        Err(_) => error_count += 1,
                    }
                }
                Err(e) => {
                    error!("任务执行失败: {}", e);
                    error_count += 1;
                }
            }
        }

        let elapsed = start_time.elapsed();
        info!(
            "K线补齐完成，成功: {}，失败: {}，耗时: {:?}",
            success_count, error_count, elapsed
        );

        Ok(())
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
