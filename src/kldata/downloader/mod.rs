// 导出下载器相关模块
mod api;
mod config;

use crate::klcommon::{Database, DownloadResult, DownloadTask, Kline, Result};
use log::{debug, error, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use chrono::Utc;
use tokio::time::{interval, Duration, Instant as TokioInstant};

/// 将时间间隔转换为毫秒数
/// 例如: "1m" -> 60000, "1h" -> 3600000
fn interval_to_milliseconds(interval: &str) -> i64 {
    let last_char = interval.chars().last().unwrap_or('m');
    let value: i64 = interval[..interval.len() - 1].parse().unwrap_or(1);

    match last_char {
        'm' => value * 60 * 1000,        // 分钟
        'h' => value * 60 * 60 * 1000,   // 小时
        'd' => value * 24 * 60 * 60 * 1000, // 天
        'w' => value * 7 * 24 * 60 * 60 * 1000, // 周
        _ => value * 60 * 1000,  // 默认为分钟
    }
}

// 重新导出
pub use api::BinanceApi;
pub use config::Config;

// 预期的K线数量（用于验证）
pub const EXPECTED_COUNTS: &[(&str, &str, i64)] = &[
    ("BTCUSDT", "1m", 1000),
    ("BTCUSDT", "5m", 1000),
    ("BTCUSDT", "30m", 1000),
    ("BTCUSDT", "4h", 1000),
    ("BTCUSDT", "1d", 1000),
    ("BTCUSDT", "1w", 200),
];

/// K线下载器
pub struct Downloader {
    config: Config,
    db: Option<Arc<Database>>,
}

impl Downloader {
    /// 创建新的下载器实例
    pub fn new(config: Config) -> Result<Self> {
        // 确保输出目录存在
        config.ensure_output_dir()?;

        // 如果启用了SQLite，创建数据库连接
        let db = if config.use_sqlite {
            let db_path = config.db_path.clone().unwrap_or_else(|| PathBuf::from("./data/klines.db"));
            Some(Arc::new(Database::new(db_path)?))
        } else {
            None
        };

        Ok(Self { config, db })
    }

    /// 运行下载流程
    pub async fn run(&self) -> Result<Vec<DownloadResult>> {
        // 创建API客户端
        let api = BinanceApi::new();

        // 获取交易所信息
        info!("获取交易所信息...");
        let exchange_info = api.get_exchange_info().await?;

        // 获取所有U本位永续合约交易对
        let mut symbols = Vec::new();

        if let Some(user_symbols) = &self.config.symbols {
            // 使用用户指定的交易对
            symbols = user_symbols.clone();
            info!("使用用户指定的交易对: {}", symbols.join(", "));
        } else {
            // 从交易所获取所有U本位永续合约交易对
            for symbol_info in &exchange_info.symbols {
                if symbol_info.symbol.ends_with("USDT") && symbol_info.status == "TRADING" {
                    symbols.push(symbol_info.symbol.clone());
                }
            }
            info!("从交易所获取到 {} 个U本位永续合约交易对", symbols.len());
        }

        // 如果数据库已启用，保存所有交易对信息
        if let Some(db) = &self.db {
            for symbol_info in &exchange_info.symbols {
                if symbols.contains(&symbol_info.symbol) {
                    db.save_symbol(
                        &symbol_info.symbol,
                        &symbol_info.base_asset,
                        &symbol_info.quote_asset,
                        &symbol_info.status,
                    )?;
                }
            }
            info!("交易对信息已保存到数据库");
        }

        // 创建下载任务
        let mut tasks = Vec::new();

        for symbol in &symbols {
            for interval in &self.config.intervals {
                // 确定起始时间和结束时间
                let mut start_time = self.config.start_time;
                let end_time = self.config.end_time.unwrap_or_else(|| Utc::now().timestamp_millis());

                // 检查数据库中是否已存在K线数据
                if let Some(db) = &self.db {
                    if let Some(latest_time) = db.get_latest_kline_timestamp(symbol, interval)? {
                        // 如果存在数据，则从最新K线的下一分钟开始
                        let start_time_value = latest_time + 60000;
                        start_time = Some(start_time_value);

                        // 计算需要补齐的K线数量
                        let time_diff = end_time - start_time_value;
                        let interval_ms = interval_to_milliseconds(interval);
                        let bars_needed = (time_diff / interval_ms) as usize + 1;

                        info!("{}/{}: 已存在历史K线，从最新时间戳 {} 开始补齐，需要补齐 {} 根K线",
                              symbol, interval, start_time_value, bars_needed);

                        // 如果需要补齐的K线数量超过1000根，则分批下载
                        if bars_needed > 1000 {
                            info!("{}/{}: 需要补齐的K线数量超过1000根，将分批下载", symbol, interval);

                            // 创建多个下载任务，每个任务最多下载1000根K线
                            let mut batch_start = start_time_value;
                            let mut remaining = bars_needed;

                            while remaining > 0 {
                                let batch_size = std::cmp::min(remaining, 1000);
                                let batch_end = if remaining <= 1000 {
                                    end_time
                                } else {
                                    batch_start + (batch_size as i64 * interval_ms)
                                };

                                info!("{}/{}: 创建分批下载任务 - 从 {} 到 {}",
                                      symbol, interval, batch_start, batch_end);

                                // 创建任务
                                let task = DownloadTask {
                                    symbol: symbol.clone(),
                                    interval: interval.clone(),
                                    start_time: Some(batch_start),
                                    end_time: Some(batch_end),
                                    limit: 1000,
                                };

                                tasks.push(task);

                                // 更新下一批的起始时间和剩余数量
                                batch_start = batch_end + 1;
                                remaining -= batch_size;
                            }

                            // 跳过外层的任务创建，因为我们已经创建了分批任务
                            continue;
                        }
                    } else {
                        // 如果不存在数据，则下载默认数量的K线
                        info!("{}/{}: 不存在历史K线，下载默认数量（1000根）的K线", symbol, interval);
                        // 使用默认配置，不指定起始时间，将下载最近1000根K线
                        start_time = None;
                    }
                }

                // 创建任务
                let task = DownloadTask {
                    symbol: symbol.clone(),
                    interval: interval.clone(),
                    start_time,
                    end_time: self.config.end_time,
                    limit: self.config.max_limit as usize,
                };

                tasks.push(task);
            }
        }

        info!("创建了 {} 个下载任务", tasks.len());

        // 并发下载
        let total_tasks = tasks.len();
        let start_time = Instant::now();

        info!("开始并发下载，并发数: {}", self.config.concurrency);

        // 使用信号量限制并发数
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.concurrency));

        let mut handles = Vec::new();

        for task in tasks {
            let api_clone = api.clone();
            let semaphore_clone = semaphore.clone();

            let handle = tokio::spawn(async move {
                // 获取信号量许可
                let _permit = semaphore_clone.acquire().await.unwrap();

                // 下载任务
                match api_clone.download_klines(&task).await {
                    Ok(klines) => {
                        Ok(DownloadResult {
                            symbol: task.symbol,
                            interval: task.interval,
                            klines,
                        })
                    }
                    Err(e) => Err(e),
                }
            });

            handles.push(handle);
        }

        // 等待所有任务完成
        let mut results = Vec::new();

        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => error!("任务执行失败: {}", e),
            }
        }

        let elapsed = start_time.elapsed();
        info!(
            "所有下载任务完成，耗时: {:?}，平均每个任务: {:?}",
            elapsed,
            elapsed / total_tasks as u32
        );

        // 处理结果
        let mut successful_results = Vec::new();
        let mut error_count = 0;

        for result in results {
            match result {
                Ok(download_result) => {
                    // 保存结果
                    self.save_download_result(&download_result)?;
                    successful_results.push(download_result);
                }
                Err(e) => {
                    error_count += 1;
                    error!("下载任务失败: {}", e);
                }
            }
        }

        info!(
            "成功下载 {}/{} 个任务",
            successful_results.len(),
            total_tasks
        );

        if error_count > 0 {
            warn!("{} 个任务失败", error_count);
        }

        // 验证主要币种的K线数量
        if let Some(db) = &self.db {
            info!("验证主要币种的K线数量...");

            for &(symbol, interval, expected) in EXPECTED_COUNTS {
                let count = db.get_kline_count(symbol, interval)?;

                if count >= expected {
                    info!("{}/{}: {} K线 (符合预期 >= {})", symbol, interval, count, expected);
                } else {
                    warn!("{}/{}: {} K线 (低于预期 {})", symbol, interval, count, expected);
                }
            }
        }

        Ok(successful_results)
    }

    /// 保存下载结果
    fn save_download_result(&self, result: &DownloadResult) -> Result<()> {
        let symbol = &result.symbol;
        let interval = &result.interval;
        let klines = &result.klines;

        if klines.is_empty() {
            info!("{}/{}: 没有新的K线数据", symbol, interval);
            return Ok(());
        }

        // 按时间排序
        let mut sorted_klines = klines.clone();
        sorted_klines.sort_by_key(|k| k.open_time);

        // 保存到数据库或CSV
        if let Some(db) = &self.db {
            // 保存交易对信息
            db.save_symbol(symbol, "", "USDT", "TRADING")?;

            // 保存K线
            let count = db.save_klines(symbol, interval, &sorted_klines)?;

            // 裁剪旧K线（如果需要）
            db.trim_klines(symbol, interval, self.config.max_klines_per_symbol as i64)?;

            info!(
                "{}/{}: {} 条K线数据下载成功",
                symbol, interval, count
            );
        } else {
            // 保存到CSV
            let output_path = self.config.get_output_path(symbol, interval);
            self.save_klines_to_csv(&sorted_klines, &output_path)?;

            info!(
                "{}/{}: {} 条K线数据下载成功",
                symbol, interval, sorted_klines.len()
            );
        }

        Ok(())
    }

    /// 保存K线数据到CSV文件
    fn save_klines_to_csv(&self, klines: &[Kline], output_path: &PathBuf) -> Result<()> {
        // 创建CSV写入器
        let mut wtr = csv::Writer::from_path(output_path)?;

        // 写入标题
        wtr.write_record(&[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ])?;

        // 写入数据
        for kline in klines {
            wtr.write_record(&kline.to_csv_record())?;
        }

        // 刷新写入器
        wtr.flush()?;

        debug!("K线数据已保存到 {}", output_path.display());
        Ok(())
    }
}

/// 启动定时更新任务，每分钟获取所有配置品种和周期的最新K线
pub async fn start_periodic_update(db: Arc<Database>, intervals: Vec<String>) {
    info!("启动定时更新任务 (每分钟获取最新K线)");
    let api = BinanceApi::new(); // Create API client for the timer

    // 等待一段时间，避免正好在分钟切换时启动，导致第一次获取不到数据
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut timer = interval(Duration::from_secs(60));
    timer.tick().await; // Consume the initial immediate tick

    loop {
        timer.tick().await;
        let cycle_start_time = TokioInstant::now();
        debug!("定时任务触发，开始获取最新K线");

        // 从数据库获取交易对列表
        let symbols = match db.get_all_symbols() { // 假设此方法存在于 Database
           Ok(s) => s,
           Err(e) => {
               error!("定时任务：无法从数据库获取交易对列表: {}", e);
               // 等待下一个周期
               continue;
           }
        };

        if symbols.is_empty() {
            warn!("定时任务：数据库中没有找到交易对，跳过本次更新");
            continue;
        }

        let mut tasks = Vec::new();
        for symbol in &symbols {
            for interval_str in &intervals {
                tasks.push(DownloadTask {
                    symbol: symbol.clone(),
                    interval: interval_str.clone(),
                    start_time: None, // 不指定开始时间
                    end_time: None,   // 不指定结束时间
                    limit: 1,         // 只获取最新的1根K线
                });
            }
        }

        debug!("定时任务：为 {} 个交易对和 {} 个周期创建了 {} 个最新K线下载任务",
               symbols.len(), intervals.len(), tasks.len());

        let task_count = tasks.len();
        let mut success_count = 0;
        let mut no_new_data_count = 0;
        let mut save_error_count = 0;
        let mut download_error_count = 0;

        // 顺序执行下载和保存，简化处理逻辑
        for task in tasks {
            match api.download_klines(&task).await {
                Ok(klines) => {
                    if !klines.is_empty() {
                        // 保存获取到的单根K线
                        // 确保 save_klines 能处理重复插入 (例如使用 INSERT OR IGNORE 或类似逻辑)
                        match db.save_klines(&task.symbol, &task.interval, &klines) {
                            Ok(count) => {
                                if count > 0 {
                                    success_count += 1;
                                } else {
                                    no_new_data_count += 1;
                                }
                            },
                            Err(e) => {
                                error!("{}/{}: 定时任务保存K线失败: {}", task.symbol, task.interval, e);
                                save_error_count += 1;
                            },
                        }
                    } else {
                        debug!("{}/{}: 定时任务未获取到新的K线数据", task.symbol, task.interval);
                        no_new_data_count += 1;
                    }
                }
                Err(e) => {
                    error!("{}/{}: 定时任务下载最新K线失败: {}", task.symbol, task.interval, e);
                    download_error_count += 1;
                }
            }
            // 添加短暂休眠，避免过于频繁请求API（尽管请求量不大）
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        debug!(
            "定时任务完成一轮更新: 总任务={}, 成功保存={}, 无新数据/已存在={}, 下载失败={}, 保存失败={}, 耗时: {:?}",
            task_count, success_count, no_new_data_count, download_error_count, save_error_count, cycle_start_time.elapsed()
        );
    }
}
