use crate::api::BinanceApi;
use crate::config::Config;
use crate::db::Database;
use crate::error::{AppError, Result};
use crate::models::{DownloadResult, DownloadTask, Kline, Symbol};
use crate::storage::Storage;
use crate::utils::{
    calculate_time_chunks, format_duration_ms, format_ms, get_default_end_time,
    get_default_start_time, get_interval_ms,
};
use futures::stream::{self, StreamExt};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Downloader for Binance kline data
pub struct Downloader {
    config: Config,
    api: BinanceApi,
    db: Option<Database>,
}

impl Downloader {
    /// Create a new downloader
    pub fn new(config: Config) -> Result<Self> {
        // 创建BinanceApi实例，使用自动切换机制
        let api = BinanceApi::new("".to_string()); // 不再使用config.api_base_url

        // Initialize database if SQLite storage is enabled
        let db = if config.use_sqlite {
            Some(Database::new(&config.db_path)?)
        } else {
            None
        };

        Ok(Self { config, api, db })
    }

    /// Run the download process
    pub async fn run(&self) -> Result<()> {
        // Ensure output directory exists
        self.config.ensure_output_dir()?;

        // Get symbols to download
        let symbols = match &self.config.symbols {
            Some(symbols) => symbols.clone(),
            None => self.api.get_usdt_perpetual_symbols().await?,
        };

        if symbols.is_empty() {
            return Err(AppError::ConfigError("No symbols to download".to_string()));
        }

        info!("Downloading data for {} symbols with intervals {:?}", symbols.len(), self.config.intervals);

        // Determine time range
        let start_time = self.config.start_time.unwrap_or(0); // 如果没有指定，则使用0，表示使用基于周期的计算
        let end_time = self.config.end_time.unwrap_or_else(get_default_end_time);

        if start_time > 0 {
            info!(
                "Fixed time range: {} to {}",
                format_ms(start_time),
                format_ms(end_time)
            );
        } else {
            info!("Using period-based time ranges for each interval");
        }

        // 计算理论上的K线数量
        let mut total_theoretical_klines = 0;
        let mut total_actual_klines = 0;
        let mut kline_stats = Vec::new();

        // Download data for each interval
        for interval in &self.config.intervals {
            info!("Processing interval: {}", interval);

            // Create download tasks for this interval
            let tasks = self.create_download_tasks(&symbols, interval, start_time, end_time)?;
            info!("Created {} download tasks for interval {}", tasks.len(), interval);

            // 计算每个任务的理论K线数量
            for task in &tasks {
                let interval_ms = match task.interval.as_str() {
                    "1m" => 60_000,
                    "5m" => 300_000,
                    "30m" => 1_800_000,
                    "4h" => 14_400_000,
                    "1d" => 86_400_000,
                    "1w" => 604_800_000,
                    _ => 60_000, // 默认1分钟
                };

                // 计算理论上的K线数量
                let theoretical_count = (task.end_time - task.start_time) / interval_ms;
                total_theoretical_klines += theoretical_count;

                debug!("Theoretical kline count for {}/{}: {}",
                      task.symbol, task.interval, theoretical_count);
            }

            // Download data with concurrency limit
            let results = self.download_with_concurrency(tasks).await?;
            info!("Completed {} download tasks for interval {}", results.len(), interval);

            // 统计实际下载的K线数量
            for result in &results {
                let actual_count = result.klines.len() as i64;
                total_actual_klines += actual_count;

                // 保存统计信息
                kline_stats.push((result.symbol.clone(), result.interval.clone(), actual_count));

                debug!("Actual kline count for {}/{}: {}",
                      result.symbol, result.interval, actual_count);
            }

            // Save results
            self.save_results(interval, &results).await?;
        }

        // 输出统计结果
        info!("K-line statistics summary:");
        info!("Total theoretical K-lines: {}", total_theoretical_klines);
        info!("Total actual K-lines downloaded: {}", total_actual_klines);
        info!("Download completion rate: {:.2}%",
              (total_actual_klines as f64 / total_theoretical_klines as f64) * 100.0);

        for (symbol, interval, count) in kline_stats {
            info!("{}/{}: {} K-lines", symbol, interval, count);
        }

        Ok(())
    }

    /// Create download tasks for each symbol and time chunk
    fn create_download_tasks(
        &self,
        symbols: &[String],
        interval: &str,
        start_time: i64,
        _end_time: i64,
    ) -> Result<Vec<DownloadTask>> {
        // 当前时间作为结束时间
        let now = chrono::Utc::now().timestamp_millis();
        let end_time = now;

        let mut tasks = Vec::new();

        for symbol in symbols {
            let mut effective_start_time = start_time;

            // 如果是更新模式，则从数据库中获取最新的时间戳
            if self.config.update_only && self.config.use_sqlite {
                if let Some(db) = &self.db {
                    if let Ok(Some(latest_timestamp)) = db.get_latest_kline_timestamp(symbol, interval) {
                        // 使用最新时间戳加1毫秒作为起始时间，避免重复
                        effective_start_time = latest_timestamp + 1;

                        info!(
                            "Update mode: Using latest timestamp for {}/{}: {} to {}",
                            symbol,
                            interval,
                            crate::utils::format_ms(effective_start_time),
                            crate::utils::format_ms(end_time)
                        );
                    } else {
                        // 如果没有找到最新时间戳，则使用指定的起始时间或计算基于周期的起始时间
                        effective_start_time = self.calculate_start_time(start_time, interval, end_time);
                    }
                }
            } else {
                // 非更新模式，使用指定的起始时间或计算基于周期的起始时间
                effective_start_time = self.calculate_start_time(start_time, interval, end_time);
            }

            // 如果起始时间大于结束时间，则跳过该任务
            if effective_start_time >= end_time {
                info!("No new data to download for {}/{}", symbol, interval);
                continue;
            }

            // 创建一个任务，获取指定时间范围的K线数据
            tasks.push(DownloadTask {
                symbol: symbol.clone(),
                interval: interval.to_string(),
                start_time: effective_start_time,
                end_time,
                limit: 1000, // 币安API的最大限制
            });
        }

        Ok(tasks)
    }

    /// Calculate start time based on configuration
    fn calculate_start_time(&self, start_time: i64, interval: &str, end_time: i64) -> i64 {
        if start_time > 0 {
            // 优先使用用户指定的起始时间
            debug!(
                "Using fixed start time for {}: {} to {}",
                interval,
                crate::utils::format_ms(start_time),
                crate::utils::format_ms(end_time)
            );
            start_time
        } else {
            // 根据不同的周期设置不同的K线数量
            let interval_ms = match interval {
                "1m" => 60_000,        // 1分钟 = 60,000毫秒
                "5m" => 300_000,       // 5分钟 = 300,000毫秒
                "30m" => 1_800_000,    // 30分钟 = 1,800,000毫秒
                "4h" => 14_400_000,    // 4小时 = 14,400,000毫秒
                "1d" => 86_400_000,    // 1天 = 86,400,000毫秒
                "1w" => 604_800_000,   // 1周 = 604,800,000毫秒
                _ => 60_000,           // 默认为1分钟
            };

            // 根据周期设置不同的K线数量
            let kline_count = match interval {
                "1m" => 5000,
                "5m" => 5000,
                "30m" => 3000,
                "4h" => 3000,
                "1d" => 2000,
                "1w" => 1000,  // 修改为1000，确保能下载到所有品种的所有周线
                _ => 5000,
            };

            // 计算起始时间 = 结束时间 - (周期毫秒数 * K线数量)
            let calculated_start_time = end_time - (interval_ms * kline_count);

            info!(
                "Using calculated start time for {}: {} to {} (requesting {} K-lines)",
                interval,
                crate::utils::format_ms(calculated_start_time),
                crate::utils::format_ms(end_time),
                kline_count
            );

            calculated_start_time
        }
    }

    /// Download data with concurrency limit
    async fn download_with_concurrency(
        &self,
        tasks: Vec<DownloadTask>,
    ) -> Result<Vec<DownloadResult>> {
        let semaphore = Arc::new(Semaphore::new(self.config.concurrency));
        let total_tasks = tasks.len();
        let start_time = Instant::now();

        let results = stream::iter(tasks)
            .map(|task| {
                let api = &self.api;
                let semaphore = Arc::clone(&semaphore);
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let task_start = Instant::now();
                    let result = self.download_task(api, &task).await;
                    let task_duration = task_start.elapsed();

                    debug!(
                        "Downloaded {}/{} in {}ms",
                        task.symbol,
                        task.interval,
                        task_duration.as_millis()
                    );

                    // Add a small delay to avoid rate limiting
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    result
                }
            })
            .buffer_unordered(self.config.concurrency)
            .collect::<Vec<Result<DownloadResult>>>()
            .await;

        let elapsed = start_time.elapsed();
        info!(
            "Downloaded data for {} tasks in {}",
            total_tasks,
            format_duration_ms(elapsed.as_millis() as i64)
        );

        // Process results
        let mut successful_results = Vec::new();
        let mut error_count = 0;

        for result in results {
            match result {
                Ok(download_result) => successful_results.push(download_result),
                Err(e) => {
                    error_count += 1;
                    error!("Download task failed: {}", e);
                }
            }
        }

        info!(
            "Successfully downloaded {}/{} tasks",
            successful_results.len(),
            total_tasks
        );

        if error_count > 0 {
            warn!("{} tasks failed", error_count);
        }

        Ok(successful_results)
    }

    /// Download a single task
    async fn download_task(&self, api: &BinanceApi, task: &DownloadTask) -> Result<DownloadResult> {
        debug!(
            "Downloading {} klines for {}/{} from {} to {}",
            task.interval,
            task.symbol,
            task.interval,
            format_ms(task.start_time),
            format_ms(task.end_time)
        );

        // 实现分页下载，确保获取指定时间范围内的所有K线
        let mut all_klines = Vec::new();
        let mut current_start_time = task.start_time;
        let end_time = task.end_time;

        // 循环下载，直到获取到指定时间范围内的所有K线
        loop {
            // 创建一个新的下载任务，每次最多下载1000根K线
            let chunk_task = DownloadTask {
                symbol: task.symbol.clone(),
                interval: task.interval.clone(),
                start_time: current_start_time,
                end_time,
                limit: 1000, // 币安API的最大限制
            };

            debug!(
                "Downloading chunk of {} klines for {}/{} from {} to {}",
                chunk_task.interval,
                chunk_task.symbol,
                chunk_task.interval,
                format_ms(chunk_task.start_time),
                format_ms(chunk_task.end_time)
            );

            // 下载当前块的K线数据
            let chunk_klines = api.download_klines(&chunk_task).await?;

            // 如果没有获取到数据，说明已经下载完所有数据
            if chunk_klines.is_empty() {
                debug!("No more klines to download for {}/{}", task.symbol, task.interval);
                break;
            }

            // 获取当前块中最新的时间戳
            let latest_time = chunk_klines.iter().map(|k| k.open_time).max().unwrap_or(current_start_time);

            // 检查当前块的数据量
            let chunk_size = chunk_klines.len();
            let is_last_chunk = chunk_size < 1000 || latest_time + 1 >= end_time;

            // 将当前块的K线数据添加到总结果中
            all_klines.extend(chunk_klines);

            // 更新下一块的起始时间
            current_start_time = latest_time + 1;

            // 如果当前块的数据少于1000根，或者已经达到结束时间，说明已经下载完所有数据
            if is_last_chunk {
                debug!("Completed downloading klines for {}/{}", task.symbol, task.interval);
                break;
            }

            // 添加延时，避免API限制
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // 按时间戳排序
        all_klines.sort_by_key(|k| k.open_time);

        // 不再限制K线数量，保留所有下载的数据
        debug!(
            "Downloaded {} klines for {}/{}",
            all_klines.len(),
            task.symbol,
            task.interval
        );

        Ok(DownloadResult {
            symbol: task.symbol.clone(),
            interval: task.interval.clone(),
            klines: all_klines,
            start_time: task.start_time,
            end_time: task.end_time,
        })
    }

    /// Save download results
    async fn save_results(&self, interval: &str, results: &[DownloadResult]) -> Result<()> {
        // Group results by symbol
        let mut symbol_klines: std::collections::HashMap<String, Vec<Kline>> = std::collections::HashMap::new();

        for result in results {
            symbol_klines
                .entry(result.symbol.clone())
                .or_insert_with(Vec::new)
                .extend(result.klines.clone());
        }

        // Save each symbol's data
        for (symbol, klines) in symbol_klines {
            if klines.is_empty() {
                warn!("No klines to save for {}/{}", symbol, interval);
                continue;
            }

            // Sort klines by open time
            let mut sorted_klines = klines;
            sorted_klines.sort_by_key(|k| k.open_time);

            // 不再限制K线数量，保留所有下载的数据
            debug!(
                "Saving {} klines for {}/{}",
                sorted_klines.len(), symbol, interval
            );

            // Save to database if enabled
            if let Some(db) = &self.db {
                // Save symbol info
                db.save_symbol(&symbol, "", "USDT", "TRADING")?;

                // Save klines
                let count = db.save_klines(&symbol, interval, &sorted_klines)?;

                // Trim old klines if needed
                db.trim_klines(&symbol, interval, self.config.max_klines_per_symbol as i64)?;

                info!(
                    "{}/{}: {} K-lines downloaded successfully",
                    symbol, interval, count
                );
            } else {
                // Save to CSV
                let output_path = self.config.get_output_path(&symbol, interval);
                Storage::save_klines_to_csv(&sorted_klines, &output_path, false)?;

                info!(
                    "{}/{}: {} K-lines downloaded successfully",
                    symbol, interval, sorted_klines.len()
                );
            }
        }

        Ok(())
    }
}
