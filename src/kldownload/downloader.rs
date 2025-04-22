use super::api::BinanceApi;
use super::config::Config;
use super::db::Database;
use super::error::{AppError, Result};
use super::models::{DownloadResult, DownloadTask, Kline};
use super::storage::Storage;
use super::utils::{
    format_duration_ms, format_ms, get_default_end_time,
};
use futures::stream::{self, StreamExt};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// All intervals list
#[allow(dead_code)]
pub const ALL_INTERVALS: [&str; 6] = ["1m", "5m", "30m", "4h", "1d", "1w"];

/// Kline count multiplier factor
pub const KLINE_MULTIPLIER: i64 = 10;
/// Expected kline counts for each interval
pub const EXPECTED_COUNTS: [i64; 6] = [
    50 * KLINE_MULTIPLIER, 50 * KLINE_MULTIPLIER, 30 * KLINE_MULTIPLIER,
    30 * KLINE_MULTIPLIER, 20 * KLINE_MULTIPLIER, 10 * KLINE_MULTIPLIER
];

/// Get kline count for a specific interval
pub fn get_kline_count_for_interval(interval: &str) -> i64 {
    match interval {
        "1m" => EXPECTED_COUNTS[0],
        "5m" => EXPECTED_COUNTS[1],
        "30m" => EXPECTED_COUNTS[2],
        "4h" => EXPECTED_COUNTS[3],
        "1d" => EXPECTED_COUNTS[4],
        "1w" => EXPECTED_COUNTS[5],
        _ => EXPECTED_COUNTS[0], // Default to 1m count
    }
}

/// Downloader for Binance kline data
pub struct Downloader {
    config: Config,
    api: BinanceApi,
    db: Option<Database>,
}

impl Downloader {
    /// Create a new downloader
    pub fn new(config: Config) -> Result<Self> {
        // Create BinanceApi instance with auto-switching mechanism
        let api = BinanceApi::new("".to_string()); // No longer using config.api_base_url

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
        let start_time = self.config.start_time.map_or(0, |dt| dt.timestamp_millis()); // If not specified, use 0 to indicate period-based calculation
        let end_time = self.config.end_time.map_or_else(get_default_end_time, |dt| dt.timestamp_millis());

        if start_time > 0 {
            info!(
                "Fixed time range: {} to {}",
                format_ms(start_time),
                format_ms(end_time)
            );
        } else {
            info!("Using period-based time ranges for each interval");
        }

        // Calculate theoretical kline counts
        let mut total_theoretical_klines = 0;
        let mut total_actual_klines = 0;
        let mut kline_stats = Vec::new();

        // Download data for each interval
        for interval in &self.config.intervals {
            info!("Processing interval: {}", interval);

            // Create download tasks for this interval
            let tasks = self.create_download_tasks(&symbols, interval, start_time, end_time)?;
            info!("Created {} download tasks for interval {}", tasks.len(), interval);

            // Calculate theoretical kline count for each task
            for task in &tasks {
                let interval_ms = match task.interval.as_str() {
                    "1m" => 60_000,
                    "5m" => 300_000,
                    "30m" => 1_800_000,
                    "4h" => 14_400_000,
                    "1d" => 86_400_000,
                    "1w" => 604_800_000,
                    _ => 60_000, // Default to 1m
                };

                // Calculate theoretical kline count
                let theoretical_count = (task.end_time - task.start_time) / interval_ms;
                total_theoretical_klines += theoretical_count;

                debug!("Theoretical kline count for {}/{}: {}",
                      task.symbol, task.interval, theoretical_count);
            }

            // Download data with concurrency limit
            let results = self.download_with_concurrency(tasks).await?;
            info!("Completed {} download tasks for interval {}", results.len(), interval);

            // Count actual downloaded klines
            for result in &results {
                let actual_count = result.klines.len() as i64;
                total_actual_klines += actual_count;

                // Save statistics
                kline_stats.push((result.symbol.clone(), result.interval.clone(), actual_count));

                debug!("Actual kline count for {}/{}: {}",
                      result.symbol, result.interval, actual_count);
            }

            // Save results
            self.save_results(interval, &results).await?;
        }

        // Output statistics
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
        // Use current time as end time
        let now = chrono::Utc::now().timestamp_millis();
        let end_time = now;

        let mut tasks = Vec::new();

        for symbol in symbols {
            let mut effective_start_time = start_time;

            // If in update mode, get latest timestamp from database
            if self.config.update_only && self.config.use_sqlite {
                if let Some(db) = &self.db {
                    if let Ok(Some(latest_timestamp)) = db.get_latest_kline_timestamp(symbol, interval) {
                        // Use latest timestamp + 1ms as start time to avoid duplicates
                        effective_start_time = latest_timestamp + 1;

                        info!(
                            "Update mode: Using latest timestamp for {}/{}: {} to {}",
                            symbol,
                            interval,
                            super::utils::format_ms(effective_start_time),
                            super::utils::format_ms(end_time)
                        );
                    } else {
                        // If no latest timestamp found, use specified start time or calculate period-based start time
                        effective_start_time = self.calculate_start_time(start_time, interval, end_time);
                    }
                }
            } else {
                // Not in update mode, use specified start time or calculate period-based start time
                effective_start_time = self.calculate_start_time(start_time, interval, end_time);
            }

            // Skip if start time is after end time
            if effective_start_time >= end_time {
                info!("No new data to download for {}/{}", symbol, interval);
                continue;
            }

            // Create a task to get klines for the specified time range
            tasks.push(DownloadTask {
                symbol: symbol.clone(),
                interval: interval.to_string(),
                start_time: effective_start_time,
                end_time,
                limit: 1000, // Binance API maximum limit
            });
        }

        Ok(tasks)
    }

    /// Calculate start time based on configuration
    fn calculate_start_time(&self, start_time: i64, interval: &str, end_time: i64) -> i64 {
        if start_time > 0 {
            // Prioritize user-specified start time
            debug!(
                "Using fixed start time for {}: {} to {}",
                interval,
                super::utils::format_ms(start_time),
                super::utils::format_ms(end_time)
            );
            start_time
        } else {
            // Set different kline counts based on interval
            let interval_ms = match interval {
                "1m" => 60_000,        // 1 minute = 60,000ms
                "5m" => 300_000,       // 5 minutes = 300,000ms
                "30m" => 1_800_000,    // 30 minutes = 1,800,000ms
                "4h" => 14_400_000,    // 4 hours = 14,400,000ms
                "1d" => 86_400_000,    // 1 day = 86,400,000ms
                "1w" => 604_800_000,   // 1 week = 604,800,000ms
                _ => 60_000,           // Default to 1 minute
            };

            // Use configuration from current module to get kline count
            let kline_count = get_kline_count_for_interval(interval);

            // Calculate start time = end time - (interval ms * kline count)
            let calculated_start_time = end_time - (interval_ms * kline_count);

            info!(
                "Using calculated start time for {}: {} to {} (requesting {} K-lines)",
                interval,
                super::utils::format_ms(calculated_start_time),
                super::utils::format_ms(end_time),
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

        // Implement paginated download to get all klines in the specified time range
        let mut all_klines = Vec::new();
        let mut current_start_time = task.start_time;
        let end_time = task.end_time;

        // Loop until all klines in the specified time range are downloaded
        loop {
            // Create a new download task, maximum 1000 klines per request
            let chunk_task = DownloadTask {
                symbol: task.symbol.clone(),
                interval: task.interval.clone(),
                start_time: current_start_time,
                end_time,
                limit: 1000, // Binance API maximum limit
            };

            debug!(
                "Downloading chunk of {} klines for {}/{} from {} to {}",
                chunk_task.interval,
                chunk_task.symbol,
                chunk_task.interval,
                format_ms(chunk_task.start_time),
                format_ms(chunk_task.end_time)
            );

            // Download klines for the current chunk
            let chunk_klines = api.download_klines(&chunk_task).await?;

            // If no data is returned, we've downloaded all data
            if chunk_klines.is_empty() {
                debug!("No more klines to download for {}/{}", task.symbol, task.interval);
                break;
            }

            // Get the latest timestamp in the current chunk
            let latest_time = chunk_klines.iter().map(|k| k.open_time).max().unwrap_or(current_start_time);

            // Check if this is the last chunk
            let chunk_size = chunk_klines.len();
            let is_last_chunk = chunk_size < 1000 || latest_time + 1 >= end_time;

            // Add the current chunk's klines to the total result
            all_klines.extend(chunk_klines);

            // Update the start time for the next chunk
            current_start_time = latest_time + 1;

            // If this is the last chunk, we're done
            if is_last_chunk {
                debug!("Completed downloading klines for {}/{}", task.symbol, task.interval);
                break;
            }

            // Add a delay to avoid API rate limits
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Sort by timestamp
        all_klines.sort_by_key(|k| k.open_time);

        // No longer limiting kline count, keep all downloaded data
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

            // No longer limiting kline count, keep all downloaded data
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
