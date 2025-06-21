use crate::klcommon::{BinanceApi, Database, DownloadTask, Result, ServerTimeSyncManager};
use tracing::{info, warn, error, debug, Instrument};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use chrono::{Utc, TimeZone};
use tokio::sync::Semaphore;

/// 最新K线更新器
///
/// 负责每分钟下载一次所有品种、所有周期的最新一根K线
pub struct LatestKlineUpdater {
    db: Arc<Database>,
    api: BinanceApi,
    intervals: Vec<String>,
    time_sync_manager: Arc<ServerTimeSyncManager>,
    concurrency: usize,
}

impl LatestKlineUpdater {
    /// 创建新的最新K线更新器实例
    pub fn new(
        db: Arc<Database>,
        intervals: Vec<String>,
        time_sync_manager: Arc<ServerTimeSyncManager>,
        concurrency: usize,
    ) -> Self {
        let api = BinanceApi::new();
        Self {
            db,
            api,
            intervals,
            time_sync_manager,
            concurrency,
        }
    }

    /// 启动最新K线更新任务
    pub async fn start(&self) -> Result<()> {
        info!("启动最新K线更新任务，并发数: {}", self.concurrency);

        loop {
            // 计算下一分钟的开始时间（第1毫秒）
            let next_minute_start = self.calculate_next_minute_start();

            // 等待到下一分钟的开始
            let wait_time = next_minute_start - Utc::now().timestamp_millis();
            if wait_time > 0 {
                debug!("等待 {}毫秒 到下一分钟开始...", wait_time);
                sleep(Duration::from_millis(wait_time as u64)).await;
            } else {
                warn!("计算的等待时间为负值: {}毫秒，立即执行更新", wait_time);
            }

            // 执行一次更新
            let start_time = Instant::now();
            match self.update_latest_klines().await {
                Ok(count) => {
                    let elapsed = start_time.elapsed();
                    info!(
                        "更新了 {} 条最新K线数据，耗时: {:.2}秒",
                        count,
                        elapsed.as_secs_f64()
                    );
                }
                Err(e) => {
                    error!("更新最新K线数据失败: {}", e);
                }
            }
        }
    }

    /// 计算下一分钟的开始时间（考虑时间差和网络延迟）
    fn calculate_next_minute_start(&self) -> i64 {
        // 获取当前本地时间
        let local_time = Utc::now().timestamp_millis();

        // 计算下一分钟的开始时间（本地时间）
        let next_minute_local = ((local_time / 60000) + 1) * 60000;

        // 获取当前的时间差值和网络延迟
        let time_diff = self.time_sync_manager.get_time_diff();
        let network_delay = self.time_sync_manager.get_network_delay();

        // 安全边际（毫秒）- 提前一点发送请求，确保在分钟开始时到达服务器
        let safety_margin = 30;

        // 计算最优发送时间
        let optimal_send_time = next_minute_local - time_diff - network_delay - safety_margin;

        optimal_send_time
    }

    /// 更新所有品种、所有周期的最新一根K线
    async fn update_latest_klines(&self) -> Result<usize> {
        // 1. 获取所有正在交易的U本位永续合约交易对
        let all_symbols = match self.api.get_trading_usdt_perpetual_symbols().await {
            Ok(symbols) => symbols,
            Err(e) => {
                error!("获取交易对信息失败: {}", e);
                return Err(e);
            }
        };

        info!("获取到 {} 个交易对", all_symbols.len());

        // 2. 创建下载任务
        let mut tasks = Vec::new();

        // 获取当前本地时间
        let local_time = Utc::now().timestamp_millis();

        // 应用时间差，获取校准后的服务器时间
        let time_diff = self.time_sync_manager.get_time_diff();
        let current_time = local_time + time_diff;

        info!("当前本地时间: {}, 时间差: {}毫秒, 校准后的服务器时间: {}",
              self.format_timestamp(local_time),
              time_diff,
              self.format_timestamp(current_time));

        for symbol in &all_symbols {
            for interval in &self.intervals {
                // 计算起始时间（只获取最新的一根K线）
                let interval_ms = match interval.as_str() {
                    "1m" => 60 * 1000,
                    "5m" => 5 * 60 * 1000,
                    "30m" => 30 * 60 * 1000,
                    "4h" => 4 * 60 * 60 * 1000,
                    "1d" => 24 * 60 * 60 * 1000,
                    "1w" => 7 * 24 * 60 * 60 * 1000,
                    _ => 60 * 1000, // 默认1分钟
                };

                // 起始时间设置为当前时间减去一个周期，确保能获取到最新的一根K线
                let start_time = current_time - interval_ms;

                // 创建下载任务
                let task = DownloadTask {
                    symbol: symbol.clone(),
                    interval: interval.clone(),
                    start_time: Some(start_time),
                    end_time: Some(current_time),
                    limit: 1, // 只获取最新的一根K线
                };

                tasks.push(task);
            }
        }

        info!("创建了 {} 个最新K线下载任务", tasks.len());

        if tasks.is_empty() {
            info!("没有需要更新的K线数据");
            return Ok(0);
        }

        // 3. 执行下载任务
        let semaphore = Arc::new(Semaphore::new(self.concurrency)); // 使用指定的并发数
        let mut handles = Vec::new();
        let success_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        for task in tasks {
            let api_clone = self.api.clone();
            let semaphore_clone = semaphore.clone();
            let db_clone = self.db.clone();
            let success_count_clone = success_count.clone();

            let symbol = task.symbol.clone();
            let interval = task.interval.clone();
            let handle = tokio::spawn(async move {
                // 获取信号量许可
                let _permit = semaphore_clone.acquire().await.unwrap();

                let symbol = task.symbol.clone();
                let interval = task.interval.clone();

                // 下载任务
                match api_clone.download_continuous_klines(&task).await {
                    Ok(klines) => {
                        if klines.is_empty() {
                            debug!("{}/{}: 没有新的K线数据", symbol, interval);
                            return Ok(());
                        }

                        // 按时间排序
                        let mut sorted_klines = klines.clone();
                        sorted_klines.sort_by_key(|k| k.open_time);

                        // 保存到数据库
                        let _count = db_clone.save_klines(&symbol, &interval, &sorted_klines)?;

                        // 增加成功计数
                        success_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        debug!("{}/{}: 更新最新K线成功", symbol, interval);
                        Ok(())
                    }
                    Err(e) => {
                        error!("{}/{}: 更新最新K线失败: {}", symbol, interval, e);
                        Err(e)
                    }
                }
            }.instrument(tracing::info_span!(
                "update_latest_kline_task",
                symbol = %symbol,
                interval = %interval,
                target = "latest_updater"
            )));

            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            let _ = handle.await;
        }

        let total_success = success_count.load(std::sync::atomic::Ordering::SeqCst);
        Ok(total_success)
    }

    /// 将毫秒时间戳格式化为可读的日期时间字符串
    fn format_timestamp(&self, timestamp_ms: i64) -> String {
        let dt = chrono::Utc.timestamp_millis(timestamp_ms);
        dt.format("%Y-%m-%d %H:%M:%S.%3f").to_string()
    }
}
