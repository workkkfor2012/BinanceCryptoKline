// K线处理器 - 负责将KlineBar转换为Kline并批量存储到数据库
use crate::klcommon::models::{KlineBar, Kline};
use crate::klcommon::{Database, Result};
use log::{info, error, debug};
use tokio::sync::mpsc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::Arc;

/// K线处理器 - 负责将KlineBar转换为Kline并批量存储到数据库
pub struct KlineProcessor {
    /// 数据库实例
    db: Arc<Database>,
    /// 批处理大小
    batch_size: usize,
    /// 批处理间隔（秒）
    batch_interval_secs: u64,
}

impl KlineProcessor {
    /// 创建新的K线处理器
    pub fn new(db: Arc<Database>, batch_size: usize, batch_interval_secs: u64) -> Self {
        Self {
            db,
            batch_size,
            batch_interval_secs,
        }
    }

    /// 运行处理任务
    pub async fn run(self, mut completed_kline_receiver: mpsc::Receiver<KlineBar>) -> Result<()> {
        info!("[KlineProcessor] 启动");

        // 批量写入缓冲区
        let mut kline_batch = Vec::with_capacity(self.batch_size);

        // 批量写入间隔
        let mut batch_interval = tokio::time::interval(Duration::from_secs(self.batch_interval_secs));

        // 统计信息
        let mut total_klines = 0;
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(30);

        loop {
            tokio::select! {
                Some(kline) = completed_kline_receiver.recv() => {
                    kline_batch.push(kline);
                    total_klines += 1;

                    // 批量大小达到阈值，立即写入
                    if kline_batch.len() >= self.batch_size {
                        if let Err(e) = self.process_batch(&mut kline_batch).await {
                            error!("[KlineProcessor] 批量处理失败: {}", e);
                        }
                    }
                }
                _ = batch_interval.tick() => {
                    // 定时写入未满的批次
                    if !kline_batch.is_empty() {
                        if let Err(e) = self.process_batch(&mut kline_batch).await {
                            error!("[KlineProcessor] 定时批量处理失败: {}", e);
                        }
                    }

                    // 输出统计信息
                    if last_stats_time.elapsed() >= stats_interval {
                        info!("[KlineProcessor] 已处理 {} 条K线", total_klines);
                        last_stats_time = Instant::now();
                    }
                }
                else => {
                    // 通道关闭且没有更多的tick
                    if !kline_batch.is_empty() {
                        info!("[KlineProcessor] 通道关闭，处理剩余 {} 条K线", kline_batch.len());
                        if let Err(e) = self.process_batch(&mut kline_batch).await {
                            error!("[KlineProcessor] 最终批量处理失败: {}", e);
                        }
                    }
                    info!("[KlineProcessor] 完成的K线通道已关闭，退出");
                    break;
                }
            }
        }

        Ok(())
    }

    /// 处理一批K线数据
    async fn process_batch(&self, kline_batch: &mut Vec<KlineBar>) -> Result<()> {
        if kline_batch.is_empty() {
            return Ok(());
        }

        // 按交易对和周期分组
        let mut grouped_klines: HashMap<(String, String), Vec<Kline>> = HashMap::new();

        for kline in kline_batch.iter() {
            // 将周期从毫秒转换为标准周期字符串
            let interval = self.ms_to_interval(kline.period_ms);
            let key = (kline.symbol.clone(), interval);

            // 将KlineBar转换为Kline
            let db_kline = kline.to_kline();

            // 添加到对应的组
            grouped_klines.entry(key).or_default().push(db_kline);
        }

        // 按组写入数据库
        for ((symbol, interval), klines) in grouped_klines {
            let symbol_clone = symbol.clone();
            let interval_clone = interval.clone();
            let db_clone = self.db.clone();
            let klines_clone = klines.clone();

            // 使用tokio::task::spawn_blocking将同步操作包装为异步
            match tokio::task::spawn_blocking(move || {
                db_clone.save_klines(&symbol_clone, &interval_clone, &klines_clone)
            }).await {
                Ok(Ok(count)) => {
                    debug!("[KlineProcessor] 成功保存 {} 条K线到 {}/{}", count, symbol, interval);
                }
                Ok(Err(e)) => {
                    error!("[KlineProcessor] 保存K线失败 {}/{}: {}", symbol, interval, e);
                }
                Err(e) => {
                    error!("[KlineProcessor] 异步任务执行失败 {}/{}: {}", symbol, interval, e);
                }
            }
        }

        // 清空批次
        let batch_size = kline_batch.len();
        kline_batch.clear();

        debug!("[KlineProcessor] 成功处理 {} 条K线", batch_size);

        Ok(())
    }

    /// 将毫秒周期转换为标准周期字符串
    fn ms_to_interval(&self, period_ms: i64) -> String {
        match period_ms {
            60000 => "1m".to_string(),
            300000 => "5m".to_string(),
            900000 => "15m".to_string(),
            1800000 => "30m".to_string(),
            3600000 => "1h".to_string(),
            14400000 => "4h".to_string(),
            86400000 => "1d".to_string(),
            604800000 => "1w".to_string(),
            _ => format!("{}ms", period_ms),
        }
    }
}
