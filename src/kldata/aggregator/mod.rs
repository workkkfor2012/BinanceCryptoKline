// 导出聚合器相关模块
mod processor;

use crate::klcommon::{Database, Kline, Result};
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;
use tokio::time;
use once_cell::sync::OnceCell;

// 重新导出
pub use processor::process_klines;

// 全局实例
static INSTANCE: OnceCell<Arc<KlineAggregator>> = OnceCell::new();

/// K线聚合器
pub struct KlineAggregator {
    db: Arc<Database>,
    klines: TokioMutex<HashMap<String, Vec<Kline>>>,
}

impl KlineAggregator {
    /// 创建新的K线聚合器
    pub fn new(db: Arc<Database>) -> Arc<Self> {
        let aggregator = Arc::new(Self {
            db,
            klines: TokioMutex::new(HashMap::new()),
        });

        // 设置全局实例
        let _ = INSTANCE.set(aggregator.clone());

        aggregator
    }

    /// 获取全局实例
    pub fn get_instance() -> Option<Arc<Self>> {
        INSTANCE.get().cloned()
    }

    /// 启动聚合器
    pub async fn start(&self) -> Result<()> {
        info!("启动K线聚合器");

        // 每秒执行一次聚合
        let mut interval = time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            // 执行聚合
            if let Err(e) = self.aggregate().await {
                error!("聚合失败: {}", e);
            }
        }
    }

    /// 执行聚合
    async fn aggregate(&self) -> Result<()> {
        // 获取所有交易对
        let symbols = self.db.get_all_symbols()?;

        // 聚合每个交易对
        for symbol in &symbols {
            // 获取1分钟K线
            let klines_1m = self.db.get_latest_klines(symbol, "1m", 1000)?;

            if klines_1m.is_empty() {
                continue;
            }

            // 聚合5分钟K线
            let klines_5m = process_klines(&klines_1m, 5)?;
            self.save_klines(symbol, "5m", &klines_5m)?;

            // 聚合30分钟K线
            let klines_30m = process_klines(&klines_1m, 30)?;
            self.save_klines(symbol, "30m", &klines_30m)?;

            // 聚合4小时K线
            let klines_4h = process_klines(&klines_1m, 240)?;
            self.save_klines(symbol, "4h", &klines_4h)?;

            // 聚合1天K线
            let klines_1d = process_klines(&klines_1m, 1440)?;
            self.save_klines(symbol, "1d", &klines_1d)?;

            // 聚合1周K线
            let klines_1w = process_klines(&klines_1m, 10080)?;
            self.save_klines(symbol, "1w", &klines_1w)?;
        }

        Ok(())
    }

    /// 保存K线数据
    fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline]) -> Result<()> {
        if klines.is_empty() {
            return Ok(());
        }

        // 保存到数据库
        self.db.save_klines(symbol, interval, klines)?;

        Ok(())
    }

    /// 处理新的K线数据
    pub async fn process_kline(&self, symbol: &str, kline: &Kline) -> Result<()> {
        // 获取锁
        let mut klines_map = self.klines.lock().await;

        // 获取交易对的K线列表
        let key = symbol.to_string();
        let klines = klines_map.entry(key).or_insert_with(Vec::new);

        // 添加新K线
        klines.push(kline.clone());

        // 保持最多1000条K线
        if klines.len() > 1000 {
            klines.remove(0);
        }

        Ok(())
    }
}
