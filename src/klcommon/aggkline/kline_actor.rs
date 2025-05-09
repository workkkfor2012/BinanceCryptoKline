// K线Actor - 负责处理特定交易对的K线生成
use crate::klcommon::aggkline::models::{AppAggTrade, KlineBar};
use crate::klcommon::aggkline::kline_generator::KlineGenerator;
use crate::klcommon::{AppError, Result};
use log::{info, error};
use tokio::sync::mpsc;
use std::time::{Duration, Instant};

/// K线Actor - 负责处理特定交易对的K线生成
pub struct KlineActor {
    /// 交易对
    pub symbol: String,
    /// 支持的K线周期 (毫秒)
    pub kline_periods_ms: Vec<i64>,
    /// 交易数据接收器
    pub trade_receiver: mpsc::Receiver<AppAggTrade>,
    /// 完成的K线发送器
    pub completed_kline_sender: mpsc::Sender<KlineBar>,
    /// K线生成器列表 (每个周期一个)
    pub kline_generators: Vec<KlineGenerator>,
}

impl KlineActor {
    /// 创建新的K线Actor
    pub fn new(
        symbol: String,
        kline_periods_ms: Vec<i64>,
        trade_receiver: mpsc::Receiver<AppAggTrade>,
        completed_kline_sender: mpsc::Sender<KlineBar>,
    ) -> Self {
        // 为每个周期创建一个K线生成器
        let kline_generators = kline_periods_ms
            .iter()
            .map(|&period_ms| KlineGenerator::new(symbol.clone(), period_ms))
            .collect();

        Self {
            symbol: symbol.clone(),
            kline_periods_ms,
            trade_receiver,
            completed_kline_sender,
            kline_generators,
        }
    }

    /// 运行Actor
    pub async fn run(mut self) -> Result<()> {
        info!("[KlineActor-{}] 启动", self.symbol);

        // 统计信息
        let mut trade_count = 0;
        let mut kline_count = 0;
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(60);

        while let Some(trade) = self.trade_receiver.recv().await {
            trade_count += 1;

            // 每分钟输出一次统计信息
            if last_stats_time.elapsed() >= stats_interval {
                info!("[KlineActor-{}] 已处理 {} 条交易数据，生成 {} 条K线",
                      self.symbol, trade_count, kline_count);
                last_stats_time = Instant::now();
            }

            // 对每个周期的K线生成器处理交易数据
            for generator in self.kline_generators.iter_mut() {
                if let Some(completed_bar) = generator.update_with_trade(&trade) {
                    kline_count += 1;

                    // 发送完成的K线
                    if let Err(e) = self.completed_kline_sender.send(completed_bar).await {
                        error!("[KlineActor-{}] 发送完成的K线失败: {}", self.symbol, e);
                        return Err(AppError::ChannelError(format!("发送完成的K线失败: {}", e)));
                    }
                }
            }
        }

        info!("[KlineActor-{}] 交易数据通道已关闭，退出", self.symbol);
        Ok(())
    }
}
