// K线Actor - 负责处理特定交易对的K线生成
use crate::klcommon::models::{AppAggTrade, KlineBar};
use crate::klcommon::aggkline::kline_generator::KlineGenerator;
use crate::klcommon::aggkline::double_buffered_store::{DoubleBufferedKlineStore, KlineData};
use crate::klcommon::aggkline::global_registry::GlobalSymbolPeriodRegistry;
use crate::klcommon::{AppError, Result};
use log::{info, error, debug};
use tokio::sync::mpsc;
use std::time::{Duration, Instant};
use std::sync::Arc;

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
    /// 双缓冲K线存储
    pub kline_store: Option<Arc<DoubleBufferedKlineStore>>,
    /// 全局品种周期注册表
    pub registry: Option<Arc<GlobalSymbolPeriodRegistry>>,
    /// 品种索引
    pub symbol_index: Option<usize>,
    /// 周期索引映射 (period_ms -> index)
    pub period_indices: Vec<(i64, usize)>,
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
            kline_store: None,
            registry: None,
            symbol_index: None,
            period_indices: Vec::new(),
        }
    }

    /// 创建新的K线Actor（带双缓冲存储和注册表）
    pub fn new_with_store(
        symbol: String,
        kline_periods_ms: Vec<i64>,
        trade_receiver: mpsc::Receiver<AppAggTrade>,
        completed_kline_sender: mpsc::Sender<KlineBar>,
        kline_store: Arc<DoubleBufferedKlineStore>,
        registry: Arc<GlobalSymbolPeriodRegistry>,
    ) -> Self {
        // 为每个周期创建一个K线生成器
        let kline_generators = kline_periods_ms
            .iter()
            .map(|&period_ms| KlineGenerator::new(symbol.clone(), period_ms))
            .collect();

        // 获取品种索引
        let symbol_index = registry.get_symbol_index(&symbol);

        // 获取周期索引映射
        let mut period_indices = Vec::new();
        for &period_ms in &kline_periods_ms {
            // 将毫秒转换为周期字符串
            let period = match period_ms {
                60_000 => "1m",
                300_000 => "5m",
                1_800_000 => "30m",
                3_600_000 => "1h",
                14_400_000 => "4h",
                86_400_000 => "1d",
                604_800_000 => "1w",
                _ => continue, // 跳过未知周期
            };

            // 获取周期索引
            if let Some(index) = registry.get_period_index(period) {
                period_indices.push((period_ms, index));
            }
        }

        if let Some(idx) = symbol_index {
            info!("[KlineActor-{}] 初始化，品种索引: {}, 周期索引: {:?}", symbol, idx, period_indices);
        } else {
            error!("[KlineActor-{}] 初始化失败，未找到品种索引", symbol);
        }

        Self {
            symbol: symbol.clone(),
            kline_periods_ms,
            trade_receiver,
            completed_kline_sender,
            kline_generators,
            kline_store: Some(kline_store),
            registry: Some(registry),
            symbol_index,
            period_indices,
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
            for (i, generator) in self.kline_generators.iter_mut().enumerate() {
                let period_ms = self.kline_periods_ms[i];

                // 更新K线生成器
                let updated_bar = generator.update_with_trade(&trade);

                // 如果使用双缓冲存储，则更新存储
                if let (Some(kline_store), Some(symbol_idx)) = (&self.kline_store, self.symbol_index) {
                    // 获取当前K线数据
                    if let Some(current_bar) = &generator.current_bar {
                        // 查找对应的周期索引
                        if let Some(period_idx) = self.period_indices.iter().find(|(ms, _)| *ms == period_ms).map(|(_, idx)| *idx) {
                            // 创建KlineData
                            let kline_data = KlineData {
                                open_time_ms: current_bar.open_time_ms,
                                open: current_bar.open,
                                high: current_bar.high,
                                low: current_bar.low,
                                close: current_bar.close,
                                volume: current_bar.volume,
                                quote_asset_volume: current_bar.turnover,
                            };

                            // 写入双缓冲存储
                            if !kline_store.write_kline(symbol_idx, period_idx, kline_data) {
                                error!("[KlineActor-{}] 写入双缓冲存储失败: symbol_idx={}, period_idx={}",
                                       self.symbol, symbol_idx, period_idx);
                            } else {
                                debug!("[KlineActor-{}] 更新双缓冲存储: period={}ms, symbol_idx={}, period_idx={}",
                                       self.symbol, period_ms, symbol_idx, period_idx);
                            }
                        }
                    }
                }

                // 如果K线完成，发送到数据库存储队列
                if let Some(completed_bar) = updated_bar {
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
