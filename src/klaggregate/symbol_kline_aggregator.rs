//! 单品种K线聚合模块
//! 
//! 负责单个交易品种的实时K线聚合，支持多个时间周期的同时聚合。

use crate::klaggregate::{AggTradeData, KlineData, BufferedKlineStore, PeriodInfo};
use crate::klcommon::{Result, AppError, api::get_aligned_time, ServerTimeSyncManager};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{instrument, event, Level, debug, warn, info};

/// 单品种K线聚合器
pub struct SymbolKlineAggregator {
    /// 品种名称
    symbol: String,
    
    /// 品种索引
    symbol_index: u32,
    
    /// 支持的时间周期信息
    period_infos: Vec<PeriodInfo>,
    
    /// 当前聚合状态（周期索引 -> K线聚合状态）
    aggregation_states: Arc<RwLock<HashMap<u32, KlineAggregationState>>>,
    
    /// 双缓冲存储引用
    buffered_store: Arc<BufferedKlineStore>,

    /// 服务器时间同步管理器
    time_sync_manager: Arc<ServerTimeSyncManager>,
}

/// K线聚合状态
#[derive(Debug, Clone)]
struct KlineAggregationState {
    /// 当前K线数据
    current_kline: KlineData,
    /// 周期信息
    period_info: PeriodInfo,
    /// 是否已初始化（收到第一笔交易）
    initialized: bool,
}

impl KlineAggregationState {
    /// 创建新的聚合状态
    fn new(symbol_index: u32, period_info: PeriodInfo) -> Self {
        Self {
            current_kline: KlineData::new(symbol_index, period_info.index, 0),
            period_info,
            initialized: false,
        }
    }
    
    /// 重置到新的时间周期
    fn reset_to_new_period(&mut self, new_open_time: i64) {
        self.current_kline.reset(new_open_time);
        self.initialized = false;
    }
    
    /// 检查交易时间是否属于当前K线周期
    fn is_trade_in_current_period(&self, trade_time: i64) -> bool {
        if !self.initialized {
            return true; // 如果还没初始化，任何交易都可以开始新周期
        }
        
        let period_end_time = self.current_kline.open_time + self.period_info.duration_ms;
        trade_time >= self.current_kline.open_time && trade_time < period_end_time
    }
    
    /// 检查当前K线是否应该结束
    fn should_finalize(&self, current_time: i64) -> bool {
        if !self.initialized {
            return false;
        }
        
        let period_end_time = self.current_kline.open_time + self.period_info.duration_ms;
        current_time >= period_end_time
    }
    
    /// 计算下一个周期的开始时间
    fn get_next_period_start(&self, trade_time: i64) -> i64 {
        get_aligned_time(trade_time, &self.period_info.interval)
    }
}

impl SymbolKlineAggregator {
    /// 创建新的品种K线聚合器
    #[instrument(target = "SymbolKlineAggregator", name="new_aggregator", fields(symbol = %symbol, symbol_index), skip_all, err)]
    pub async fn new(
        symbol: String,
        symbol_index: u32,
        supported_intervals: Vec<String>,
        buffered_store: Arc<BufferedKlineStore>,
        time_sync_manager: Arc<ServerTimeSyncManager>,
    ) -> Result<Self> {
        info!(target: "SymbolKlineAggregator", event_name = "聚合器初始化开始", symbol = %symbol, symbol_index = symbol_index, "创建品种K线聚合器: symbol={}, symbol_index={}", symbol, symbol_index);
        
        // 创建周期信息
        let mut period_infos = Vec::new();
        for (index, interval) in supported_intervals.iter().enumerate() {
            period_infos.push(PeriodInfo::new(interval.clone(), index as u32));
        }

        // 初始化聚合状态
        let mut aggregation_states = HashMap::new();
        for period_info in &period_infos {
            let state = KlineAggregationState::new(symbol_index, period_info.clone());
            aggregation_states.insert(period_info.index, state);
        }

        let period_count = period_infos.len();
        let aggregator = Self {
            symbol: symbol.clone(),
            symbol_index,
            period_infos,
            aggregation_states: Arc::new(RwLock::new(aggregation_states)),
            buffered_store,
            time_sync_manager,
        };

        info!(target: "SymbolKlineAggregator", event_name = "聚合器初始化完成", symbol = %symbol, symbol_index = symbol_index, period_count = period_count, "品种K线聚合器创建完成: symbol={}, symbol_index={}, period_count={}", symbol, symbol_index, period_count);
        Ok(aggregator)
    }
    
    /// 处理归集交易数据
    #[instrument(
        target = "SymbolKlineAggregator",
        name = "process_agg_trade",
        fields(
            symbol = %trade.symbol,
            price = %trade.price,
            quantity = %trade.quantity,
            timestamp_ms = %trade.timestamp_ms,
            is_buyer_maker = %trade.is_buyer_maker
        ),
        skip(self),
        err
    )]
    pub async fn process_agg_trade(&self, trade: &AggTradeData) -> Result<()> {
        if trade.symbol != self.symbol {
            return Err(AppError::DataError(format!(
                "交易品种不匹配: 期望 {}, 实际 {}",
                self.symbol,
                trade.symbol
            )));
        }

        // 使用校准后的服务器时间进行K线结束判断
        let current_time = if self.time_sync_manager.is_time_sync_valid() {
            // 使用校准后的服务器时间
            self.time_sync_manager.get_calibrated_server_time()
        } else {
            // 如果时间同步失效，使用交易时间作为备选
            warn!(target: "SymbolKlineAggregator", event_name = "时间同步失效", trade_timestamp = trade.timestamp_ms, symbol = %trade.symbol, "服务器时间同步失效，使用交易时间: trade_timestamp={}, symbol={}", trade.timestamp_ms, trade.symbol);
            trade.timestamp_ms
        };

        let mut states = self.aggregation_states.write().await;
        
        // 处理每个时间周期
        for period_info in &self.period_infos {
            if let Some(state) = states.get_mut(&period_info.index) {
                self.process_trade_for_period(state, trade, current_time).await?;
            }
        }
        
        Ok(())
    }
    
    /// 为特定周期处理交易
    #[instrument(target = "SymbolKlineAggregator", name="process_trade_for_period", fields(interval = %state.period_info.interval), skip_all)]
    async fn process_trade_for_period(
        &self,
        state: &mut KlineAggregationState,
        trade: &AggTradeData,
        current_time: i64,
    ) -> Result<()> {
        // 检查是否需要结束当前K线
        if state.should_finalize(current_time) && state.initialized {
            // 标记当前K线为最终状态
            state.current_kline.is_final = true;

            // 记录K线完成事件
            event!(
                Level::INFO,
                target = "SymbolKlineAggregator",
                event_name = "K线已完成",
                is_high_freq = true,
                symbol = %self.symbol,
                interval = %state.period_info.interval,
                open_time = state.current_kline.open_time,
                is_final = state.current_kline.is_final,
                current_time = current_time,
                open = state.current_kline.open,
                high = state.current_kline.high,
                low = state.current_kline.low,
                close = state.current_kline.close,
                volume = state.current_kline.volume,
                trade_count = state.current_kline.trade_count,
                "K线已完成"
            );

            // 写入到缓冲存储
            self.buffered_store.write_kline_data(
                self.symbol_index,
                state.period_info.index,
                &state.current_kline,
            ).await?;

            debug!(target: "SymbolKlineAggregator", "K线完成详情: symbol={}, interval={}, open_time={}, open={}, high={}, low={}, close={}, volume={}", self.symbol, state.period_info.interval, state.current_kline.open_time, state.current_kline.open, state.current_kline.high, state.current_kline.low, state.current_kline.close, state.current_kline.volume);

            // 重置到下一个周期
            let next_period_start = state.get_next_period_start(trade.timestamp_ms);
            state.reset_to_new_period(next_period_start);
        }
        
        // 检查交易是否属于当前周期
        if !state.is_trade_in_current_period(trade.timestamp_ms) {
            // 交易属于新周期，重置状态
            let new_period_start = state.get_next_period_start(trade.timestamp_ms);
            state.reset_to_new_period(new_period_start);
        }
        
        // 聚合交易数据到当前K线
        self.aggregate_trade_to_kline(state, trade)?;
        
        // 写入当前状态到缓冲存储（未完成的K线）
        self.buffered_store.write_kline_data(
            self.symbol_index,
            state.period_info.index,
            &state.current_kline,
        ).await?;
        
        Ok(())
    }
    
    /// 将交易数据聚合到K线
    fn aggregate_trade_to_kline(
        &self,
        state: &mut KlineAggregationState,
        trade: &AggTradeData,
    ) -> Result<()> {
        if !state.initialized {
            // 第一笔交易，初始化K线
            let open_time = state.get_next_period_start(trade.timestamp_ms);
            let kline = &mut state.current_kline;
            kline.open_time = open_time;
            kline.open = trade.price;
            kline.high = trade.price;
            kline.low = trade.price;
            kline.close = trade.price;
            kline.volume = trade.quantity;
            kline.turnover = trade.price * trade.quantity;
            kline.trade_count = 1;
            
            if trade.is_buyer_maker {
                kline.taker_buy_volume = 0.0;
                kline.taker_buy_turnover = 0.0;
            } else {
                kline.taker_buy_volume = trade.quantity;
                kline.taker_buy_turnover = trade.price * trade.quantity;
            }
            
            state.initialized = true;

            // 记录K线生成事件
            event!(
                Level::DEBUG,
                target = "SymbolKlineAggregator",
                event_name = "新K线已生成",
                symbol = %self.symbol,
                interval = %state.period_info.interval,
                open_time = kline.open_time,
                is_final = kline.is_final,
                volume = kline.volume,
                trade_count = kline.trade_count,
                "新K线已生成"
            );
        } else {
            // 更新现有K线
            let kline = &mut state.current_kline;
            kline.high = kline.high.max(trade.price);
            kline.low = kline.low.min(trade.price);
            kline.close = trade.price;
            kline.volume += trade.quantity;
            kline.turnover += trade.price * trade.quantity;
            kline.trade_count += 1;

            if !trade.is_buyer_maker {
                kline.taker_buy_volume += trade.quantity;
                kline.taker_buy_turnover += trade.price * trade.quantity;
            }
        }
        
        Ok(())
    }
    
    /// 强制完成所有当前K线（用于系统关闭时）
    #[instrument(target = "SymbolKlineAggregator", name="finalize_all", skip(self), err)]
    pub async fn finalize_all_klines(&self) -> Result<()> {
        let mut states = self.aggregation_states.write().await;
        
        for (period_index, state) in states.iter_mut() {
            if state.initialized && !state.current_kline.is_final {
                state.current_kline.is_final = true;
                
                self.buffered_store.write_kline_data(
                    self.symbol_index,
                    *period_index,
                    &state.current_kline,
                ).await?;
                
                debug!(target: "SymbolKlineAggregator", event_name = "K线强制完成", symbol = %self.symbol, interval = %state.period_info.interval, open_time = state.current_kline.open_time, period_index = *period_index, "强制完成K线: symbol={}, interval={}, open_time={}, period_index={}", self.symbol, state.period_info.interval, state.current_kline.open_time, period_index);
            }
        }
        
        Ok(())
    }
    
    /// 获取当前所有K线状态
    pub async fn get_current_klines(&self) -> Result<Vec<KlineData>> {
        let states = self.aggregation_states.read().await;
        let mut klines = Vec::new();
        
        for state in states.values() {
            if state.initialized {
                klines.push(state.current_kline.clone());
            }
        }
        
        Ok(klines)
    }
    
    /// 获取品种名称
    pub fn get_symbol(&self) -> &str {
        &self.symbol
    }
    
    /// 获取品种索引
    pub fn get_symbol_index(&self) -> u32 {
        self.symbol_index
    }
    
    /// 获取支持的周期数量
    pub fn get_period_count(&self) -> usize {
        self.period_infos.len()
    }
    
    /// 获取聚合统计信息
    pub async fn get_statistics(&self) -> AggregatorStatistics {
        let states = self.aggregation_states.read().await;
        let mut initialized_periods = 0;
        let mut finalized_klines = 0;
        
        for state in states.values() {
            if state.initialized {
                initialized_periods += 1;
                if state.current_kline.is_final {
                    finalized_klines += 1;
                }
            }
        }
        
        AggregatorStatistics {
            symbol: self.symbol.clone(),
            symbol_index: self.symbol_index,
            total_periods: self.period_infos.len(),
            initialized_periods,
            finalized_klines,
        }
    }
}

/// 聚合器统计信息
#[derive(Debug, Clone)]
pub struct AggregatorStatistics {
    /// 品种名称
    pub symbol: String,
    /// 品种索引
    pub symbol_index: u32,
    /// 总周期数
    pub total_periods: usize,
    /// 已初始化的周期数
    pub initialized_periods: usize,
    /// 已完成的K线数
    pub finalized_klines: usize,
}
