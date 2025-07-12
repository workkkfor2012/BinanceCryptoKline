//! 交易品种元数据注册模块
//! 
//! 负责管理所有交易品种的元数据，包括品种索引分配、上市时间查询等功能。

use crate::klaggregate::{AggregateConfig, SymbolInfo, PeriodInfo};
use crate::klaggregate::log_targets::SYMBOL_METADATA_REGISTRY;
use crate::klcommon::{Result, AppError, BinanceApi, Database};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug, instrument};

/// 交易品种元数据注册表
pub struct SymbolMetadataRegistry {
    /// 配置
    config: AggregateConfig,
    
    /// 币安API客户端
    api_client: BinanceApi,
    
    /// 数据库连接
    database: Arc<Database>,
    
    /// 品种名到索引的映射
    symbol_to_index: Arc<RwLock<HashMap<String, u32>>>,
    
    /// 索引到品种名的映射
    index_to_symbol: Arc<RwLock<HashMap<u32, String>>>,
    
    /// 品种信息映射
    symbol_info: Arc<RwLock<HashMap<String, SymbolInfo>>>,
    
    /// 周期信息映射
    period_info: Arc<RwLock<HashMap<String, PeriodInfo>>>,
    
    /// 总的K线存储槽数量
    total_kline_slots: usize,
}

impl SymbolMetadataRegistry {
    /// 创建新的品种元数据注册表
    #[instrument(target = SYMBOL_METADATA_REGISTRY, name="new_registry", fields(total_kline_slots), skip_all, err)]
    pub async fn new(config: AggregateConfig) -> Result<Self> {
        let total_kline_slots = config.get_total_kline_slots();
        tracing::Span::current().record("total_kline_slots", total_kline_slots);

        info!(target: SYMBOL_METADATA_REGISTRY, log_type = "module", event_name = "注册表初始化", max_symbols = config.max_symbols, supported_intervals_count = config.supported_intervals.len(), "🔧 初始化交易品种元数据注册表 (最大品种: {}, 支持周期: {})", config.max_symbols, config.supported_intervals.len());
        
        // 创建API客户端
        let api_client = BinanceApi::new();
        
        // 创建数据库连接
        let database = Arc::new(Database::new(&config.database.database_path)?);
        
        // 使用之前计算的总存储槽数量
        
        let registry = Self {
            config: config.clone(),
            api_client,
            database,
            symbol_to_index: Arc::new(RwLock::new(HashMap::new())),
            index_to_symbol: Arc::new(RwLock::new(HashMap::new())),
            symbol_info: Arc::new(RwLock::new(HashMap::new())),
            period_info: Arc::new(RwLock::new(HashMap::new())),
            total_kline_slots,
        };
        
        // 初始化周期信息
        registry.initialize_period_info().await?;
        
        // 初始化品种信息
        registry.initialize_symbol_info().await?;
        
        info!(target: SYMBOL_METADATA_REGISTRY, log_type = "module", event_name = "注册表初始化完成", total_kline_slots = total_kline_slots, "✅ 交易品种元数据注册表初始化完成 (总K线槽位: {})", total_kline_slots);
        Ok(registry)
    }

    /// 初始化周期信息
    #[instrument(target = SYMBOL_METADATA_REGISTRY, fields(intervals_count = self.config.supported_intervals.len()), skip(self), err)]
    async fn initialize_period_info(&self) -> Result<()> {
        info!(target: SYMBOL_METADATA_REGISTRY, event_name = "周期信息初始化", intervals_count = self.config.supported_intervals.len(), "初始化周期信息 (周期数: {})", self.config.supported_intervals.len());

        let mut period_info = self.period_info.write().await;

        for (index, interval) in self.config.supported_intervals.iter().enumerate() {
            let info = PeriodInfo::new(interval.clone(), index as u32);
            period_info.insert(interval.clone(), info);
            debug!(target: SYMBOL_METADATA_REGISTRY, event_name = "周期注册", interval = %interval, index = index, "注册周期 {} (索引: {})", interval, index);
        }

        info!(target: SYMBOL_METADATA_REGISTRY, log_type = "module", event_name = "周期信息初始化完成", periods_count = period_info.len(), "✅ 已注册时间周期 (数量: {})", period_info.len());
        Ok(())
    }
    
    /// 初始化品种信息
    #[instrument(target = "SymbolMetadataRegistry", fields(symbols_count = 0, registered_count = 0), skip(self), err)]
    async fn initialize_symbol_info(&self) -> Result<()> {
        info!(target: "SymbolMetadataRegistry", event_name = "品种信息初始化", "初始化品种信息");

        // 1. 获取当前所有活跃的交易品种
        let symbols = self.fetch_active_symbols().await?;
        tracing::Span::current().record("symbols_count", symbols.len());

        info!(target: "SymbolMetadataRegistry", log_type = "module", event_name = "活跃品种获取", symbols_count = symbols.len(), "✅ 从API获取到活跃交易品种 (数量: {})", symbols.len());

        // 2. 批量查询所有品种的上市时间（优化性能）
        info!(target: "SymbolMetadataRegistry", event_name = "上市时间查询开始", symbols_count = symbols.len(), "批量查询品种上市时间 (品种数: {})", symbols.len());
        let symbol_listing_times = self.batch_get_symbol_listing_times(&symbols).await?;

        info!(target: "SymbolMetadataRegistry", event_name = "上市时间查询完成", valid_symbols_count = symbol_listing_times.len(), skipped_symbols_count = symbols.len() - symbol_listing_times.len(), "批量查询品种上市时间完成 (有效: {}, 跳过: {})", symbol_listing_times.len(), symbols.len() - symbol_listing_times.len());

        // 3. 按上市时间排序
        let mut sorted_symbols = symbol_listing_times;
        sorted_symbols.sort_by_key(|(_, listing_time)| *listing_time);

        // 4. 分配索引
        let mut symbol_to_index = self.symbol_to_index.write().await;
        let mut index_to_symbol = self.index_to_symbol.write().await;
        let mut symbol_info = self.symbol_info.write().await;

        for (index, (symbol, listing_time)) in sorted_symbols.into_iter().enumerate() {
            let symbol_index = index as u32;

            // 检查是否超过最大支持数量
            if index >= self.config.max_symbols {
                warn!(target: "SymbolMetadataRegistry", event_name = "品种数量超限", max_symbols = self.config.max_symbols, symbol = %symbol, current_index = index, "品种数量超过最大支持数量，跳过品种 {}", symbol);
                break;
            }

            // 建立映射关系
            symbol_to_index.insert(symbol.clone(), symbol_index);
            index_to_symbol.insert(symbol_index, symbol.clone());

            // 创建品种信息
            let info = SymbolInfo {
                symbol: symbol.clone(),
                index: symbol_index,
                listing_time,
            };
            symbol_info.insert(symbol.clone(), info);

            debug!(target: "SymbolMetadataRegistry", event_name = "品种注册", symbol = %symbol, symbol_index = symbol_index, listing_time = listing_time, "注册品种 {} (索引: {}, 上市时间: {})", symbol, symbol_index, listing_time);
        }

        let registered_count = symbol_to_index.len();
        tracing::Span::current().record("registered_count", registered_count);

        info!(target: "SymbolMetadataRegistry", event_name = "品种信息初始化完成", registered_count = registered_count, "已注册交易品种 (数量: {})", registered_count);
        Ok(())
    }

    /// 批量获取品种上市时间（性能优化）
    #[instrument(target = "SymbolMetadataRegistry", fields(symbols_count = symbols.len(), valid_count = 0, skipped_count = 0), skip(self, symbols), err)]
    async fn batch_get_symbol_listing_times(&self, symbols: &[String]) -> Result<Vec<(String, i64)>> {
        info!(target: "SymbolMetadataRegistry", event_name = "批量上市时间查询开始", symbols_count = symbols.len(), "开始批量查询品种上市时间 (品种数: {})", symbols.len());

        // 批量查询数据库中所有品种的最早日K线时间（更准确的上市时间）
        let batch_results = self.database.batch_get_earliest_kline_timestamps(symbols, "1d")?;

        let mut valid_symbols = Vec::new();
        let mut skipped_count = 0;

        for (symbol, timestamp_opt) in batch_results {
            match timestamp_opt {
                Some(timestamp) => {
                    valid_symbols.push((symbol.clone(), timestamp));
                    debug!(target: "SymbolMetadataRegistry", "品种上市时间: symbol={}, listing_time={}", symbol, timestamp);
                }
                None => {
                    warn!(target: "SymbolMetadataRegistry", event_name = "品种跳过无数据", symbol = %symbol, "跳过品种 {} ，无历史数据", symbol);
                    skipped_count += 1;
                }
            }
        }

        // 检查是否有足够的有效品种
        if valid_symbols.is_empty() {
            error!(target: "SymbolMetadataRegistry", event_name = "无有效品种", "没有任何品种有历史K线数据，请先下载历史数据");
            return Err(AppError::DataError(
                "没有任何品种有历史K线数据，请先下载历史数据".to_string()
            ));
        }

        if skipped_count > symbols.len() * 3 / 4 {
            warn!(target: "SymbolMetadataRegistry", event_name = "高跳过率警告", skipped_count = skipped_count, total_count = symbols.len(), skip_percentage = skipped_count * 100 / symbols.len(), "超过75%的品种没有历史数据，建议补充历史数据 (跳过: {}/{}, 比例: {}%)", skipped_count, symbols.len(), skipped_count * 100 / symbols.len());
        }

        tracing::Span::current().record("valid_count", valid_symbols.len());
        tracing::Span::current().record("skipped_count", skipped_count);

        info!(target: "SymbolMetadataRegistry", event_name = "批量上市时间查询结束", valid_count = valid_symbols.len(), skipped_count = skipped_count, "批量查询完成 (有效: {}, 跳过: {})", valid_symbols.len(), skipped_count);

        Ok(valid_symbols)
    }
    
    /// 获取活跃的交易品种列表
    #[instrument(target = "SymbolMetadataRegistry", fields(symbols_count = 0), skip(self), err)]
    async fn fetch_active_symbols(&self) -> Result<Vec<String>> {
        // 检查是否启用测试模式
        if let Ok(test_mode) = std::env::var("KLINE_TEST_MODE") {
            if test_mode.to_lowercase() == "true" {
                info!(target: "SymbolMetadataRegistry", log_type = "module", event_name = "测试模式激活", "🧪 测试模式已激活，只返回 'btcusdt' 交易对");
                tracing::Span::current().record("symbols_count", 1);
                return Ok(vec!["btcusdt".to_string()]);
            }
        }

        const MAX_RETRIES: usize = 3;
        const RETRY_DELAY_SECS: u64 = 2;

        for attempt in 1..=MAX_RETRIES {
            match self.api_client.get_trading_usdt_perpetual_symbols().await {
                Ok(symbols) => {
                    if symbols.is_empty() {
                        warn!(target: "SymbolMetadataRegistry", event_name = "API返回空列表", attempt = attempt, max_retries = MAX_RETRIES, "API返回空的交易品种列表 (尝试: {}/{})", attempt, MAX_RETRIES);
                    } else {
                        tracing::Span::current().record("symbols_count", symbols.len());
                        return Ok(symbols);
                    }
                }
                Err(e) => {
                    error!(target: "SymbolMetadataRegistry", log_type = "module", event_name = "获取品种API错误", attempt = attempt, max_retries = MAX_RETRIES, error = %e, "❌ 获取交易品种列表失败 (尝试: {}/{}, 错误: {})", attempt, MAX_RETRIES, e);
                }
            }

            if attempt < MAX_RETRIES {
                tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
            }
        }

        error!(target: "SymbolMetadataRegistry", log_type = "module", event_name = "获取品种最终失败", max_retries = MAX_RETRIES, "❌ 获取交易品种列表最终失败，已重试 {} 次", MAX_RETRIES);
        Err(AppError::ApiError(format!("获取交易品种列表失败，已重试 {} 次", MAX_RETRIES)))
    }
    
    /// 获取品种的上市时间（首个日K线时间）
    ///
    /// 这是关键逻辑，必须确保获取到准确的上市时间以保证索引稳定性
    #[instrument(target = "SymbolMetadataRegistry", fields(symbol = %symbol, listing_time = 0), skip(self), err)]
    async fn get_symbol_listing_time(&self, symbol: &str) -> Result<i64> {
        // 查询数据库中该品种的最早日K线时间（更准确的上市时间）
        match self.database.get_earliest_kline_timestamp(symbol, "1d") {
            Ok(Some(timestamp)) => {
                tracing::Span::current().record("listing_time", timestamp);
                debug!(target: "SymbolMetadataRegistry", event_name = "数据库上市时间", symbol = %symbol, listing_time = timestamp, "从数据库获取品种 {} 上市时间: {}", symbol, timestamp);
                Ok(timestamp)
            }
            Ok(None) => {
                // 数据库中没有数据，这是关键错误，不能使用默认值
                error!(target: "SymbolMetadataRegistry", event_name = "数据库上市时间未找到", symbol = %symbol, "数据库中没有品种 {} 的历史K线数据，无法确定上市时间", symbol);
                Err(AppError::DataError(format!(
                    "品种 {} 缺少历史K线数据，无法确定上市时间。请先下载该品种的历史数据。",
                    symbol
                )))
            }
            Err(e) => {
                error!(target: "SymbolMetadataRegistry", event_name = "数据库上市时间查询失败", symbol = %symbol, error = %e, "查询品种 {} 上市时间失败", symbol);
                Err(AppError::DataError(format!(
                    "查询品种 {} 的上市时间失败: {}。请检查数据库连接和数据完整性。",
                    symbol, e
                )))
            }
        }
    }
    
    /// 通过品种名获取索引
    pub async fn get_symbol_index(&self, symbol: &str) -> Option<u32> {
        self.symbol_to_index.read().await.get(symbol).copied()
    }
    
    /// 通过索引获取品种名
    pub async fn get_symbol_by_index(&self, index: u32) -> Option<String> {
        self.index_to_symbol.read().await.get(&index).cloned()
    }
    
    /// 获取所有已注册的品种及其索引
    pub async fn get_all_symbols(&self) -> Result<Vec<(String, u32)>> {
        let symbol_to_index = self.symbol_to_index.read().await;
        Ok(symbol_to_index.iter().map(|(symbol, &index)| (symbol.clone(), index)).collect())
    }
    
    /// 获取品种信息
    pub async fn get_symbol_info(&self, symbol: &str) -> Option<SymbolInfo> {
        self.symbol_info.read().await.get(symbol).cloned()
    }
    
    /// 获取周期信息
    pub async fn get_period_info(&self, interval: &str) -> Option<PeriodInfo> {
        self.period_info.read().await.get(interval).cloned()
    }
    
    /// 获取周期索引
    pub async fn get_period_index(&self, interval: &str) -> Option<u32> {
        self.period_info.read().await.get(interval).map(|info| info.index)
    }
    
    /// 通过索引获取周期字符串
    pub async fn get_interval_by_index(&self, index: u32) -> Option<String> {
        let period_info = self.period_info.read().await;
        period_info.values().find(|info| info.index == index).map(|info| info.interval.clone())
    }
    
    /// 获取总的K线存储槽数量
    pub fn get_total_kline_slots(&self) -> usize {
        self.total_kline_slots
    }
    
    /// 获取支持的周期数量
    pub fn get_periods_per_symbol(&self) -> usize {
        self.config.supported_intervals.len()
    }
    
    /// 计算扁平化存储索引
    pub fn calculate_flat_index(&self, symbol_index: u32, period_index: u32) -> usize {
        (symbol_index as usize) * self.get_periods_per_symbol() + (period_index as usize)
    }
    
    /// 从扁平化索引解析品种和周期索引
    pub fn parse_flat_index(&self, flat_index: usize) -> (u32, u32) {
        let periods_per_symbol = self.get_periods_per_symbol();
        let symbol_index = (flat_index / periods_per_symbol) as u32;
        let period_index = (flat_index % periods_per_symbol) as u32;
        (symbol_index, period_index)
    }
    
    /// 获取注册的品种数量
    pub async fn get_symbol_count(&self) -> usize {
        self.symbol_to_index.read().await.len()
    }
    
    /// 获取支持的周期列表
    pub fn get_supported_intervals(&self) -> &[String] {
        &self.config.supported_intervals
    }
}
