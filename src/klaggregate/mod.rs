//! K线聚合模块
//! 
//! 本模块实现基于币安归集交易数据的实时K线聚合系统，包含以下核心组件：
//! 
//! - `MarketDataIngestor`: 行情数据接入与解析模块
//! - `TradeEventRouter`: 交易事件路由模块  
//! - `SymbolKlineAggregator`: 单品种K线聚合模块
//! - `SymbolMetadataRegistry`: 交易品种元数据注册模块
//! - `BufferedKlineStore`: 自调度双缓冲K线存储模块
//! - `KlineDataPersistence`: K线数据持久化模块

pub mod types;
pub mod symbol_metadata_registry;
pub mod buffered_kline_store;
pub mod symbol_kline_aggregator;
pub mod market_data_ingestor;
pub mod trade_event_router;
pub mod kline_data_persistence;
pub mod config;
pub mod observability;
pub mod validation_rules;

// 重新导出核心类型
pub use types::*;
pub use symbol_metadata_registry::SymbolMetadataRegistry;
pub use buffered_kline_store::BufferedKlineStore;
pub use symbol_kline_aggregator::SymbolKlineAggregator;
pub use market_data_ingestor::MarketDataIngestor;
pub use trade_event_router::TradeEventRouter;
pub use kline_data_persistence::KlineDataPersistence;
pub use config::AggregateConfig;

use crate::klcommon::{Result, ServerTimeSyncManager};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error, instrument, Instrument};

/// K线聚合系统的主要协调器
#[derive(Clone)]
pub struct KlineAggregateSystem {
    config: AggregateConfig,
    symbol_registry: Arc<SymbolMetadataRegistry>,
    buffered_store: Arc<BufferedKlineStore>,
    market_ingestor: Arc<MarketDataIngestor>,
    trade_router: Arc<TradeEventRouter>,
    persistence: Arc<KlineDataPersistence>,
    aggregators: Arc<RwLock<Vec<Arc<SymbolKlineAggregator>>>>,
    time_sync_manager: Arc<ServerTimeSyncManager>,
}

impl KlineAggregateSystem {
    /// 创建新的K线聚合系统
    #[instrument(target = "KlineAggregateSystem", skip_all, err)]
    pub async fn new(config: AggregateConfig) -> Result<Self> {
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 开始初始化");
        info!(target: "KlineAggregateSystem", event_name = "系统初始化开始", "初始化K线聚合系统...");

        // 初始化服务器时间同步管理器
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 创建时间同步管理器");
        let time_sync_manager = Arc::new(ServerTimeSyncManager::new());

        // 进行一次时间同步
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 开始时间同步");
        info!(target: "KlineAggregateSystem", event_name = "时间同步初始化", "初始化服务器时间同步...");
        match time_sync_manager.sync_time_once().await {
            Ok(_) => {
                println!("✅ [DEBUG] KlineAggregateSystem::new - 时间同步成功");
                info!(target: "KlineAggregateSystem", event_name = "时间同步完成", "服务器时间同步完成");
            }
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::new - 时间同步失败: {}", e);
                return Err(e);
            }
        }

        // 初始化符号元数据注册表
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 初始化符号元数据注册表");
        let symbol_registry = match SymbolMetadataRegistry::new(config.clone()).await {
            Ok(registry) => {
                println!("✅ [DEBUG] KlineAggregateSystem::new - 符号元数据注册表创建成功");
                Arc::new(registry)
            }
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::new - 符号元数据注册表创建失败: {}", e);
                return Err(e);
            }
        };

        // 初始化双缓冲存储
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 初始化双缓冲存储");
        let buffered_store = match BufferedKlineStore::new(
            symbol_registry.clone(),
            config.buffer_swap_interval_ms,
        ).await {
            Ok(store) => {
                println!("✅ [DEBUG] KlineAggregateSystem::new - 双缓冲存储创建成功");
                Arc::new(store)
            }
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::new - 双缓冲存储创建失败: {}", e);
                return Err(e);
            }
        };

        // 初始化数据持久化模块
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 初始化数据持久化模块");
        let persistence = match KlineDataPersistence::new(
            config.clone(),
            buffered_store.clone(),
            symbol_registry.clone(),
        ).await {
            Ok(persistence) => {
                println!("✅ [DEBUG] KlineAggregateSystem::new - 数据持久化模块创建成功");
                Arc::new(persistence)
            }
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::new - 数据持久化模块创建失败: {}", e);
                return Err(e);
            }
        };

        // 初始化交易事件路由器
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 初始化交易事件路由器");
        let trade_router = Arc::new(TradeEventRouter::new());
        println!("✅ [DEBUG] KlineAggregateSystem::new - 交易事件路由器创建成功");

        // 初始化市场数据接入器
        println!("🔧 [DEBUG] KlineAggregateSystem::new - 初始化市场数据接入器");
        let market_ingestor = match MarketDataIngestor::new(
            config.clone(),
            trade_router.clone(),
        ).await {
            Ok(ingestor) => {
                println!("✅ [DEBUG] KlineAggregateSystem::new - 市场数据接入器创建成功");
                Arc::new(ingestor)
            }
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::new - 市场数据接入器创建失败: {}", e);
                return Err(e);
            }
        };

        println!("✅ [DEBUG] KlineAggregateSystem::new - 所有组件初始化完成");
        info!(target: "KlineAggregateSystem", event_name = "系统初始化完成", "K线聚合系统初始化完成");
        Ok(Self {
            config,
            symbol_registry,
            buffered_store,
            market_ingestor,
            trade_router,
            persistence,
            aggregators: Arc::new(RwLock::new(Vec::new())),
            time_sync_manager,
        })
    }
    
    /// 启动整个聚合系统
    #[instrument(target = "KlineAggregateSystem", skip(self), err)]
    pub async fn start(&self) -> Result<()> {
        println!("🔧 [DEBUG] KlineAggregateSystem::start - 开始启动系统");
        info!(target: "KlineAggregateSystem", event_name = "系统启动开始", "启动K线聚合系统");

        // 1. 启动服务器时间同步任务
        println!("🔧 [DEBUG] KlineAggregateSystem::start - 启动时间同步任务");
        let time_sync_manager = self.time_sync_manager.clone();
        tokio::spawn(async move {
            if let Err(e) = time_sync_manager.start().await {
                error!(target: "KlineAggregateSystem", event_name = "时间同步任务失败", error = %e, "服务器时间同步任务失败");
            }
        }.instrument(tracing::info_span!("time_sync_manager_task")));
        println!("✅ [DEBUG] KlineAggregateSystem::start - 时间同步任务启动完成");

        // 2. 启动双缓冲存储的定时切换
        println!("🔧 [DEBUG] KlineAggregateSystem::start - 启动双缓冲存储调度器");
        match self.buffered_store.start_scheduler().await {
            Ok(_) => println!("✅ [DEBUG] KlineAggregateSystem::start - 双缓冲存储调度器启动成功"),
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::start - 双缓冲存储调度器启动失败: {}", e);
                return Err(e);
            }
        }

        // 3. 启动数据持久化
        println!("🔧 [DEBUG] KlineAggregateSystem::start - 启动数据持久化");
        match self.persistence.start().await {
            Ok(_) => println!("✅ [DEBUG] KlineAggregateSystem::start - 数据持久化启动成功"),
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::start - 数据持久化启动失败: {}", e);
                return Err(e);
            }
        }

        // 4. 为每个交易品种创建聚合器
        println!("🔧 [DEBUG] KlineAggregateSystem::start - 初始化聚合器");
        match self.initialize_aggregators().await {
            Ok(_) => println!("✅ [DEBUG] KlineAggregateSystem::start - 聚合器初始化成功"),
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::start - 聚合器初始化失败: {}", e);
                return Err(e);
            }
        }

        // 5. 启动市场数据接入
        println!("🔧 [DEBUG] KlineAggregateSystem::start - 启动市场数据接入器");
        match self.market_ingestor.start().await {
            Ok(_) => println!("✅ [DEBUG] KlineAggregateSystem::start - 市场数据接入器启动成功"),
            Err(e) => {
                println!("❌ [DEBUG] KlineAggregateSystem::start - 市场数据接入器启动失败: {}", e);
                return Err(e);
            }
        }

        println!("✅ [DEBUG] KlineAggregateSystem::start - 系统启动完成");
        info!(target: "KlineAggregateSystem", event_name = "系统启动完成", "K线聚合系统启动完成");
        Ok(())
    }
    
    /// 初始化所有交易品种的聚合器
    #[instrument(target = "KlineAggregateSystem", skip(self), err)]
    async fn initialize_aggregators(&self) -> Result<()> {
        let symbols = self.symbol_registry.get_all_symbols().await?;
        let mut aggregators = self.aggregators.write().await;

        info!(target: "KlineAggregateSystem", event_name = "聚合器初始化开始", symbols_count = symbols.len(), "开始初始化 {} 个品种的K线聚合器", symbols.len());

        for (symbol, symbol_index) in symbols {
            let aggregator = Arc::new(SymbolKlineAggregator::new(
                symbol.clone(),
                symbol_index,
                self.config.supported_intervals.clone(),
                self.buffered_store.clone(),
                self.time_sync_manager.clone(),
            ).await?);

            // 注册到路由器
            self.trade_router.register_aggregator(symbol, aggregator.clone()).await?;

            aggregators.push(aggregator);
        }

        info!(target: "KlineAggregateSystem", event_name = "聚合器初始化完成", aggregators_count = aggregators.len(), "已初始化 {} 个品种的K线聚合器", aggregators.len());
        Ok(())
    }
    
    /// 停止系统
    #[instrument(target = "KlineAggregateSystem", skip(self), err)]
    pub async fn stop(&self) -> Result<()> {
        info!(target: "KlineAggregateSystem", event_name = "系统停止开始", "停止K线聚合系统...");

        // 停止市场数据接入
        self.market_ingestor.stop().await?;

        // 停止数据持久化
        self.persistence.stop().await?;

        // 停止双缓冲存储调度器
        self.buffered_store.stop_scheduler().await?;

        info!(target: "KlineAggregateSystem", event_name = "系统停止完成", "K线聚合系统已停止");
        Ok(())
    }
    
    /// 获取系统状态
    pub async fn get_status(&self) -> SystemStatus {
        let aggregators = self.aggregators.read().await;
        
        SystemStatus {
            total_symbols: aggregators.len(),
            active_connections: self.market_ingestor.get_connection_count().await,
            buffer_swap_count: self.buffered_store.get_swap_count().await,
            persistence_status: self.persistence.get_status().await,
        }
    }
}

/// 系统状态信息
#[derive(Debug, Clone)]
pub struct SystemStatus {
    pub total_symbols: usize,
    pub active_connections: usize,
    pub buffer_swap_count: u64,
    pub persistence_status: String,
}
