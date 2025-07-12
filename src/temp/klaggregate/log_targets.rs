//! 日志目标常量定义
//!
//! 统一管理 klaggregate 模块中所有组件的日志 target 常量，
//! 便于统一修改和维护。

/// 双缓冲K线存储模块的日志目标
pub const BUFFERED_KLINE_STORE: &str = "BufferedKlineStore";

/// 品种元数据注册表模块的日志目标
pub const SYMBOL_METADATA_REGISTRY: &str = "SymbolMetadataRegistry";

/// 品种K线聚合器模块的日志目标
pub const SYMBOL_KLINE_AGGREGATOR: &str = "SymbolKlineAggregator";

/// 行情数据接入器模块的日志目标
pub const MARKET_DATA_INGESTOR: &str = "MarketDataIngestor";

/// 交易事件路由器模块的日志目标
pub const TRADE_EVENT_ROUTER: &str = "TradeEventRouter";

/// K线数据持久化器模块的日志目标
pub const KLINE_DATA_PERSISTENCE: &str = "KlineDataPersistence";

/// K线聚合系统模块的日志目标
pub const KLINE_AGGREGATE_SYSTEM: &str = "KlineAggregateSystem";
