// src/klaggnew/mod.rs

pub mod aggregator;
pub mod persistence;

// Re-export public types
pub use aggregator::{RealtimeAggregator, AggTradeData, KlineData};
pub use persistence::PersistenceService;
