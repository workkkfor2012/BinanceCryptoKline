// src/engine/events.rs
use crate::klcommon::websocket::AggTradeData;
use std::sync::Arc;

/// 驱动 KlineEngine 状态变更的唯一事件源
#[derive(Debug)]
pub enum AppEvent {
    /// 来自 WebSocket 的实时交易数据，使用 Box 避免枚举变得过大。
    AggTrade(Box<AggTradeData>),
    /// 来自品种管理器的动态添加品种指令
    AddSymbol {
        symbol: String,
        // 移除 global_index 参数，让 Engine 自己分配索引
        first_kline_open_time: i64,
    },
}

/// 状态更新结构，用于高性能的增量状态推送
#[derive(Clone, Debug, Default)]
pub struct StateUpdate {
    /// 对当前K线状态完整快照的共享引用
    pub kline_snapshot: Arc<Vec<crate::engine::KlineState>>,
    /// 本次更新中发生变化的K线索引列表
    pub dirty_indices: Arc<Vec<usize>>,
}
