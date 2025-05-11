// 归集交易K线模块 - 基于Actor模型的K线合成系统
pub mod models; // 保留这个模块以向后兼容，但它现在只是重新导出crate::klcommon::models中的类型
mod kline_generator;
mod kline_actor;
mod trade_parser;
mod trade_dispatcher;
mod kline_processor; // 新增处理器模块，替代sqlite_storage
mod symbol_partitioner;
mod websocket_handler;

// 重新导出公共接口
pub use crate::klcommon::models::{BinanceRawAggTrade, AppAggTrade, KlineBar, KlineBarDataInternal};
pub use kline_generator::KlineGenerator;
pub use kline_actor::KlineActor;
pub use trade_parser::{parse_agg_trade, run_trade_parser_task};
pub use trade_dispatcher::run_app_trade_dispatcher_task;
pub use kline_processor::KlineProcessor; // 导出新的处理器
pub use symbol_partitioner::partition_symbols;
pub use websocket_handler::run_websocket_connection_task;

// 常量
pub const KLINE_PERIODS_MS: &[i64] = &[
    60 * 1000,             // 1m
    5 * 60 * 1000,         // 5m
    30 * 60 * 1000,        // 30m
    4 * 60 * 60 * 1000,    // 4h
    24 * 60 * 60 * 1000,   // 1d
    7 * 24 * 60 * 60 * 1000, // 1w
];

// WebSocket连接数量
pub const NUM_WEBSOCKET_CONNECTIONS: usize = 5;

// 币安WebSocket URL
pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/stream";

// 归集交易流名称后缀
pub const AGG_TRADE_STREAM_NAME: &str = "aggTrade";
