// 导出共享模块
pub mod models;
pub mod db;
pub mod error;
pub mod api;
pub mod websocket;
pub mod server_time_sync; // 服务器时间同步模块
pub mod kline_builder; // K线构建器模块
pub mod agg_trade_handler; // 归集交易消息处理器
pub mod proxy; // 代理配置模块

// 重新导出常用类型，方便使用
pub use models::{Kline, Symbol, ExchangeInfo, DownloadTask, DownloadResult, KlineData};
pub use db::Database;
pub use error::{Result, AppError};
pub use api::BinanceApi;
pub use websocket::{
    WebSocketClient, WebSocketConnection, WebSocketConfig,
    ContinuousKlineClient, ContinuousKlineConfig,
    AggTradeClient, AggTradeConfig,
    BINANCE_WS_URL, MAX_STREAMS_PER_CONNECTION
};
pub use server_time_sync::ServerTimeSyncManager; // 导出服务器时间同步管理器
pub use kline_builder::{KlineBuilder, AggTrade, process_agg_trade, parse_agg_trade_message};
pub use agg_trade_handler::AggTradeMessageHandler;
pub use proxy::{ProxyConfig, get_proxy_url, PROXY_HOST, PROXY_PORT};
