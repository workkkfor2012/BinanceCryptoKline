// 导出共享模块
pub mod models;
pub mod db;
pub mod error;
pub mod api;
pub mod websocket;
pub mod server_time_sync; // 服务器时间同步模块
// pub mod aggkline; // 归集交易K线模块 - 暂时注释掉
pub mod proxy; // 代理配置模块

// 重新导出常用类型，方便使用
pub use models::{Kline, Symbol, ExchangeInfo, DownloadTask, DownloadResult, KlineData, BinanceRawAggTrade};
pub use db::Database;
pub use error::{Result, AppError};
pub use api::BinanceApi;
pub use websocket::{
    WebSocketClient, WebSocketConnection, WebSocketConfig,
    ContinuousKlineClient, ContinuousKlineConfig,
    AggTradeClient, AggTradeConfig,
    BINANCE_WS_URL, WEBSOCKET_CONNECTION_COUNT
};
pub use server_time_sync::ServerTimeSyncManager; // 导出服务器时间同步管理器
// pub use aggkline::{
//     AppAggTrade, KlineBar, KlineBarDataInternal, BinanceRawAggTrade,
//     KlineGenerator, KlineActor,
//     parse_agg_trade, run_trade_parser_task,
//     run_app_trade_dispatcher_task, KlineProcessor,
//     partition_symbols, run_websocket_connection_task,
//     KLINE_PERIODS_MS, NUM_WEBSOCKET_CONNECTIONS, AGG_TRADE_STREAM_NAME
// };
pub use proxy::{ProxyConfig, get_proxy_url, PROXY_HOST, PROXY_PORT};
