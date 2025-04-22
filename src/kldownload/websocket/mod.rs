// WebSocket模块入口
// 导出公共接口

mod config;
mod models;
mod connection;
mod message_handler;
mod stats;
mod client;

// 重新导出主要的公共接口
pub use config::ContinuousKlineConfig;
pub use client::ContinuousKlineClient;

// 导出测试客户端（如果需要）
#[cfg(test)]
pub use client::TestContinuousKlineClient;

