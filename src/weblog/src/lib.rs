//! WebLog - 通用Web日志显示系统 - 简化版
//!
//! 专注于日志数据的缓存和实时传输

pub mod types;
pub mod log_parser;
pub mod web_server;

// 重新导出核心类型
pub use types::*;
pub use log_parser::*;
pub use web_server::create_app;



/// 验证日志条目的有效性
pub fn validate_log_entry(log_entry: &LogEntry) -> bool {
    // 基本验证：确保必要字段不为空
    // 对于span事件和结构化日志，message可以为空（信息在fields中）
    !log_entry.level.is_empty() && !log_entry.target.is_empty()
}
