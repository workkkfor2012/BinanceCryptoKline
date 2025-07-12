// 日志处理模块
//
// AI日志处理模块
//
// 这个模块包含以下功能：
// 1. ai_log.rs - AI日志核心模块 (真相之源)，负责捕获所有tracing事件并发送到log_mcp_daemon
// 2. ai_problem_summary.rs - AI问题摘要层，只捕获WARN/ERROR生成问题摘要文件

pub mod ai_log;

// 使用 #[macro_use] 来确保 soft_assert! 宏在整个项目中都可用
#[macro_use]
pub mod ai_problem_summary;

pub mod low_freq_log;
pub mod beacon_log;

// 导出AI日志系统核心类型
pub use ai_log::{
    McpLayer,
    init_log_sender,
    shutdown_log_sender, // 导出关闭函数
    SpanModel,
    SpanEvent,
    StructuredLog,
    JsonVisitor,
    SpanContext,
};

pub use ai_problem_summary::{
    ProblemSummaryLayer,
    ProblemSummary,
    init_problem_summary_log,
    create_problem_summary_layer,
};



pub use low_freq_log::{
    LowFreqLogLayer,
    init_low_freq_log,
};

pub use beacon_log::{
    BeaconLogLayer,
    init_beacon_log,
};


