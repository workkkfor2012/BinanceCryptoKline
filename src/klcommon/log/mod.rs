// 日志处理模块
//
// 这个模块包含以下功能：
// 1. observability.rs - 规格验证层、性能监控和命名管道日志管理
// 2. module_logging.rs - 模块级别的人类可读日志处理
// 3. trace_visualization.rs - 支持函数执行路径可视化
// 4. trace_distiller.rs - 为大模型生成简洁的函数执行路径摘要
// 5. assert - 运行时断言系统，作为日志功能的一部分

pub mod observability;
pub mod module_logging;
pub mod trace_visualization;
pub mod trace_distiller;
pub mod assert;

// 重新导出常用类型，保持向后兼容
pub use observability::{
    NamedPipeLogManager,
};

// 导出模块日志相关类型
pub use module_logging::{
    ModuleLayer,
    NamedPipeLogForwardingLayer,
};

// 导出 trace 可视化相关类型
pub use trace_visualization::{
    TraceVisualizationLayer,
    JsonVisitor,
};

// 导出 trace 提炼器相关类型（为大模型分析设计）
pub use trace_distiller::{
    TraceDistillerStore,
    TraceDistillerLayer,
    distill_trace_to_text,
    distill_all_completed_traces_to_text,
};

// 导出运行时断言系统相关类型
pub use assert::{
    AssertEngine,
    AssertLayer,
    create_default_assert_layer,
};
