// 日志处理模块
// 
// 这个模块包含两种不同的日志处理方式：
// 1. observability.rs - 支持前端按target分模块显示日志
// 2. trace_visualization.rs - 支持函数执行路径可视化

pub mod observability;
pub mod trace_visualization;

// 重新导出常用类型，保持向后兼容
pub use observability::{
    ModuleLayer,
    NamedPipeLogForwardingLayer,
    NamedPipeLogManager,
};

// 导出 trace 可视化相关类型
pub use trace_visualization::{
    TraceVisualizationLayer,
    JsonVisitor,
};
