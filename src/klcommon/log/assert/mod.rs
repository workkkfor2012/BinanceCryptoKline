//! 运行时断言系统
//! 
//! 提供运行时验证和偏差检测功能，作为日志系统的一部分

pub mod types;
pub mod rules;
pub mod engine;

pub use engine::{AssertEngine, AssertLayer};
pub use types::{AssertConfig, ValidationContext, ValidationResult, ValidationRule, Deviation};

/// 创建默认的断言层和引擎
pub fn create_default_assert_layer() -> (AssertLayer, AssertEngine) {
    let config = AssertConfig::default();
    AssertLayer::new(config)
}
