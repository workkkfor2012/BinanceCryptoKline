//! Cerberus 超级断言系统
//! 
//! 基于"日志即规格"理念的运行时验证引擎，通过生成AI友好型的结构化日志，
//! 将程序的内部状态和逻辑路径翻译成可供AI分析的"程序自述报告"。

pub mod types;
pub mod engine;
pub mod rules;

// 重新导出核心类型
pub use types::{
    ValidationContext, 
    ValidationResult, 
    DeviationEvent, 
    ValidationRule,
    StatefulValidationRule,
    ValidationPriority
};
pub use engine::{CerberusEngine, CerberusLayer};
pub use rules::get_all_rules;

use tracing_subscriber::Layer;

/// Cerberus 系统的全局配置
#[derive(Debug, Clone)]
pub struct CerberusConfig {
    /// 是否启用 Cerberus 验证
    pub enabled: bool,
    
    /// 异步验证任务队列大小
    pub validation_queue_size: usize,
    
    /// 状态管理器的TTL（秒）
    pub state_ttl_seconds: u64,
    
    /// 性能报告的Top N数量
    pub performance_top_n: usize,
    
    /// 验证任务的并发数
    pub validation_concurrency: usize,
}

impl Default for CerberusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            validation_queue_size: 10000,
            state_ttl_seconds: 300, // 5分钟
            performance_top_n: 10,
            validation_concurrency: 4,
        }
    }
}

/// 创建并配置 Cerberus 验证层
///
/// 这是 Cerberus 系统的主要入口点，返回一个可以集成到 tracing subscriber 的 Layer 和引擎
pub fn create_cerberus_layer(config: CerberusConfig) -> (impl Layer<tracing_subscriber::Registry> + Send + Sync, CerberusEngine) {
    CerberusLayer::new(config)
}

/// 创建默认配置的 Cerberus 验证层
pub fn create_default_cerberus_layer() -> (impl Layer<tracing_subscriber::Registry> + Send + Sync, CerberusEngine) {
    create_cerberus_layer(CerberusConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = CerberusConfig::default();
        assert!(config.enabled);
        assert_eq!(config.validation_queue_size, 10000);
        assert_eq!(config.state_ttl_seconds, 300);
        assert_eq!(config.performance_top_n, 10);
        assert_eq!(config.validation_concurrency, 4);
    }
    
    #[test]
    fn test_create_layer() {
        let layer = create_default_cerberus_layer();
        // 基本的创建测试，确保不会panic
        drop(layer);
    }
}
