//! Cerberus 核心数据类型定义
//! 
//! 定义了验证上下文、验证结果、偏差事件等核心数据结构

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 验证优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ValidationPriority {
    /// Critical - 始终开启，性能开销 < 0.1%
    Critical = 3,
    /// Standard - 开发/测试环境，性能开销 < 0.5%
    Standard = 2,
    /// Diagnostic - 问题排查时，性能开销 < 1%
    Diagnostic = 1,
}

/// 验证上下文 - 从 tracing::Event 提取的结构化信息
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// 事件来源模块 (如 "SymbolKlineAggregator")
    pub target: String,
    
    /// 事件名称 (如 "kline_updated")
    pub event_name: String,
    
    /// 事件时间戳 (毫秒)
    pub timestamp: i64,
    
    /// 事件字段数据
    pub fields: HashMap<String, serde_json::Value>,
    
    /// Span 相关数据
    pub span_data: Option<HashMap<String, String>>,
    
    /// Trace ID (用于跨模块追踪)
    pub trace_id: Option<String>,
}

impl ValidationContext {
    /// 创建新的验证上下文
    pub fn new(target: String, event_name: String) -> Self {
        Self {
            target,
            event_name,
            timestamp: chrono::Utc::now().timestamp_millis(),
            fields: HashMap::new(),
            span_data: None,
            trace_id: None,
        }
    }
    
    /// 添加字段数据
    pub fn with_field(mut self, key: &str, value: serde_json::Value) -> Self {
        self.fields.insert(key.to_string(), value);
        self
    }
    
    /// 添加多个字段数据
    pub fn with_fields(mut self, fields: HashMap<String, serde_json::Value>) -> Self {
        self.fields.extend(fields);
        self
    }
    
    /// 设置 Trace ID
    pub fn with_trace_id(mut self, trace_id: String) -> Self {
        self.trace_id = Some(trace_id);
        self
    }
    
    /// 获取字段值
    pub fn get_field(&self, key: &str) -> Option<&serde_json::Value> {
        self.fields.get(key)
    }
    
    /// 获取字符串字段值
    pub fn get_string_field(&self, key: &str) -> Option<String> {
        self.fields.get(key)?.as_str().map(|s| s.to_string())
    }
    
    /// 获取数值字段值
    pub fn get_number_field(&self, key: &str) -> Option<f64> {
        self.fields.get(key)?.as_f64()
    }
    
    /// 获取布尔字段值
    pub fn get_bool_field(&self, key: &str) -> Option<bool> {
        self.fields.get(key)?.as_bool()
    }
}

/// 验证结果
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// 验证通过
    Pass,
    /// 发现偏差 - 包含详细的证据信息
    Deviation(DeviationEvent),
    /// 验证跳过 (如规则不适用)
    Skip(String),
}

impl ValidationResult {
    /// 创建通过结果
    pub fn pass() -> Self {
        Self::Pass
    }
    
    /// 创建偏差结果
    pub fn deviation(rule_id: &str, deviation_type: &str, evidence: serde_json::Value) -> Self {
        Self::Deviation(DeviationEvent {
            rule_id: rule_id.to_string(),
            deviation_type: deviation_type.to_string(),
            evidence,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }
    
    /// 创建跳过结果
    pub fn skip(reason: &str) -> Self {
        Self::Skip(reason.to_string())
    }
    
    /// 检查是否为偏差
    pub fn is_deviation(&self) -> bool {
        matches!(self, Self::Deviation(_))
    }
}

/// 偏差事件 - AI友好的"案发现场报告"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviationEvent {
    /// 验证规则ID
    pub rule_id: String,
    
    /// 偏差类型 (如 "price_volatility_excessive")
    pub deviation_type: String,
    
    /// 证据数据 - 包含实际值、预期值、触发源等
    pub evidence: serde_json::Value,
    
    /// 事件时间戳
    pub timestamp: i64,
}

impl DeviationEvent {
    /// 转换为结构化日志格式
    pub fn to_log_json(&self) -> serde_json::Value {
        serde_json::json!({
            "event_type": "CERBERUS_DEVIATION",
            "rule_id": self.rule_id,
            "deviation_type": self.deviation_type,
            "evidence": self.evidence,
            "timestamp": self.timestamp,
            "timestamp_iso": chrono::DateTime::from_timestamp_millis(self.timestamp)
                .unwrap_or_default()
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string()
        })
    }
}

/// 验证规则 trait - 无状态验证规则
pub trait ValidationRule: Send + Sync {
    /// 规则唯一标识符
    fn id(&self) -> &str;
    
    /// 规则描述
    fn description(&self) -> &str;
    
    /// 规则优先级
    fn priority(&self) -> ValidationPriority;
    
    /// 检查规则是否适用于给定上下文
    fn is_applicable(&self, context: &ValidationContext) -> bool;
    
    /// 执行验证逻辑
    fn validate(&self, context: &ValidationContext) -> ValidationResult;
}

/// 有状态验证规则 trait
pub trait StatefulValidationRule: ValidationRule {
    /// 状态类型
    type State: Send + Sync + Clone + 'static;
    
    /// 创建初始状态
    fn create_initial_state(&self) -> Self::State;
    
    /// 获取状态键 (用于状态管理器)
    fn get_state_key(&self, context: &ValidationContext) -> String;
    
    /// 执行有状态验证
    fn validate_with_state(&self, context: &ValidationContext, state: &mut Self::State) -> ValidationResult;
    
    /// 检查是否应该清理状态
    fn should_cleanup_state(&self, state: &Self::State) -> bool;
}

/// 性能统计数据
#[derive(Debug, Clone)]
pub struct PerfStats {
    /// 调用次数
    pub count: u64,
    /// 总耗时 (纳秒)
    pub total_duration_ns: u64,
    /// 最大耗时 (纳秒)
    pub max_duration_ns: u64,
    /// 最小耗时 (纳秒)
    pub min_duration_ns: u64,
}

impl PerfStats {
    /// 创建新的性能统计
    pub fn new() -> Self {
        Self {
            count: 0,
            total_duration_ns: 0,
            max_duration_ns: 0,
            min_duration_ns: u64::MAX,
        }
    }
    
    /// 添加一次测量
    pub fn add_measurement(&mut self, duration_ns: u64) {
        self.count += 1;
        self.total_duration_ns += duration_ns;
        self.max_duration_ns = self.max_duration_ns.max(duration_ns);
        self.min_duration_ns = self.min_duration_ns.min(duration_ns);
    }
    
    /// 计算平均耗时 (纳秒)
    pub fn avg_duration_ns(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.total_duration_ns / self.count
        }
    }
    
    /// 计算平均耗时 (毫秒)
    pub fn avg_duration_ms(&self) -> f64 {
        self.avg_duration_ns() as f64 / 1_000_000.0
    }
}

impl Default for PerfStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_validation_context() {
        let mut context = ValidationContext::new("TestModule".to_string(), "test_event".to_string());
        context = context.with_field("test_key", serde_json::json!("test_value"));
        
        assert_eq!(context.target, "TestModule");
        assert_eq!(context.event_name, "test_event");
        assert_eq!(context.get_string_field("test_key"), Some("test_value".to_string()));
    }
    
    #[test]
    fn test_validation_result() {
        let pass_result = ValidationResult::pass();
        assert!(!pass_result.is_deviation());
        
        let deviation_result = ValidationResult::deviation(
            "TEST_RULE", 
            "test_deviation", 
            serde_json::json!({"test": "evidence"})
        );
        assert!(deviation_result.is_deviation());
    }
    
    #[test]
    fn test_perf_stats() {
        let mut stats = PerfStats::new();
        stats.add_measurement(1000000); // 1ms
        stats.add_measurement(2000000); // 2ms
        
        assert_eq!(stats.count, 2);
        assert_eq!(stats.avg_duration_ns(), 1500000);
        assert_eq!(stats.avg_duration_ms(), 1.5);
    }
}
