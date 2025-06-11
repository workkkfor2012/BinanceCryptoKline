//! 模块聚合管理器
//!
//! 管理多个模块的日志聚合，包括：
//! - 模块级别的日志聚合
//! - 实时日志管理
//! - 原始日志快照处理
//! - 统一的数据接口

use crate::types::{LogEntry, RealtimeLogData};
use crate::log_aggregator::{LogAggregator, AggregatorConfig, DisplayLogEntry};
use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::Serialize;
use chrono::{DateTime, Utc};

/// 模块聚合管理器配置
#[derive(Debug, Clone)]
pub struct ModuleManagerConfig {
    pub aggregator_config: AggregatorConfig,
    pub max_realtime_logs: usize,
}

impl Default for ModuleManagerConfig {
    fn default() -> Self {
        Self {
            aggregator_config: AggregatorConfig::default(),
            max_realtime_logs: 10000000,
        }
    }
}

/// 模块显示数据
#[derive(Debug, Clone, Serialize)]
pub struct ModuleDisplayData {
    pub total_logs: usize,
    pub error_count: usize,
    pub warn_count: usize,
    pub info_count: usize,
    pub displayed_logs: Vec<DisplayLogEntry>,
    pub recent_logs: Vec<LogEntry>, // 完整的原始日志历史（最多1000条）
    pub has_more_history: bool, // 是否还有更多历史数据
    pub total_history_count: usize, // 历史数据总数
}

/// 原始日志快照数据
#[derive(Debug, Clone, Serialize)]
pub struct RawSnapshotData {
    pub timestamp: DateTime<Utc>,
    pub total_count: usize,
    pub displayed_logs: Vec<DisplayLogEntry>,
}

// 实时日志数据现在使用types.rs中的RealtimeLogData

/// 仪表板数据
#[derive(Debug, Clone, Serialize)]
pub struct DashboardData {
    pub uptime_seconds: u64,
    pub health_score: u8,
    pub module_logs: HashMap<String, ModuleDisplayData>,
    pub realtime_log_data: RealtimeLogData,
    pub raw_log_snapshot: RawSnapshotData,
}

/// 模块聚合管理器
pub struct ModuleAggregatorManager {
    module_aggregators: RwLock<HashMap<String, LogAggregator>>,
    raw_snapshot_aggregator: RwLock<LogAggregator>,
    config: ModuleManagerConfig,
}

impl ModuleAggregatorManager {
    /// 创建新的模块管理器
    pub fn new(config: ModuleManagerConfig) -> Self {
        Self {
            module_aggregators: RwLock::new(HashMap::new()),
            raw_snapshot_aggregator: RwLock::new(LogAggregator::new(config.aggregator_config.clone())),
            config,
        }
    }

    /// 创建默认的模块管理器
    pub fn default() -> Self {
        Self::new(ModuleManagerConfig::default())
    }

    /// 处理模块日志
    pub async fn process_module_logs(&self, module_name: &str, logs: Vec<LogEntry>) {
        let mut aggregators = self.module_aggregators.write().await;
        let aggregator = aggregators.entry(module_name.to_string())
            .or_insert_with(|| LogAggregator::new(self.config.aggregator_config.clone()));
        
        for log_entry in logs {
            aggregator.add_log_entry(log_entry);
        }
    }

    /// 处理单个日志条目
    pub async fn process_log_entry(&self, log_entry: LogEntry) {
        // 根据target字段确定模块名称
        let module_name = log_entry.target.clone();
        
        // 添加到对应模块的聚合器
        self.process_module_logs(&module_name, vec![log_entry.clone()]).await;
        
        // 同时添加到原始日志快照聚合器
        let mut raw_aggregator = self.raw_snapshot_aggregator.write().await;
        raw_aggregator.add_log_entry(log_entry);
    }

    /// 处理原始日志快照（已禁用，只通过process_log_entry处理）
    pub async fn process_raw_log_snapshot(&self, _logs: Vec<String>) {
        // 不再支持直接快照处理，只通过主要流程处理
    }

    /// 添加实时日志（已禁用，只通过AppState的add_raw_log处理）
    pub async fn add_realtime_log(&self, _log_line: String) {
        // 不再支持直接添加实时日志，只通过AppState的add_raw_log处理
    }

    /// 获取模块数据
    pub async fn get_module_data(&self, module_name: &str) -> Option<ModuleDisplayData> {
        let aggregators = self.module_aggregators.read().await;
        aggregators.get(module_name).map(|aggregator| {
            let stats = aggregator.get_stats();
            let all_raw_logs = aggregator.get_all_raw_logs();
            let total_history_count = all_raw_logs.len();

            ModuleDisplayData {
                total_logs: stats.total_processed,
                error_count: all_raw_logs.iter()
                    .filter(|log| log.level == "ERROR")
                    .count(),
                warn_count: all_raw_logs.iter()
                    .filter(|log| log.level == "WARN")
                    .count(),
                info_count: all_raw_logs.iter()
                    .filter(|log| log.level == "INFO")
                    .count(),
                displayed_logs: aggregator.get_displayed_logs(),
                recent_logs: all_raw_logs, // 完整的原始日志历史（最多1000条）
                has_more_history: total_history_count >= 1000, // 如果达到1000条，可能还有更多
                total_history_count,
            }
        })
    }

    /// 获取所有模块数据
    pub async fn get_all_modules_data(&self) -> HashMap<String, ModuleDisplayData> {
        let aggregators = self.module_aggregators.read().await;
        let mut result = HashMap::new();

        for (module_name, aggregator) in aggregators.iter() {
            let stats = aggregator.get_stats();
            let all_raw_logs = aggregator.get_all_raw_logs();
            let total_history_count = all_raw_logs.len();

            let module_data = ModuleDisplayData {
                total_logs: stats.total_processed,
                error_count: all_raw_logs.iter()
                    .filter(|log| log.level == "ERROR")
                    .count(),
                warn_count: all_raw_logs.iter()
                    .filter(|log| log.level == "WARN")
                    .count(),
                info_count: all_raw_logs.iter()
                    .filter(|log| log.level == "INFO")
                    .count(),
                displayed_logs: aggregator.get_displayed_logs(),
                recent_logs: all_raw_logs, // 完整的原始日志历史（最多1000条）
                has_more_history: total_history_count >= 1000, // 如果达到1000条，可能还有更多
                total_history_count,
            };
            result.insert(module_name.clone(), module_data);
        }

        result
    }

    /// 获取原始快照数据
    pub async fn get_raw_snapshot_data(&self) -> RawSnapshotData {
        let aggregator = self.raw_snapshot_aggregator.read().await;
        let stats = aggregator.get_stats();
        RawSnapshotData {
            timestamp: Utc::now(),
            total_count: stats.total_processed,
            displayed_logs: aggregator.get_displayed_logs(),
        }
    }

    /// 获取实时日志（从AppState获取）
    pub async fn get_realtime_logs(&self) -> Vec<String> {
        // 实时日志现在只从AppState获取，不在这里维护
        Vec::new()
    }

    /// 获取仪表板数据（不包含实时日志，由AppState提供）
    pub async fn get_dashboard_data(&self, uptime_seconds: u64, health_score: u8) -> DashboardData {
        let module_logs = self.get_all_modules_data().await;
        let raw_snapshot = self.get_raw_snapshot_data().await;

        DashboardData {
            uptime_seconds,
            health_score,
            module_logs,
            realtime_log_data: RealtimeLogData {
                recent_logs: Vec::new(), // 实时日志由AppState提供
                total_count: 0,
                last_update_time: "--:--:--".to_string(),
                logs_per_second: 0.0,
            },
            raw_log_snapshot: raw_snapshot,
        }
    }

    /// 获取活跃模块列表
    pub async fn get_active_modules(&self) -> Vec<String> {
        let aggregators = self.module_aggregators.read().await;
        aggregators.keys().cloned().collect()
    }

    /// 清理过期数据
    pub async fn cleanup_expired_data(&self) {
        // 清理各模块聚合器的过期数据
        let mut aggregators = self.module_aggregators.write().await;
        let now = Utc::now().timestamp_millis();
        
        for aggregator in aggregators.values_mut() {
            aggregator.cleanup_expired(now);
        }
        
        // 清理原始快照聚合器
        let mut raw_aggregator = self.raw_snapshot_aggregator.write().await;
        raw_aggregator.cleanup_expired(now);
    }
}

/// 从日志行提取级别
fn extract_level_from_log_line(log_line: &str) -> String {
    if log_line.contains("ERROR") {
        "ERROR".to_string()
    } else if log_line.contains("WARN") {
        "WARN".to_string()
    } else if log_line.contains("INFO") {
        "INFO".to_string()
    } else if log_line.contains("DEBUG") {
        "DEBUG".to_string()
    } else {
        "INFO".to_string()
    }
}

/// 从日志行提取消息
fn extract_message_from_log_line(log_line: &str) -> String {
    // 简单的消息提取，可以根据需要改进
    log_line.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_module_manager() {
        let manager = ModuleAggregatorManager::new(ModuleManagerConfig::default());
        
        let log_entry = LogEntry {
            timestamp: Utc::now(),
            level: "INFO".to_string(),
            message: "Test message".to_string(),
            target: "test_module".to_string(),
            module_path: Some("test::module".to_string()),
            file: Some("test.rs".to_string()),
            line: Some(42),
            fields: HashMap::new(),
            span: None,
        };
        
        manager.process_log_entry(log_entry).await;
        
        let modules = manager.get_active_modules().await;
        assert!(modules.contains(&"test_module".to_string()));
        
        let module_data = manager.get_module_data("test_module").await;
        assert!(module_data.is_some());
        assert_eq!(module_data.unwrap().displayed_logs.len(), 1);
    }

    #[tokio::test]
    async fn test_realtime_logs() {
        let manager = ModuleAggregatorManager::new(ModuleManagerConfig::default());

        // 实时日志现在由AppState管理，这里只测试方法不报错
        manager.add_realtime_log("Test log 1".to_string()).await;
        manager.add_realtime_log("Test log 2".to_string()).await;

        let logs = manager.get_realtime_logs().await;
        assert_eq!(logs.len(), 0); // 现在返回空数组
    }
}

impl std::fmt::Debug for ModuleAggregatorManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleAggregatorManager")
            .field("config", &self.config)
            .finish()
    }
}
