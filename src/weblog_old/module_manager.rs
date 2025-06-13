use crate::types::{LogEntry, RawLogSnapshot};
use crate::high_freq_aggregator::{HighFreqLogAggregator, HighFreqConfig};
use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::Serialize;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct ModuleManagerConfig {
    pub high_freq_config: HighFreqConfig,
    pub max_normal_logs: usize,
}

impl Default for ModuleManagerConfig {
    fn default() -> Self {
        Self {
            high_freq_config: HighFreqConfig::default(),
            max_normal_logs: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ModuleDisplayData {
    pub total_logs: usize,
    pub error_count: usize,
    pub warn_count: usize,
    pub info_count: usize,
    pub display_logs: Vec<DisplayLogEntry>,  // 统一的显示日志
}

/// 统一的显示日志条目
#[derive(Debug, Clone, Serialize)]
pub struct DisplayLogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub message: String,
    pub count: usize,           // 1 = 普通日志，>1 = 聚合日志
    pub is_aggregated: bool,    // 是否为聚合日志
    pub event_type: Option<String>, // 聚合日志的事件类型
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardData {
    pub uptime_seconds: u64,
    pub health_score: u8,
    pub module_logs: HashMap<String, ModuleDisplayData>,
    pub raw_log_snapshot: Option<RawLogSnapshot>,  // 恢复原始日志快照
}

pub struct ModuleAggregatorManager {
    module_aggregators: RwLock<HashMap<String, HighFreqLogAggregator>>,
    normal_logs: RwLock<Vec<LogEntry>>,
    raw_log_aggregator: RwLock<HighFreqLogAggregator>,  // 全局原始日志聚合器
    config: ModuleManagerConfig,
    start_time: std::time::SystemTime,
}

impl ModuleAggregatorManager {
    pub fn new(config: ModuleManagerConfig) -> Self {
        Self {
            module_aggregators: RwLock::new(HashMap::new()),
            normal_logs: RwLock::new(Vec::new()),
            raw_log_aggregator: RwLock::new(HighFreqLogAggregator::new(config.high_freq_config.clone())),
            config,
            start_time: std::time::SystemTime::now(),
        }
    }

    pub fn default() -> Self {
        Self::new(ModuleManagerConfig::default())
    }

    pub async fn process_log_entry(&self, log_entry: LogEntry) -> bool {
        let module_name = log_entry.target.clone();

        // 1. 处理模块级别的聚合
        let mut aggregators = self.module_aggregators.write().await;
        let aggregator = aggregators.entry(module_name)
            .or_insert_with(|| HighFreqLogAggregator::new(self.config.high_freq_config.clone()));

        let was_aggregated = aggregator.process_log_entry(log_entry.clone());
        drop(aggregators); // 释放锁

        // 2. 处理全局原始日志聚合（不区分模块）
        let mut raw_aggregator = self.raw_log_aggregator.write().await;
        raw_aggregator.process_log_entry(log_entry.clone());
        drop(raw_aggregator); // 释放锁

        // 检查是否为高频类型的日志
        let is_high_freq_type = log_entry.fields.get("event_type").is_some()
            && log_entry.fields.get("is_high_freq")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

        // 只有当日志既没有被模块聚合，也不是高频类型时，才视为"普通日志"
        if !was_aggregated && !is_high_freq_type {
            let mut normal_logs = self.normal_logs.write().await;
            normal_logs.push(log_entry);

            if normal_logs.len() > self.config.max_normal_logs {
                normal_logs.remove(0);
            }
        }

        was_aggregated
    }

    pub async fn get_dashboard_data(&self, uptime_seconds: u64, health_score: u8) -> DashboardData {
        let aggregators = self.module_aggregators.read().await;
        let normal_logs = self.normal_logs.read().await;
        let mut module_logs = HashMap::new();

        for (module_name, aggregator) in aggregators.iter() {
            let high_freq_logs = aggregator.get_high_freq_logs();
            let module_normal_logs: Vec<_> = normal_logs.iter()
                .filter(|log| &log.target == module_name)
                .cloned()
                .collect();

            // 转换为统一的显示日志格式
            let mut display_logs = Vec::new();

            // 添加普通日志（转换为 DisplayLogEntry）
            for log in &module_normal_logs {
                display_logs.push(DisplayLogEntry {
                    timestamp: log.timestamp,
                    level: log.level.clone(),
                    message: log.message.clone(),
                    count: 1,
                    is_aggregated: false,
                    event_type: None,
                });
            }

            // 添加高频聚合日志（转换为 DisplayLogEntry）
            for high_freq_log in &high_freq_logs {
                let aggregated_message = format!(
                    "🔄 {} (聚合 {} 次) - {}",
                    high_freq_log.event_type,
                    high_freq_log.count,
                    high_freq_log.latest_log.message
                );

                display_logs.push(DisplayLogEntry {
                    timestamp: high_freq_log.last_updated,
                    level: high_freq_log.latest_log.level.clone(),
                    message: aggregated_message,
                    count: high_freq_log.count,
                    is_aggregated: true,
                    event_type: Some(high_freq_log.event_type.clone()),
                });
            }

            // 按时间戳排序，最新的在后面
            display_logs.sort_by_key(|entry| entry.timestamp);

            let total_logs = high_freq_logs.iter().map(|entry| entry.count).sum::<usize>()
                + module_normal_logs.len();

            let error_count = module_normal_logs.iter()
                .filter(|log| log.level == "ERROR")
                .count();
            let warn_count = module_normal_logs.iter()
                .filter(|log| log.level == "WARN")
                .count();
            let info_count = module_normal_logs.iter()
                .filter(|log| log.level == "INFO")
                .count();

            module_logs.insert(module_name.clone(), ModuleDisplayData {
                total_logs,
                error_count,
                warn_count,
                info_count,
                display_logs,
            });
        }

        // 生成原始日志快照数据（全局视图，包含所有日志但避免重复）
        let raw_log_snapshot = {
            let raw_aggregator = self.raw_log_aggregator.read().await;
            let high_freq_logs = raw_aggregator.get_high_freq_logs();
            let normal_logs = self.normal_logs.read().await;

            // 转换为统一的显示日志格式
            let mut all_display_logs = Vec::new();

            // 添加高频聚合日志
            for high_freq_log in &high_freq_logs {
                let aggregated_message = format!(
                    "🔄 {} (聚合 {} 次) - {}",
                    high_freq_log.event_type,
                    high_freq_log.count,
                    high_freq_log.latest_log.message
                );

                all_display_logs.push(DisplayLogEntry {
                    timestamp: high_freq_log.last_updated,
                    level: high_freq_log.latest_log.level.clone(),
                    message: aggregated_message,
                    count: high_freq_log.count,
                    is_aggregated: true,
                    event_type: Some(high_freq_log.event_type.clone()),
                });
            }

            // 添加普通日志（现在normal_logs中不会包含高频日志，无需额外过滤）
            for log in normal_logs.iter() {
                all_display_logs.push(DisplayLogEntry {
                    timestamp: log.timestamp,
                    level: log.level.clone(),
                    message: log.message.clone(),
                    count: 1,
                    is_aggregated: false,
                    event_type: None,
                });
            }

            // 按时间戳排序
            all_display_logs.sort_by_key(|entry| entry.timestamp);

            // 转换为 RawLogSnapshot 格式
            let logs: Vec<String> = all_display_logs.iter().map(|entry| {
                let timestamp = entry.timestamp.format("%H:%M:%S").to_string();
                format!("[{}] {} {}", timestamp, entry.level, entry.message)
            }).collect();

            Some(RawLogSnapshot {
                timestamp: chrono::Utc::now().format("%H:%M:%S").to_string(),
                logs,
                total_count: all_display_logs.len(),
            })
        };

        DashboardData {
            uptime_seconds,
            health_score,
            module_logs,
            raw_log_snapshot,
        }
    }

    pub async fn get_module_data(&self, module_name: &str) -> Option<ModuleDisplayData> {
        let aggregators = self.module_aggregators.read().await;
        let normal_logs = self.normal_logs.read().await;

        if let Some(aggregator) = aggregators.get(module_name) {
            let high_freq_logs = aggregator.get_high_freq_logs();
            let module_normal_logs: Vec<_> = normal_logs.iter()
                .filter(|log| log.target == module_name)
                .cloned()
                .collect();

            // 转换为统一的显示日志格式
            let mut display_logs = Vec::new();

            // 添加普通日志（转换为 DisplayLogEntry）
            for log in &module_normal_logs {
                display_logs.push(DisplayLogEntry {
                    timestamp: log.timestamp,
                    level: log.level.clone(),
                    message: log.message.clone(),
                    count: 1,
                    is_aggregated: false,
                    event_type: None,
                });
            }

            // 添加高频聚合日志（转换为 DisplayLogEntry）
            for high_freq_log in &high_freq_logs {
                let aggregated_message = format!(
                    "🔄 {} (聚合 {} 次) - {}",
                    high_freq_log.event_type,
                    high_freq_log.count,
                    high_freq_log.latest_log.message
                );

                display_logs.push(DisplayLogEntry {
                    timestamp: high_freq_log.last_updated,
                    level: high_freq_log.latest_log.level.clone(),
                    message: aggregated_message,
                    count: high_freq_log.count,
                    is_aggregated: true,
                    event_type: Some(high_freq_log.event_type.clone()),
                });
            }

            // 按时间戳排序，最新的在后面
            display_logs.sort_by_key(|entry| entry.timestamp);

            let total_logs = high_freq_logs.iter().map(|entry| entry.count).sum::<usize>()
                + module_normal_logs.len();

            let error_count = module_normal_logs.iter()
                .filter(|log| log.level == "ERROR")
                .count();
            let warn_count = module_normal_logs.iter()
                .filter(|log| log.level == "WARN")
                .count();
            let info_count = module_normal_logs.iter()
                .filter(|log| log.level == "INFO")
                .count();

            Some(ModuleDisplayData {
                total_logs,
                error_count,
                warn_count,
                info_count,
                display_logs,
            })
        } else {
            None
        }
    }

    pub async fn cleanup_expired_data(&self) {
        let mut aggregators = self.module_aggregators.write().await;
        let now = Utc::now();

        for aggregator in aggregators.values_mut() {
            aggregator.cleanup_expired(now);
        }

        let mut normal_logs = self.normal_logs.write().await;
        let cutoff = now - chrono::Duration::hours(1);
        normal_logs.retain(|log| log.timestamp > cutoff);
    }
}

impl std::fmt::Debug for ModuleAggregatorManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleAggregatorManager")
            .field("config", &self.config)
            .finish()
    }
}