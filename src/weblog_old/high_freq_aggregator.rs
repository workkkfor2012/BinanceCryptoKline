//! 高频日志聚合器模块
//!
//! 基于event_type字段的高频日志聚合功能，包括：
//! - 基于时间窗口的高频检测（5秒内2次）
//! - 字段变化对比
//! - O(1)性能的HashMap聚合

use crate::types::LogEntry;
use std::collections::{HashMap, VecDeque};
use serde::Serialize;
use chrono::{DateTime, Utc};

/// 高频日志聚合配置
#[derive(Debug, Clone)]
pub struct HighFreqConfig {
    /// 时间窗口（秒）
    pub time_window_seconds: i64,
    /// 最小次数阈值
    pub min_count_threshold: usize,
    /// 最大保存条目数
    pub max_entries: usize,
}

impl Default for HighFreqConfig {
    fn default() -> Self {
        Self {
            time_window_seconds: 5,     // 5秒窗口
            min_count_threshold: 2,     // 2次就算高频
            max_entries: 1000,          // 最多保存1000个不同的event_type
        }
    }
}

/// 高频日志聚合条目
#[derive(Debug, Clone, Serialize)]
pub struct HighFreqLogEntry {
    pub event_type: String,
    pub count: usize,                    // 总次数
    pub latest_log: LogEntry,            // 最新日志
    pub previous_log: Option<LogEntry>,  // 上一条日志（用于对比）
    pub field_changes: HashMap<String, FieldChange>, // 字段变化
    pub last_updated: DateTime<Utc>,
    #[serde(skip)] // 不序列化到前端
    pub timestamps: VecDeque<DateTime<Utc>>, // 用于时间窗口计算
}

/// 字段变化
#[derive(Debug, Clone, Serialize)]
pub struct FieldChange {
    pub field_name: String,
    pub previous_value: Option<serde_json::Value>,
    pub current_value: serde_json::Value,
    pub changed: bool,
}

/// 高频日志聚合器
#[derive(Debug)]
pub struct HighFreqLogAggregator {
    high_freq_logs: HashMap<String, HighFreqLogEntry>,
    config: HighFreqConfig,
}

impl HighFreqLogAggregator {
    /// 创建新的高频日志聚合器
    pub fn new(config: HighFreqConfig) -> Self {
        Self {
            high_freq_logs: HashMap::new(),
            config,
        }
    }

    /// 处理日志条目，返回是否被聚合
    pub fn process_log_entry(&mut self, log_entry: LogEntry) -> bool {
        // 检查是否是高频日志
        let event_type = log_entry.fields.get("event_type")
            .and_then(|v| v.as_str());
        let is_high_freq = log_entry.fields.get("is_high_freq")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // 添加调试日志
        if log_entry.target == "buffered_kline_store" {
            println!("🔍 [高频聚合器] 处理日志: target={}, event_type={:?}, is_high_freq={}, fields={:?}",
                log_entry.target, event_type, is_high_freq, log_entry.fields);
        }

        if let Some(event_type) = event_type {
            if is_high_freq {
                println!("✅ [高频聚合器] 检测到高频日志: event_type={}, target={}", event_type, log_entry.target);
                return self.handle_high_freq_log(event_type.to_string(), log_entry);
            }
        }

        // 不是高频日志，不处理
        false
    }

    /// 处理高频日志
    fn handle_high_freq_log(&mut self, event_type: String, log_entry: LogEntry) -> bool {
        let now = Utc::now();

        println!("🔄 [高频聚合器] 处理高频日志: event_type={}", event_type);

        // 先检查是否存在，避免借用冲突
        let should_aggregate = if let Some(existing) = self.high_freq_logs.get_mut(&event_type) {
            // 清理过期的时间戳（5秒外的）
            let cutoff = now - chrono::Duration::seconds(self.config.time_window_seconds);
            let old_len = existing.timestamps.len();
            while let Some(&front_time) = existing.timestamps.front() {
                if front_time < cutoff {
                    existing.timestamps.pop_front();
                } else {
                    break;
                }
            }
            let new_len = existing.timestamps.len();
            if old_len != new_len {
                println!("🧹 [高频聚合器] 清理过期时间戳: {} -> {}", old_len, new_len);
            }

            // 添加新时间戳
            existing.timestamps.push_back(now);
            let current_count = existing.timestamps.len();

            println!("📊 [高频聚合器] 时间窗口内计数: {}/{} (阈值: {})",
                current_count, self.config.time_window_seconds, self.config.min_count_threshold);

            // 检查是否达到高频阈值（5秒内2次）
            current_count >= self.config.min_count_threshold
        } else {
            println!("🆕 [高频聚合器] 首次遇到此event_type: {}", event_type);
            false
        };

        if should_aggregate {
            // 重新获取可变引用进行更新
            if let Some(existing) = self.high_freq_logs.get_mut(&event_type) {
                // 计算字段差异
                let field_changes = Self::calculate_field_changes_static(&existing.latest_log, &log_entry);

                // 更新聚合条目
                existing.previous_log = Some(existing.latest_log.clone());
                existing.latest_log = log_entry;
                existing.count += 1;
                existing.field_changes = field_changes;
                existing.last_updated = now;

                println!("🔄 [高频聚合器] 聚合日志: event_type={}, 总计数={}", event_type, existing.count);
                return true; // 已聚合，不需要单独显示
            }
        } else if !self.high_freq_logs.contains_key(&event_type) {
            // 创建新条目
            let mut timestamps = VecDeque::new();
            timestamps.push_back(now);

            let entry = HighFreqLogEntry {
                event_type: event_type.clone(),
                count: 1,
                latest_log: log_entry,
                previous_log: None,
                field_changes: HashMap::new(),
                last_updated: now,
                timestamps,
            };

            self.high_freq_logs.insert(event_type.clone(), entry);
            println!("📝 [高频聚合器] 创建新聚合条目: event_type={}", event_type);
        }

        // 还没达到高频阈值，正常显示
        println!("⏳ [高频聚合器] 未达到聚合阈值，正常显示日志");
        false
    }

    /// 计算字段变化（静态方法避免借用冲突）
    fn calculate_field_changes_static(
        previous: &LogEntry,
        current: &LogEntry
    ) -> HashMap<String, FieldChange> {
        let mut changes = HashMap::new();
        
        // 比较当前日志的所有字段
        for (key, current_value) in &current.fields {
            let previous_value = previous.fields.get(key);
            let changed = match previous_value {
                Some(prev_val) => prev_val != current_value,
                None => true, // 新字段算作变化
            };
            
            changes.insert(key.clone(), FieldChange {
                field_name: key.clone(),
                previous_value: previous_value.cloned(),
                current_value: current_value.clone(),
                changed,
            });
        }
        
        changes
    }

    /// 获取所有高频日志条目
    pub fn get_high_freq_logs(&self) -> Vec<HighFreqLogEntry> {
        self.high_freq_logs.values().cloned().collect()
    }

    /// 清理过期的高频日志条目
    pub fn cleanup_expired(&mut self, now: DateTime<Utc>) {
        let cutoff = now - chrono::Duration::minutes(30); // 30分钟后清理
        
        self.high_freq_logs.retain(|_, entry| {
            entry.last_updated > cutoff
        });
        
        // 限制条目数量
        if self.high_freq_logs.len() > self.config.max_entries {
            // 简单策略：清理最老的条目
            let mut entries: Vec<_> = self.high_freq_logs.iter()
                .map(|(k, v)| (k.clone(), v.last_updated))
                .collect();
            entries.sort_by_key(|(_, last_updated)| *last_updated);

            let to_remove = entries.len() - self.config.max_entries;
            for (event_type, _) in entries.iter().take(to_remove) {
                self.high_freq_logs.remove(event_type);
            }
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> HighFreqStats {
        HighFreqStats {
            total_event_types: self.high_freq_logs.len(),
            total_aggregated_logs: self.high_freq_logs.values()
                .map(|entry| entry.count)
                .sum(),
        }
    }
}

/// 高频日志统计信息
#[derive(Debug, Clone, Serialize)]
pub struct HighFreqStats {
    pub total_event_types: usize,
    pub total_aggregated_logs: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_log(event_type: &str, is_high_freq: bool, extra_fields: HashMap<String, serde_json::Value>) -> LogEntry {
        let mut fields = HashMap::new();
        fields.insert("event_type".to_string(), serde_json::Value::String(event_type.to_string()));
        fields.insert("is_high_freq".to_string(), serde_json::Value::Bool(is_high_freq));
        
        for (k, v) in extra_fields {
            fields.insert(k, v);
        }

        LogEntry {
            timestamp: Utc::now(),
            level: "INFO".to_string(),
            target: "test".to_string(),
            message: "test message".to_string(),
            module_path: None,
            file: None,
            line: None,
            fields,
            span: None,
        }
    }

    #[test]
    fn test_high_freq_aggregation() {
        let mut aggregator = HighFreqLogAggregator::new(HighFreqConfig::default());

        // 第一次日志 - 不会被聚合
        let log1 = create_test_log("TEST_EVENT", true, {
            let mut fields = HashMap::new();
            fields.insert("count".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
            fields
        });

        assert!(!aggregator.process_log_entry(log1));
        assert_eq!(aggregator.high_freq_logs.len(), 1);

        // 第二次日志 - 开始聚合
        let log2 = create_test_log("TEST_EVENT", true, {
            let mut fields = HashMap::new();
            fields.insert("count".to_string(), serde_json::Value::Number(serde_json::Number::from(2)));
            fields
        });

        assert!(aggregator.process_log_entry(log2));

        let high_freq_logs = aggregator.get_high_freq_logs();
        assert_eq!(high_freq_logs.len(), 1);
        assert_eq!(high_freq_logs[0].count, 2);
        assert_eq!(high_freq_logs[0].event_type, "TEST_EVENT");
    }

    #[test]
    fn test_multiple_event_types_separate_aggregation() {
        let mut aggregator = HighFreqLogAggregator::new(HighFreqConfig::default());

        // 测试多个不同的 event_type 是否分别聚合

        // EVENT_A 的第一次日志
        let log_a1 = create_test_log("EVENT_A", true, {
            let mut fields = HashMap::new();
            fields.insert("value".to_string(), serde_json::Value::Number(serde_json::Number::from(100)));
            fields
        });
        assert!(!aggregator.process_log_entry(log_a1)); // 首次不聚合

        // EVENT_B 的第一次日志
        let log_b1 = create_test_log("EVENT_B", true, {
            let mut fields = HashMap::new();
            fields.insert("value".to_string(), serde_json::Value::Number(serde_json::Number::from(200)));
            fields
        });
        assert!(!aggregator.process_log_entry(log_b1)); // 首次不聚合

        // 现在应该有两个不同的 event_type 条目
        assert_eq!(aggregator.high_freq_logs.len(), 2);

        // EVENT_A 的第二次日志 - 开始聚合
        let log_a2 = create_test_log("EVENT_A", true, {
            let mut fields = HashMap::new();
            fields.insert("value".to_string(), serde_json::Value::Number(serde_json::Number::from(101)));
            fields
        });
        assert!(aggregator.process_log_entry(log_a2)); // 开始聚合

        // EVENT_B 的第二次日志 - 开始聚合
        let log_b2 = create_test_log("EVENT_B", true, {
            let mut fields = HashMap::new();
            fields.insert("value".to_string(), serde_json::Value::Number(serde_json::Number::from(201)));
            fields
        });
        assert!(aggregator.process_log_entry(log_b2)); // 开始聚合

        // 验证两个 event_type 分别聚合
        let high_freq_logs = aggregator.get_high_freq_logs();
        assert_eq!(high_freq_logs.len(), 2);

        // 找到对应的聚合条目
        let event_a_entry = high_freq_logs.iter().find(|entry| entry.event_type == "EVENT_A").unwrap();
        let event_b_entry = high_freq_logs.iter().find(|entry| entry.event_type == "EVENT_B").unwrap();

        // 验证各自的计数
        assert_eq!(event_a_entry.count, 2);
        assert_eq!(event_b_entry.count, 2);

        // 验证最新值
        assert_eq!(event_a_entry.latest_log.fields.get("value").unwrap(), &serde_json::Value::Number(serde_json::Number::from(101)));
        assert_eq!(event_b_entry.latest_log.fields.get("value").unwrap(), &serde_json::Value::Number(serde_json::Number::from(201)));

        // 继续添加 EVENT_A 的第三次日志
        let log_a3 = create_test_log("EVENT_A", true, {
            let mut fields = HashMap::new();
            fields.insert("value".to_string(), serde_json::Value::Number(serde_json::Number::from(102)));
            fields
        });
        assert!(aggregator.process_log_entry(log_a3)); // 继续聚合

        // 验证只有 EVENT_A 的计数增加了
        let high_freq_logs = aggregator.get_high_freq_logs();
        let event_a_entry = high_freq_logs.iter().find(|entry| entry.event_type == "EVENT_A").unwrap();
        let event_b_entry = high_freq_logs.iter().find(|entry| entry.event_type == "EVENT_B").unwrap();

        assert_eq!(event_a_entry.count, 3); // EVENT_A 增加到 3
        assert_eq!(event_b_entry.count, 2); // EVENT_B 保持 2
    }
}
