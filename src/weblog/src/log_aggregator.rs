//! 日志聚合器模块
//!
//! 实现高频日志聚合功能，包括：
//! - 相似度计算算法
//! - 日志聚合逻辑
//! - 过期数据清理
//! - 内存使用控制

use crate::types::*;
use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// 聚合器配置
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// 最大显示日志数量
    pub max_displayed: usize,
    /// 最大历史保留数量
    pub max_history: usize,
    /// 相似度阈值 (0.0-1.0)
    pub similarity_threshold: f64,
    /// 聚合时间窗口 (毫秒)
    pub time_window_ms: i64,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            max_displayed: usize::MAX, // 后端不限制显示数量，由前端控制
            max_history: 1000,
            similarity_threshold: 0.5,
            time_window_ms: 3600000, // 1小时，避免过期清理影响日志显示
        }
    }
}

/// 显示的日志条目（聚合后）
#[derive(Debug, Clone, Serialize)]
pub struct DisplayLogEntry {
    pub message: String,
    pub level: String,
    pub timestamp: DateTime<Utc>,
    pub count: usize,
    pub is_aggregated: bool,
    pub variations: Vec<String>,
    pub all_logs: Vec<LogEntry>,
}

/// 消息频率统计
#[derive(Debug, Clone)]
pub struct MessageFrequency {
    pub times: Vec<i64>,
    pub aggregated: bool,
    pub variations: Vec<String>,
    pub base_message: String,
}

/// 日志聚合器
#[derive(Debug, Clone)]
pub struct LogAggregator {
    displayed_logs: VecDeque<DisplayLogEntry>,
    message_frequency: HashMap<String, MessageFrequency>,
    processed_logs: HashSet<String>,
    max_displayed: usize,
    max_history: usize,
    similarity_threshold: f64,
    time_window_ms: i64,
}

impl LogAggregator {
    /// 创建新的日志聚合器
    pub fn new(config: AggregatorConfig) -> Self {
        Self {
            displayed_logs: VecDeque::new(),
            message_frequency: HashMap::new(),
            processed_logs: HashSet::new(),
            max_displayed: config.max_displayed,
            max_history: config.max_history,
            similarity_threshold: config.similarity_threshold,
            time_window_ms: config.time_window_ms,
        }
    }

    /// 添加日志条目（已禁用聚合功能，直接显示所有日志）
    pub fn add_log_entry(&mut self, log_entry: LogEntry) -> bool {
        let now = Utc::now().timestamp_millis();
        let log_key = self.generate_log_key(&log_entry);

        // 为了支持聚合，我们使用时间戳+消息的组合来检查重复
        // 这样相同消息在不同时间可以被聚合，但完全相同的日志条目会被跳过
        let unique_key = format!("{}-{}", log_entry.timestamp.timestamp_millis(), log_key);

        // 检查是否已处理这个具体的日志条目
        if self.processed_logs.contains(&unique_key) {
            return false;
        }

        self.processed_logs.insert(unique_key);

        // 清理过期数据
        self.cleanup_expired(now);

        // 🚫 禁用聚合功能：直接创建新条目，不进行相似度检测和聚合
        // 注释掉原来的聚合逻辑：
        // if let Some((base_message, _similarity)) = self.find_similar_message(&log_entry.message) {
        //     self.update_existing_aggregation(base_message, log_entry, now);
        // } else {
        //     self.create_new_entry(log_entry, now);
        // }

        // 直接创建新条目，每个日志都单独显示
        self.create_new_entry(log_entry, now);

        // 限制显示数量
        self.limit_displayed_logs();

        true
    }

    /// 查找相似消息
    fn find_similar_message(&self, message: &str) -> Option<(String, f64)> {
        for (base_message, _) in &self.message_frequency {
            let similarity = calculate_similarity(message, base_message);
            if similarity >= self.similarity_threshold {
                return Some((base_message.clone(), similarity));
            }
        }
        None
    }

    /// 更新现有聚合
    fn update_existing_aggregation(&mut self, base_message: String, log_entry: LogEntry, now: i64) {
        // 更新频率记录
        if let Some(frequency) = self.message_frequency.get_mut(&base_message) {
            frequency.times.push(now);
            frequency.times.retain(|&time| now - time <= self.time_window_ms);
            
            if !frequency.variations.contains(&log_entry.message) {
                frequency.variations.push(log_entry.message.clone());
            }
            
            // 更新显示条目
            if let Some(display_entry) = self.displayed_logs.iter_mut()
                .find(|entry| entry.message == base_message) {

                display_entry.count = frequency.times.len();
                display_entry.all_logs.push(log_entry);
                display_entry.variations = frequency.variations.clone();
                display_entry.timestamp = Utc::now();
                display_entry.is_aggregated = display_entry.count > 1;

                // 移动到最前面
                let index = self.displayed_logs.iter()
                    .position(|entry| entry.message == base_message)
                    .unwrap();
                let entry = self.displayed_logs.remove(index).unwrap();
                self.displayed_logs.push_front(entry);
            }
        }
    }

    /// 创建新条目
    fn create_new_entry(&mut self, log_entry: LogEntry, now: i64) {
        // 创建频率记录
        let frequency = MessageFrequency {
            times: vec![now],
            aggregated: false,
            variations: vec![log_entry.message.clone()],
            base_message: log_entry.message.clone(),
        };
        
        self.message_frequency.insert(log_entry.message.clone(), frequency);
        
        // 创建显示条目
        let display_entry = DisplayLogEntry {
            message: log_entry.message.clone(),
            level: log_entry.level.clone(),
            timestamp: log_entry.timestamp,
            count: 1,
            is_aggregated: false,
            variations: vec![log_entry.message.clone()],
            all_logs: vec![log_entry],
        };
        
        self.displayed_logs.push_front(display_entry);
    }

    /// 清理过期数据
    pub fn cleanup_expired(&mut self, now: i64) {
        let mut expired_messages = Vec::new();
        
        for (message, frequency) in &mut self.message_frequency {
            frequency.times.retain(|&time| now - time <= self.time_window_ms);
            
            if frequency.times.is_empty() {
                expired_messages.push(message.clone());
            }
        }
        
        for message in expired_messages {
            self.message_frequency.remove(&message);
            self.displayed_logs.retain(|entry| entry.message != message);
        }
        
        // 清理过期的已处理日志记录
        let one_hour_ago = now - 3600000; // 1小时
        self.processed_logs.retain(|log_key| {
            if let Some(timestamp_str) = log_key.split('-').next() {
                if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                    return timestamp > one_hour_ago;
                }
            }
            false
        });
    }

    /// 限制显示日志数量
    fn limit_displayed_logs(&mut self) {
        while self.displayed_logs.len() > self.max_displayed {
            self.displayed_logs.pop_back();
        }
    }

    /// 生成日志键
    /// 注意：不包含时间戳，以便相同消息可以被聚合
    fn generate_log_key(&self, log_entry: &LogEntry) -> String {
        format!("{}-{}-{}",
            log_entry.target,
            log_entry.message,
            log_entry.level
        )
    }

    /// 获取显示的日志
    pub fn get_displayed_logs(&self) -> Vec<DisplayLogEntry> {
        self.displayed_logs.iter().cloned().collect()
    }

    /// 获取所有原始日志条目（按时间倒序）
    pub fn get_all_raw_logs(&self) -> Vec<LogEntry> {
        let mut all_logs = Vec::new();

        // 从所有显示条目中收集原始日志
        for display_entry in &self.displayed_logs {
            all_logs.extend(display_entry.all_logs.clone());
        }

        // 按时间戳倒序排序（最新的在前）
        all_logs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        all_logs
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> AggregatorStats {
        AggregatorStats {
            total_processed: self.processed_logs.len(),
            displayed_count: self.displayed_logs.len(),
            aggregated_count: self.displayed_logs.iter()
                .filter(|entry| entry.is_aggregated)
                .count(),
        }
    }
}

/// 聚合器统计信息
#[derive(Debug, Clone, Serialize)]
pub struct AggregatorStats {
    pub total_processed: usize,
    pub displayed_count: usize,
    pub aggregated_count: usize,
}

/// 计算两个字符串的相似度
/// 使用简化的字符匹配算法，与前端保持一致
pub fn calculate_similarity(str1: &str, str2: &str) -> f64 {
    if str1 == str2 {
        return 1.0;
    }
    if str1.is_empty() || str2.is_empty() {
        return 0.0;
    }

    let max_len = str1.len().max(str2.len()) as f64;
    let shorter = if str1.len() <= str2.len() { str1 } else { str2 };
    let longer = if str1.len() > str2.len() { str1 } else { str2 };
    
    let mut common_chars = 0;
    for ch in shorter.chars() {
        if longer.contains(ch) {
            common_chars += 1;
        }
    }
    
    common_chars as f64 / max_len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_similarity_calculation() {
        assert_eq!(calculate_similarity("hello", "hello"), 1.0);
        assert_eq!(calculate_similarity("", "hello"), 0.0);
        assert_eq!(calculate_similarity("hello", ""), 0.0);
        
        // 测试部分相似
        let sim = calculate_similarity("hello world", "hello rust");
        assert!(sim > 0.5 && sim < 1.0);
    }

    #[test]
    fn test_log_aggregator() {
        let mut aggregator = LogAggregator::new(AggregatorConfig::default());

        let log1 = LogEntry {
            timestamp: Utc::now(),
            level: "INFO".to_string(),
            message: "Test message".to_string(),
            target: "test".to_string(),
            module_path: Some("test::module".to_string()),
            file: Some("test.rs".to_string()),
            line: Some(42),
            fields: HashMap::new(),
            span: None,
        };

        assert!(aggregator.add_log_entry(log1));
        assert_eq!(aggregator.get_displayed_logs().len(), 1);
    }

    #[test]
    fn test_log_aggregation_disabled() {
        let mut aggregator = LogAggregator::new(AggregatorConfig::default());

        let base_time = Utc::now();

        // 添加第一条日志
        let log1 = LogEntry {
            timestamp: base_time,
            level: "INFO".to_string(),
            message: "测试消息1".to_string(),
            target: "test_module".to_string(),
            module_path: None,
            file: None,
            line: None,
            fields: HashMap::new(),
            span: None,
        };

        assert!(aggregator.add_log_entry(log1));
        assert_eq!(aggregator.get_displayed_logs().len(), 1);
        assert_eq!(aggregator.get_displayed_logs()[0].count, 1);
        assert!(!aggregator.get_displayed_logs()[0].is_aggregated); // 聚合已禁用

        // 添加相同消息但不同时间戳的日志
        let log2 = LogEntry {
            timestamp: base_time + chrono::Duration::milliseconds(1),
            level: "INFO".to_string(),
            message: "测试消息1".to_string(),
            target: "test_module".to_string(),
            module_path: None,
            file: None,
            line: None,
            fields: HashMap::new(),
            span: None,
        };

        assert!(aggregator.add_log_entry(log2));
        // 聚合已禁用，应该有2个独立的条目
        assert_eq!(aggregator.get_displayed_logs().len(), 2);
        assert_eq!(aggregator.get_displayed_logs()[0].count, 1);
        assert_eq!(aggregator.get_displayed_logs()[1].count, 1);
        assert!(!aggregator.get_displayed_logs()[0].is_aggregated);
        assert!(!aggregator.get_displayed_logs()[1].is_aggregated);

        // 测试获取所有原始日志
        let all_raw_logs = aggregator.get_all_raw_logs();
        assert_eq!(all_raw_logs.len(), 2);
        assert_eq!(all_raw_logs[0].message, "测试消息1");
        assert_eq!(all_raw_logs[1].message, "测试消息1");
    }
}
