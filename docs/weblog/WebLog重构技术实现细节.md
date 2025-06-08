# WebLog重构技术实现细节

## 1. 相似度算法实现

### 1.1 算法选择
基于最长公共子序列(LCS)的相似度计算，与前端保持一致：

```rust
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
```

### 1.2 性能优化
- 使用字符集合预计算
- 短路优化：长度差异过大时直接返回低相似度
- 缓存常见消息的相似度结果

## 2. 聚合器核心实现

### 2.1 LogAggregator结构
```rust
use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

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

#[derive(Debug, Clone)]
pub struct MessageFrequency {
    pub times: Vec<i64>,
    pub aggregated: bool,
    pub variations: Vec<String>,
    pub base_message: String,
}
```

### 2.2 核心聚合逻辑
```rust
impl LogAggregator {
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

    pub fn add_log_entry(&mut self, log_entry: LogEntry) -> bool {
        let now = Utc::now().timestamp_millis();
        let log_key = self.generate_log_key(&log_entry);
        
        // 检查是否已处理
        if self.processed_logs.contains(&log_key) {
            return false;
        }
        
        self.processed_logs.insert(log_key);
        
        // 清理过期数据
        self.cleanup_expired(now);
        
        // 查找相似消息
        if let Some((base_message, similarity)) = self.find_similar_message(&log_entry.message) {
            self.update_existing_aggregation(base_message, log_entry, now);
        } else {
            self.create_new_entry(log_entry, now);
        }
        
        // 限制显示数量
        self.limit_displayed_logs();
        
        true
    }

    fn find_similar_message(&self, message: &str) -> Option<(String, f64)> {
        for (base_message, _) in &self.message_frequency {
            let similarity = calculate_similarity(message, base_message);
            if similarity >= self.similarity_threshold {
                return Some((base_message.clone(), similarity));
            }
        }
        None
    }

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
                
                // 移动到最前面
                let index = self.displayed_logs.iter()
                    .position(|entry| entry.message == base_message)
                    .unwrap();
                let entry = self.displayed_logs.remove(index).unwrap();
                self.displayed_logs.push_front(entry);
            }
        }
    }

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

    fn cleanup_expired(&mut self, now: i64) {
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

    fn limit_displayed_logs(&mut self) {
        while self.displayed_logs.len() > self.max_displayed {
            self.displayed_logs.pop_back();
        }
    }

    fn generate_log_key(&self, log_entry: &LogEntry) -> String {
        format!("{}-{}-{}", 
            log_entry.timestamp.timestamp_millis(),
            log_entry.message,
            log_entry.level
        )
    }

    pub fn get_displayed_logs(&self) -> Vec<DisplayLogEntry> {
        self.displayed_logs.iter().cloned().collect()
    }
}
```

## 3. 模块管理器实现

### 3.1 ModuleAggregatorManager结构
```rust
use std::collections::HashMap;
use tokio::sync::RwLock;

pub struct ModuleAggregatorManager {
    module_aggregators: RwLock<HashMap<String, LogAggregator>>,
    realtime_logs: RwLock<VecDeque<String>>,
    raw_snapshot_aggregator: RwLock<LogAggregator>,
    config: AggregatorConfig,
}

#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    pub max_displayed: usize,
    pub max_history: usize,
    pub similarity_threshold: f64,
    pub time_window_ms: i64,
    pub max_realtime_logs: usize,
}

impl ModuleAggregatorManager {
    pub fn new(config: AggregatorConfig) -> Self {
        Self {
            module_aggregators: RwLock::new(HashMap::new()),
            realtime_logs: RwLock::new(VecDeque::new()),
            raw_snapshot_aggregator: RwLock::new(LogAggregator::new(config.clone())),
            config,
        }
    }

    pub async fn process_module_logs(&self, module_name: &str, logs: Vec<LogEntry>) {
        let mut aggregators = self.module_aggregators.write().await;
        let aggregator = aggregators.entry(module_name.to_string())
            .or_insert_with(|| LogAggregator::new(self.config.clone()));
        
        for log_entry in logs {
            aggregator.add_log_entry(log_entry);
        }
    }

    pub async fn process_raw_log_snapshot(&self, logs: Vec<String>) {
        let mut aggregator = self.raw_snapshot_aggregator.write().await;
        let base_timestamp = Utc::now().timestamp_millis();
        
        for (index, log_line) in logs.iter().enumerate() {
            let log_entry = LogEntry {
                timestamp: DateTime::from_timestamp_millis(base_timestamp + index as i64)
                    .unwrap_or_else(|| Utc::now()),
                level: extract_level_from_log_line(log_line),
                message: extract_message_from_log_line(log_line),
                module: "RawLogSnapshot".to_string(),
            };
            
            aggregator.add_log_entry(log_entry);
        }
    }

    pub async fn add_realtime_log(&self, log_line: String) {
        let mut logs = self.realtime_logs.write().await;
        logs.push_front(log_line);
        
        while logs.len() > self.config.max_realtime_logs {
            logs.pop_back();
        }
    }

    pub async fn get_module_data(&self, module_name: &str) -> Option<ModuleDisplayData> {
        let aggregators = self.module_aggregators.read().await;
        aggregators.get(module_name).map(|aggregator| {
            ModuleDisplayData {
                total_logs: aggregator.processed_logs.len(),
                error_count: aggregator.get_displayed_logs().iter()
                    .filter(|log| log.level == "ERROR")
                    .count(),
                displayed_logs: aggregator.get_displayed_logs(),
            }
        })
    }

    pub async fn get_all_modules_data(&self) -> HashMap<String, ModuleDisplayData> {
        let aggregators = self.module_aggregators.read().await;
        let mut result = HashMap::new();
        
        for (module_name, aggregator) in aggregators.iter() {
            let module_data = ModuleDisplayData {
                total_logs: aggregator.processed_logs.len(),
                error_count: aggregator.get_displayed_logs().iter()
                    .filter(|log| log.level == "ERROR")
                    .count(),
                displayed_logs: aggregator.get_displayed_logs(),
            };
            result.insert(module_name.clone(), module_data);
        }
        
        result
    }

    pub async fn get_raw_snapshot_data(&self) -> RawSnapshotData {
        let aggregator = self.raw_snapshot_aggregator.read().await;
        RawSnapshotData {
            timestamp: Utc::now(),
            total_count: aggregator.processed_logs.len(),
            displayed_logs: aggregator.get_displayed_logs(),
        }
    }

    pub async fn get_realtime_logs(&self) -> Vec<String> {
        let logs = self.realtime_logs.read().await;
        logs.iter().cloned().collect()
    }
}

## 4. WebSocket集成

### 4.1 消息处理集成
```rust
use tokio_tungstenite::tungstenite::Message;
use serde_json;

// 在现有的WebSocket处理中集成聚合管理器
pub struct WebLogServer {
    aggregator_manager: Arc<ModuleAggregatorManager>,
    // ... 其他字段
}

impl WebLogServer {
    pub async fn handle_dashboard_request(&self) -> Result<Message, Box<dyn std::error::Error>> {
        // 收集所有聚合数据
        let module_logs = self.aggregator_manager.get_all_modules_data().await;
        let realtime_logs = self.aggregator_manager.get_realtime_logs().await;
        let raw_snapshot = self.aggregator_manager.get_raw_snapshot_data().await;

        let dashboard_data = DashboardData {
            uptime_seconds: self.get_uptime_seconds(),
            health_score: self.calculate_health_score(),
            module_logs,
            realtime_log_data: RealtimeLogData {
                recent_logs: realtime_logs,
            },
            raw_log_snapshot: raw_snapshot,
        };

        let response = serde_json::to_string(&dashboard_data)?;
        Ok(Message::Text(response))
    }

    pub async fn process_incoming_log(&self, log_entry: LogEntry) {
        // 根据日志来源分发到不同的聚合器
        match log_entry.module.as_str() {
            "realtime" => {
                self.aggregator_manager.add_realtime_log(log_entry.message).await;
            }
            "raw_snapshot" => {
                // 原始日志快照通过专门的接口处理
            }
            module_name => {
                self.aggregator_manager.process_module_logs(module_name, vec![log_entry]).await;
            }
        }
    }
}
```

### 4.2 数据结构定义
```rust
#[derive(Debug, Serialize)]
pub struct DashboardData {
    pub uptime_seconds: u64,
    pub health_score: u8,
    pub module_logs: HashMap<String, ModuleDisplayData>,
    pub realtime_log_data: RealtimeLogData,
    pub raw_log_snapshot: RawSnapshotData,
}

#[derive(Debug, Serialize)]
pub struct ModuleDisplayData {
    pub total_logs: usize,
    pub error_count: usize,
    pub displayed_logs: Vec<DisplayLogEntry>,
}

#[derive(Debug, Serialize)]
pub struct RealtimeLogData {
    pub recent_logs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RawSnapshotData {
    pub timestamp: DateTime<Utc>,
    pub total_count: usize,
    pub displayed_logs: Vec<DisplayLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub message: String,
    pub module: String,
}
```

## 5. 配置管理

### 5.1 配置文件结构
```rust
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebLogConfig {
    pub server: ServerConfig,
    pub aggregation: AggregationConfig,
    pub performance: PerformanceConfig,
    pub features: FeatureConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregationConfig {
    pub similarity_threshold: f64,
    pub time_window_seconds: u64,
    pub max_displayed_logs: usize,
    pub max_history_logs: usize,
    pub max_realtime_logs: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    pub batch_size: usize,
    pub cleanup_interval_seconds: u64,
    pub max_memory_mb: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureConfig {
    pub enable_aggregation: bool,
    pub enable_realtime_logs: bool,
    pub enable_raw_snapshot: bool,
    pub enable_performance_monitoring: bool,
}

impl WebLogConfig {
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: WebLogConfig = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                max_connections: 100,
            },
            aggregation: AggregationConfig {
                similarity_threshold: 0.5,
                time_window_seconds: 5,
                max_displayed_logs: 20,
                max_history_logs: 1000,
                max_realtime_logs: 100,
            },
            performance: PerformanceConfig {
                batch_size: 100,
                cleanup_interval_seconds: 60,
                max_memory_mb: 512,
            },
            features: FeatureConfig {
                enable_aggregation: true,
                enable_realtime_logs: true,
                enable_raw_snapshot: true,
                enable_performance_monitoring: true,
            },
        }
    }
}
```

## 6. 性能监控

### 6.1 监控指标收集
```rust
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct PerformanceMetrics {
    pub logs_processed: AtomicU64,
    pub aggregations_created: AtomicU64,
    pub similarity_calculations: AtomicU64,
    pub memory_usage_bytes: AtomicUsize,
    pub active_modules: AtomicUsize,
    pub connected_clients: AtomicUsize,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            logs_processed: AtomicU64::new(0),
            aggregations_created: AtomicU64::new(0),
            similarity_calculations: AtomicU64::new(0),
            memory_usage_bytes: AtomicUsize::new(0),
            active_modules: AtomicUsize::new(0),
            connected_clients: AtomicUsize::new(0),
        }
    }

    pub fn increment_logs_processed(&self) {
        self.logs_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_aggregations_created(&self) {
        self.aggregations_created.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_similarity_calculations(&self) {
        self.similarity_calculations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_memory_usage(&self, bytes: usize) {
        self.memory_usage_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn get_snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            logs_processed: self.logs_processed.load(Ordering::Relaxed),
            aggregations_created: self.aggregations_created.load(Ordering::Relaxed),
            similarity_calculations: self.similarity_calculations.load(Ordering::Relaxed),
            memory_usage_bytes: self.memory_usage_bytes.load(Ordering::Relaxed),
            active_modules: self.active_modules.load(Ordering::Relaxed),
            connected_clients: self.connected_clients.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub logs_processed: u64,
    pub aggregations_created: u64,
    pub similarity_calculations: u64,
    pub memory_usage_bytes: usize,
    pub active_modules: usize,
    pub connected_clients: usize,
}
```

---

**文档版本**: v1.0
**创建时间**: 2024年12月
**最后更新**: 2024年12月
