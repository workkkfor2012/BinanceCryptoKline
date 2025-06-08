//! WebLog系统的核心数据类型定义
//!
//! 基于Rust tracing规范的日志数据结构

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;
use chrono::{DateTime, Utc};

/// 日志条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub target: String,
    pub message: String,
    pub module_path: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub fields: HashMap<String, serde_json::Value>,
    pub span: Option<SpanInfo>,
}

/// Span信息（简化版）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanInfo {
    pub name: String,
    pub target: String,
    pub id: Option<String>,
    pub parent_id: Option<String>,
}

/// Span信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    pub span_id: String,
    pub trace_id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub target: String,
    pub level: String,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub duration_ms: Option<f64>,
    pub fields: HashMap<String, serde_json::Value>,
    pub events: Vec<LogEntry>,
}

/// Trace信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub trace_id: String,
    pub root_span_id: Option<String>,
    pub root_span_name: Option<String>,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub duration_ms: Option<f64>,
    pub span_count: usize,
    pub spans: HashMap<String, Span>,
    pub status: TraceStatus,
}

/// Trace状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceStatus {
    Active,
    Completed,
    Error,
}

/// WebSocket消息协议
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum WebSocketMessage {
    /// 新的日志条目
    LogEntry { data: LogEntry },
    /// Trace更新
    TraceUpdate { trace_id: String, trace: Trace },
    /// Span更新
    SpanUpdate { span_id: String, span: Span },
    /// 可用目标列表更新
    TargetsUpdate { targets: Vec<String> },
    /// Trace列表更新
    TraceList { traces: Vec<Trace> },
    /// 统计信息更新
    StatsUpdate { data: LogStats },
    /// 系统状态更新（兼容前端格式）
    SystemStatus { data: SystemStatus },
    /// 仪表板数据更新（新的聚合格式）
    DashboardUpdate { data: crate::module_manager::DashboardData },
}



/// 系统状态（兼容前端期望的格式）
#[derive(Debug, Clone, Serialize)]
pub struct SystemStatus {
    pub uptime_seconds: u64,
    pub health_score: f64,
    pub validation_stats: ValidationStats,
    pub performance_stats: PerformanceStats,
    pub module_logs: HashMap<String, ModuleLogStats>,
    pub raw_log_snapshot: Option<RawLogSnapshot>,
    pub realtime_log_data: RealtimeLogData,
}

/// 验证统计（兼容前端格式）
#[derive(Debug, Clone, Serialize)]
pub struct ValidationStats {
    pub total_events: u64,
    pub pass_count: u64,
    pub fail_count: u64,
    pub warn_count: u64,
}

/// 性能统计（兼容前端格式）
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceStats {
    pub total_spans: u64,
    pub avg_duration_ms: f64,
    pub max_duration_ms: f64,
}

/// 模块日志条目（兼容前端格式）
#[derive(Debug, Clone, Serialize)]
pub struct ModuleLogEntry {
    pub timestamp: String,
    pub level: String,
    pub module: String,
    pub message: String,
}

/// 模块日志统计（兼容前端格式）
#[derive(Debug, Clone, Serialize)]
pub struct ModuleLogStats {
    pub total_logs: usize,
    pub error_count: usize,
    pub warn_count: usize,
    pub info_count: usize,
    pub recent_logs: Vec<ModuleLogEntry>,
}

/// 原始日志快照（兼容前端格式）
#[derive(Debug, Clone, Serialize)]
pub struct RawLogSnapshot {
    pub timestamp: String,
    pub logs: Vec<String>,
    pub total_count: usize,
}

/// 实时日志数据（兼容前端格式）
#[derive(Debug, Clone, Serialize)]
pub struct RealtimeLogData {
    pub recent_logs: Vec<String>,
    pub total_count: usize,
    pub last_update_time: String,
    pub logs_per_second: f64,
}

/// 日志统计信息
#[derive(Debug, Clone, Serialize)]
pub struct LogStats {
    pub uptime_seconds: u64,
    pub total_logs: u64,
    pub logs_by_level: HashMap<String, u64>,
    pub logs_by_target: HashMap<String, u64>,
    pub trace_stats: TraceStats,
    pub recent_logs: Vec<LogEntry>,
    pub logs_per_second: f64,
}

/// Trace统计信息
#[derive(Debug, Default, Clone, Serialize)]
pub struct TraceStats {
    pub total_traces: u64,
    pub active_traces: u64,
    pub completed_traces: u64,
    pub total_spans: u64,
    pub avg_trace_duration_ms: f64,
    pub max_trace_duration_ms: f64,
    pub avg_span_duration_ms: f64,
    pub spans_by_level: HashMap<String, u64>,
}

/// 日志级别统计
#[derive(Debug, Clone, Serialize)]
pub struct LogLevelStats {
    pub level: String,
    pub count: u64,
    pub percentage: f64,
}

/// 目标统计
#[derive(Debug, Clone, Serialize)]
pub struct TargetStats {
    pub target: String,
    pub log_count: u64,
    pub span_count: u64,
    pub recent_logs: Vec<LogEntry>,
}

/// 应用状态
#[derive(Debug)]
pub struct AppState {
    pub start_time: SystemTime,
    pub log_stats: std::sync::Arc<std::sync::Mutex<LogStats>>,
    pub target_stats: std::sync::Arc<std::sync::Mutex<HashMap<String, TargetStats>>>,
    pub recent_logs: std::sync::Arc<std::sync::Mutex<VecDeque<LogEntry>>>,
    pub trace_manager: std::sync::Arc<std::sync::Mutex<crate::TraceManager>>,
    pub log_sender: tokio::sync::broadcast::Sender<LogEntry>,

    // 新增字段以支持前端期望的格式
    pub module_logs: std::sync::Arc<std::sync::Mutex<HashMap<String, ModuleLogStats>>>,
    pub raw_logs: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    pub raw_log_snapshot: std::sync::Arc<std::sync::Mutex<Option<RawLogSnapshot>>>,
    pub realtime_logs: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    pub log_timestamps: std::sync::Arc<std::sync::Mutex<Vec<SystemTime>>>,

    // 新的聚合管理器
    pub module_aggregator_manager: std::sync::Arc<crate::module_manager::ModuleAggregatorManager>,
}

impl AppState {
    pub fn new() -> (Self, tokio::sync::broadcast::Receiver<LogEntry>) {
        let (log_sender, log_receiver) = tokio::sync::broadcast::channel(1000);

        let log_stats = LogStats {
            uptime_seconds: 0,
            total_logs: 0,
            logs_by_level: HashMap::new(),
            logs_by_target: HashMap::new(),
            trace_stats: TraceStats::default(),
            recent_logs: Vec::new(),
            logs_per_second: 0.0,
        };

        let state = Self {
            start_time: SystemTime::now(),
            log_stats: std::sync::Arc::new(std::sync::Mutex::new(log_stats)),
            target_stats: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            recent_logs: std::sync::Arc::new(std::sync::Mutex::new(VecDeque::new())),
            trace_manager: std::sync::Arc::new(std::sync::Mutex::new(crate::TraceManager::new(1000))),
            log_sender,

            // 初始化新字段
            module_logs: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            raw_logs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            raw_log_snapshot: std::sync::Arc::new(std::sync::Mutex::new(None)),
            realtime_logs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            log_timestamps: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),

            // 初始化模块聚合管理器
            module_aggregator_manager: std::sync::Arc::new(
                crate::module_manager::ModuleAggregatorManager::default()
            ),
        };

        (state, log_receiver)
    }

    /// 获取系统状态（兼容前端格式）
    pub fn get_system_status(&self) -> SystemStatus {
        let uptime = self.start_time.elapsed().unwrap_or_default().as_secs();

        // 获取模块日志
        let module_logs = self.module_logs.lock().unwrap().clone();

        // 获取原始日志快照
        let raw_log_snapshot = self.raw_log_snapshot.lock().unwrap().clone();

        // 获取实时日志数据
        let realtime_log_data = self.get_realtime_log_data();

        // 计算健康分数（简单实现）
        let health_score = 95.0; // 默认健康分数

        SystemStatus {
            uptime_seconds: uptime,
            health_score,
            validation_stats: ValidationStats {
                total_events: 0,
                pass_count: 0,
                fail_count: 0,
                warn_count: 0,
            },
            performance_stats: PerformanceStats {
                total_spans: 0,
                avg_duration_ms: 0.0,
                max_duration_ms: 0.0,
            },
            module_logs,
            raw_log_snapshot,
            realtime_log_data,
        }
    }

    /// 获取实时日志数据
    pub fn get_realtime_log_data(&self) -> RealtimeLogData {
        let realtime_logs = self.realtime_logs.lock().unwrap();
        let timestamps = self.log_timestamps.lock().unwrap();

        // 计算每秒日志数
        let logs_per_second = if timestamps.len() > 1 {
            timestamps.len() as f64 / 60.0 // 最近60秒的平均值
        } else {
            0.0
        };

        let last_update_time = if !realtime_logs.is_empty() {
            chrono::Utc::now().format("%H:%M:%S").to_string()
        } else {
            "--:--:--".to_string()
        };

        RealtimeLogData {
            recent_logs: realtime_logs.clone(),
            total_count: self.raw_logs.lock().unwrap().len(),
            last_update_time,
            logs_per_second,
        }
    }

    /// 添加原始日志
    pub fn add_raw_log(&self, log_line: String) {
        let now = SystemTime::now();

        // 添加到原始日志
        {
            let mut logs = self.raw_logs.lock().unwrap();
            logs.push(log_line.clone());

            // 保持最近1000条日志
            if logs.len() > 1000 {
                let excess = logs.len() - 1000;
                logs.drain(0..excess);
            }
        }

        // 添加到实时日志
        {
            let mut realtime_logs = self.realtime_logs.lock().unwrap();
            realtime_logs.push(log_line);

            // 保持最近20条实时日志
            if realtime_logs.len() > 20 {
                realtime_logs.remove(0);
            }
        }

        // 记录时间戳用于计算频率
        {
            let mut timestamps = self.log_timestamps.lock().unwrap();
            timestamps.push(now);

            // 只保留最近60秒的时间戳
            let cutoff = now - std::time::Duration::from_secs(60);
            timestamps.retain(|&t| t > cutoff);
        }

        // 不再更新原始日志快照，只通过ModuleAggregatorManager处理
    }

    /// 更新原始日志快照（已禁用）
    fn update_raw_log_snapshot(&self) {
        // 不再更新AppState中的快照，只使用ModuleAggregatorManager
    }
}
