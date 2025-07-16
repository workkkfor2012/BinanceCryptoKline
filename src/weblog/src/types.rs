//! WebLog系统的核心数据类型定义 - 简化版
//!
//! 基于Rust tracing规范的日志数据结构，专注于数据传输

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;
use chrono::{DateTime, Utc};

/// 日志条目 - 支持 log_type 字段用于前端分类处理
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
    /// 日志类型：用于前端区分处理 ("module" 或 "trace")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_type: Option<String>,
}

/// Span信息（增强版）- 支持层级结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanInfo {
    pub name: String,
    pub target: String,
    pub id: Option<String>,
    pub parent_id: Option<String>,
    /// 调用层级结构（从根到当前的函数名数组）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hierarchy: Option<Vec<String>>,
}

/// WebSocket消息协议 - 极简版本
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum WebSocketMessage {
    /// 新的日志条目
    LogEntry { data: LogEntry },
    /// 历史日志发送完成信令
    HistoryComplete,
    /// 新会话开始信令 - 告知前端清空缓存并重新初始化
    SessionStart { session_id: String },
}

/// 应用状态 - 极简版本
#[derive(Debug)]
pub struct AppState {
    /// 启动时间，用于计算运行时长
    pub start_time: SystemTime,
    /// 历史日志缓存队列
    pub recent_logs: std::sync::Arc<std::sync::Mutex<VecDeque<LogEntry>>>,
    /// 实时日志广播发送器
    pub log_sender: tokio::sync::broadcast::Sender<LogEntry>,
    /// 当前会话ID
    pub session_id: std::sync::Arc<std::sync::Mutex<String>>,
    /// WebSocket消息广播发送器 - 用于发送SessionStart等控制消息
    pub websocket_sender: tokio::sync::broadcast::Sender<WebSocketMessage>,
}

impl AppState {
    /// 创建新的应用状态
    pub fn new() -> (Self, tokio::sync::broadcast::Receiver<LogEntry>, tokio::sync::broadcast::Receiver<WebSocketMessage>) {
        let (log_sender, log_receiver) = tokio::sync::broadcast::channel(10000000);
        let (websocket_sender, websocket_receiver) = tokio::sync::broadcast::channel(1000);

        let state = Self {
            start_time: SystemTime::now(),
            recent_logs: std::sync::Arc::new(std::sync::Mutex::new(VecDeque::new())),
            log_sender,
            session_id: std::sync::Arc::new(std::sync::Mutex::new(Self::generate_session_id())),
            websocket_sender,
        };

        (state, log_receiver, websocket_receiver)
    }

    /// 生成新的会话ID
    fn generate_session_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("session_{}", timestamp)
    }

    /// 获取运行时长（秒）
    pub fn get_uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().unwrap_or_default().as_secs()
    }

    /// 添加日志到历史缓存并广播
    pub fn process_log_entry(&self, log_entry: LogEntry) {
        // 1. 添加到历史缓存
        {
            let mut logs = self.recent_logs.lock().unwrap();
            logs.push_back(log_entry.clone());
            
            // 保持最近5000条日志
            if logs.len() > 50000000 {
                logs.pop_front();
            }
        }

        // 2. 实时广播
        let _ = self.log_sender.send(log_entry);
    }

    /// 获取历史日志的克隆（用于新连接的初始数据传输）
    pub fn get_history_logs(&self) -> VecDeque<LogEntry> {
        self.recent_logs.lock().unwrap().clone()
    }

    /// 开始新会话 - 清空历史日志并生成新的会话ID，同时广播SessionStart消息
    pub fn start_new_session(&self) -> String {
        // 清空历史日志
        {
            let mut logs = self.recent_logs.lock().unwrap();
            logs.clear();
        }

        // 生成新的会话ID
        let new_session_id = Self::generate_session_id();
        {
            let mut session_id = self.session_id.lock().unwrap();
            *session_id = new_session_id.clone();
        }

        // 广播SessionStart消息给所有已连接的WebSocket客户端
        let session_start_message = WebSocketMessage::SessionStart {
            session_id: new_session_id.clone()
        };
        let _ = self.websocket_sender.send(session_start_message);

        new_session_id
    }

    /// 获取当前会话ID
    pub fn get_session_id(&self) -> String {
        self.session_id.lock().unwrap().clone()
    }
}

/// WebLog配置 - 简化版本
#[derive(Debug, Clone)]
pub struct WebLogConfig {
    /// Web服务端口
    pub web_port: u16,
    /// 日志传输方式
    pub log_transport: LogTransport,
    /// 命名管道名称（如果使用命名管道）
    pub pipe_name: Option<String>,
    /// 最大保留的日志条目数量
    pub max_log_entries: usize,
}

/// 日志传输方式
#[derive(Debug, Clone)]
pub enum LogTransport {
    /// 命名管道
    NamedPipe(String),
}
