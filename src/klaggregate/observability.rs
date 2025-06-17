//! 可观察性和规格验证模块
//! 
//! 提供基于tracing的规格验证层和性能监控功能

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{Event, Id, Subscriber, info, warn, error, debug};
use tracing_subscriber::{layer::Context, Layer};
use tokio::sync::broadcast;
use axum::{
    extract::{ws::WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};
use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use tower_http::cors::CorsLayer;

/// 验证结果状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationStatus {
    #[serde(rename = "PASS")]
    Pass,
    #[serde(rename = "FAIL")]
    Fail,
    #[serde(rename = "WARN")]
    Warn,
}

/// 验证结果事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationEvent {
    /// 事件发生时间戳
    pub timestamp: u64,
    /// 验证规则ID
    pub validation_rule_id: String,
    /// 产生事件的模块
    pub module: String,
    /// 验证状态
    pub status: ValidationStatus,
    /// 验证上下文数据
    pub context: serde_json::Value,
    /// 验证结果描述
    pub message: String,
}

/// 性能指标事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceEvent {
    /// 事件发生时间戳
    pub timestamp: u64,
    /// 事件类型
    pub event_type: String,
    /// Span名称
    pub span_name: String,
    /// 持续时间（毫秒）
    pub duration_ms: f64,
    /// 上下文数据
    pub context: serde_json::Value,
}

/// Span性能数据
#[derive(Debug, Clone)]
struct SpanPerformanceData {
    start_time: Instant,
    metadata: HashMap<String, String>,
}

/// 规格验证规则
pub struct ValidationRule {
    pub id: String,
    pub description: String,
    pub validator: Box<dyn Fn(&ValidationContext) -> ValidationResult + Send + Sync>,
}

/// 验证上下文
#[derive(Debug, Clone)]
pub struct ValidationContext {
    pub module: String,
    pub operation: String,
    pub fields: HashMap<String, serde_json::Value>,
    pub span_data: Option<HashMap<String, String>>,
}

/// 验证结果
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub status: ValidationStatus,
    pub message: String,
    pub context: serde_json::Value,
}

/// 规格验证层
pub struct SpecValidationLayer {
    /// 验证规则集合
    rules: Arc<Mutex<HashMap<String, ValidationRule>>>,
    /// Span性能数据
    span_data: Arc<Mutex<HashMap<Id, SpanPerformanceData>>>,
    /// 事件输出器
    event_sender: Arc<dyn EventSender + Send + Sync>,
}

/// 事件输出接口
pub trait EventSender {
    fn send_validation_event(&self, event: ValidationEvent);
    fn send_performance_event(&self, event: PerformanceEvent);
}

/// 控制台事件输出器
pub struct ConsoleEventSender;

impl EventSender for ConsoleEventSender {
    fn send_validation_event(&self, _event: ValidationEvent) {
        // 禁用验证事件输出
        // if let Ok(json) = serde_json::to_string(&event) {
        //     println!("VALIDATION_EVENT: {}", json);
        // }
    }

    fn send_performance_event(&self, event: PerformanceEvent) {
        if let Ok(json) = serde_json::to_string(&event) {
            info!(target: "SystemObservability", "性能事件: {}", json);
        }
    }
}

/// 命名管道日志管理器
pub struct NamedPipeLogManager {
    pipe_name: String,
    pipe_writer: Arc<tokio::sync::Mutex<Option<tokio::io::BufWriter<tokio::net::windows::named_pipe::NamedPipeClient>>>>,
    connection_task_started: Arc<std::sync::atomic::AtomicBool>,
}

impl NamedPipeLogManager {
    /// 创建新的命名管道日志管理器
    pub fn new(pipe_name: String) -> Self {
        Self {
            pipe_name,
            pipe_writer: Arc::new(tokio::sync::Mutex::new(None)),
            connection_task_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// 连接到命名管道服务器
    pub async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::net::windows::named_pipe::ClientOptions;
        use tokio::io::BufWriter;

        info!(target: "SystemObservability", "📡 尝试连接到命名管道: {}", self.pipe_name);

        // 尝试连接到命名管道服务器（同步操作）
        let client = ClientOptions::new().open(&self.pipe_name)?;
        info!(target: "SystemObservability", "✅ 成功连接到命名管道服务器");

        // 创建缓冲写入器
        let writer = BufWriter::new(client);

        // 保存连接
        let mut pipe_writer = self.pipe_writer.lock().await;
        *pipe_writer = Some(writer);

        // 发送会话开始标记
        self.send_session_start_marker().await;

        Ok(())
    }

    /// 发送会话开始标记
    async fn send_session_start_marker(&self) {
        let session_start_marker = serde_json::json!({
            "timestamp": chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
                .format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
            "level": "INFO",
            "target": "SystemObservability",
            "message": "SESSION_START",
            "fields": {
                "session_start": true,
                "session_id": format!("session_{}", std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis())
            }
        });

        if let Ok(marker_json) = serde_json::to_string(&session_start_marker) {
            self.send_log(marker_json).await;
            info!(target: "SystemObservability", "🆕 已发送会话开始标记");
        }
    }

    /// 发送日志到命名管道
    pub async fn send_log(&self, log_line: String) {
        use tokio::io::AsyncWriteExt;

        if let Ok(mut pipe_writer_guard) = self.pipe_writer.try_lock() {
            if let Some(ref mut writer) = *pipe_writer_guard {
                let line_with_newline = format!("{}\n", log_line);
                if let Err(e) = writer.write_all(line_with_newline.as_bytes()).await {
                    error!(target: "SystemObservability", "发送日志到命名管道失败: {}", e);
                    // 连接断开，清除writer
                    *pipe_writer_guard = None;
                } else {
                    // 立即刷新缓冲区
                    let _ = writer.flush().await;
                }
            }
        }
    }

    /// 启动命名管道连接任务
    pub fn start_connection_task(&self) {
        // 检查是否在Tokio运行时中
        if tokio::runtime::Handle::try_current().is_err() {
            // 不在Tokio运行时中，延迟启动
            return;
        }

        // 使用原子操作确保只启动一次
        if self.connection_task_started.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst
        ).is_err() {
            // 已经启动过了
            return;
        }

        let pipe_name = self.pipe_name.clone();
        let manager = Arc::new(self.clone());

        tokio::spawn(async move {
            loop {
                // 检查是否已连接
                {
                    let pipe_writer = manager.pipe_writer.lock().await;
                    if pipe_writer.is_some() {
                        // 已连接，等待一段时间再检查
                        drop(pipe_writer);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                }

                // 尝试连接
                info!(target: "SystemObservability", "📡 尝试连接到命名管道服务器: {}", pipe_name);
                match manager.connect().await {
                    Ok(_) => {
                        info!(target: "SystemObservability", "✅ 命名管道连接成功");
                    }
                    Err(e) => {
                        warn!(target: "SystemObservability", "❌ 命名管道连接失败: {}, 5秒后重试", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }
}

impl Clone for NamedPipeLogManager {
    fn clone(&self) -> Self {
        Self {
            pipe_name: self.pipe_name.clone(),
            pipe_writer: self.pipe_writer.clone(),
            connection_task_started: self.connection_task_started.clone(),
        }
    }
}

/// WebSocket日志管理器 - 内置WebSocket服务器
pub struct WebSocketLogManager {
    log_sender: Arc<Mutex<Option<broadcast::Sender<String>>>>,
    web_port: u16,
}

impl WebSocketLogManager {
    /// 创建新的WebSocket日志管理器
    pub fn new(web_port: u16) -> Self {
        Self {
            log_sender: Arc::new(Mutex::new(None)),
            web_port,
        }
    }

    /// 启动WebSocket服务器
    pub fn start_websocket_server(&self) {
        let web_port = self.web_port;
        let log_sender = self.log_sender.clone();

        tokio::spawn(async move {
            // 创建广播通道
            let (tx, _) = broadcast::channel::<String>(1000);

            // 设置发送器
            {
                let mut sender_guard = log_sender.lock().unwrap();
                *sender_guard = Some(tx.clone());
            }

            // 创建Web应用
            let app = Router::new()
                .route("/ws", get({
                    let tx = tx.clone();
                    move |ws: WebSocketUpgrade| websocket_handler(ws, tx)
                }))
                .route("/", get(dashboard_handler))
                .route("/trace", get(trace_viewer_handler))
                .route("/modules", get(modules_handler))
                .route("/api/status", get(api_status_handler))
                .nest_service("/static", tower_http::services::ServeDir::new("static"))
                .layer(CorsLayer::permissive());

            // 启动服务器
            let bind_addr = format!("0.0.0.0:{}", web_port);
            info!(target: "SystemObservability", "🌐 WebSocket日志服务器启动: bind_addr={}, web_port={}", bind_addr, web_port);

            let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    }

    /// 发送日志到WebSocket
    pub fn send_log(&self, log_line: String) {
        if let Ok(sender_guard) = self.log_sender.try_lock() {
            if let Some(sender) = sender_guard.as_ref() {
                // 非阻塞发送，如果通道满了就丢弃
                let _ = sender.send(log_line);
            }
        }
    }
}

impl Clone for WebSocketLogManager {
    fn clone(&self) -> Self {
        Self {
            log_sender: self.log_sender.clone(),
            web_port: self.web_port,
        }
    }
}

/// WebSocket连接处理器
async fn websocket_handler(
    ws: WebSocketUpgrade,
    log_sender: broadcast::Sender<String>,
) -> Response {
    ws.on_upgrade(move |socket| handle_websocket_connection(socket, log_sender))
}

/// 处理WebSocket连接
async fn handle_websocket_connection(
    socket: WebSocket,
    log_sender: broadcast::Sender<String>,
) {
    let (mut sender, mut receiver) = socket.split();
    let mut log_receiver = log_sender.subscribe();

    // 发送欢迎消息
    let welcome_msg = serde_json::json!({
        "type": "SystemStatus",
        "data": {
            "status": "connected",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "message": "WebSocket连接已建立"
        }
    });

    if let Ok(welcome_json) = serde_json::to_string(&welcome_msg) {
        let _ = sender.send(Message::Text(welcome_json)).await;
    }

    // 处理消息
    tokio::select! {
        // 处理接收到的日志
        _ = async {
            while let Ok(log_line) = log_receiver.recv().await {
                // 解析日志并发送到前端
                let message = serde_json::json!({
                    "type": "LogEntry",
                    "data": parse_log_line(&log_line)
                });

                if let Ok(json) = serde_json::to_string(&message) {
                    if sender.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
            }
        } => {},

        // 处理客户端消息
        _ = async {
            while let Some(msg) = receiver.next().await {
                if msg.is_err() {
                    break;
                }
            }
        } => {},
    }
}

/// 解析日志行为结构化数据
fn parse_log_line(log_line: &str) -> serde_json::Value {
    // 尝试解析为JSON
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(log_line) {
        json
    } else {
        // 如果不是JSON，创建简单的日志对象
        serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": "INFO",
            "target": "unknown",
            "message": log_line
        })
    }
}

/// 主仪表板页面处理器
async fn dashboard_handler() -> axum::response::Html<String> {
    match tokio::fs::read_to_string("static/dashboard.html").await {
        Ok(content) => axum::response::Html(content),
        Err(_) => {
            axum::response::Html(r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>K线合成系统 - 主仪表板</title>
    <style>
        body { font-family: 'Consolas', monospace; background: #1a1a1a; color: #00ff00; padding: 20px; }
        .header { text-align: center; margin-bottom: 20px; }
        .nav { margin: 20px 0; text-align: center; }
        .nav a { color: #00ff00; text-decoration: none; margin: 0 20px; }
        .nav a:hover { color: #ffff00; }
    </style>
</head>
<body>
    <div class="header">
        <h1>K线合成系统 - 主仪表板</h1>
        <p>实时监控和日志可视化</p>
    </div>
    <div class="nav">
        <a href="/">主仪表板</a>
        <a href="/trace">Trace可视化</a>
        <a href="/modules">模块监控</a>
    </div>
    <div id="content">
        <p>正在加载...</p>
    </div>
    <script>
        // 连接WebSocket
        const ws = new WebSocket('ws://localhost:3000/ws');
        ws.onopen = () => console.log('WebSocket连接已建立');
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log('收到数据:', data);
        };
    </script>
</body>
</html>"#.to_string())
        }
    }
}

/// Trace可视化页面处理器
async fn trace_viewer_handler() -> axum::response::Html<String> {
    match tokio::fs::read_to_string("static/trace_viewer.html").await {
        Ok(content) => axum::response::Html(content),
        Err(_) => {
            axum::response::Html(r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>Trace可视化</title>
</head>
<body>
    <h1>Trace可视化</h1>
    <p>开发中...</p>
</body>
</html>"#.to_string())
        }
    }
}

/// API状态处理器
async fn api_status_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "service": "kline_aggregate_service",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// 模块监控页面处理器
async fn modules_handler() -> axum::response::Html<String> {
    // 尝试读取项目根目录的 module_monitor.html
    match tokio::fs::read_to_string("static/module_monitor.html").await {
        Ok(content) => axum::response::Html(content),
        Err(_) => {
            // 如果文件不存在，返回简单的模块监控页面
            axum::response::Html(r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>K线合成系统 - 模块监控</title>
    <style>
        body { font-family: 'Consolas', monospace; background: #1a1a1a; color: #00ff00; padding: 20px; }
        .header { text-align: center; margin-bottom: 20px; }
        .status { padding: 10px; border: 1px solid #333; margin: 10px 0; }
        .log-container { background: #000; padding: 10px; height: 400px; overflow-y: auto; border: 1px solid #333; }
        .log-entry { margin: 2px 0; font-size: 12px; }
        .error { color: #ff0000; }
        .warn { color: #ffff00; }
        .info { color: #00ff00; }
    </style>
</head>
<body>
    <div class="header">
        <h1>K线合成系统 - 模块监控</h1>
        <p>WebSocket直连模式</p>
    </div>
    <div class="status">
        <h3>连接状态</h3>
        <p id="connection-status">正在连接...</p>
    </div>
    <div class="status">
        <h3>实时日志</h3>
        <div id="log-container" class="log-container"></div>
    </div>
    <script>
        const ws = new WebSocket('ws://localhost:3000/ws');
        const logContainer = document.getElementById('log-container');
        const connectionStatus = document.getElementById('connection-status');

        ws.onopen = function() {
            connectionStatus.textContent = '已连接到K线合成系统';
            connectionStatus.style.color = '#00ff00';
        };

        ws.onclose = function() {
            connectionStatus.textContent = '连接已断开';
            connectionStatus.style.color = '#ff0000';
        };

        ws.onerror = function() {
            connectionStatus.textContent = '连接错误';
            connectionStatus.style.color = '#ff0000';
        };

        ws.onmessage = function(event) {
            try {
                const data = JSON.parse(event.data);
                console.log('收到消息:', data);

                if (data.type === 'LogEntry') {
                    addLogEntry(data.data);
                } else if (data.type === 'SystemStatus') {
                    console.log('系统状态:', data.data);
                }
            } catch (e) {
                console.error('解析消息失败:', e);
            }
        };

        function addLogEntry(logData) {
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry';

            const level = logData.level || 'INFO';
            const timestamp = new Date(logData.timestamp).toLocaleTimeString();
            const target = logData.target || 'unknown';
            const message = logData.message || '';

            logEntry.className += ' ' + level.toLowerCase();
            logEntry.innerHTML = `<span style="color: #666;">[${timestamp}]</span> <span style="color: inherit;">[${level}]</span> <span style="color: #ccc;">${target}: ${message}</span>`;

            logContainer.insertBefore(logEntry, logContainer.firstChild);

            // 限制显示的日志数量
            while (logContainer.children.length > 100) {
                logContainer.removeChild(logContainer.lastChild);
            }
        }
    </script>
</body>
</html>"#.to_string())
        }
    }
}

/// 日志传输方式
pub enum LogTransport {
    WebSocket(Arc<WebSocketLogManager>),
    NamedPipe(Arc<NamedPipeLogManager>),
}

/// 通用事件输出器 - 只支持WebSocket传输
pub struct UniversalEventSender {
    transport: LogTransport,
}

impl UniversalEventSender {
    /// 创建使用WebSocket的事件发送器
    pub fn new_websocket(web_port: u16) -> Self {
        let manager = Arc::new(WebSocketLogManager::new(web_port));
        manager.start_websocket_server();

        Self {
            transport: LogTransport::WebSocket(manager)
        }
    }

    /// 创建使用命名管道的事件发送器
    pub fn new_named_pipe(pipe_name: String) -> Self {
        let manager = Arc::new(NamedPipeLogManager::new(pipe_name.clone()));
        // 不在这里启动连接任务，延迟到第一次使用时启动

        Self {
            transport: LogTransport::NamedPipe(manager)
        }
    }

    /// 发送日志
    fn send_log(&self, log_line: String) {
        match &self.transport {
            LogTransport::WebSocket(manager) => manager.send_log(log_line),
            LogTransport::NamedPipe(manager) => {
                // 确保连接任务已启动（如果在Tokio运行时中）
                if tokio::runtime::Handle::try_current().is_ok() {
                    manager.start_connection_task();
                }

                let manager = manager.clone();
                let log_line = log_line.clone();
                // 使用 Handle::try_current() 检查是否在Tokio运行时中
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        manager.send_log(log_line).await;
                    });
                } else {
                    // 如果不在Tokio运行时中，使用阻塞方式发送
                    // 这种情况下我们需要创建一个简单的运行时来处理
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async move {
                            manager.send_log(log_line).await;
                        });
                    });
                }
            }
        }
    }
}

/// 命名管道事件输出器 - 向后兼容
pub type NamedPipeEventSender = UniversalEventSender;

impl EventSender for UniversalEventSender {
    fn send_validation_event(&self, _event: ValidationEvent) {
        // 禁用验证事件输出
        // if let Ok(json) = serde_json::to_string(&event) {
        //     let log_line = format!("VALIDATION_EVENT: {}", json);
        //     // 发送到命名管道
        //     self.send_log(log_line.clone());
        //     // 同时输出到控制台（可配置）
        //     println!("{}", log_line);
        // }
    }

    fn send_performance_event(&self, event: PerformanceEvent) {
        if let Ok(json) = serde_json::to_string(&event) {
            let log_line = format!("PERFORMANCE_EVENT: {}", json);

            // 发送到命名管道
            self.send_log(log_line.clone());

            // 同时输出到控制台（可配置）
            info!(target: "SystemObservability", "性能事件: {}", json);
        }
    }
}

impl Clone for UniversalEventSender {
    fn clone(&self) -> Self {
        Self {
            transport: match &self.transport {
                LogTransport::WebSocket(manager) => LogTransport::WebSocket(manager.clone()),
                LogTransport::NamedPipe(manager) => LogTransport::NamedPipe(manager.clone()),
            }
        }
    }
}

/// 通用日志转发层 - 将所有tracing日志转发到指定传输方式
pub struct UniversalLogForwardingLayer {
    sender: UniversalEventSender,
}

impl UniversalLogForwardingLayer {
    /// 创建命名管道日志转发层
    pub fn new_named_pipe(pipe_name: String) -> Self {
        Self {
            sender: UniversalEventSender::new_named_pipe(pipe_name),
        }
    }

    /// 创建WebSocket日志转发层
    pub fn new_websocket(web_port: u16) -> Self {
        Self {
            sender: UniversalEventSender::new_websocket(web_port),
        }
    }
}

/// 命名管道日志转发层 - 向后兼容
pub type NamedPipeLogForwardingLayer = UniversalLogForwardingLayer;

impl NamedPipeLogForwardingLayer {
    pub fn new(pipe_name: String) -> Self {
        Self::new_named_pipe(pipe_name)
    }
}

// 向后兼容的别名
pub type WebSocketEventSender = UniversalEventSender;
pub type WebSocketLogForwardingLayer = UniversalLogForwardingLayer;

impl WebSocketLogForwardingLayer {
    /// 创建WebSocket日志转发层（恢复原有功能）
    pub fn new_websocket_compat(web_port: u16) -> Self {
        UniversalLogForwardingLayer::new_websocket(web_port)
    }
}

impl<S> Layer<S> for UniversalLogForwardingLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // 格式化事件为JSON格式，遵循WebLog日志规范
        let metadata = event.metadata();
        let level = metadata.level().to_string().to_uppercase();
        let target = metadata.target();

        // 提取字段
        let mut fields = HashMap::new();
        let mut visitor = FieldVisitor::new(&mut fields);
        event.record(&mut visitor);

        // 提取消息
        let message = fields.remove("message")
            .map(|v| match v {
                serde_json::Value::String(s) => s,
                _ => v.to_string().trim_matches('"').to_string(),
            })
            .unwrap_or_else(|| "".to_string());

        // 获取span信息
        let current_span = ctx.lookup_current();
        let (span_id, trace_id) = if let Some(span) = current_span {
            let span_id = format!("span_{}", span.id().into_u64());
            // 简化的trace_id生成，实际应用中可能需要更复杂的逻辑
            let trace_id = format!("trace_{}", span.id().into_u64());
            (Some(span_id), Some(trace_id))
        } else {
            (None, None)
        };

        // 构建JSON日志对象
        let mut log_obj = serde_json::json!({
            "timestamp": chrono::DateTime::<chrono::Utc>::from(SystemTime::now())
                .format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
            "level": level,
            "target": target,
            "message": message
        });

        // 添加结构化字段
        if !fields.is_empty() {
            log_obj["fields"] = serde_json::Value::Object(
                fields.into_iter().collect()
            );
        }

        // 添加span信息
        if span_id.is_some() || trace_id.is_some() {
            let mut span_obj = serde_json::Map::new();
            if let Some(id) = span_id {
                span_obj.insert("id".to_string(), serde_json::Value::String(id));
            }
            if let Some(trace) = trace_id {
                span_obj.insert("trace_id".to_string(), serde_json::Value::String(trace));
            }
            log_obj["span"] = serde_json::Value::Object(span_obj);
        }

        // 序列化为JSON字符串
        if let Ok(log_line) = serde_json::to_string(&log_obj) {
            self.sender.send_log(log_line);
        }
    }
}

impl SpecValidationLayer {
    /// 创建新的规格验证层
    pub fn new(event_sender: Arc<dyn EventSender + Send + Sync>) -> Self {
        let mut layer = Self {
            rules: Arc::new(Mutex::new(HashMap::new())),
            span_data: Arc::new(Mutex::new(HashMap::new())),
            event_sender,
        };
        
        // 注册默认验证规则
        layer.register_default_rules();
        layer
    }

    /// 注册验证规则
    pub fn register_rule(&self, rule: ValidationRule) {
        if let Ok(mut rules) = self.rules.lock() {
            rules.insert(rule.id.clone(), rule);
        }
    }

    /// 注册默认验证规则
    fn register_default_rules(&mut self) {
        // 这里将在后续实现具体的验证规则
    }

    /// 从事件中提取验证上下文
    fn extract_validation_context<S>(&self, event: &Event, _ctx: &Context<S>) -> Option<ValidationContext>
    where
        S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
    {
        let metadata = event.metadata();
        let module = metadata.target().split("::").nth(2).unwrap_or("unknown").to_string();
        
        let mut fields = HashMap::new();
        let mut visitor = FieldVisitor::new(&mut fields);
        event.record(&mut visitor);

        Some(ValidationContext {
            module,
            operation: metadata.name().to_string(),
            fields,
            span_data: None,
        })
    }

    /// 应用验证规则
    fn apply_validation_rules(&self, _context: &ValidationContext) {
        // 禁用验证规则应用
        // if let Ok(rules) = self.rules.lock() {
        //     for (rule_id, rule) in rules.iter() {
        //         let result = (rule.validator)(context);
        //
        //         let event = ValidationEvent {
        //             timestamp: SystemTime::now()
        //                 .duration_since(UNIX_EPOCH)
        //                 .unwrap_or_default()
        //                 .as_millis() as u64,
        //             validation_rule_id: rule_id.clone(),
        //             module: context.module.clone(),
        //             status: result.status,
        //             context: result.context,
        //             message: result.message,
        //         };
        //
        //         self.event_sender.send_validation_event(event);
        //     }
        // }
    }
}

/// 字段访问器，用于提取事件字段
struct FieldVisitor<'a> {
    fields: &'a mut HashMap<String, serde_json::Value>,
}

impl<'a> FieldVisitor<'a> {
    fn new(fields: &'a mut HashMap<String, serde_json::Value>) -> Self {
        Self { fields }
    }
}

impl<'a> tracing::field::Visit for FieldVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(format!("{:?}", value)),
        );
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(num) = serde_json::Number::from_f64(value) {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::Number(num),
            );
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Bool(value),
        );
    }
}

impl<S> Layer<S> for SpecValidationLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
        let _metadata = attrs.metadata();
        let mut fields = HashMap::new();
        let mut visitor = FieldVisitor::new(&mut fields);
        attrs.record(&mut visitor);

        let span_data = SpanPerformanceData {
            start_time: Instant::now(),
            metadata: fields.into_iter()
                .map(|(k, v)| (k, v.to_string()))
                .collect(),
        };

        if let Ok(mut data) = self.span_data.lock() {
            data.insert(id.clone(), span_data);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if let Some(validation_context) = self.extract_validation_context(event, &ctx) {
            self.apply_validation_rules(&validation_context);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Ok(mut data) = self.span_data.lock() {
            if let Some(span_data) = data.remove(&id) {
                let duration = span_data.start_time.elapsed();

                if let Some(span) = ctx.span(&id) {
                    let metadata = span.metadata();

                    let performance_event = PerformanceEvent {
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                        event_type: "PerformanceMetric".to_string(),
                        span_name: metadata.name().to_string(),
                        duration_ms: duration.as_secs_f64() * 1000.0,
                        context: serde_json::to_value(&span_data.metadata).unwrap_or_default(),
                    };

                    self.event_sender.send_performance_event(performance_event);
                }
            }
        }
    }
}

/// 验证规则ID常量
pub mod validation_rules {
    pub const SYMBOL_INDEX_STABILITY: &str = "SYMBOL_INDEX_STABILITY";
    pub const KLINE_OPEN_TIME_ACCURACY: &str = "KLINE_OPEN_TIME_ACCURACY";
    pub const KLINE_IS_FINAL_CORRECTNESS: &str = "KLINE_IS_FINAL_CORRECTNESS";
    pub const EMPTY_KLINE_HANDLING: &str = "EMPTY_KLINE_HANDLING";
    pub const BUFFER_SWAP_NOTIFICATION: &str = "BUFFER_SWAP_NOTIFICATION";
    pub const PERSISTENCE_UPSERT_LOGIC: &str = "PERSISTENCE_UPSERT_LOGIC";
    pub const DATA_FLOW_INTEGRITY: &str = "DATA_FLOW_INTEGRITY";
}
