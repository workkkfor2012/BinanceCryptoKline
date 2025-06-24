//! 可观察性和规格验证模块
//!
//! 提供基于tracing的规格验证层、性能监控功能和命名管道日志管理

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{Event, Id, Subscriber, info, warn, error, Instrument};
use tracing_subscriber::{layer::Context, Layer};
use tokio::sync::mpsc;
use tokio::io::{AsyncWriteExt, BufWriter};

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

/// 命名管道日志管理器 - 基于MPSC Channel重构
/// 负责将日志高效、安全地发送到命名管道。
#[derive(Clone)]
pub struct NamedPipeLogManager {
    log_sender: mpsc::UnboundedSender<String>,
}

impl NamedPipeLogManager {
    /// 创建新的命名管道日志管理器。
    /// 注意：后台任务将在第一次调用send_log时自动启动。
    pub fn new(pipe_name: String) -> Self {
        // 1. 创建一个无界 MPSC channel。
        // log_sender 可以被安全地克隆并分发给多个生产者（Layer）。
        // log_receiver 是唯一的，将被移动到消费者任务中。
        let (log_sender, log_receiver) = mpsc::unbounded_channel();

        // 2. 启动一个独立的后台任务来处理所有I/O操作。
        // 这个任务是唯一的"消费者"，负责连接管道和写入日志。
        // 使用try_current来检查是否在tokio运行时中
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(
                Self::connection_and_write_loop(pipe_name, log_receiver)
                    .instrument(tracing::info_span!("named_pipe_consumer_task"))
            );
        } else {
            // 如果不在tokio运行时中，创建一个新的运行时来处理
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(
                    Self::connection_and_write_loop(pipe_name, log_receiver)
                        .instrument(tracing::info_span!("named_pipe_consumer_task"))
                );
            });
        }

        // 3. 返回一个包含发送端的管理器实例。
        Self { log_sender }
    }

    /// 发送日志到队列中（此操作非阻塞且极速）。
    /// 这个方法会被 ModuleLayer 和 TraceVisualizationLayer 高频调用。
    pub fn send_log(&self, log_line: String) {
        // `send` 操作只是将一个指针放入队列，非常快。
        // 如果接收端已关闭（例如任务崩溃），发送会失败，但我们在此处忽略错误。
        let _ = self.log_sender.send(log_line);
    }

    /// 后台任务：管理连接并从channel读取日志进行写入。
    async fn connection_and_write_loop(
        pipe_name: String,
        mut receiver: mpsc::UnboundedReceiver<String>
    ) {
        use tokio::net::windows::named_pipe::ClientOptions;

        // 这个无限循环确保了连接的持久性和自动重连。
        loop {
            // ---- 连接阶段 ----
            info!(target: "SystemObservability", "📡 尝试连接到命名管道: {}", &pipe_name);
            match ClientOptions::new().open(&pipe_name) {
                Ok(client) => {
                    info!(target: "SystemObservability", "✅ 成功连接到命名管道服务器");
                    let mut writer = BufWriter::new(client);

                    // 发送会话开始标记，通知前端一个新的会话开始了。
                    let session_start_marker = Self::create_session_start_marker();
                    if writer.write_all(session_start_marker.as_bytes()).await.is_ok() {
                         let _ = writer.flush().await; // 确保标记被立即发送
                         info!(target: "SystemObservability", "🆕 已发送会话开始标记");
                    }

                    // ---- 写入阶段 ----
                    // 循环从 channel 接收日志并写入管道。
                    // `receiver.recv()` 在没有日志时会异步地等待。
                    while let Some(log_line) = receiver.recv().await {
                        let line_with_newline = format!("{}\n", log_line);

                        // 尝试写入日志。
                        if writer.write_all(line_with_newline.as_bytes()).await.is_err() {
                            error!(target: "SystemObservability", "写入命名管道失败，连接可能已断开，准备重连...");
                            // 跳出内层写入循环，进入外层的重连循环。
                            break;
                        }

                        // 每次写入后都刷新，确保日志低延迟地到达前端。
                        if writer.flush().await.is_err() {
                             error!(target: "SystemObservability", "刷新命名管道失败，连接可能已断开，准备重连...");
                             break;
                        }
                    }
                },
                Err(e) => {
                    warn!(target: "SystemObservability", "❌ 命名管道连接失败: {}. 5秒后重试", e);
                }
            }

            // 如果连接失败或中途写入失败，等待5秒后重试整个循环。
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }

    /// 创建会话开始标记的JSON字符串
    fn create_session_start_marker() -> String {
        let session_start_marker = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
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
        // 在这里直接转换为 String，避免在异步任务中处理 Result
        serde_json::to_string(&session_start_marker).unwrap_or_default() + "\n"
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
