//! 业务追踪日志层
//!
//! 专门用于处理 `log_type="transaction"` 的事件，
//! 生成以业务ID为核心的、扁平化的事件流，保存到 logs/transaction_log 文件夹。

// ✨ [修改] 导入 Tokio 的异步 I/O 工具
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter as TokioBufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use std::path::Path;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

/// 业务追踪日志层
pub struct TransactionLayer {
    // ✨ [修复] 不再直接持有writer，而是持有channel的发送端
    log_sender: mpsc::UnboundedSender<String>,
}

/// 业务追踪日志管理器，用于优雅关闭
pub struct TransactionLogManager {
    // ✨ [修复] 持有后台任务的句柄，用于优雅关闭
    shutdown_handle: Option<JoinHandle<()>>,
    // ✨ [新增] 用于关闭的发送端副本
    shutdown_sender: Option<mpsc::UnboundedSender<String>>,
}

impl TransactionLayer {
    /// 创建一个新的业务追踪日志层，返回层和管理器
    pub fn new() -> std::io::Result<(Self, TransactionLogManager)> {
        // 确保日志目录存在
        let log_dir = Path::new("logs/transaction_log");
        if !log_dir.exists() {
            std::fs::create_dir_all(log_dir)?;
        }

        // 创建日志文件，使用当前时间戳命名
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let log_file_path = log_dir.join(format!("transaction_{}.log", timestamp));

        // ✨ [修改] 仍然使用标准库同步地打开/创建文件，这是安全的，因为它只在初始化时执行一次
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path)?;

        // ✨ [新增] 创建一个无界MPSC Channel
        let (log_sender, mut log_receiver) = mpsc::unbounded_channel();

        // ✨ [修改] 启动唯一的、长生命周期的后台消费者任务
        let shutdown_handle = tokio::spawn(async move {
            // ✨ [核心修复] 将 std::fs::File 转换为 tokio::fs::File
            let file = TokioFile::from_std(file);
            // ✨ [核心修复] 使用 Tokio 的异步 BufWriter
            let mut writer = TokioBufWriter::new(file);

            // 从channel接收日志，直到channel被关闭
            while let Some(log_line) = log_receiver.recv().await {
                let line_with_newline = format!("{}\n", log_line);

                // ✨ [核心修复] 使用异步的 .write_all().await
                if writer.write_all(line_with_newline.as_bytes()).await.is_err() {
                    eprintln!("Error writing to transaction log file.");
                    break;
                }
            }

            // ✨ [核心修复] 确保所有缓冲的日志都被异步地写入文件
            let _ = writer.flush().await;
        });

        let layer = Self {
            log_sender: log_sender.clone(),
        };

        let manager = TransactionLogManager {
            shutdown_handle: Some(shutdown_handle),
            shutdown_sender: Some(log_sender),
        };

        Ok((layer, manager))
    }
}

impl TransactionLogManager {
    /// ✨ [新增] 优雅关闭方法
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.shutdown_handle.take() {
            // 关闭发送端，使后台任务的recv()循环结束
            if let Some(sender) = self.shutdown_sender.take() {
                drop(sender); // 关闭channel
            }
            // 等待后台任务完成所有写入操作
            let _ = handle.await;
        }
    }
}

impl<S> Layer<S> for TransactionLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = serde_json::Map::new();
        // 复用已有的JsonVisitor来提取字段
        let mut visitor = crate::klcommon::log::trace_visualization::JsonVisitor::new(&mut fields);
        event.record(&mut visitor);

        // [关键逻辑] 只处理明确标记为 log_type = "transaction" 的事件
        if fields.get("log_type").and_then(|v| v.as_str()) != Some("transaction") {
            return;
        }

        let metadata = event.metadata();

        // 保留从 event 中提取的所有字段，包括 transaction_id, event_name 等
        // 如果有 "message" 字段，它也会被保留在 fields 中

        // [关键逻辑] 构建日志对象，target 使用 metadata 的原始 target
        let transaction_log_obj = serde_json::json!({
            "log_type": "transaction",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": metadata.level().to_string(),
            "target": metadata.target(), // <-- 使用原始的模块路径作为target
            "fields": fields,
        });

        // ✨ [修复] 不再直接写入文件，而是将日志发送到channel
        if let Ok(log_line) = serde_json::to_string(&transaction_log_obj) {
            // send 是非阻塞的，速度极快
            let _ = self.log_sender.send(log_line);
        }
    }
}