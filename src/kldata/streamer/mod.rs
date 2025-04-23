// 导出WebSocket相关模块
mod config;
mod connection;
mod message;

use crate::klcommon::{Database, Result};
use log::{info, error};
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use futures_util::future::join_all;

// 重新导出
pub use config::{ContinuousKlineConfig, MAX_STREAMS_PER_CONNECTION};
use connection::ConnectionManager;
use message::process_messages;

/// WebSocket连接状态
#[derive(Debug, Clone)]
pub struct WebSocketConnection {
    pub id: usize,
    pub streams: Vec<String>,
    pub status: String,
    pub message_count: usize,
}

/// 连续合约K线客户端
pub struct ContinuousKlineClient {
    config: ContinuousKlineConfig,
    db: Arc<Database>,
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
}

impl ContinuousKlineClient {
    /// 创建新的连续合约K线客户端
    pub fn new(config: ContinuousKlineConfig, db: Arc<Database>) -> Self {
        Self {
            config,
            db,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
        }
    }

    /// 启动客户端
    pub async fn start(&mut self) -> Result<()> {
        info!("启动连续合约K线客户端");
        info!("使用代理: {}", self.config.use_proxy);

        if self.config.use_proxy {
            info!("代理地址: {}:{}", self.config.proxy_addr, self.config.proxy_port);
        }

        // 确保日志目录存在
        let log_dir = Path::new("logs");
        if !log_dir.exists() {
            create_dir_all(log_dir)?;
        }

        // 创建连接管理器
        let connection_manager = ConnectionManager::new(
            self.config.use_proxy,
            self.config.proxy_addr.clone(),
            self.config.proxy_port,
        );

        // 创建消息通道
        let (tx, rx) = mpsc::channel(1000);

        // 获取所有交易对和周期的组合
        let mut streams = Vec::new();

        for symbol in &self.config.symbols {
            for interval in &self.config.intervals {
                // 使用连续合约K线格式
                let stream = format!("{}@continuousKline_{}", symbol.to_lowercase(), interval);
                streams.push(stream);
            }
        }

        info!("总共 {} 个流需要订阅", streams.len());

        // 计算需要的连接数
        let connection_count = (streams.len() + MAX_STREAMS_PER_CONNECTION - 1) / MAX_STREAMS_PER_CONNECTION;
        info!("需要 {} 个WebSocket连接", connection_count);

        // 分配流到连接
        let mut connection_streams = Vec::new();

        for chunk in streams.chunks(MAX_STREAMS_PER_CONNECTION) {
            connection_streams.push(chunk.to_vec());
        }

        // 启动消息处理器
        let db_clone = self.db.clone();
        let connections_clone = self.connections.clone();

        let message_handler = tokio::spawn(async move {
            process_messages(rx, db_clone, connections_clone).await;
        });

        // 启动所有连接
        let mut connection_handles = Vec::new();

        for streams in connection_streams {
            let connection_id = self.connection_id_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let tx_clone = tx.clone();
            let connection_manager_clone = connection_manager.clone();
            let connections_clone = self.connections.clone();

            // 更新连接状态
            {
                let mut connections = connections_clone.lock().await;
                connections.insert(connection_id, WebSocketConnection {
                    id: connection_id,
                    streams: streams.clone(),
                    status: "初始化".to_string(),
                    message_count: 0,
                });
            }

            // 启动连接
            let handle = tokio::spawn(async move {
                // 更新状态
                {
                    let mut connections = connections_clone.lock().await;
                    if let Some(conn) = connections.get_mut(&connection_id) {
                        conn.status = "连接中".to_string();
                    }
                }

                // 建立连接
                match connection_manager_clone.connect(&streams).await {
                    Ok(mut ws_stream) => {
                        // 更新状态
                        {
                            let mut connections = connections_clone.lock().await;
                            if let Some(conn) = connections.get_mut(&connection_id) {
                                conn.status = "已连接".to_string();
                            }
                        }

                        info!("连接 {} 已建立，订阅 {} 个流", connection_id, streams.len());

                        // 处理消息
                        connection_manager_clone.handle_messages(connection_id, &mut ws_stream, tx_clone, connections_clone).await;
                    }
                    Err(e) => {
                        // 更新状态
                        {
                            let mut connections = connections_clone.lock().await;
                            if let Some(conn) = connections.get_mut(&connection_id) {
                                conn.status = format!("连接失败: {}", e);
                            }
                        }

                        error!("连接 {} 失败: {}", connection_id, e);
                    }
                }
            });

            connection_handles.push(handle);
        }

        // 等待所有连接完成
        join_all(connection_handles).await;

        // 等待消息处理器完成
        if let Err(e) = message_handler.await {
            error!("消息处理器错误: {}", e);
        }

        info!("连续合约K线客户端已停止");
        Ok(())
    }
}
