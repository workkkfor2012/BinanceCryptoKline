//! Web服务器模块 - 简化版
//!
//! 提供基于Axum的Web服务器，专注于：
//! - WebSocket实时数据推送（历史数据 + 增量更新）
//! - 静态文件服务

use crate::types::*;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tower_http::services::ServeDir;

/// 创建Web应用路由 - 极简版本
pub fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        // WebSocket端点
        .route("/ws", get(websocket_handler))
        // 主页
        .route("/", get(index_handler))
        // 静态文件服务
        .nest_service("/static", ServeDir::new("src/weblog/static").fallback(ServeDir::new("static")))
        .with_state(state)
}

/// WebSocket处理器
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| websocket_connection(socket, state))
}

/// WebSocket连接处理 - 两阶段模式：历史数据 + 实时数据
async fn websocket_connection(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();

    // === 阶段1：发送历史日志 ===
    tracing::info!("开始发送历史日志");

    // 获取历史日志（快速克隆以释放锁）
    let history_logs = state.get_history_logs();
    let history_count = history_logs.len();

    // 逐条发送历史日志
    for log_entry in history_logs {
        if send_log_entry(&mut sender, log_entry).await.is_err() {
            tracing::warn!("客户端在接收历史数据时断开连接");
            return;
        }
    }

    // 发送历史数据完成信令
    if send_history_complete(&mut sender).await.is_err() {
        tracing::warn!("发送历史完成信令失败");
        return;
    }

    tracing::info!("历史日志发送完成，共 {} 条", history_count);

    // === 阶段2：实时转发新日志 ===
    tracing::info!("开始实时日志转发");

    let mut log_receiver = state.log_sender.subscribe();

    loop {
        tokio::select! {
            // 接收并转发新日志
            log_result = log_receiver.recv() => {
                match log_result {
                    Ok(log_entry) => {
                        if send_log_entry(&mut sender, log_entry).await.is_err() {
                            tracing::info!("客户端断开连接");
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("WebSocket客户端处理速度过慢，跳过了 {} 条日志", skipped);
                        // 继续处理，不断开连接
                    }
                    Err(_) => {
                        tracing::error!("日志接收器错误");
                        break;
                    }
                }
            }

            // 监听客户端断开
            _ = receiver.next() => {
                tracing::info!("客户端主动断开连接");
                break;
            }
        }
    }

    tracing::info!("WebSocket连接已关闭");
}

/// 发送日志条目到WebSocket客户端
async fn send_log_entry(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    log_entry: LogEntry,
) -> Result<(), axum::Error> {
    let message = WebSocketMessage::LogEntry { data: log_entry };

    match serde_json::to_string(&message) {
        Ok(json) => {
            sender.send(Message::Text(json)).await.map_err(|_| axum::Error::new("发送失败"))
        }
        Err(e) => {
            tracing::error!("序列化日志条目失败: {}", e);
            Err(axum::Error::new("序列化失败"))
        }
    }
}

/// 发送历史数据完成信令
async fn send_history_complete(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Result<(), axum::Error> {
    let message = WebSocketMessage::HistoryComplete;

    match serde_json::to_string(&message) {
        Ok(json) => {
            sender.send(Message::Text(json)).await.map_err(|_| axum::Error::new("发送失败"))
        }
        Err(e) => {
            tracing::error!("序列化历史完成信令失败: {}", e);
            Err(axum::Error::new("序列化失败"))
        }
    }
}

/// 主页处理器
async fn index_handler() -> impl IntoResponse {
    // 运行时读取静态文件，支持热更新
    match tokio::fs::read_to_string("src/weblog/static/index.html").await {
        Ok(content) => Html(content).into_response(),
        Err(_) => {
            // 如果读取失败，尝试相对路径
            match tokio::fs::read_to_string("static/index.html").await {
                Ok(content) => Html(content).into_response(),
                Err(e) => {
                    tracing::error!("无法读取index.html文件: {}", e);
                    Html("<html><body><h1>Error: 无法加载页面</h1><p>请确保index.html文件存在</p></body></html>".to_string()).into_response()
                }
            }
        }
    }
}