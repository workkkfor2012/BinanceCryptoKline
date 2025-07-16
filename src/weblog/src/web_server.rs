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
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tower_http::services::ServeDir;
use serde::Deserialize;
use chrono;

/// 全局连接计数器
static CONNECTION_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// 订阅类型
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
enum SubscriptionType {
    Module,
    Trace,
}

/// 订阅消息
#[derive(Deserialize, Debug)]
struct SubscriptionMessage {
    subscribe: SubscriptionType,
}

/// 创建Web应用路由 - 极简版本
pub fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        // WebSocket端点
        .route("/ws", get(websocket_handler))
        // 主页
        .route("/", get(index_handler))
        // Trace可视化页面
        .route("/trace", get(trace_handler))
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

/// WebSocket连接处理 - 订阅模式：等待订阅 → 历史数据 → 实时数据
async fn websocket_connection(socket: WebSocket, state: Arc<AppState>) {
    // 分配连接ID并增加计数器
    let connection_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
    let active_connections = CONNECTION_COUNTER.load(Ordering::SeqCst);

    let now = chrono::Utc::now().format("%H:%M:%S%.3f");
    tracing::info!("🔗 [{}] 新WebSocket连接 #{} (当前活跃: {})", now, connection_id, active_connections);

    let (mut sender, mut receiver) = socket.split();

    // === 阶段0.5：等待客户端订阅 ===
    let subscription_type = match receiver.next().await {
        Some(Ok(Message::Text(text))) => {
            match serde_json::from_str::<SubscriptionMessage>(&text) {
                Ok(msg) => {
                    tracing::info!("📡 连接 #{} 订阅类型: {:?}", connection_id, msg.subscribe);
                    msg.subscribe
                }
                Err(e) => {
                    tracing::warn!("连接 #{} 发送无效订阅消息: {}，断开连接", connection_id, e);
                    CONNECTION_COUNTER.fetch_sub(1, Ordering::SeqCst);
                    return;
                }
            }
        }
        _ => {
            tracing::warn!("连接 #{} 未发送订阅消息，断开连接", connection_id);
            CONNECTION_COUNTER.fetch_sub(1, Ordering::SeqCst);
            return;
        }
    };

    // === 阶段1：发送会话开始信令 ===
    let session_id = state.get_session_id();
    if send_session_start(&mut sender, session_id.clone()).await.is_err() {
        tracing::warn!("连接 #{} 发送会话开始信令失败", connection_id);
        CONNECTION_COUNTER.fetch_sub(1, Ordering::SeqCst);
        return;
    }
    tracing::debug!("连接 #{} 已发送会话开始信令: {}", connection_id, session_id);

    // === 阶段2：发送过滤后的历史日志 ===
    tracing::debug!("连接 #{} 开始发送 {:?} 类型的历史日志", connection_id, subscription_type);

    // 获取历史日志并按订阅类型过滤
    let history_logs = state.get_history_logs();
    let mut history_count = 0;

    for log_entry in history_logs {
        let should_send = match subscription_type {
            SubscriptionType::Module => log_entry.log_type.as_deref() != Some("trace"),
            SubscriptionType::Trace => log_entry.log_type.as_deref() == Some("trace"),
        };

        if should_send {
            history_count += 1;
            if send_log_entry(&mut sender, log_entry).await.is_err() {
                tracing::warn!("连接 #{} 客户端在接收历史数据时断开连接", connection_id);
                CONNECTION_COUNTER.fetch_sub(1, Ordering::SeqCst);
                return;
            }
        }
    }

    // 发送历史数据完成信令
    if send_history_complete(&mut sender).await.is_err() {
        tracing::warn!("连接 #{} 发送历史完成信令失败", connection_id);
        CONNECTION_COUNTER.fetch_sub(1, Ordering::SeqCst);
        return;
    }

    tracing::debug!("连接 #{} 历史日志发送完成，共 {} 条", connection_id, history_count);

    // === 阶段3：实时转发过滤后的新日志 + 监听WebSocket控制消息 ===
    tracing::debug!("连接 #{} 开始实时转发 {:?} 类型的日志", connection_id, subscription_type);

    let mut log_receiver = state.log_sender.subscribe();
    let mut websocket_receiver = state.websocket_sender.subscribe();

    loop {
        tokio::select! {
            // 接收并转发新日志
            log_result = log_receiver.recv() => {
                match log_result {
                    Ok(log_entry) => {
                        // 【核心过滤逻辑】根据订阅类型过滤日志
                        let should_send = match subscription_type {
                            SubscriptionType::Module => log_entry.log_type.as_deref() != Some("trace"),
                            SubscriptionType::Trace => log_entry.log_type.as_deref() == Some("trace"),
                        };

                        if should_send {
                            if send_log_entry(&mut sender, log_entry).await.is_err() {
                                tracing::info!("连接 #{} 客户端断开连接", connection_id);
                                break;
                            }
                        }
                        // 如果不应该发送，则静默跳过，不记录日志
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("连接 #{} WebSocket客户端处理速度过慢，跳过了 {} 条日志", connection_id, skipped);
                        // 继续处理，不断开连接
                    }
                    Err(_) => {
                        tracing::error!("连接 #{} 日志接收器错误", connection_id);
                        break;
                    }
                }
            }

            // 接收并转发WebSocket控制消息（如SessionStart）
            websocket_result = websocket_receiver.recv() => {
                match websocket_result {
                    Ok(websocket_message) => {
                        if send_websocket_message(&mut sender, websocket_message).await.is_err() {
                            tracing::info!("连接 #{} 发送WebSocket控制消息失败，客户端断开连接", connection_id);
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("连接 #{} WebSocket控制消息处理速度过慢，跳过了 {} 条消息", connection_id, skipped);
                        // 继续处理，不断开连接
                    }
                    Err(_) => {
                        tracing::error!("连接 #{} WebSocket控制消息接收器错误", connection_id);
                        break;
                    }
                }
            }

            // 监听客户端断开
            _ = receiver.next() => {
                tracing::info!("连接 #{} 客户端主动断开连接", connection_id);
                break;
            }
        }
    }

    // 连接关闭时减少计数器
    CONNECTION_COUNTER.fetch_sub(1, Ordering::SeqCst);
    let remaining_connections = CONNECTION_COUNTER.load(Ordering::SeqCst);
    let now = chrono::Utc::now().format("%H:%M:%S%.3f");
    tracing::info!("🔌 [{}] 连接 #{} 已关闭 (剩余活跃: {})", now, connection_id, remaining_connections);
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

/// 发送会话开始信令
async fn send_session_start(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    session_id: String,
) -> Result<(), axum::Error> {
    let message = WebSocketMessage::SessionStart { session_id };

    match serde_json::to_string(&message) {
        Ok(json) => {
            sender.send(Message::Text(json)).await.map_err(|_| axum::Error::new("发送失败"))
        }
        Err(e) => {
            tracing::error!("序列化会话开始信令失败: {}", e);
            Err(axum::Error::new("序列化失败"))
        }
    }
}

/// 发送WebSocket控制消息（通用方法）
async fn send_websocket_message(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    websocket_message: WebSocketMessage,
) -> Result<(), axum::Error> {
    match serde_json::to_string(&websocket_message) {
        Ok(json) => {
            sender.send(Message::Text(json)).await.map_err(|_| axum::Error::new("发送失败"))
        }
        Err(e) => {
            tracing::error!("序列化WebSocket控制消息失败: {}", e);
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

/// Trace可视化页面处理器
async fn trace_handler() -> impl IntoResponse {
    // 运行时读取静态文件，支持热更新
    match tokio::fs::read_to_string("src/weblog/static/trace_viewer.html").await {
        Ok(content) => Html(content).into_response(),
        Err(_) => {
            // 如果读取失败，尝试相对路径
            match tokio::fs::read_to_string("static/trace_viewer.html").await {
                Ok(content) => Html(content).into_response(),
                Err(e) => {
                    tracing::error!("无法读取trace_viewer.html文件: {}", e);
                    Html("<html><body><h1>Error: 无法加载Trace页面</h1><p>请确保trace_viewer.html文件存在</p></body></html>".to_string()).into_response()
                }
            }
        }
    }
}