//! Web服务器模块
//!
//! 提供基于Axum的Web服务器，包括：
//! - WebSocket实时数据推送
//! - RESTful API接口
//! - 静态文件服务
//! - tracing日志处理和可视化

use crate::types::*;
use crate::log_parser::*;
use std::collections::HashMap;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tower_http::services::ServeDir;

/// 创建Web应用路由
pub fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        // WebSocket端点
        .route("/ws", get(websocket_handler))
        // API端点
        .route("/api/log", post(log_api_handler))
        .route("/api/status", get(status_api_handler))
        .route("/api/traces", get(traces_api_handler))
        .route("/api/trace/:trace_id", get(trace_detail_api_handler))
        .route("/api/targets", get(targets_api_handler))
        .route("/api/dashboard", get(dashboard_api_handler))
        .route("/api/module/:module_name/history", get(module_history_api_handler))
        // 页面路由 - 所有路径都指向模块监控页面
        .route("/", get(index_handler))
        .route("/modules", get(index_handler))
        .route("/trace", get(index_handler))
        .route("/logs", get(index_handler))
        // 静态文件服务 - 支持多个路径
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

/// WebSocket连接处理
async fn websocket_connection(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut log_receiver = state.log_sender.subscribe();

    // 只发送聚合仪表板数据（统一使用新格式）
    send_dashboard_data(&mut sender, &state).await;

    // 移除旧的SystemStatus发送逻辑，统一使用DashboardUpdate

    // 启动定期发送系统状态的任务（只发送状态信息，不发送日志数据）
    let (status_tx, mut status_rx) = tokio::sync::mpsc::channel::<crate::types::SystemStatus>(100);
    let state_clone = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10)); // 每10秒更新一次系统状态
        loop {
            interval.tick().await;
            let system_status = state_clone.get_system_status();
            if status_tx.send(system_status).await.is_err() {
                break;
            }
        }
    });

    // 启动定期发送仪表板数据的任务（用于RawLogSnapshot的定期更新）
    let (dashboard_tx, mut dashboard_rx) = tokio::sync::mpsc::channel::<()>(10);
    let state_clone2 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5)); // 每5秒更新一次仪表板数据
        loop {
            interval.tick().await;
            if dashboard_tx.send(()).await.is_err() {
                break;
            }
        }
    });

    // 处理消息循环
    tokio::select! {
        // 处理来自客户端的消息
        _ = async {
            while let Some(msg) = receiver.next().await {
                if let Ok(Message::Text(_)) = msg {
                    // 处理客户端消息（如果需要）
                }
            }
        } => {},

        // 处理日志广播和状态更新
        _ = async {
            loop {
                tokio::select! {
                    // 处理日志条目 - 立即增量转发
                    log_result = log_receiver.recv() => {
                        if let Ok(log_entry) = log_result {
                            // 立即处理日志到聚合器，检查是否被聚合
                            let was_aggregated = state.module_aggregator_manager.process_log_entry(log_entry.clone()).await;

                            // 只有未被聚合的日志才转发给前端
                            if !was_aggregated {
                                let message = WebSocketMessage::LogEntry { data: log_entry };
                                if let Ok(json) = serde_json::to_string(&message) {
                                    if sender.send(Message::Text(json)).await.is_err() {
                                        break;
                                    }
                                }
                            }

                            // 移除频繁的全量推送，让RawLogSnapshot通过定期更新获得数据
                            // send_dashboard_data(&mut sender, &state).await;
                        }
                    }

                    // 处理系统状态更新（低频）
                    status_result = status_rx.recv() => {
                        if let Some(system_status) = status_result {
                            let message = WebSocketMessage::SystemStatus { data: system_status };
                            if let Ok(json) = serde_json::to_string(&message) {
                                if sender.send(Message::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }

                    // 处理定期仪表板数据更新（用于RawLogSnapshot等聚合数据的更新）
                    dashboard_result = dashboard_rx.recv() => {
                        if dashboard_result.is_some() {
                            send_dashboard_data(&mut sender, &state).await;
                        }
                    }
                }
            }
        } => {},
    }
}

/// 日志API处理器 - 接收外部日志（已禁用，只支持命名管道）
async fn log_api_handler(
    State(_state): State<Arc<AppState>>,
    _body: String,
) -> impl IntoResponse {
    // 不再支持HTTP API接收日志，只支持命名管道
    StatusCode::METHOD_NOT_ALLOWED
}

/// 状态API处理器
async fn status_api_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match get_log_stats(&state).await {
        Ok(stats) => Json(stats).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Traces API处理器
async fn traces_api_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let trace_manager = state.trace_manager.lock().unwrap();
    let traces = trace_manager.get_traces();
    Json(traces)
}

/// Trace详情API处理器
async fn trace_detail_api_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(trace_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let trace_manager = state.trace_manager.lock().unwrap();
    match trace_manager.get_trace(&trace_id) {
        Some(trace) => Json(trace).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// 目标API处理器
async fn targets_api_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let target_stats = state.target_stats.lock().unwrap();
    let targets: Vec<String> = target_stats.keys().cloned().collect();
    Json(targets)
}

/// 仪表板数据API处理器
async fn dashboard_api_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().unwrap_or_default().as_secs();
    let health_score = 95; // 简化的健康分数

    let dashboard_data = state.module_aggregator_manager
        .get_dashboard_data(uptime, health_score).await;

    Json(dashboard_data)
}

/// 模块历史数据API处理器
async fn module_history_api_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(module_name): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    use serde_json::json;

    // 解析查询参数
    let page: usize = params.get("page").and_then(|p| p.parse().ok()).unwrap_or(0);
    let page_size: usize = params.get("page_size").and_then(|p| p.parse().ok()).unwrap_or(20);

    // 获取模块数据
    match state.module_aggregator_manager.get_module_data(&module_name).await {
        Some(module_data) => {
            let total_logs = module_data.display_logs.len();
            let start_index = page * page_size;
            let end_index = std::cmp::min(start_index + page_size, total_logs);

            if start_index >= total_logs {
                // 请求的页面超出范围
                return Json(json!({
                    "logs": [],
                    "total_count": total_logs,
                    "page": page,
                    "page_size": page_size,
                    "has_more": false
                })).into_response();
            }

            // 获取指定页面的日志（按时间倒序，最新的在前）
            let page_logs: Vec<_> = module_data.display_logs
                .iter()
                .skip(start_index)
                .take(page_size)
                .cloned()
                .collect();

            Json(json!({
                "logs": page_logs,
                "total_count": total_logs,
                "page": page,
                "page_size": page_size,
                "has_more": end_index < total_logs
            })).into_response()
        }
        None => {
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// 模块监控页面处理器 - 统一的index.html处理器
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

// 移除旧的send_system_status函数，统一使用send_dashboard_data

/// 发送仪表板数据到WebSocket客户端
async fn send_dashboard_data(
    sender: &mut futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>,
    state: &Arc<AppState>,
) {
    let uptime = state.start_time.elapsed().unwrap_or_default().as_secs();
    let health_score = 95; // 简化的健康分数

    let dashboard_data = state.module_aggregator_manager
        .get_dashboard_data(uptime, health_score).await;

    let message = WebSocketMessage::DashboardUpdate { data: dashboard_data };

    if let Ok(json) = serde_json::to_string(&message) {
        let _ = sender.send(axum::extract::ws::Message::Text(json)).await;
    }
}

/// 获取日志统计信息
async fn get_log_stats(state: &Arc<AppState>) -> Result<LogStats, Box<dyn std::error::Error + Send + Sync>> {
    let uptime = state.start_time.elapsed()?.as_secs();

    let mut log_stats = state.log_stats.lock().unwrap().clone();
    log_stats.uptime_seconds = uptime;

    // 更新Trace统计
    {
        let trace_manager = state.trace_manager.lock().unwrap();
        let trace_stats = trace_manager.get_stats();
        log_stats.trace_stats = TraceStats {
            total_traces: trace_stats.total_traces,
            active_traces: trace_stats.active_traces as u64,
            completed_traces: trace_stats.completed_traces,
            total_spans: trace_stats.total_spans,
            avg_trace_duration_ms: trace_stats.avg_trace_duration_ms,
            max_trace_duration_ms: trace_stats.max_trace_duration_ms,
            avg_span_duration_ms: 0.0, // TODO: 计算平均Span持续时间
            spans_by_level: HashMap::new(), // TODO: 统计Span级别分布
        };
    }

    // 计算日志速率
    if uptime > 0 {
        log_stats.logs_per_second = log_stats.total_logs as f64 / uptime as f64;
    }

    Ok(log_stats)
}


