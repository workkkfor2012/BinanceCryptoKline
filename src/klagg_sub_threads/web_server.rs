//! 可视化测试Web服务器模块
//!
//! 包含一个基于Axum的Web服务器，用于通过WebSocket实时推送K线数据。
//! 这个模块是自包含的，外部只需要调用 `run_visual_test_server` 即可启动。

use crate::{
    klagg_sub_threads::{DeltaBatch, KlineData},
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, get_service},
    Router,
};
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, RwLock};
use tower_http::services::ServeDir;
use tracing::{error, info, instrument, trace, warn};

// --- 公开API ---

/// 启动可视化测试服务器。这是从外部调用此模块的唯一入口点。
#[instrument(target = "WebServer", skip_all)]
pub async fn run_visual_test_server(
    klines_watch_rx: watch::Receiver<Arc<DeltaBatch>>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let app_state = Arc::new(AppState {
        klines_watch_rx,
        index_to_symbol,
        periods,
        subscriptions: Arc::new(DashMap::new()),
    });

    // 启动后台数据推送任务
    tokio::spawn(pusher_task(app_state.clone(), shutdown_rx));

    // 配置并运行Axum服务器
    let static_dir = std::env::current_dir()
        .unwrap()
        .join("src")
        .join("klagg_sub_threads")
        .join("static");

    info!(target: "WebServer", static_path = ?static_dir, "静态文件目录");

    let app = Router::new()
        .route("/ws", get(ws_handler))
        // 所有其他请求都由静态文件服务处理, 它会从模块内部的 "static" 目录查找文件
        .fallback_service(get_service(ServeDir::new(static_dir)))
        .with_state(app_state);

    let listener = match tokio::net::TcpListener::bind("0.0.0.0:9090").await {
        Ok(l) => l,
        Err(e) => {
            error!(target: "WebServer", error = ?e, "无法绑定到端口 9090");
            return;
        }
    };
    info!(target: "WebServer", log_type="low_freq", addr = "http://0.0.0.0:9090", "可视化Web服务器已启动");
    if let Err(e) = axum::serve(listener, app).await {
        error!(target: "WebServer", error = ?e, "Web服务器遇到致命错误");
    }
}

// --- 内部实现 ---

/// 用于API响应和WebSocket推送的K线结构体
#[derive(Debug, Clone, Serialize)]
struct ApiKline {
    pub period: String,
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_final: bool,
}

/// Web服务器的共享状态
#[derive(Clone)]
struct AppState {
    klines_watch_rx: watch::Receiver<Arc<DeltaBatch>>,
    index_to_symbol: Arc<RwLock<Vec<String>>>,
    periods: Arc<Vec<String>>,
    subscriptions: Arc<DashMap<String, Vec<mpsc::Sender<String>>>>,
}

/// 【核心修改】后台任务，现在使用Gateway的watch通道
#[instrument(target = "Pusher", skip_all)]
async fn pusher_task(
    state: Arc<AppState>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!(target = "Pusher", log_type="low_freq", "Gateway数据订阅推送器已启动");

    let mut klines_watch_rx = state.klines_watch_rx.clone();

    loop {
        tokio::select! {
            // 监听Gateway的增量数据更新
            Ok(_) = klines_watch_rx.changed() => {
                // 获取最新的增量批次
                let delta_batch = klines_watch_rx.borrow_and_update().clone();

                // 过滤出有效的K线数据
                let all_klines: Vec<KlineData> = delta_batch.klines.iter()
                    .filter(|k| k.open_time > 0)  // 过滤掉默认/空的K线
                    .cloned()
                    .collect();

                // 如果没有收集到任何更新的K线，则直接进入下一次循环
                if all_klines.is_empty() {
                    continue;
                }
                trace!(target = "Pusher", batch_id = delta_batch.batch_id, kline_count = all_klines.len(), "拉取到增量K线数据");

                let index_guard = state.index_to_symbol.read().await;
                let klines_by_symbol: DashMap<String, Vec<ApiKline>> = DashMap::new();

                for kline in all_klines {
                    if let (Some(symbol), Some(period)) = (
                        index_guard.get(kline.global_symbol_index),
                        state.periods.get(kline.period_index),
                    ) {
                        klines_by_symbol.entry(symbol.clone()).or_default().push(ApiKline {
                            period: period.clone(),
                            open_time: kline.open_time,
                            open: kline.open,
                            high: kline.high,
                            low: kline.low,
                            close: kline.close,
                            volume: kline.volume,
                            is_final: kline.is_final,
                        });
                    }
                }
                drop(index_guard);

                // 将分组后的数据推送给对应的订阅者
                for entry in klines_by_symbol.iter() {
                    let symbol = entry.key();
                    let klines_json = serde_json::to_string(entry.value()).unwrap_or_default();

                    if let Some(mut subscribers) = state.subscriptions.get_mut(symbol) {
                        let mut broken_indices = Vec::new();
                        for (i, tx) in subscribers.iter().enumerate() {
                            // 使用 try_send 避免单个慢客户端阻塞
                            if tx.try_send(klines_json.clone()).is_err() {
                                broken_indices.push(i);
                            }
                        }
                        // 清理已断开的客户端连接
                        for i in broken_indices.iter().rev() {
                            subscribers.remove(*i);
                        }
                    }
                }
            },
            // 处理关闭信号
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
        }
    }
    warn!(target = "Pusher", "Pusher任务已退出");
}

/// WebSocket升级处理器
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// 处理单个WebSocket连接的生命周期
async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let (tx, mut rx) = mpsc::channel::<String>(32);
    let mut current_subscription: Option<String> = None;

    loop {
        tokio::select! {
            Some(msg_to_send) = rx.recv() => {
                if socket.send(Message::Text(msg_to_send)).await.is_err() {
                    break;
                }
            },
            Some(Ok(msg)) = socket.recv() => {
                if let Message::Text(text) = msg {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(symbol) = val.get("subscribe").and_then(|v| v.as_str()) {
                            let symbol = symbol.to_uppercase();
                            info!(target = "WebServer", "请求订阅: {}", symbol);
                            if let Some(old_symbol) = current_subscription.take() {
                                if let Some(mut subs) = state.subscriptions.get_mut(&old_symbol) {
                                    subs.retain(|s| !s.same_channel(&tx));
                                }
                            }
                            state.subscriptions.entry(symbol.clone()).or_default().push(tx.clone());
                            current_subscription = Some(symbol);
                        }
                    }
                } else if let Message::Close(_) = msg {
                    break;
                }
            },
            else => { break; }
        }
    }

    if let Some(symbol) = current_subscription {
        if let Some(mut subs) = state.subscriptions.get_mut(&symbol) {
            subs.retain(|s| !s.same_channel(&tx));
            info!(target = "WebServer", "客户端断开，已清理订阅: {}", symbol);
        }
    }
}
