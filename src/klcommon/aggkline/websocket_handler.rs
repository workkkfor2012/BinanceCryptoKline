// WebSocket处理器 - 管理WebSocket连接和处理消息
use crate::klcommon::{AppError, Result};
use crate::klcommon::proxy::{PROXY_HOST, PROXY_PORT};
use log::{info, error, debug, warn};
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use url::Url;
use std::time::Duration;
use tokio_socks::tcp::Socks5Stream;

/// 运行WebSocket连接任务
///
/// # 参数
/// * `task_id` - 任务ID，用于日志标识
/// * `ws_url` - WebSocket URL
/// * `streams_to_subscribe` - 要订阅的流列表
/// * `raw_frame_sender` - 原始帧发送器
pub async fn run_websocket_connection_task(
    task_id: String,
    ws_url: String,
    streams_to_subscribe: Option<Vec<String>>, // 如果ws_url是基础URL，则用此列表发送订阅消息
    raw_frame_sender: mpsc::Sender<String>,
) -> Result<()> {
    info!("[{}] 连接到 {}...", task_id, ws_url);

    // 构建订阅消息 (如果需要)
    // 币安的多stream订阅URL格式: wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade
    // 或者连接后发送JSON RPC订阅消息
    let final_url = if let Some(streams) = &streams_to_subscribe {
        if streams.len() > 1 {
            // 多个流使用组合流订阅格式
            let streams_query = streams.join("/");
            if ws_url.ends_with("/stream") {
                format!("{}?streams={}", ws_url, streams_query)
            } else if ws_url.ends_with("/ws") {
                // 将 /ws 替换为 /stream?streams=
                let base_url = ws_url.trim_end_matches("/ws");
                format!("{}/stream?streams={}", base_url, streams_query)
            } else {
                format!("{}/stream?streams={}", ws_url, streams_query)
            }
        } else if streams.len() == 1 {
            // 单个流使用直接连接格式
            if ws_url.ends_with("/ws") {
                format!("{}/{}", ws_url, streams[0])
            } else {
                format!("{}/ws/{}", ws_url, streams[0])
            }
        } else {
            ws_url.clone()
        }
    } else {
        if !ws_url.ends_with("/ws") {
            format!("{}/ws", ws_url)
        } else {
            ws_url.clone()
        }
    };

    info!("[{}] 最终URL: {}", task_id, final_url);

    let mut reconnect_attempts = 0;
    let max_reconnect_attempts = 10; // 最大重连次数

    loop { // 自动重连循环
        // 解析URL
        let url = Url::parse(&final_url)
            .map_err(|e| AppError::WebSocketError(format!("URL解析失败: {}", e)))?;

        // 获取主机和端口
        let host = url.host_str().ok_or_else(|| AppError::WebSocketError("无法获取主机名".to_string()))?;
        let port = url.port().unwrap_or(443);

        info!("[{}] 通过代理 {}:{} 连接到 {}:{}", task_id, PROXY_HOST, PROXY_PORT, host, port);

        // 尝试通过代理连接
        let socket_result = Socks5Stream::connect((PROXY_HOST, PROXY_PORT), (host, port)).await;

        match socket_result {
            Ok(socks_stream) => {
                // 获取TCP流
                let tcp_stream = socks_stream.into_inner();

                // 使用TCP流创建WebSocket连接
                let ws_stream_result = tokio_tungstenite::client_async(url.to_string(), tcp_stream).await;

                match ws_stream_result {
                    Ok((ws_stream, _response)) => {
                        info!("[{}] 连接成功", task_id);
                        reconnect_attempts = 0; // 重置重连计数

                        let (mut write, mut read) = ws_stream.split();

                        // （可选）如果不是通过URL参数订阅，而是在连接后发送订阅消息
                        if let Some(streams) = &streams_to_subscribe {
                            if !final_url.contains("?streams=") {
                                let subscribe_msg = serde_json::json!({
                                    "method": "SUBSCRIBE",
                                    "params": streams,
                                    "id": 1 // 或者使用唯一的ID
                                });

                                if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                                    error!("[{}] 发送订阅消息失败: {}", task_id, e);
                                    tokio::time::sleep(Duration::from_secs(5)).await; // 等待后重连
                                    continue;
                                }

                                info!("[{}] 已发送订阅消息，订阅 {} 个流", task_id, streams.len());
                            }
                        }

                        // 处理消息
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    // 简单地将原始文本帧发送出去
                                    // 可以在这里做初步的检查，例如是否是 `aggTrade` 事件的包裹体
                                    // 币安多stream格式: {"stream":"<streamName>","data":<raw_payload>}
                                    // 如果是单stream, 直接是 raw_payload
                                    if raw_frame_sender.send(text).await.is_err() {
                                        error!("[{}] 原始帧接收器已关闭，退出任务", task_id);
                                        return Ok(());
                                    }
                                }
                                Ok(Message::Ping(ping_data)) => { // 处理Ping, 回复Pong
                                    debug!("[{}] 收到Ping，发送Pong", task_id);
                                    if write.send(Message::Pong(ping_data)).await.is_err() {
                                        error!("[{}] 发送Pong失败，连接可能已断开", task_id);
                                        break; // 跳出内部循环，触发重连
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    info!("[{}] WebSocket连接被服务器关闭", task_id);
                                    break; // 跳出内部循环，触发重连
                                }
                                Err(e) => {
                                    error!("[{}] WebSocket错误: {}", task_id, e);
                                    break; // 跳出内部循环，触发重连
                                }
                                _ => { /* 忽略其他消息类型，如Binary, Pong */ }
                            }
                        }
                    }
                    Err(e) => {
                        reconnect_attempts += 1;
                        error!("[{}] WebSocket连接失败: {}. 重试 {}/{}", task_id, e, reconnect_attempts, max_reconnect_attempts);
                    }
                }
            }
            Err(e) => {
                reconnect_attempts += 1;
                error!("[{}] 代理连接失败: {}. 重试 {}/{}", task_id, e, reconnect_attempts, max_reconnect_attempts);
            }
        }

        if reconnect_attempts >= max_reconnect_attempts {
            error!("[{}] 达到最大重连次数，退出任务", task_id);
            return Err(AppError::WebSocketError(format!("达到最大重连次数: {}", max_reconnect_attempts)));
        }

        // 等待一段时间后重连，使用指数退避策略
        let delay = std::cmp::min(30, 2_u64.pow(reconnect_attempts as u32)); // 最大30秒
        info!("[{}] 将在 {} 秒后重连", task_id, delay);
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}
