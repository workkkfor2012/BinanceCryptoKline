use crate::klcommon::{AppError, Result};
use crate::kldata::streamer::config::{BINANCE_WS_URL, create_subscribe_message};
use crate::kldata::streamer::WebSocketConnection;

use log::{info, error, debug, warn};
use url::Url;
use std::sync::Arc;
use std::collections::HashMap;

use tokio_socks::tcp::Socks5Stream;

use tokio_tungstenite::{
    tungstenite::{protocol::Message, protocol::WebSocketConfig},
    client_async_with_config,
    WebSocketStream,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use serde_json::Value;

/// 连接管理器
#[derive(Clone)]
pub struct ConnectionManager {
    use_proxy: bool,
    proxy_addr: String,
    proxy_port: u16,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new(use_proxy: bool, proxy_addr: String, proxy_port: u16) -> Self {
        Self {
            use_proxy,
            proxy_addr,
            proxy_port,
        }
    }

    /// 连接到WebSocket服务器
    pub async fn connect(&self, streams: &[String]) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
        // 构建URL
        let url_str = if streams.is_empty() {
            BINANCE_WS_URL.to_string()
        } else {
            format!("{}?streams={}", BINANCE_WS_URL, streams.join("/"))
        };

        let url = Url::parse(&url_str)?;
        info!("连接到WebSocket: {}", url);

        // WebSocket配置
        let ws_config = WebSocketConfig {
            max_send_queue: Some(1024), // 已废弃，但仍然需要
            max_message_size: Some(64 << 20), // 64 MiB
            max_frame_size: Some(16 << 20),   // 16 MiB
            accept_unmasked_frames: false,
            max_write_buffer_size: 1024,
            write_buffer_size: 1024,
        };

        // 建立连接
        let (ws_stream, _) = if self.use_proxy {
            // 使用SOCKS5代理
            info!("通过代理 {}:{} 连接", self.proxy_addr, self.proxy_port);

            // 连接到代理
            let stream = Socks5Stream::connect(
                (self.proxy_addr.as_str(), self.proxy_port),
                (url.host_str().unwrap_or("fstream.binance.com"), url.port().unwrap_or(443))
            )
            .await
            .map_err(|e| AppError::WebSocketError(format!("代理连接失败: {}", e)))?;

            // 获取TCP流
            let socket = stream.into_inner();

            // 建立WebSocket连接
            client_async_with_config(url, tokio_tungstenite::MaybeTlsStream::Plain(socket), Some(ws_config))
                .await
                .map_err(|e| AppError::WebSocketError(format!("WebSocket连接失败: {}", e)))?
        } else {
            // 直接连接
            let socket = TcpStream::connect(format!("{0}:{1}", url.host_str().unwrap_or("fstream.binance.com"), url.port().unwrap_or(443))).await?;
            client_async_with_config(url.clone(), tokio_tungstenite::MaybeTlsStream::Plain(socket), Some(ws_config))
                .await
                .map_err(|e| AppError::WebSocketError(format!("WebSocket连接失败: {}", e)))?
        };

        info!("WebSocket连接已建立");

        // 如果使用的是流列表URL，则不需要发送订阅消息
        if !url_str.contains("?streams=") && !streams.is_empty() {
            // 发送订阅消息
            let mut ws_stream = ws_stream;
            let subscribe_msg = create_subscribe_message(streams);
            info!("发送订阅消息: {}", subscribe_msg);

            ws_stream.send(Message::Text(subscribe_msg)).await
                .map_err(|e| AppError::WebSocketError(format!("发送订阅消息失败: {}", e)))?;

            // 等待订阅响应
            if let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        info!("收到订阅响应: {}", text);

                        // 解析响应
                        if let Ok(json) = serde_json::from_str::<Value>(&text) {
                            if let Some(result) = json.get("result") {
                                if result.is_null() {
                                    info!("订阅成功");
                                } else {
                                    warn!("订阅响应异常: {}", text);
                                }
                            } else {
                                warn!("订阅响应缺少result字段: {}", text);
                            }
                        } else {
                            warn!("订阅响应不是有效的JSON: {}", text);
                        }
                    }
                    Ok(msg) => warn!("收到非文本订阅响应: {:?}", msg),
                    Err(e) => {
                        error!("接收订阅响应失败: {}", e);
                        return Err(AppError::WebSocketError(format!("接收订阅响应失败: {}", e)));
                    }
                }
            } else {
                warn!("没有收到订阅响应");
            }

            return Ok(ws_stream);
        }

        Ok(ws_stream)
    }

    /// 处理WebSocket消息
    pub async fn handle_messages(
        &self,
        connection_id: usize,
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        tx: mpsc::Sender<(usize, String)>,
        connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    ) {
        info!("开始处理连接 {} 的消息", connection_id);

        // 处理消息
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // 更新消息计数
                    {
                        let mut connections = connections.lock().await;
                        if let Some(conn) = connections.get_mut(&connection_id) {
                            conn.message_count += 1;
                        }
                    }

                    // 发送消息到处理器
                    if let Err(e) = tx.send((connection_id, text)).await {
                        error!("发送消息到处理器失败: {}", e);
                        break;
                    }
                }
                Ok(Message::Ping(data)) => {
                    debug!("收到Ping，发送Pong");
                    if let Err(e) = ws_stream.send(Message::Pong(data)).await {
                        error!("发送Pong失败: {}", e);
                        break;
                    }
                }
                Ok(Message::Close(frame)) => {
                    info!("收到关闭消息: {:?}", frame);
                    break;
                }
                Ok(msg) => {
                    debug!("收到其他消息: {:?}", msg);
                }
                Err(e) => {
                    error!("WebSocket错误: {}", e);
                    break;
                }
            }
        }

        // 更新连接状态
        {
            let mut connections = connections.lock().await;
            if let Some(conn) = connections.get_mut(&connection_id) {
                conn.status = "已断开".to_string();
            }
        }

        info!("连接 {} 已关闭", connection_id);
    }
}
