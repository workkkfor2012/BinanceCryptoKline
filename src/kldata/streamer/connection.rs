use crate::klcommon::{AppError, Result};
use crate::kldata::streamer::config::{BINANCE_WS_URL, create_subscribe_message};
use crate::kldata::streamer::WebSocketConnection;

use log::{info, error, debug, warn};
use url::Url;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;

use tokio_socks::tcp::Socks5Stream;

// 使用 async-tungstenite 库
use async_tungstenite::WebSocketStream;
use async_tungstenite::tungstenite::{protocol::Message, protocol::WebSocketConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use serde_json::Value;
use tokio_native_tls;



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
    pub async fn connect(&self, streams: &[String]) -> Result<WebSocketStream<async_tungstenite::stream::Stream<async_tungstenite::tokio::TokioAdapter<TcpStream>, async_tungstenite::tokio::TokioAdapter<tokio_native_tls::TlsStream<TcpStream>>>>> {
        // 构建URL
        let url_str = if streams.is_empty() {
            BINANCE_WS_URL.to_string()
        } else if streams.len() == 1 {
            // 单个流使用直接连接格式
            format!("{}/{}", BINANCE_WS_URL, streams[0])
        } else {
            // 多个流使用组合流订阅格式
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
            max_write_buffer_size: 4096,  // 必须大于 write_buffer_size
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
            // 使用默认的TLS配置，但通过代理连接
            let request = url.to_string();

            // 使用 async-tungstenite 库的 client_async_tls_with_config 函数
            async_tungstenite::tokio::client_async_tls_with_config(request, socket, Some(ws_config))
                .await
                .map_err(|e| AppError::WebSocketError(format!("WebSocket连接失败: {}", e)))?
        } else {
            // 直接连接
            let socket = TcpStream::connect(format!("{0}:{1}", url.host_str().unwrap_or("fstream.binance.com"), url.port().unwrap_or(443))).await?;

            // 创建请求
            let request = url.to_string();

            // 使用 async-tungstenite 库的 client_async_tls_with_config 函数
            async_tungstenite::tokio::client_async_tls_with_config(request, socket, Some(ws_config))
                .await
                .map_err(|e| AppError::WebSocketError(format!("WebSocket连接失败: {}", e)))?
        };

        info!("WebSocket连接已建立");

        // 如果使用的是直接连接格式（单个流），则不需要发送订阅消息
        // 如果使用的是组合流订阅格式（多个流），则需要发送订阅消息
        if url_str.contains("?streams=") && !streams.is_empty() {
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
        ws_stream: &mut WebSocketStream<async_tungstenite::stream::Stream<async_tungstenite::tokio::TokioAdapter<TcpStream>, async_tungstenite::tokio::TokioAdapter<tokio_native_tls::TlsStream<TcpStream>>>>,
        tx: mpsc::Sender<(usize, String)>,
        connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    ) {
        info!("开始处理连接 {} 的消息", connection_id);

        // 处理消息，添加超时处理
        loop {
            // 使用 tokio::time::timeout 添加超时处理
            match tokio::time::timeout(Duration::from_secs(30), ws_stream.next()).await {
                Ok(Some(msg)) => {
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
                Ok(None) => {
                    info!("WebSocket连接已关闭");
                    break;
                }
                Err(_) => {
                    // 超时，发送ping以保持连接
                    debug!("WebSocket连接超时，发送Ping");
                    if let Err(e) = ws_stream.send(Message::Ping(vec![])).await {
                        error!("发送Ping失败: {}", e);
                        break;
                    }
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
