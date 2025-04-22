// WebSocket connection management module
use crate::kldownload::error::{AppError, Result};
use crate::kldownload::websocket::config::{BINANCE_WS_URL, create_subscribe_message};
use crate::kldownload::websocket::models::WebSocketConnection;

use log::info;
use url::Url;
use std::sync::Arc;
use tokio_socks::tcp::Socks5Stream;
use tokio_rustls::{TlsConnector, rustls::{ClientConfig, RootCertStore}};
use tokio_tungstenite::{
    tungstenite::{protocol::Message, protocol::WebSocketConfig},
    client_async_with_config,
};
use futures_util::SinkExt;
use serde_json;

/// Connection manager
pub struct ConnectionManager {
    use_proxy: bool,
    proxy_addr: String,
    proxy_port: u16,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new(use_proxy: bool, proxy_addr: String, proxy_port: u16) -> Self {
        Self {
            use_proxy,
            proxy_addr,
            proxy_port,
        }
    }

    /// Connect to WebSocket server and subscribe to streams
    pub async fn connect_and_subscribe(&self, id: usize, streams: Vec<String>) -> Result<WebSocketConnection> {
        info!("Connection {} connecting to Binance WebSocket server...", id + 1);

        // Use base URL
        let url = Url::parse(BINANCE_WS_URL)?;
        let host = url.host_str().ok_or_else(|| AppError::WebSocketError("Invalid URL host".to_string()))?.to_string();
        let port = url.port_or_known_default().ok_or_else(|| AppError::WebSocketError("Invalid URL port".to_string()))?;
        let target_addr = format!("{}:{}", host, port);

        info!("Connection {} target server: {}", id + 1, target_addr);

        // 1. Establish TCP connection (through SOCKS5 proxy)
        let socks_stream = if self.use_proxy {
            info!("Connection {} using proxy: {}:{}", id + 1, self.proxy_addr, self.proxy_port);
            let proxy_addr = format!("{}:{}", self.proxy_addr, self.proxy_port);
            Socks5Stream::connect(&proxy_addr[..], &target_addr[..]).await
                .map_err(|e| AppError::WebSocketError(format!("SOCKS5 proxy connection failed: {}", e)))?
        } else {
            let tcp_stream = tokio::net::TcpStream::connect(target_addr.clone()).await
                .map_err(|e| AppError::WebSocketError(format!("TCP connection failed: {}", e)))?;
            // Use TCP stream directly, without SOCKS5
            Socks5Stream::connect_with_socket(tcp_stream, &target_addr[..])
                .await
                .map_err(|e| AppError::WebSocketError(format!("TCP connection failed: {}", e)))?
        };

        info!("Connection {} TCP connection established successfully through SOCKS5 proxy", id + 1);

        // 2. Establish TLS connection on top of TCP stream
        let mut root_cert_store = RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().map_err(|e| AppError::WebSocketError(format!("Failed to load native certificates: {}", e)))? {
            // Convert certificate type
            let rustls_cert = rustls::Certificate(cert.0.clone());
            root_cert_store.add(&rustls_cert).map_err(|e| AppError::WebSocketError(format!("Failed to add certificate: {}", e)))?;
        }

        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = rustls::ServerName::try_from(host.as_str())
            .map_err(|_| AppError::WebSocketError(format!("Invalid DNS name: {}", host)))?;

        let tls_stream = connector.connect(server_name, socks_stream).await
            .map_err(|e| AppError::WebSocketError(format!("TLS connection failed: {}", e)))?;
        info!("Connection {} TLS connection established successfully", id + 1);

        // 3. Perform WebSocket handshake on top of TLS stream
        let ws_config = WebSocketConfig {
            max_message_size: Some(64 << 20), // 64 MiB
            max_frame_size: Some(16 << 20),   // 16 MiB
            accept_unmasked_frames: false,
            ..Default::default()
        };

        let (ws_stream, response) = client_async_with_config(url.clone(), tls_stream, Some(ws_config)).await
            .map_err(|e| AppError::WebSocketError(format!("WebSocket handshake failed: {}", e)))?;

        info!("Connection {} WebSocket handshake successful, server response: {:?}", id + 1, response.status());

        // 4. Subscribe to streams
        let mut connection = WebSocketConnection::new(id, streams.clone());
        connection.ws_stream = Some(ws_stream);

        // Create subscription message
        let subscribe_msg = create_subscribe_message(streams, (id + 1) as i32);

        // Send subscription message
        let subscribe_text = serde_json::to_string(&subscribe_msg)?;
        info!("Connection {} sending subscription request: {}", id + 1, subscribe_text);

        if let Some(ws_stream) = &mut connection.ws_stream {
            ws_stream.send(Message::Text(subscribe_text.clone())).await
                .map_err(|e| AppError::WebSocketError(format!("Failed to send subscription message: {}", e)))?;
            info!("Connection {} subscription request sent", id + 1);
        }

        Ok(connection)
    }
}
