// 文件: src/klcommon/websocket.rs
// WebSocket模块 - 提供通用的WebSocket连接管理功能 (使用 fastwebsockets 实现)
use crate::klcommon::{AppError, Result, PROXY_HOST, PROXY_PORT};
use tracing::{info, error, debug, warn, instrument, Instrument};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::future::Future;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_socks::tcp::Socks5Stream;
use serde_json::json;
use serde::Deserialize;
use crate::klagg_sub_threads::{AggTradePayload, RawTradePayload};

use bytes::Bytes;
use fastwebsockets::{FragmentCollector, Frame, OpCode};
use hyper_util::rt::tokio::TokioIo;
use hyper::upgrade::Upgraded;
use http_body_util::Empty;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::Request;
use tokio_rustls::rustls::{ClientConfig, OwnedTrustAnchor, ServerName};
use tokio_rustls::TlsConnector;


//=============================================================================
// 常量和配置
//=============================================================================

/// 币安WebSocket URL
pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws";

/// WebSocket连接数量
/// 所有品种将平均分配到这些连接中
pub const WEBSOCKET_CONNECTION_COUNT: usize = 1;

/// WebSocket连接重试配置
pub const MAX_RETRY_ATTEMPTS: usize = 5;
pub const INITIAL_RETRY_DELAY_MS: u64 = 1000; // 1秒
pub const MAX_RETRY_DELAY_MS: u64 = 30000; // 30秒

/// 归集交易日志目标
pub const AGG_TRADE_TARGET: &str = "归集交易";

/// WebSocket连接日志目标
pub const WEBSOCKET_CONNECTION_TARGET: &str = "websocket连接";

/// 全市场精简Ticker日志目标
pub const MINI_TICKER_TARGET: &str = "全市场精简Ticker";

//=============================================================================
// 上下文感知解析类型定义
//=============================================================================

/// 【新增】明确定义WebSocket端点的类型，用于上下文感知解析。
#[derive(Debug, Clone, Copy)]
pub enum EndpointType {
    /// 对应 /stream 端点，返回格式: {"stream": "...", "data": {...}}
    CombinedStream,
    /// 对应 /ws 端点，返回原始消息格式: {...}
    SingleStream,
}

/// 【新增】用于从组合流（Combined Stream）中解包数据的结构体。
/// 使用 `RawValue` 来避免对内层 `data` 进行不必要的解析，保持零拷贝优势。
#[derive(Deserialize)]
struct CombinedStreamPayload<'a> {
    #[serde(rename = "stream")]
    _stream: &'a str,
    #[serde(borrow, rename = "data")]
    data: &'a serde_json::value::RawValue,
}

//=============================================================================
// WebSocket配置
//=============================================================================

/// WebSocket配置接口
pub trait WebSocketConfig {
    /// 获取代理设置
    fn get_proxy_settings(&self) -> (bool, String, u16);
    /// 获取流列表
    fn get_streams(&self) -> Vec<String>;
}



/// 归集交易配置
#[derive(Clone)]
pub struct AggTradeConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    /// 代理地址
    pub proxy_addr: String,
    /// 代理端口
    pub proxy_port: u16,
    /// 交易对列表
    pub symbols: Vec<String>,
}

impl Default for AggTradeConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_addr: PROXY_HOST.to_string(),
            proxy_port: PROXY_PORT,
            symbols: Vec::new(),
        }
    }
}

impl WebSocketConfig for AggTradeConfig {
    // #[instrument] 移除：简单的配置读取函数，追踪会产生噪音
    fn get_proxy_settings(&self) -> (bool, String, u16) {
        (self.use_proxy, self.proxy_addr.clone(), self.proxy_port)
    }

    // #[instrument] 移除：简单的流名称构建函数，追踪会产生噪音
    fn get_streams(&self) -> Vec<String> {
        self.symbols.iter()
            .map(|symbol| format!("{}@aggTrade", symbol.to_lowercase()))
            .collect()
    }
}

/// 全市场精简Ticker的WebSocket配置
#[derive(Clone)]
pub struct MiniTickerConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    /// 代理地址
    pub proxy_addr: String,
    /// 代理端口
    pub proxy_port: u16,
}

impl Default for MiniTickerConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            proxy_addr: PROXY_HOST.to_string(),
            proxy_port: PROXY_PORT,
        }
    }
}

impl WebSocketConfig for MiniTickerConfig {
    fn get_proxy_settings(&self) -> (bool, String, u16) {
        (self.use_proxy, self.proxy_addr.clone(), self.proxy_port)
    }

    fn get_streams(&self) -> Vec<String> {
        // Note: 这个流是固定的，不需要任何参数。
        vec!["!miniTicker@arr".to_string()]
    }
}

/// 创建订阅消息
#[instrument(skip_all)]
pub fn create_subscribe_message(streams: &[String]) -> String {
    json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    })
    .to_string()
}

//=============================================================================
// WebSocket客户端接口
//=============================================================================

/// WebSocket连接状态
#[derive(Debug, Clone)]
pub struct WebSocketConnection {
    pub id: usize,
    pub streams: Vec<String>,
    pub status: String,
    pub message_count: usize,
}

/// WebSocket客户端接口
pub trait WebSocketClient {
    /// 启动WebSocket客户端
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// 获取连接状态
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send;
}

//=============================================================================
// 数据结构
//=============================================================================

/// 全市场精简Ticker数据
///
/// 从 `!miniTicker@arr` 流接收。
#[derive(serde::Deserialize, Debug, Clone)]
pub struct MiniTickerData {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub total_traded_volume: String,
    #[serde(rename = "q")]
    pub total_traded_quote_volume: String,
}

/// 币安原始归集交易数据
#[derive(Debug, Clone)]
pub struct BinanceRawAggTrade {
    pub event_type: String,
    pub event_time: u64,
    pub symbol: String,
    pub aggregate_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub trade_time: u64,
    pub is_buyer_maker: bool,
}

/// 归集交易数据 - 从WebSocket接收的原始数据解析后的结构
///
/// 这是系统中AggTradeData的权威定义，包含币安原始数据的所有字段
/// 使用 #[repr(C)] 确保内存布局的可预测性，提高缓存效率
#[repr(C)]
#[derive(Debug, Clone, Deserialize)]
pub struct AggTradeData {
    /// 交易品种
    pub symbol: String,
    /// 成交价格
    pub price: f64,
    /// 成交数量
    pub quantity: f64,
    /// 成交时间戳（毫秒）
    pub timestamp_ms: i64,
    /// 买方是否为做市商
    pub is_buyer_maker: bool,
    /// 归集交易ID
    pub agg_trade_id: i64,
    /// 首个交易ID
    pub first_trade_id: i64,
    /// 最后交易ID
    pub last_trade_id: i64,
    /// 事件时间戳（毫秒）
    pub event_time_ms: i64,
}

impl AggTradeData {
    /// 从币安原始归集交易数据创建
    pub fn from_binance_raw(raw: &BinanceRawAggTrade) -> Self {
        Self {
            symbol: raw.symbol.clone(),
            price: raw.price.parse::<f64>().unwrap_or(0.0),
            quantity: raw.quantity.parse::<f64>().unwrap_or(0.0),
            timestamp_ms: raw.trade_time as i64,
            is_buyer_maker: raw.is_buyer_maker,
            agg_trade_id: raw.aggregate_trade_id as i64,
            first_trade_id: raw.first_trade_id as i64,
            last_trade_id: raw.last_trade_id as i64,
            event_time_ms: raw.event_time as i64,
        }
    }
}

//=============================================================================
// 消息处理
//=============================================================================

/// 【修改】消息处理接口
pub trait MessageHandler {
    /// 处理WebSocket消息 - 新增 endpoint_type 参数用于上下文感知
    fn handle_message(
        &self,
        connection_id: usize,
        payload: &[u8],
        endpoint_type: EndpointType, // <-- 【核心修改】新增上下文参数
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// 公开的WebSocket命令，用于动态控制
#[derive(Debug, Clone)]
pub enum WsCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}



/// 归集交易消息处理器，用于K线聚合系统 - 入口优化版本
pub struct AggTradeMessageHandler {
    sender: tokio::sync::mpsc::Sender<AggTradePayload>,
    symbol_to_global_index: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    error_count: Arc<AtomicUsize>, // 用于主动监控解析错误率
}

impl AggTradeMessageHandler {
    /// 构造函数 - 入口优化版本
    pub fn new(
        sender: tokio::sync::mpsc::Sender<AggTradePayload>,
        symbol_to_global_index: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    ) -> Self {
        Self {
            sender,
            symbol_to_global_index,
            error_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 【新增】提取出的公共处理逻辑的辅助函数
    async fn process_raw_trade(&self, raw_trade: RawTradePayload<'_>) -> Result<()> {
        let global_symbol_index = {
            let guard = self.symbol_to_global_index.read().await;
            match guard.get(raw_trade.symbol) {
                Some(index) => *index,
                None => {
                    tracing::trace!(target: AGG_TRADE_TARGET, "收到未索引的品种交易: {}", raw_trade.symbol);
                    return Ok(());
                }
            }
        };

        // 【新增】添加交易数据接收日志
        // info!(target: AGG_TRADE_TARGET, "收到归集交易: {} {} @ {}",
        //     raw_trade.symbol, raw_trade.quantity, raw_trade.price);

        let price = match raw_trade.price.parse::<f64>() {
            Ok(p) => p,
            Err(_) => {
                warn!(target: AGG_TRADE_TARGET, "解析价格失败: {}", raw_trade.price);
                self.error_count.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };
        let quantity = match raw_trade.quantity.parse::<f64>() {
            Ok(q) => q,
            Err(_) => {
                warn!(target: AGG_TRADE_TARGET, "解析数量失败: {}", raw_trade.quantity);
                self.error_count.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };

        let agg_payload = AggTradePayload {
            global_symbol_index,
            price,
            quantity,
            timestamp_ms: raw_trade.timestamp_ms,
            is_buyer_maker: raw_trade.is_buyer_maker,
        };

        // 【增加此处】测量发送到MPSC通道的等待时间
        let send_start = std::time::Instant::now();

        if let Err(e) = self.sender.send(agg_payload).await {
            error!(target: AGG_TRADE_TARGET, "发送解析后的交易到计算核心失败: {}", e);
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }

        // 【增加此处】检测通道背压
        let send_duration_micros = send_start.elapsed().as_micros();
        if send_duration_micros > 500 { // 等待超过500微秒就值得关注
            warn!(
                target: AGG_TRADE_TARGET,
                log_type = "performance_alert",
                wait_micros = send_duration_micros,
                "发送交易到计算核心的通道出现等待，可能存在背压！"
            );
        }

        Ok(())
    }
}

/// 全市场精简Ticker消息处理器
pub struct MiniTickerMessageHandler {
    /// 用于将解析后的数据向外发送的通道
    pub data_sender: tokio::sync::mpsc::UnboundedSender<Vec<MiniTickerData>>,
}

impl MiniTickerMessageHandler {
    /// 创建一个新的 MiniTickerMessageHandler
    pub fn new(data_sender: tokio::sync::mpsc::UnboundedSender<Vec<MiniTickerData>>) -> Self {
        Self { data_sender }
    }
}

impl MessageHandler for MiniTickerMessageHandler {
    /// 【修改】增加 endpoint_type 参数但忽略它，因为 MiniTicker 格式是固定的
    fn handle_message(&self, connection_id: usize, payload: &[u8], _endpoint_type: EndpointType) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // 从 &[u8] 转换为 &str
            let message = match std::str::from_utf8(payload) {
                Ok(s) => s,
                Err(e) => {
                    warn!(target: MINI_TICKER_TARGET, "收到无效的UTF-8消息，已丢弃: {}", e);
                    return Ok(());
                }
            };

            // MiniTicker 流直接是一个JSON数组
            match serde_json::from_str::<Vec<MiniTickerData>>(message) {
                Ok(tickers) => {
                    debug!(target: MINI_TICKER_TARGET, "连接 {} 收到 {} 条 MiniTicker 更新", connection_id, tickers.len());
                    // 将解析后的数据发送出去
                    if let Err(e) = self.data_sender.send(tickers) {
                        error!(target: MINI_TICKER_TARGET, "发送 MiniTicker 数据失败: {}", e);
                    }
                }
                Err(e) => {
                    // 检查是否是订阅成功等非数据消息
                    if !message.contains("result") {
                         warn!(target: MINI_TICKER_TARGET, "连接 {} 解析 MiniTicker 消息失败: {}, 原始消息: {}", connection_id, e, message);
                    }
                }
            }
            Ok(())
        }
    }
}

/// 【修改】AggTradeMessageHandler 的实现，采用上下文感知解析
impl MessageHandler for AggTradeMessageHandler {
    fn handle_message(&self, _connection_id: usize, payload: &[u8], endpoint_type: EndpointType) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // --- 【核心修改】基于上下文的确定性解析 ---
            match endpoint_type {
                EndpointType::CombinedStream => {
                    // 1. 优先尝试按组合流格式解析
                    if let Ok(combined_payload) = serde_json::from_slice::<CombinedStreamPayload>(payload) {
                        // 2. 如果成功，从 `data` 字段的原始字节中解析出 `RawTradePayload`
                        match serde_json::from_slice(combined_payload.data.get().as_bytes()) {
                            Ok(raw_trade) => {
                                // 3. 调用辅助函数处理
                                return self.process_raw_trade(raw_trade).await;
                            },
                            Err(e) => {
                                warn!(target: AGG_TRADE_TARGET, "从组合流的data字段反序列化交易数据失败: {}, 原始data: {}", e, combined_payload.data.get());
                                self.error_count.fetch_add(1, Ordering::Relaxed);
                            }
                        };
                    } else {
                        // 这通常是订阅确认等消息，可以安全忽略或记录为 trace
                        tracing::trace!(target: AGG_TRADE_TARGET, "忽略非组合流格式或非交易消息: {}", String::from_utf8_lossy(payload));
                    }
                }
                EndpointType::SingleStream => {
                    // 1. 按原始格式解析 (兼容单流)
                    if let Ok(raw_trade) = serde_json::from_slice::<RawTradePayload>(payload) {
                         // 2. 检查事件类型是否真的是 aggTrade，这是一个很好的健壮性补充
                         if raw_trade.event_type == "aggTrade" {
                            // 3. 调用辅助函数处理
                            return self.process_raw_trade(raw_trade).await;
                         }
                    }
                    // 如果不是 aggTrade，则忽略（可能是订阅确认等消息）
                    tracing::trace!(target: AGG_TRADE_TARGET, "忽略非aggTrade原始流消息: {}", String::from_utf8_lossy(payload));
                }
            }
            Ok(())
        }
    }
}



/// 处理WebSocket消息 - 入口优化版本，直接处理字节数据
#[instrument(skip_all)]
pub async fn process_messages<H: MessageHandler>(
    mut rx: mpsc::Receiver<(usize, Vec<u8>)>,
    handler: Arc<H>,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
) {
    info!(target: WEBSOCKET_CONNECTION_TARGET,   log_type = "low_freq", "🚀 启动WebSocket消息处理器");

    // 统计信息
    let mut _message_count = 0;
    let mut last_stats_time = Instant::now();
    let stats_interval = Duration::from_secs(30);

    // 处理消息
    while let Some((connection_id, payload)) = rx.recv().await {
        _message_count += 1;

        // 每30秒输出一次统计信息
        let now = Instant::now();
        if now.duration_since(last_stats_time) >= stats_interval {
            //info!("WebSocket统计: 已处理 {} 条消息", message_count);

            // 输出每个连接的统计信息
            let connections_guard = connections.lock().await;
            for (_id, _conn) in connections_guard.iter() {
               //info!("连接 {}: {} 条消息, 状态: {}", id, conn.message_count, conn.status);
            }

            last_stats_time = now;
        }

        // 处理消息 - 直接传递字节数据
        // 注意：这里使用 SingleStream 作为默认值，具体的端点类型应该由调用方决定
        if let Err(e) = handler.handle_message(connection_id, &payload, EndpointType::SingleStream).await {
            error!(target: WEBSOCKET_CONNECTION_TARGET, "处理消息失败: {}", e);
        }
    }

    info!( target: WEBSOCKET_CONNECTION_TARGET,   log_type = "low_freq", "✅ WebSocket消息处理器已停止");
}

//=============================================================================
// 实现 hyper 的 Executor trait
//=============================================================================

/// 实现 hyper 的 Executor trait，用于 fastwebsockets 握手
struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

//=============================================================================
// 连接管理
//=============================================================================

/// WebSocket连接管理器
#[derive(Clone)]
pub struct ConnectionManager {
    /// 是否使用代理
    use_proxy: bool,
    /// 代理地址
    proxy_addr: String,
    /// 代理端口
    proxy_port: u16,
    /// 可选的流列表，用于 /stream 端点
    streams: Option<Vec<String>>,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    // #[instrument] 移除：简单的构造函数，追踪会产生噪音
    pub fn new(use_proxy: bool, proxy_addr: String, proxy_port: u16) -> Self {
        Self {
            use_proxy,
            proxy_addr,
            proxy_port,
            streams: None,
        }
    }

    /// 创建带有流列表的连接管理器，用于 /stream 端点
    pub fn new_with_streams(use_proxy: bool, proxy_addr: String, proxy_port: u16, streams: Vec<String>) -> Self {
        Self {
            use_proxy,
            proxy_addr,
            proxy_port,
            streams: Some(streams),
        }
    }

    /// 连接到WebSocket服务器（带重试机制）
    #[instrument(skip_all, err)]
    pub async fn connect(&self) -> Result<FragmentCollector<TokioIo<Upgraded>>> {
        let mut last_error = None;
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(15); // 新增：为每次尝试设置15秒超时

        for attempt in 1..=MAX_RETRY_ATTEMPTS {
            info!(
                target: WEBSOCKET_CONNECTION_TARGET,
                log_type = "low_freq",
                "🔄 WebSocket连接尝试 {}/{}",
                attempt,
                MAX_RETRY_ATTEMPTS
            );

            // [修改逻辑] 使用 tokio::time::timeout 为单次连接尝试增加超时
            match tokio::time::timeout(CONNECT_TIMEOUT, self.connect_once()).await {
                Ok(Ok(ws)) => { // 超时内成功连接
                    if attempt > 1 {
                        info!(
                            target: WEBSOCKET_CONNECTION_TARGET,
                            log_type = "low_freq",
                            "✅ WebSocket连接在第{}次尝试后成功建立",
                            attempt
                        );
                    }
                    return Ok(ws);
                }
                Ok(Err(e)) => { // 超时内连接失败
                    last_error = Some(e);
                }
                Err(_) => { // 连接超时
                    last_error = Some(AppError::WebSocketError(format!("连接尝试超过 {} 秒未响应，已超时", CONNECT_TIMEOUT.as_secs())));
                }
            }

            if attempt < MAX_RETRY_ATTEMPTS {
                let delay_ms = std::cmp::min(
                    INITIAL_RETRY_DELAY_MS * (2_u64.pow((attempt - 1) as u32)),
                    MAX_RETRY_DELAY_MS
                );

                // [修改逻辑] 将 warn! 提升为 error! 以确保日志可见性
                error!(
                    target: WEBSOCKET_CONNECTION_TARGET,
                    log_type = "low_freq",
                    error_chain = format!("{:#}", last_error.as_ref().unwrap()),
                    "❌ WebSocket连接第{}次尝试失败，{}ms后重试",
                    attempt,
                    delay_ms
                );

                sleep(Duration::from_millis(delay_ms)).await;
            } else {
                error!(
                    target: WEBSOCKET_CONNECTION_TARGET,
                    log_type = "low_freq",
                    error_chain = format!("{:#}", last_error.as_ref().unwrap()),
                    "💥 WebSocket连接在{}次尝试后全部失败",
                    MAX_RETRY_ATTEMPTS
                );
            }
        }

        // 返回最后一次的错误
        Err(last_error.unwrap())
    }

    /// 单次连接尝试（内部方法）
    #[instrument(skip_all, err)]
    async fn connect_once(&self) -> Result<FragmentCollector<TokioIo<Upgraded>>> {
        // 设置主机和端口
        let host = "fstream.binance.com";
        let port = 443;
        let addr = format!("{}:{}", host, port);

        // 根据是否有streams参数决定使用哪种端点
        let (path, full_url) = if let Some(ref streams) = self.streams {
            // 使用 /stream 端点，直接在URL中指定streams
            let streams_param = streams.join("/");
            let path = format!("/stream?streams={}", streams_param);
            let full_url = format!("wss://{}:{}{}", host, port, path);
            (path, full_url)
        } else {
            // 使用 /ws 端点，支持动态订阅
            let path = "/ws".to_string();
            let full_url = format!("wss://{}:{}{}", host, port, path);
            (path, full_url)
        };
        info!(
            target: WEBSOCKET_CONNECTION_TARGET,
            log_type = "low_freq",
            "🔗 WebSocket连接详情 - 统一端点URL: {}",
            full_url
        );
        info!(
            target: WEBSOCKET_CONNECTION_TARGET,
            log_type = "low_freq",
            "🌐 代理设置: 启用={}, 地址={}:{}",
            self.use_proxy,
            self.proxy_addr,
            self.proxy_port
        );

        // 建立TCP连接（通过代理或直接）
        let tcp_stream = if self.use_proxy {
            debug!(target: WEBSOCKET_CONNECTION_TARGET, log_type = "module", "🌐 通过代理 {}:{} 连接", self.proxy_addr, self.proxy_port);

            // 连接到代理
            let socks_stream = Socks5Stream::connect(
                (self.proxy_addr.as_str(), self.proxy_port),
                (host, port)
            )
            .await
            .map_err(|e| AppError::WebSocketError(format!("代理连接失败: {}", e)))?;

            // 获取TCP流
            socks_stream.into_inner()
        } else {
            // 直接连接
            TcpStream::connect(addr).await?
        };

        debug!(target: WEBSOCKET_CONNECTION_TARGET, log_type = "module", "✅ TCP连接已建立");

        // 创建 TLS 连接
        let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
        root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(
            |ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            },
        ));

        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host)
            .map_err(|_| AppError::WebSocketError("无效的域名".to_string()))?;

        debug!(target: WEBSOCKET_CONNECTION_TARGET, log_type = "module", "🔐 建立TLS连接...");
        let tls_stream = connector.connect(server_name, tcp_stream).await?;
        debug!(target: WEBSOCKET_CONNECTION_TARGET, log_type = "module", "✅ TLS连接已建立");

        // 创建 HTTP 请求
        let req = Request::builder()
            .method("GET")
            .uri(format!("https://{}{}", host, path))
            .header("Host", host)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())
            .map_err(|e| AppError::WebSocketError(format!("创建HTTP请求失败: {}", e)))?;

        debug!(target: WEBSOCKET_CONNECTION_TARGET, "执行WebSocket握手...");

        // 执行 WebSocket 握手
        let (ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await
            .map_err(|e| AppError::WebSocketError(format!("WebSocket握手失败: {}", e)))?;
        let ws_collector = FragmentCollector::new(ws);

        debug!(target: WEBSOCKET_CONNECTION_TARGET, "WebSocket握手成功");

        // [修改逻辑] 移除这里的订阅逻辑。此函数现在只负责连接。
        // 调用者（如 run_io_loop）将负责在连接成功后发送订阅消息。

        Ok(ws_collector)
    }

    /// 处理WebSocket消息
    #[instrument(skip_all)]
    pub async fn handle_messages(
        &self,
        connection_id: usize,
        ws: &mut FragmentCollector<TokioIo<Upgraded>>,
        tx: mpsc::Sender<(usize, Vec<u8>)>,
        connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    ) {
        info!(target: WEBSOCKET_CONNECTION_TARGET, "开始处理连接 {} 的消息", connection_id);

        // 处理消息，添加超时处理
        loop {
            // 使用 tokio::time::timeout 添加超时处理
            match tokio::time::timeout(Duration::from_secs(30), ws.read_frame()).await {
                Ok(result) => {
                    match result {
                        Ok(frame) => {
                            match frame.opcode {
                                OpCode::Text => {
                                    // 直接发送字节数据，避免String转换
                                    let payload = frame.payload.to_vec();

                                    // 更新消息计数
                                    {
                                        let mut connections = connections.lock().await;
                                        if let Some(conn) = connections.get_mut(&connection_id) {
                                            conn.message_count += 1;
                                        }
                                    }

                                    // 发送消息到处理器 - 直接发送字节数据
                                    if let Err(e) = tx.send((connection_id, payload)).await {
                                        error!(target: WEBSOCKET_CONNECTION_TARGET, "发送消息到处理器失败: {}", e);
                                        break;
                                    }
                                },
                                OpCode::Binary => {
                                    debug!(target: WEBSOCKET_CONNECTION_TARGET, "收到二进制消息，长度: {}", frame.payload.len());
                                },
                                OpCode::Ping => {
                                    debug!(target: WEBSOCKET_CONNECTION_TARGET, "收到Ping，发送Pong");
                                    if let Err(e) = ws.write_frame(Frame::new(true, OpCode::Pong, None, frame.payload)).await {
                                        error!(target: WEBSOCKET_CONNECTION_TARGET, "发送Pong失败: {}", e);
                                        break;
                                    }
                                },
                                OpCode::Pong => {
                                    debug!(target: WEBSOCKET_CONNECTION_TARGET, "收到Pong");
                                },
                                OpCode::Close => {
                                    info!(target: WEBSOCKET_CONNECTION_TARGET, "收到关闭消息，连接将关闭");
                                    break;
                                },
                                _ => {
                                    debug!(target: WEBSOCKET_CONNECTION_TARGET, "收到其他类型的消息");
                                }
                            }
                        },
                        Err(e) => {
                            error!(target: WEBSOCKET_CONNECTION_TARGET, "WebSocket错误: {}", e);
                            break;
                        }
                    }
                },
                Err(_) => {
                    // 超时，发送ping以保持连接
                    debug!(target: WEBSOCKET_CONNECTION_TARGET, "WebSocket连接超时，发送Ping");
                    if let Err(e) = ws.write_frame(Frame::new(true, OpCode::Ping, None, vec![].into())).await {
                        error!(target: WEBSOCKET_CONNECTION_TARGET, "发送Ping失败: {}", e);
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

        info!(target: WEBSOCKET_CONNECTION_TARGET, "连接 {} 已关闭", connection_id);
    }
}

//=============================================================================
// 归集交易客户端
//=============================================================================

/// 归集交易客户端
pub struct AggTradeClient {
    config: AggTradeConfig,
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    /// 外部注入的消息处理器（可选）
    external_handler: Option<Arc<AggTradeMessageHandler>>,
    /// 用于从外部接收命令的发送端
    command_tx: Option<mpsc::Sender<WsCommand>>,
    /// 【新增】用于接收命令的接收端，会被start()方法取走
    command_rx: Option<mpsc::Receiver<WsCommand>>,
}

impl AggTradeClient {
    /// 创建新的归集交易客户端
    #[instrument(skip_all)]
    pub fn new(config: AggTradeConfig) -> Self {
        Self {
            config,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
            external_handler: None,
            command_tx: None,
            command_rx: None,
        }
    }

    /// 创建带有外部消息处理器的归集交易客户端
    #[instrument(skip_all)]
    pub fn new_with_handler(
        config: AggTradeConfig,
        handler: Arc<AggTradeMessageHandler>
    ) -> Self {
        // 【修改】同时保存 sender 和 receiver
        let (command_tx, command_rx) = mpsc::channel(16);
        Self {
            config,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
            external_handler: Some(handler),
            command_tx: Some(command_tx),
            command_rx: Some(command_rx), // 【新增】保存 receiver
        }
    }

    /// 允许外部获取命令发送端
    pub fn get_command_sender(&self) -> Option<mpsc::Sender<WsCommand>> {
        self.command_tx.clone()
    }
}

impl WebSocketClient for AggTradeClient {
    /// 启动客户端
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            info!(target: AGG_TRADE_TARGET, "启动归集交易客户端 (支持动态订阅)");
            info!(target: AGG_TRADE_TARGET, "使用代理: {}", self.config.use_proxy);

            if self.config.use_proxy {
                info!(target: AGG_TRADE_TARGET, "代理地址: {}:{}", self.config.proxy_addr, self.config.proxy_port);
            }

            // 确保日志目录存在
            let log_dir = Path::new("logs");
            if !log_dir.exists() {
                create_dir_all(log_dir)?;
            }

            // 【核心修改】从 self 中取出接收端
            let mut command_rx = if let Some(rx) = self.command_rx.take() {
                rx
            } else {
                // 如果没有命令通道，创建一个永远不会收到消息的通道
                let (_tx, rx) = mpsc::channel(1);
                rx
            };

            // 获取初始流
            let initial_streams = self.config.get_streams();
            info!(target: AGG_TRADE_TARGET, "初始订阅 {} 个流: {:?}", initial_streams.len(), initial_streams);

            // 创建连接管理器，使用 /stream 端点
            let (use_proxy, proxy_addr, proxy_port) = self.config.get_proxy_settings();
            let connection_manager = ConnectionManager::new_with_streams(
                use_proxy,
                proxy_addr,
                proxy_port,
                initial_streams.clone(),
            );

            // 获取消息处理器 - 现在必须从外部提供
            let handler = self.external_handler.as_ref()
                .expect("AggTradeClient 必须通过 new_with_handler 创建并提供 handler")
                .clone();

            // 使用单连接模式，支持动态订阅
            let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);
            let connections_clone = self.connections.clone();

            // 更新连接状态
            {
                let mut connections = connections_clone.lock().await;
                connections.insert(connection_id, WebSocketConnection {
                    id: connection_id,
                    streams: initial_streams.clone(),
                    status: "初始化".to_string(),
                    message_count: 0,
                });
            }

            // 启动单个连接任务，支持动态订阅
            let connection_task = tokio::spawn(
                (async move {
                    let mut current_streams = initial_streams;

                    loop {
                        // 更新状态为连接中
                        {
                            let mut connections = connections_clone.lock().await;
                            if let Some(conn) = connections.get_mut(&connection_id) {
                                conn.status = "连接中".to_string();
                            }
                        }

                        // 建立连接
                        match connection_manager.connect().await {
                            Ok(mut ws) => {
                                // 更新状态为已连接
                                {
                                    let mut connections = connections_clone.lock().await;
                                    if let Some(conn) = connections.get_mut(&connection_id) {
                                        conn.status = "已连接".to_string();
                                        conn.streams = current_streams.clone();
                                    }
                                }

                                info!(target: AGG_TRADE_TARGET, "连接 {} 已建立", connection_id);

                                // [新增] 打印完整的连接信息 - 使用 /stream 端点
                                let streams_param = current_streams.join("/");
                                info!(
                                    target: AGG_TRADE_TARGET,
                                    log_type = "low_freq",
                                    connection_id = connection_id,
                                    stream_count = current_streams.len(),
                                    "🔗 AggTradeClient 完整连接信息 - URL: wss://fstream.binance.com:443/stream?streams={}",
                                    streams_param
                                );

                                // 使用 /stream 端点，流已在URL中指定，但仍支持动态订阅
                                info!(target: AGG_TRADE_TARGET, "📋 使用 /stream 端点，品种已在URL中指定: {} 个品种", current_streams.len());
                                if current_streams.len() <= 10 {
                                    info!(target: AGG_TRADE_TARGET, "📋 品种列表: {:?}", current_streams);
                                } else {
                                    info!(target: AGG_TRADE_TARGET, "📋 前10个品种: {:?}", &current_streams[..10]);
                                    info!(target: AGG_TRADE_TARGET, "📋 后10个品种: {:?}", &current_streams[current_streams.len()-10..]);
                                }

                                // 注意：/stream 端点返回combined格式 {"stream":"<streamName>","data":<rawPayload>}
                                info!(target: AGG_TRADE_TARGET, "📋 /stream 端点将返回combined格式的消息");

                                // [新增] 添加定期状态检查
                                let mut status_interval = tokio::time::interval(Duration::from_secs(30));
                                let mut last_message_time = std::time::Instant::now();

                                // 核心消息循环，支持动态订阅
                                'message_loop: loop {


                                    tokio::select! {
                                        // [新增-我的优化] 优先处理网络I/O，而不是随机轮询
                                        biased;

                                        // 定期状态检查
                                        _ = status_interval.tick() => {
                                            let elapsed = last_message_time.elapsed();
                                            // 如果超过2分钟没有收到消息，可能有问题
                                            if elapsed > Duration::from_secs(120) {
                                                warn!(target: WEBSOCKET_CONNECTION_TARGET,
                                                    "⚠️ 连接 {} 超过2分钟未收到消息，可能连接异常", connection_id);
                                            }
                                        },

                                        // 处理WebSocket消息
                                        result = ws.read_frame() => {
                                            match result {
                                                Ok(frame) => {
                                                    match frame.opcode {
                                                        OpCode::Text => {
                                                            let payload = frame.payload.to_vec();

                                                            last_message_time = std::time::Instant::now();
                                                            {
                                                                let mut connections = connections_clone.lock().await;
                                                                if let Some(conn) = connections.get_mut(&connection_id) {
                                                                    conn.message_count += 1;
                                                                }
                                                            }
                                                            // --- 【核心修改】传入正确的 EndpointType 上下文 ---
                                                            if let Err(e) = handler.handle_message(connection_id, &payload, EndpointType::CombinedStream).await {
                                                                warn!(target: AGG_TRADE_TARGET, "Message handler failed: {}", e);
                                                            }
                                                        },
                                                        OpCode::Close => {
                                                            break 'message_loop;
                                                        },
                                                        OpCode::Ping => {
                                                            if let Err(e) = ws.write_frame(Frame::new(true, OpCode::Pong, None, frame.payload)).await {
                                                                error!(target: AGG_TRADE_TARGET, "发送Pong失败: {}", e);
                                                                break 'message_loop;
                                                            }
                                                        },
                                                        OpCode::Pong => {
                                                        },
                                                        _ => {
                                                        }
                                                    }
                                                },
                                                Err(e) => {
                                                    error!(target: AGG_TRADE_TARGET, "WebSocket read error: {}", e);
                                                    break 'message_loop;
                                                }
                                            }
                                        },

                                        // 分支2：处理外部命令
                                        Some(command) = command_rx.recv() => {
                                            match command {
                                                WsCommand::Subscribe(new_symbols) => {
                                                    info!(target: AGG_TRADE_TARGET, "连接 {} 收到动态订阅: {:?}", connection_id, new_symbols);
                                                    let new_streams: Vec<String> = new_symbols.iter()
                                                        .map(|s| format!("{}@aggTrade", s.to_lowercase()))
                                                        .collect();

                                                    let sub_msg = serde_json::json!({
                                                        "method": "SUBSCRIBE",
                                                        "params": &new_streams,
                                                        "id": chrono::Utc::now().timestamp_millis()
                                                    }).to_string();

                                                    info!(
                                                        target: AGG_TRADE_TARGET,
                                                        log_type = "low_freq",
                                                        connection_id = connection_id,
                                                        original_symbols = ?new_symbols,
                                                        formatted_streams = ?new_streams,
                                                        subscription_message = %sub_msg,
                                                        "📤 AggTradeClient 发送动态订阅消息"
                                                    );

                                                    info!(target: AGG_TRADE_TARGET, "🔄 动态订阅: {} 个新品种", new_symbols.len());
                                                    info!(target: AGG_TRADE_TARGET, "🔄 动态订阅消息: {}", sub_msg);
                                                    info!(target: AGG_TRADE_TARGET, "🔄 新品种: {:?}", new_symbols);

                                                    if let Err(e) = ws.write_frame(Frame::text(sub_msg.into_bytes().into())).await {
                                                        error!(target: AGG_TRADE_TARGET, "发送动态订阅失败: {}", e);
                                                        break 'message_loop; // 发送失败，退出重连
                                                    }

                                                    current_streams.extend(new_streams.clone()); // 更新当前连接管理的流

                                                    {
                                                        let mut connections = connections_clone.lock().await;
                                                        if let Some(conn) = connections.get_mut(&connection_id) {
                                                            conn.streams = current_streams.clone();
                                                        }
                                                    }
                                                    info!(target: AGG_TRADE_TARGET, "动态订阅成功，当前管理 {} 个流", current_streams.len());
                                                }

                                                WsCommand::Unsubscribe(remove_symbols) => {
                                                    info!(target: AGG_TRADE_TARGET, "连接 {} 收到动态取消订阅: {:?}", connection_id, remove_symbols);
                                                    let remove_streams: Vec<String> = remove_symbols.iter()
                                                        .map(|s| format!("{}@aggTrade", s.to_lowercase()))
                                                        .collect();

                                                    let unsub_msg = serde_json::json!({
                                                        "method": "UNSUBSCRIBE",
                                                        "params": &remove_streams,
                                                        "id": chrono::Utc::now().timestamp_millis()
                                                    }).to_string();

                                                    info!(
                                                        target: AGG_TRADE_TARGET,
                                                        log_type = "low_freq",
                                                        connection_id = connection_id,
                                                        original_symbols = ?remove_symbols,
                                                        formatted_streams = ?remove_streams,
                                                        unsubscription_message = %unsub_msg,
                                                        "📤 AggTradeClient 发送动态取消订阅消息"
                                                    );

                                                    info!(target: AGG_TRADE_TARGET, "🔄 动态取消订阅: {} 个品种", remove_symbols.len());
                                                    info!(target: AGG_TRADE_TARGET, "🔄 动态取消订阅消息: {}", unsub_msg);
                                                    info!(target: AGG_TRADE_TARGET, "🔄 移除品种: {:?}", remove_symbols);

                                                    if let Err(e) = ws.write_frame(Frame::text(unsub_msg.into_bytes().into())).await {
                                                        error!(target: AGG_TRADE_TARGET, "发送动态取消订阅失败: {}", e);
                                                        break 'message_loop; // 发送失败，退出重连
                                                    }

                                                    // 从当前流列表中移除指定的流
                                                    for stream in &remove_streams {
                                                        current_streams.retain(|s| s != stream);
                                                    }

                                                    {
                                                        let mut connections = connections_clone.lock().await;
                                                        if let Some(conn) = connections.get_mut(&connection_id) {
                                                            conn.streams = current_streams.clone();
                                                        }
                                                    }
                                                    info!(target: AGG_TRADE_TARGET, "动态取消订阅成功，当前管理 {} 个流", current_streams.len());
                                                }
                                            }
                                        }
                                    }
                                }
                            }, // 结束 Ok(mut ws) => { ... } 分支
                            Err(e) => {
                                // 更新状态
                                {
                                    let mut connections = connections_clone.lock().await;
                                    if let Some(conn) = connections.get_mut(&connection_id) {
                                        conn.status = format!("连接失败: {}", e);
                                    }
                                }

                                error!(target: AGG_TRADE_TARGET, "连接 {} 失败: {}", connection_id, e);
                            }
                        }

                        // 重连延迟
                        sleep(Duration::from_secs(5)).await;
                    } // 结束 'reconnect_loop: loop
                })
                .instrument(tracing::info_span!("websocket_connection", connection_id = connection_id))
            );

            // 等待连接任务完成
            if let Err(e) = connection_task.await {
                error!(target: AGG_TRADE_TARGET, "连接任务错误: {}", e);
            }

            info!(target: AGG_TRADE_TARGET, "归集交易客户端已停止");
            Ok(())
        }
    }

    /// 获取连接状态
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send {
        async move {
            let connections = self.connections.lock().await;
            connections.values().cloned().collect()
        }
    }
}

//=============================================================================
// 全市场精简Ticker客户端
//=============================================================================

/// 全市场精简Ticker客户端
pub struct MiniTickerClient {
    config: MiniTickerConfig,
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
    /// 外部注入的消息处理器
    external_handler: Arc<MiniTickerMessageHandler>,
}

impl MiniTickerClient {
    /// 创建一个新的 MiniTickerClient
    #[instrument(skip_all)]
    pub fn new(config: MiniTickerConfig, handler: Arc<MiniTickerMessageHandler>) -> Self {
        Self {
            config,
            connection_id_counter: AtomicUsize::new(1),
            connections: Arc::new(TokioMutex::new(HashMap::new())),
            external_handler: handler,
        }
    }
}

impl WebSocketClient for MiniTickerClient {
    /// 启动客户端，内置健壮的重连循环
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            info!(target: MINI_TICKER_TARGET, "启动全市场精简Ticker客户端 (代理: {})", self.config.use_proxy);

            // 1. 初始化所需资源
            let (use_proxy, proxy_addr, proxy_port) = self.config.get_proxy_settings();
            let connection_manager = ConnectionManager::new(use_proxy, proxy_addr, proxy_port);
            let handler = self.external_handler.clone();
            let connections = self.connections.clone();
            let streams = self.config.get_streams();
            let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);

            // 预先插入连接状态记录
            connections.lock().await.insert(connection_id, WebSocketConnection {
                id: connection_id,
                streams: streams.clone(),
                status: "初始化".to_string(),
                message_count: 0,
            });

            // 2. 启动单个后台任务，该任务包含完整的重连和消息处理逻辑
            tokio::spawn(async move {
                // 这是主重连循环
                'reconnect_loop: loop {
                    // 更新状态为"连接中"
                    connections.lock().await.get_mut(&connection_id).map(|c| c.status = "连接中".to_string());

                    // 尝试连接
                    match connection_manager.connect().await {
                        Ok(mut ws) => {
                            connections.lock().await.get_mut(&connection_id).map(|c| c.status = "已连接".to_string());
                            info!(target: MINI_TICKER_TARGET, "MiniTicker 连接 {} 已建立，准备发送订阅", connection_id);

                            // 发送订阅消息
                            let sub_msg = create_subscribe_message(&streams);
                            if let Err(e) = ws.write_frame(Frame::text(sub_msg.into_bytes().into())).await {
                                error!(target: MINI_TICKER_TARGET, "发送订阅消息失败: {}，准备重连", e);
                                sleep(Duration::from_secs(5)).await;
                                continue 'reconnect_loop;
                            }
                            info!(target: MINI_TICKER_TARGET, "MiniTicker 订阅发送成功");

                            // 进入消息处理循环
                            loop {
                                match ws.read_frame().await {
                                    Ok(frame) => {
                                        match frame.opcode {
                                            OpCode::Text => {
                                                let payload = frame.payload.to_vec();
                                                connections.lock().await.get_mut(&connection_id).map(|c| c.message_count += 1);
                                                // --- 【核心修改】传入正确的 EndpointType 上下文 ---
                                                if let Err(e) = handler.handle_message(connection_id, &payload, EndpointType::SingleStream).await {
                                                    warn!(target: MINI_TICKER_TARGET, "消息处理失败: {}", e);
                                                }
                                            },
                                            OpCode::Close => {
                                                info!(target: MINI_TICKER_TARGET, "服务器关闭连接，准备重连...");
                                                break; // 退出消息循环，进入重连逻辑
                                            },
                                            OpCode::Ping => {
                                                let pong = Frame::pong(frame.payload);
                                                if let Err(e) = ws.write_frame(pong).await {
                                                    error!(target: MINI_TICKER_TARGET, "发送Pong失败: {}", e);
                                                    break;
                                                }
                                            },
                                            _ => {}
                                        }
                                    },
                                    Err(e) => {
                                        error!(target: MINI_TICKER_TARGET, "WebSocket读取错误: {}，准备重连", e);
                                        break; // 退出消息循环，进入重连逻辑
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            connections.lock().await.get_mut(&connection_id).map(|c| c.status = format!("连接失败: {}", e));
                            error!(target: MINI_TICKER_TARGET, "MiniTicker 连接 {} 失败: {}，准备重试", connection_id, e);
                        }
                    }

                    // 任何断开或失败后，都等待5秒再重试
                    sleep(Duration::from_secs(5)).await;
                }
            }.instrument(tracing::info_span!("mini_ticker_connection", id = connection_id)));

            Ok(())
        }
    }

    /// 获取连接状态
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send {
        async move {
            self.connections.lock().await.values().cloned().collect()
        }
    }
}