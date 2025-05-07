# WebSocket重构实施计划

> **注意**: 本计划已更新，反映了最新的实现方式。详细进度请参阅 [progress_update.md](./progress_update.md)。

## 第一阶段：创建通用WebSocket模块（已完成）

### 1. 在klcommon中创建websocket模块

原计划使用多文件结构：
```
src/klcommon/websocket/
├── mod.rs           # 模块入口，导出公共接口
├── config.rs        # 配置相关定义
├── connection.rs    # 连接管理
├── message.rs       # 消息处理
└── client.rs        # 客户端接口
```

实际实现使用单文件结构：
```
src/klcommon/
└── websocket.rs     # 包含所有WebSocket功能的单一文件
```

### 2. 定义通用接口（已完成）

在`websocket.rs`中定义通用接口：

```rust
pub trait WebSocketConfig {
    fn get_proxy_settings(&self) -> (bool, String, u16);
    fn get_streams(&self) -> Vec<String>;
}

pub trait WebSocketClient {
    // 注意：使用 impl Future 而不是 async fn，以避免编译警告
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send;
}

pub trait MessageHandler {
    // 注意：使用 impl Future 而不是 async fn，以避免编译警告
    fn handle_message(&self, connection_id: usize, message: String) -> impl std::future::Future<Output = Result<()>> + Send;
}
```

### 3. 实现通用连接管理（已完成）

在`websocket.rs`中实现通用的连接管理器：

```rust
pub struct ConnectionManager {
    use_proxy: bool,
    proxy_addr: String,
    proxy_port: u16,
}

impl ConnectionManager {
    pub fn new(use_proxy: bool, proxy_addr: String, proxy_port: u16) -> Self { ... }
    pub async fn connect(&self, streams: &[String]) -> Result<WebSocketStream> { ... }
    pub async fn handle_messages(&self, connection_id: usize, ws_stream: &mut WebSocketStream, tx: mpsc::Sender<(usize, String)>, connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>) { ... }
}
```

## 第二阶段：实现具体的WebSocket客户端（部分完成）

### 1. 实现连续合约K线客户端（已完成）

```rust
pub struct ContinuousKlineConfig {
    pub use_proxy: bool,
    pub proxy_addr: String,
    pub proxy_port: u16,
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
}

impl WebSocketConfig for ContinuousKlineConfig {
    fn get_proxy_settings(&self) -> (bool, String, u16) {
        (self.use_proxy, self.proxy_addr.clone(), self.proxy_port)
    }

    fn get_streams(&self) -> Vec<String> {
        let mut streams = Vec::new();
        for symbol in &self.symbols {
            for interval in &self.intervals {
                let stream = format!("{}_perpetual@continuousKline_{}", symbol.to_lowercase(), interval);
                streams.push(stream);
            }
        }
        streams
    }
}

pub struct ContinuousKlineClient {
    config: ContinuousKlineConfig,
    db: Arc<Database>,
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
}

impl WebSocketClient for ContinuousKlineClient {
    // 注意：使用 impl Future 而不是 async fn
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // 实现细节...
            Ok(())
        }
    }

    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send {
        async move {
            let connections = self.connections.lock().await;
            connections.values().cloned().collect()
        }
    }
}
```

### 2. 实现归集交易客户端（占位符已完成）

```rust
pub struct AggTradeConfig {
    pub use_proxy: bool,
    pub proxy_addr: String,
    pub proxy_port: u16,
    pub symbols: Vec<String>,
}

impl WebSocketConfig for AggTradeConfig {
    fn get_proxy_settings(&self) -> (bool, String, u16) {
        (self.use_proxy, self.proxy_addr.clone(), self.proxy_port)
    }

    fn get_streams(&self) -> Vec<String> {
        self.symbols.iter()
            .map(|symbol| format!("{}@aggTrade", symbol.to_lowercase()))
            .collect()
    }
}

pub struct AggTradeClient {
    config: AggTradeConfig,
    db: Arc<Database>,
    connection_id_counter: AtomicUsize,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
}

impl WebSocketClient for AggTradeClient {
    // 注意：目前只是占位符实现
    fn start(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            info!("归集交易客户端尚未实现");
            Ok(())
        }
    }

    fn get_connections(&self) -> impl std::future::Future<Output = Vec<WebSocketConnection>> + Send {
        async move {
            let connections = self.connections.lock().await;
            connections.values().cloned().collect()
        }
    }
}
```

## 第三阶段：实现消息处理（部分完成）

### 1. 实现连续合约K线消息处理（已完成）

```rust
pub struct ContinuousKlineMessageHandler {
    db: Arc<Database>,
}

impl MessageHandler for ContinuousKlineMessageHandler {
    fn handle_message(&self, _connection_id: usize, text: String) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // 解析消息
            match parse_message(&text) {
                Ok(Some((symbol, interval, kline_data))) => {
                    // 处理K线数据
                    process_kline_data(&symbol, &interval, &kline_data, &self.db).await;
                }
                Ok(None) => {
                    // 非K线消息，忽略
                }
                Err(e) => {
                    error!("解析消息失败: {}", e);
                }
            }
            Ok(())
        }
    }
}
```

### 2. 实现归集交易消息处理（待实现）

```rust
// 待实现
pub struct AggTradeMessageHandler {
    db: Arc<Database>,
}

impl MessageHandler for AggTradeMessageHandler {
    fn handle_message(&self, _connection_id: usize, message: String) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // 解析归集交易消息
            // 将归集交易数据转换为K线数据
            // 处理K线数据
            Ok(())
        }
    }
}
```

## 第四阶段：修改kldata模块（已完成）

### 1. 删除kldata/streamer模块中的WebSocket连接实现（已完成）

### 2. 在kldata模块中使用klcommon/websocket模块（已完成）

```rust
// src/kldata/mod.rs
pub mod downloader;
pub mod aggregator;
pub mod backfill;

// 重新导出常用模块，方便使用
pub use downloader::{Downloader, Config};
pub use crate::klcommon::websocket::{ContinuousKlineClient, ContinuousKlineConfig};
pub use backfill::KlineBackfiller;
```

## 第五阶段：实现归集交易数据处理（待实现）

### 1. 实现从归集交易数据生成K线数据的逻辑

```rust
pub struct KlineBuilder {
    symbol: String,
    interval: String,
    open_time: i64,
    close_time: i64,
    open: Option<String>,
    high: Option<String>,
    low: Option<String>,
    close: Option<String>,
    volume: f64,
    quote_volume: f64,
    number_of_trades: i64,
    taker_buy_volume: f64,
    taker_buy_quote_volume: f64,
}

impl KlineBuilder {
    pub fn new(symbol: String, interval: String, open_time: i64) -> Self { ... }
    pub fn add_trade(&mut self, trade: &AggTrade) { ... }
    pub fn build(self) -> Kline { ... }
}
```

### 2. 处理归集交易数据与连续合约K线数据的差异

- 实现时间戳计算逻辑，确定交易属于哪个K线周期
- 实现K线数据的聚合逻辑，包括开盘价、最高价、最低价、收盘价的计算
- 实现成交量的累加逻辑
