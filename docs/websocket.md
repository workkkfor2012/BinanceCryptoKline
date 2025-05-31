# WebSocket接口文档

## WebSocket客户端

### 连接配置
- **WebSocket URL**: `wss://fstream.binance.com/ws`
- **代理支持**: SOCKS5代理 (127.0.0.1:1080)
- **TLS加密**: 支持TLS连接
- **每连接最大流数**: 1个流

## 主要客户端类型

### 1. ContinuousKlineClient - 连续合约K线客户端

#### 初始化
```rust
let config = ContinuousKlineConfig {
    use_proxy: true,
    proxy_addr: "127.0.0.1".to_string(),
    proxy_port: 1080,
    symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
    intervals: vec!["1m".to_string(), "5m".to_string()],
};
let client = ContinuousKlineClient::new(config, db);
```

#### 主要方法
```rust
// 启动客户端
pub async fn start(&mut self) -> Result<()>

// 获取连接状态
pub async fn get_connections(&self) -> Vec<WebSocketConnection>
```

#### 流格式
连续合约K线流格式：`{pair}_perpetual@continuousKline_{interval}`
- 示例: `btcusdt_perpetual@continuousKline_1m`

### 2. AggTradeClient - 归集交易客户端

#### 初始化
```rust
let config = AggTradeConfig {
    use_proxy: true,
    proxy_addr: "127.0.0.1".to_string(),
    proxy_port: 1080,
    symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
};
let client = AggTradeClient::new(config, db, intervals);
```

#### 流格式
归集交易流格式：`{symbol}@aggTrade`
- 示例: `btcusdt@aggTrade`

## 配置结构

### ContinuousKlineConfig
```rust
pub struct ContinuousKlineConfig {
    pub use_proxy: bool,        // 是否使用代理
    pub proxy_addr: String,     // 代理地址
    pub proxy_port: u16,        // 代理端口
    pub symbols: Vec<String>,   // 交易对列表
    pub intervals: Vec<String>, // K线周期列表
}
```

### AggTradeConfig
```rust
pub struct AggTradeConfig {
    pub use_proxy: bool,        // 是否使用代理
    pub proxy_addr: String,     // 代理地址
    pub proxy_port: u16,        // 代理端口
    pub symbols: Vec<String>,   // 交易对列表
}
```

### WebSocketConnection - 连接状态
```rust
pub struct WebSocketConnection {
    pub id: usize,              // 连接ID
    pub streams: Vec<String>,   // 订阅的流列表
    pub status: String,         // 连接状态
    pub message_count: usize,   // 消息计数
}
```

## 数据结构

### KlineData - WebSocket K线数据
```rust
pub struct KlineData {
    pub start_time: i64,                    // K线开始时间戳(毫秒)
    pub end_time: i64,                      // K线结束时间戳(毫秒)
    pub interval: String,                   // 时间周期
    pub first_trade_id: i64,                // 首个交易ID
    pub last_trade_id: i64,                 // 最后交易ID
    pub open: String,                       // 开盘价
    pub close: String,                      // 收盘价
    pub high: String,                       // 最高价
    pub low: String,                        // 最低价
    pub volume: String,                     // 成交量
    pub number_of_trades: i64,              // 成交笔数
    pub is_closed: bool,                    // 是否收盘
    pub quote_volume: String,               // 报价资产成交量
    pub taker_buy_volume: String,           // 主动买入成交量
    pub taker_buy_quote_volume: String,     // 主动买入报价成交量
    pub ignore: String,                     // 忽略字段
}
```

### BinanceRawAggTrade - 原始归集交易数据
```rust
pub struct BinanceRawAggTrade {
    pub symbol: String,         // 交易对
    pub price: String,          // 成交价格
    pub quantity: String,       // 成交量
    pub trade_time: i64,        // 成交时间(毫秒)
    pub agg_id: i64,           // 归集成交ID
    pub first_trade_id: i64,    // 首个交易ID
    pub last_trade_id: i64,     // 最后交易ID
    pub is_buyer_maker: bool,   // 买方是否做市方
}
```

### AppAggTrade - 应用内部交易数据
```rust
pub struct AppAggTrade {
    pub symbol: String,         // 交易对
    pub price: f64,            // 成交价格
    pub quantity: f64,         // 成交量
    pub timestamp_ms: i64,     // 成交时间戳(毫秒)
    pub is_buyer_maker: bool,  // 买方是否做市方
}
```

## 连接管理

### ConnectionManager
```rust
// 创建连接管理器
let manager = ConnectionManager::new(use_proxy, proxy_addr, proxy_port);

// 连接到WebSocket
let ws = manager.connect(&streams).await?;

// 处理消息
manager.handle_messages(connection_id, &mut ws, tx, connections).await;
```

### 连接特性
- **自动重连**: 连接断开时自动重连
- **心跳检测**: 30秒超时检测，自动发送Ping
- **代理支持**: 支持SOCKS5代理连接
- **TLS加密**: 使用TLS加密连接
- **消息缓冲**: 1000条消息缓冲区

## 消息处理

### MessageHandler接口
```rust
pub trait MessageHandler {
    async fn handle_message(&self, connection_id: usize, message: String) -> Result<()>;
}
```

### 消息处理流程
1. **接收消息**: 从WebSocket接收原始消息
2. **解析消息**: 解析JSON格式的消息
3. **数据转换**: 转换为内部数据结构
4. **数据库操作**: 保存或更新数据库

### 连续合约K线消息处理
```rust
// 解析连续合约K线消息
fn parse_message(text: &str) -> Result<Option<(String, String, KlineData)>>

// 处理K线数据
async fn process_kline_data(symbol: &str, interval: &str, kline_data: &KlineData, db: &Arc<Database>)
```

## 订阅消息格式

### 单流订阅
直接连接格式：`/ws/{stream}`
- 示例: `/ws/btcusdt_perpetual@continuousKline_1m`

### 多流订阅
组合流格式：`/stream?streams={stream1}/{stream2}/...`
- 需要发送订阅消息：
```json
{
    "method": "SUBSCRIBE",
    "params": ["stream1", "stream2"],
    "id": 1
}
```

## 支持的时间周期
- `1m` - 1分钟
- `5m` - 5分钟
- `30m` - 30分钟
- `1h` - 1小时
- `4h` - 4小时
- `1d` - 1天
- `1w` - 1周

## 错误处理
- **连接失败**: 自动重试连接
- **消息解析失败**: 记录错误并继续处理
- **数据库操作失败**: 记录错误并继续处理
- **网络超时**: 发送Ping保持连接

## 性能特性
- **并发连接**: 支持多个并发WebSocket连接
- **消息缓冲**: 1000条消息异步处理缓冲
- **无锁设计**: 使用原子操作和通道进行线程间通信
- **内存优化**: 高效的消息解析和数据转换

## 使用示例

### 启动连续合约K线客户端
```rust
let config = ContinuousKlineConfig {
    use_proxy: true,
    proxy_addr: "127.0.0.1".to_string(),
    proxy_port: 1080,
    symbols: vec!["BTCUSDT".to_string()],
    intervals: vec!["1m".to_string()],
};
let mut client = ContinuousKlineClient::new(config, db);
client.start().await?;
```

### 启动归集交易客户端
```rust
let config = AggTradeConfig {
    use_proxy: true,
    proxy_addr: "127.0.0.1".to_string(),
    proxy_port: 1080,
    symbols: vec!["BTCUSDT".to_string()],
};
let mut client = AggTradeClient::new(config, db, intervals);
client.start().await?;
```
