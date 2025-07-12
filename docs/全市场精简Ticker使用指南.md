# 全市场精简Ticker使用指南

## 概述

全市场精简Ticker功能允许您实时接收币安期货所有交易对的24小时精简ticker信息。该功能基于币安的 `!miniTicker@arr` WebSocket流实现。

## 数据格式

每个ticker包含以下信息：

```rust
pub struct MiniTickerData {
    pub event_type: String,           // 事件类型: "24hrMiniTicker"
    pub event_time: u64,              // 事件时间(毫秒)
    pub symbol: String,               // 交易对
    pub close_price: String,          // 最新成交价格
    pub open_price: String,           // 24小时前开始第一笔成交价格
    pub high_price: String,           // 24小时内最高成交价
    pub low_price: String,            // 24小时内最低成交价
    pub total_traded_volume: String,  // 成交量
    pub total_traded_quote_volume: String, // 成交额
}
```

## 基本使用方法

### 1. 创建数据接收通道

```rust
use tokio::sync::mpsc;
use kline_server::klcommon::websocket::MiniTickerData;

let (data_sender, mut data_receiver) = mpsc::unbounded_channel::<Vec<MiniTickerData>>();
```

### 2. 创建消息处理器

```rust
use std::sync::Arc;
use kline_server::klcommon::websocket::MiniTickerMessageHandler;

let message_handler = Arc::new(MiniTickerMessageHandler::new(data_sender));
```

### 3. 配置客户端

```rust
use kline_server::klcommon::websocket::MiniTickerConfig;

// 使用默认配置（包含代理设置）
let config = MiniTickerConfig::default();

// 或者自定义配置
let config = MiniTickerConfig {
    use_proxy: true,
    proxy_addr: "127.0.0.1".to_string(),
    proxy_port: 7890,
};
```

### 4. 创建并启动客户端

```rust
use kline_server::klcommon::websocket::{MiniTickerClient, WebSocketClient};

let mut client = MiniTickerClient::new(config, message_handler);

// 启动客户端（这是一个异步操作）
tokio::spawn(async move {
    if let Err(e) = client.start().await {
        eprintln!("客户端启动失败: {}", e);
    }
});
```

### 5. 处理接收到的数据

```rust
while let Some(tickers) = data_receiver.recv().await {
    for ticker in tickers {
        println!(
            "{}: {} -> {} (成交量: {})",
            ticker.symbol,
            ticker.open_price,
            ticker.close_price,
            ticker.total_traded_volume
        );
    }
}
```

## 完整示例

参见 `examples/mini_ticker_example.rs` 文件，其中包含了一个完整的使用示例。

运行示例：

```bash
cargo run --example mini_ticker_example
```

## 特性说明

### 数据更新频率
- 更新速度：1000ms（1秒）
- 只有发生变化的ticker才会被推送

### 连接管理
- 自动重连机制
- 心跳保持连接
- 错误处理和日志记录

### 代理支持
- 支持SOCKS5代理
- 可配置代理地址和端口
- 默认启用代理（适合国内网络环境）

## 注意事项

1. **数据量大**：全市场ticker包含所有交易对，数据量较大，请确保您的应用能够处理高频数据流。

2. **网络稳定性**：建议在稳定的网络环境下使用，如果网络不稳定可能会影响数据接收。

3. **内存使用**：由于数据量大，请注意监控内存使用情况，及时处理接收到的数据。

4. **错误处理**：建议在生产环境中添加适当的错误处理和重试机制。

## 与其他功能的集成

MiniTicker客户端可以与系统中的其他组件集成：

- 可以将ticker数据存储到数据库
- 可以基于ticker数据进行价格监控和报警
- 可以与K线数据结合进行技术分析

## 故障排除

### 常见问题

1. **连接失败**：检查网络连接和代理设置
2. **数据解析错误**：检查币安API是否有变更
3. **内存占用过高**：优化数据处理逻辑，及时释放不需要的数据

### 日志调试

启用详细日志来调试问题：

```rust
tracing_subscriber::fmt()
    .with_target(true)
    .with_level(true)
    .with_max_level(tracing::Level::DEBUG)
    .init();
```
