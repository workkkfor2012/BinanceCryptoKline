# WebSocket连接重构

> **进度更新**: 查看[最新进度](./progress_update.md)和[更新后的实施计划](./implementation_plan.md)

## 背景

当前系统使用连续合约K线WebSocket连接获取实时K线数据。我们需要将其修改为使用归集交易(aggTrade)数据流，以获取更精确的交易数据。

## 重构目标

1. 将WebSocket连接管理逻辑从`kldata`模块移动到`klcommon`模块，使其成为一个通用的WebSocket连接管理组件
2. 支持多种WebSocket数据流类型，包括连续合约K线和归集交易
3. 保持良好的扩展性，以便将来可以添加其他类型的WebSocket连接
4. 将`kldata`中的连续合约K线WebSocket连接替换为归集交易WebSocket连接

## 重构步骤

### 第一步：分析当前实现

当前的WebSocket连接实现位于`kldata/streamer`模块中，主要包括以下组件：

- `ContinuousKlineClient`: 连续合约K线客户端，负责管理WebSocket连接
- `ConnectionManager`: 连接管理器，负责建立和维护WebSocket连接
- `process_messages`: 消息处理函数，负责解析和处理WebSocket消息

### 第二步：设计新的WebSocket模块

在`klcommon`模块中创建一个新的`websocket`模块，包括以下组件：

- `WebSocketClient`: 通用的WebSocket客户端接口
- `WebSocketConfig`: 通用的WebSocket配置接口
- `WebSocketConnection`: WebSocket连接状态
- `ConnectionManager`: 通用的连接管理器
- `MessageHandler`: 消息处理接口

### 第三步：实现具体的WebSocket客户端

基于通用接口，实现两种具体的WebSocket客户端：

- `ContinuousKlineClient`: 连续合约K线客户端
- `AggTradeClient`: 归集交易客户端

### 第四步：修改kldata模块

1. 删除`kldata/streamer`模块中的WebSocket连接实现
2. 在`kldata`模块中使用`klcommon/websocket`模块提供的WebSocket客户端

### 第五步：实现归集交易数据处理

1. 实现从归集交易数据生成K线数据的逻辑
2. 处理归集交易数据与连续合约K线数据的差异

## 技术细节

### 归集交易数据与连续合约K线数据的差异

1. **数据结构不同**：
   - 连续合约K线数据是完整的K线数据，包含开盘价、最高价、最低价、收盘价等
   - 归集交易数据是单个交易数据，只包含交易价格和数量

2. **数据处理不同**：
   - 连续合约K线数据可以直接使用，只需根据`is_closed`字段决定是插入新记录还是更新现有记录
   - 归集交易数据需要聚合成K线数据，需要自行计算开盘价、最高价、最低价、收盘价等

3. **成交量处理不同**：
   - 连续合约K线数据中的成交量是累计值，需要计算增量
   - 归集交易数据中的成交量是单个交易的成交量，可以直接累加

### 新的WebSocket模块设计

#### 通用接口

```rust
/// WebSocket配置接口
pub trait WebSocketConfig {
    /// 获取代理设置
    fn get_proxy_settings(&self) -> (bool, String, u16);
    /// 获取流列表
    fn get_streams(&self) -> Vec<String>;
}

/// WebSocket客户端接口
pub trait WebSocketClient {
    /// 创建新的WebSocket客户端
    fn new(config: impl WebSocketConfig, db: Arc<Database>) -> Self;
    /// 启动WebSocket客户端
    async fn start(&mut self) -> Result<()>;
    /// 获取连接状态
    async fn get_connections(&self) -> Vec<WebSocketConnection>;
}

/// 消息处理接口
pub trait MessageHandler {
    /// 处理WebSocket消息
    async fn handle_message(&self, message: String) -> Result<()>;
}
```

#### 具体实现

```rust
/// 连续合约K线配置
pub struct ContinuousKlineConfig {
    pub use_proxy: bool,
    pub proxy_addr: String,
    pub proxy_port: u16,
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
}

/// 归集交易配置
pub struct AggTradeConfig {
    pub use_proxy: bool,
    pub proxy_addr: String,
    pub proxy_port: u16,
    pub symbols: Vec<String>,
}

/// 连续合约K线客户端
pub struct ContinuousKlineClient {
    config: ContinuousKlineConfig,
    db: Arc<Database>,
    // ...
}

/// 归集交易客户端
pub struct AggTradeClient {
    config: AggTradeConfig,
    db: Arc<Database>,
    // ...
}
```

## 注意事项

1. 归集交易数据是近似tick数据，需要自行计算K线数据
2. 对于1分钟K线，没有新K线标志，需要根据时间戳计算
3. 成交量是纯增量，不需要计算增量
4. 需要保持与现有系统的兼容性
