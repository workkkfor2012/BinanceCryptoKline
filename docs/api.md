# API接口文档

## BinanceApi客户端

### 初始化
```rust
// 使用默认URL (https://fapi.binance.com)
let api = BinanceApi::new();

// 使用自定义URL
let api = BinanceApi::new_with_url("https://fapi.binance.com".to_string());
```

### 主要接口方法

#### 1. 获取交易所信息
```rust
pub async fn get_exchange_info(&self) -> Result<ExchangeInfo>
```
- **端点**: `/fapi/v1/exchangeInfo`
- **返回**: 交易所信息，包含所有交易对详情

#### 2. 获取U本位永续合约交易对
```rust
pub async fn get_trading_usdt_perpetual_symbols(&self) -> Result<Vec<String>>
```
- **功能**: 获取所有正在交易的U本位永续合约
- **过滤条件**: 以USDT结尾 + 状态为TRADING + 合约类型为PERPETUAL
- **重试**: 最多5次，间隔1秒
- **返回**: 交易对列表 (如: ["BTCUSDT", "ETHUSDT", ...])

#### 3. 下载连续合约K线数据
```rust
pub async fn download_continuous_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>>
```
- **端点**: `/fapi/v1/continuousKlines`
- **参数**: DownloadTask结构体，自动设置contractType=PERPETUAL
- **返回**: 连续合约K线数据列表

#### 4. 获取服务器时间
```rust
pub async fn get_server_time(&self) -> Result<ServerTime>
```
- **端点**: `/fapi/v1/time`
- **重试**: 最多5次，间隔1秒
- **返回**: 服务器时间戳

## 数据结构

### DownloadTask - 下载任务
```rust
pub struct DownloadTask {
    pub symbol: String,           // 交易对 (如: BTCUSDT)
    pub interval: String,         // 时间周期 (如: 1m, 5m, 1h)
    pub start_time: Option<i64>,  // 开始时间戳(毫秒)，可选
    pub end_time: Option<i64>,    // 结束时间戳(毫秒)，可选
    pub limit: usize,            // 每次请求的K线数量 (最大1000)
}
```

### ExchangeInfo - 交易所信息
```rust
pub struct ExchangeInfo {
    pub exchange_filters: Vec<serde_json::Value>,  // 交易所过滤器
    pub rate_limits: Vec<RateLimit>,               // 频率限制
    pub server_time: i64,                          // 服务器时间
    pub assets: Vec<serde_json::Value>,            // 资产信息
    pub symbols: Vec<Symbol>,                      // 交易对列表
    pub timezone: String,                          // 时区
}
```

### Symbol - 交易对信息
```rust
pub struct Symbol {
    pub symbol: String,              // 交易对名称
    pub status: String,              // 交易状态 (TRADING/BREAK等)
    pub base_asset: String,          // 基础资产
    pub quote_asset: String,         // 报价资产
    pub margin_asset: String,        // 保证金资产
    pub price_precision: i32,        // 价格精度
    pub quantity_precision: i32,     // 数量精度
    pub base_asset_precision: i32,   // 基础资产精度
    pub quote_precision: i32,        // 报价精度
    pub contract_type: String,       // 合约类型 (PERPETUAL等)
    pub delivery_date: i64,          // 交割日期
    pub onboard_date: i64,           // 上币日期
    // ... 其他字段
}
```

### ServerTime - 服务器时间
```rust
pub struct ServerTime {
    pub server_time: i64,  // 服务器时间戳(毫秒)
}
```

### RateLimit - 频率限制
```rust
pub struct RateLimit {
    pub rate_limit_type: String,  // 限制类型 (REQUEST_WEIGHT/ORDERS)
    pub interval: String,         // 时间间隔 (MINUTE)
    pub interval_num: i32,        // 间隔数量
    pub limit: i32,              // 限制数量
}
```

## 工具函数

### 时间间隔转换
```rust
pub fn interval_to_milliseconds(interval: &str) -> i64
```
- **功能**: 将时间间隔字符串转换为毫秒数
- **示例**: "1m" → 60000, "1h" → 3600000, "1d" → 86400000

### 时间对齐
```rust
pub fn get_aligned_time(timestamp_ms: i64, interval: &str) -> i64
```
- **功能**: 获取对齐到特定周期的时间戳
- **规则**:
  - 分钟K线(1m,5m,30m): 对齐到分钟00秒
  - 小时K线(1h,4h): 对齐到小时00分00秒
  - 日K线(1d): 对齐到UTC 00:00:00
  - 周K线(1w): 对齐到周一UTC 00:00:00

## 支持的时间周期
- `1m` - 1分钟
- `5m` - 5分钟
- `30m` - 30分钟
- `1h` - 1小时
- `4h` - 4小时
- `1d` - 1天
- `1w` - 1周

## 配置参数
- **API端点**: https://fapi.binance.com (币安期货API)
- **超时设置**: 连接超时10秒，请求超时30秒
- **代理支持**: 自动检测并使用SOCKS5代理
- **连接池**: 禁用连接池，每次请求创建新连接
- **重试机制**: 关键接口支持最多5次重试
- **User-Agent**: 模拟Chrome浏览器

## 错误处理
- **网络错误**: 自动重试(适用于特定接口)
- **HTTP错误**: 返回详细的状态码和错误信息
- **JSON解析错误**: 返回解析失败的详细信息
- **数据验证错误**: 返回数据格式或内容错误信息

## 使用示例

### 获取交易对列表
```rust
let api = BinanceApi::new();
let symbols = api.get_trading_usdt_perpetual_symbols().await?;
println!("获取到 {} 个交易对", symbols.len());
```

### 下载连续合约K线数据
```rust
let task = DownloadTask {
    symbol: "BTCUSDT".to_string(),
    interval: "1m".to_string(),
    start_time: Some(1640995200000), // 2022-01-01 00:00:00
    end_time: Some(1641081600000),   // 2022-01-02 00:00:00
    limit: 1000,
};
let klines = api.download_continuous_klines(&task).await?;
```

### 获取服务器时间
```rust
let server_time = api.get_server_time().await?;
println!("服务器时间: {}", server_time.server_time);
```
