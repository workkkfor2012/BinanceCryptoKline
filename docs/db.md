# 数据库字段定义

## 数据表结构

### 1. symbols表
```sql
CREATE TABLE symbols (
    symbol TEXT PRIMARY KEY,           -- 交易对符号 (BTCUSDT)
    base_asset TEXT,                   -- 基础资产 (BTC)
    quote_asset TEXT,                  -- 报价资产 (USDT)
    status TEXT,                       -- 交易状态
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. K线数据表 (分表存储)
表名格式：`k_{symbol}_{interval}` (如: k_btc_1m, k_eth_5m)

```sql
CREATE TABLE k_{symbol}_{interval} (
    open_time INTEGER PRIMARY KEY,     -- K线开盘时间戳(毫秒)
    open TEXT NOT NULL,                -- 开盘价
    high TEXT NOT NULL,                -- 最高价
    low TEXT NOT NULL,                 -- 最低价
    close TEXT NOT NULL,               -- 收盘价
    volume TEXT NOT NULL,              -- 成交量
    close_time INTEGER NOT NULL,       -- K线收盘时间戳(毫秒)
    quote_asset_volume TEXT NOT NULL   -- 报价资产成交量(成交额)
);
```

## 数据结构

### Kline结构体 (数据库存储格式)
```rust
pub struct Kline {
    pub open_time: i64,                          // K线开盘时间戳(毫秒)
    pub open: String,                            // 开盘价
    pub high: String,                            // 最高价
    pub low: String,                             // 最低价
    pub close: String,                           // 收盘价
    pub volume: String,                          // 成交量
    pub close_time: i64,                         // K线收盘时间戳(毫秒)
    pub quote_asset_volume: String,              // 报价资产成交量
    pub number_of_trades: i64,                   // 成交笔数
    pub taker_buy_base_asset_volume: String,     // 主动买入基础资产成交量
    pub taker_buy_quote_asset_volume: String,    // 主动买入报价资产成交量
    pub ignore: String,                          // 忽略字段
}
```

### KlineBar结构体 (内存计算格式)
```rust
pub struct KlineBar {
    pub symbol: String,                    // 交易对
    pub period_ms: i64,                    // K线周期(毫秒)
    pub open_time_ms: i64,                 // 开盘时间戳(毫秒)
    pub open: f64,                         // 开盘价
    pub high: f64,                         // 最高价
    pub low: f64,                          // 最低价
    pub close: f64,                        // 收盘价
    pub volume: f64,                       // 成交量
    pub turnover: f64,                     // 成交额
    pub number_of_trades: i64,             // 成交笔数
    pub taker_buy_volume: f64,             // 主动买入成交量
    pub taker_buy_quote_volume: f64,       // 主动买入成交额
}
```

## 主要操作接口

```rust
// 保存K线数据
pub fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline]) -> Result<usize>

// 获取最新时间戳
pub fn get_latest_kline_timestamp(&self, symbol: &str, interval: &str) -> Result<Option<i64>>

// 获取K线数量
pub fn get_kline_count(&self, symbol: &str, interval: &str) -> Result<i64>

// 查询最新K线
pub fn get_latest_klines(&self, symbol: &str, interval: &str, limit: usize) -> Result<Vec<Kline>>

// 查询时间范围K线
pub fn get_klines_in_range(&self, symbol: &str, interval: &str, start_time: i64, end_time: i64) -> Result<Vec<Kline>>
```

## 表命名规则

- 交易对处理: `BTCUSDT` → `btc` (移除USDT后缀，转小写)
- 时间周期: `1M` → `1m` (转小写)
- 表名格式: `k_{symbol}_{interval}`

**示例:**
- BTCUSDT + 1m → k_btc_1m
- ETHUSDT + 5m → k_eth_5m

## 支持的时间周期
1m, 5m, 30m, 1h, 4h, 1d, 1w

## 关键配置
- 数据库: SQLite with WAL mode
- 连接池: 10个连接
- 写入队列: 5000任务缓冲
- 价格字段: TEXT类型保持精度
- 主键: open_time (防重复)
