# 归集交易(aggTrade)与连续合约K线(continuousKline)对比

## 归集交易数据流描述

归集交易(aggTrade)是币安提供的一种高频交易数据流，具有以下特点：

- **数据聚合**：同一价格、同一方向、同一时间(100ms计算)的trade会被聚合为一条
- **Stream名称**：`<symbol>@aggTrade`（例如：`btcusdt@aggTrade`）
- **更新速度**：约100ms

### WebSocket连接方式

- **单一stream**：`wss://fstream.binance.com/ws/<streamName>`
  例如：`wss://fstream.binance.com/ws/btcusdt@aggTrade`

- **组合streams**：`wss://fstream.binance.com/stream?streams=<streamName1>/<streamName2>/<streamName3>`
  例如：`wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade`

> 注意：组合streams时，事件payload会以这样的格式封装 `{"stream":"<streamName>","data":<rawPayload>}`

## 数据结构对比

### 连续合约K线(continuousKline)

```json
{
  "e": "continuous_kline",     // 事件类型
  "E": 123456789,              // 事件时间
  "ps": "BTCUSDT",             // 交易对
  "ct": "PERPETUAL",           // 合约类型
  "k": {
    "t": 123400000,            // 这根K线的起始时间
    "T": 123460000,            // 这根K线的结束时间
    "i": "1m",                 // K线间隔
    "f": 100,                  // 这根K线期间第一笔成交ID
    "L": 200,                  // 这根K线期间末一笔成交ID
    "o": "0.0010",             // 开盘价
    "c": "0.0020",             // 收盘价
    "h": "0.0025",             // 最高价
    "l": "0.0015",             // 最低价
    "v": "1000",               // 成交量
    "n": 100,                  // 成交笔数
    "x": false,                // 这根K线是否完结(是否已经开始了下一根K线)
    "q": "1.0000",             // 成交额
    "V": "500",                // 主动买入的成交量
    "Q": "0.500",              // 主动买入的成交额
    "B": "123456"              // 忽略此参数
  }
}
```

### 归集交易(aggTrade)

```json
{
  "e": "aggTrade",             // 事件类型
  "E": 123456789,              // 事件时间
  "s": "BTCUSDT",              // 交易对
  "a": 5933014,                // 归集成交ID
  "p": "0.001",                // 成交价格
  "q": "100",                  // 成交量
  "f": 100,                    // 被归集的首个交易ID
  "l": 105,                    // 被归集的末次交易ID
  "T": 123456785,              // 成交时间
  "m": true                    // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
}
```

## 主要差异

1. **数据完整性**：
   - 连续合约K线提供完整的K线数据，包括开盘价、最高价、最低价、收盘价等
   - 归集交易只提供单个交易的数据，需要自行聚合成K线

2. **更新频率**：
   - 连续合约K线每秒更新一次
   - 归集交易实时更新，更新频率更高（约100ms）

3. **K线标志**：
   - 连续合约K线有`x`字段表示K线是否完结
   - 归集交易没有K线完结标志，需要根据时间戳自行判断

4. **成交量处理**：
   - 连续合约K线的成交量是累计值，需要计算增量
   - 归集交易的成交量是单个交易的成交量，可以直接累加

## 从归集交易生成K线的步骤

1. **确定K线周期**：
   根据交易时间戳确定该交易属于哪个K线周期
   ```rust
   let interval_ms = 60 * 1000; // 1分钟 = 60,000毫秒
   let open_time = (trade_time / interval_ms) * interval_ms;
   let close_time = open_time + interval_ms - 1;
   ```

2. **更新K线数据**：
   - 如果是该周期的第一笔交易，设置开盘价
   - 更新最高价和最低价
   - 更新收盘价
   - 累加成交量和成交额
   - 增加成交笔数

3. **处理K线完结**：
   当收到的交易时间戳超过当前K线的结束时间，表示当前K线已完结，需要：
   - 保存当前K线数据
   - 创建新的K线数据
   - 处理新交易

## 代码实现示例

```rust
// 从归集交易生成K线
fn process_agg_trade(trade: &AggTrade, kline_map: &mut HashMap<i64, KlineBuilder>) -> Result<()> {
    // 计算K线周期
    let interval_ms = 60 * 1000; // 1分钟 = 60,000毫秒
    let open_time = (trade.trade_time / interval_ms) * interval_ms;

    // 获取或创建K线构建器
    let kline_builder = kline_map.entry(open_time).or_insert_with(|| {
        KlineBuilder::new(
            trade.symbol.clone(),
            "1m".to_string(),
            open_time,
        )
    });

    // 添加交易数据
    kline_builder.add_trade(trade);

    Ok(())
}
```

## 优势与挑战

### 优势

1. **更高的实时性**：归集交易提供近实时的交易数据，更新频率更高
2. **更精确的交易数据**：可以获取每笔交易的详细信息
3. **更灵活的K线生成**：可以自定义K线周期和聚合逻辑

### 挑战

1. **需要自行聚合K线**：需要实现从交易数据到K线数据的转换逻辑
2. **需要处理时间戳**：需要根据时间戳确定K线周期
3. **需要处理数据一致性**：确保生成的K线数据与币安官方K线数据一致

## WebSocket连接注意事项

1. **连接有效期**：每个连接有效期不超过24小时，请妥善处理断线重连
2. **心跳机制**：服务端每3分钟会发送ping帧，客户端应当在10分钟内回复pong帧，否则服务端会主动断开连接
3. **订阅限制**：
   - Websocket服务器每秒最多接受10个订阅消息
   - 单个连接最多可以订阅1024个Streams
4. **大小写敏感**：stream名称中所有交易对均为小写
