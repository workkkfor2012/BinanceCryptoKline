# K线合成算法文档

## 概述

本文档描述了如何从币安WebSocket推送的1分钟K线数据实时合成6个周期（1m, 5m, 30m, 4h, 1d, 1w）的K线数据。

## 币安WebSocket连接格式

合约K线WebSocket连接使用以下格式：

```
wss://fstream.binance.com/ws/<pair>_<contractType>@continuousKline_<interval>
```

例如，订阅BTCUSDT永续合约的1分钟K线：

```
wss://fstream.binance.com/ws/btcusdt_perpetual@continuousKline_1m
```

对于组合流订阅，使用以下格式：

```
wss://fstream.binance.com/stream?streams=<pair1>_<contractType>@continuousKline_<interval1>/<pair2>_<contractType>@continuousKline_<interval2>
```

例如，同时订阅BTCUSDT和ETHUSDT永续合约的1分钟K线：

```
wss://fstream.binance.com/stream?streams=btcusdt_perpetual@continuousKline_1m/ethusdt_perpetual@continuousKline_1m
```

## 币安WebSocket K线数据格式

币安WebSocket推送的K线数据格式如下：

```json
{
  "e":"continuous_kline",	// 事件类型
  "E":1607443058651,		// 事件时间
  "ps":"BTCUSDT",			// 标的交易对
  "ct":"PERPETUAL",			// 合约类型
  "k":{
    "t":1607443020000,		// 这根K线的起始时间
    "T":1607443079999,		// 这根K线的结束时间
    "i":"1m",				// K线间隔
    "f":116467658886,		// 这根K线期间第一笔更新ID
    "L":116468012423,		// 这根K线期间末一笔更新ID
    "o":"18787.00",			// 这根K线期间第一笔成交价
    "c":"18804.04",			// 这根K线期间末一笔成交价
    "h":"18804.04",			// 这根K线期间最高成交价
    "l":"18786.54",			// 这根K线期间最低成交价
    "v":"197.664",			// 这根K线期间成交量
    "n":543,				// 这根K线期间成交笔数
    "x":false,				// 这根K线是否完结(是否已经开始下一根K线)
    "q":"3715253.19494",	// 这根K线期间成交额
    "V":"184.769",			// 主动买入的成交量
    "Q":"3472925.84746",	// 主动买入的成交额
    "B":"0"					// 忽略此参数
  }
}
```

## K线合成算法

### 关键参数说明

- `x` (is_closed): 表示K线是否已完结。当一根K线的时间周期结束时，该值为true，表示这根K线已经完成；当K线仍在形成过程中，该值为false，表示这根K线还在更新中。

### 合成流程

1. **接收WebSocket数据**：
   - 从WebSocket接收1分钟K线数据
   - 解析数据，提取K线信息

2. **处理1分钟K线**：
   - 如果`is_closed=true`，表示这根1分钟K线已完成，将其插入数据库
   - 如果`is_closed=false`，表示这根1分钟K线仍在更新，更新数据库中的对应记录

3. **合成高级别周期K线**：
   - 每次接收到WebSocket推送的1分钟K线数据时（约250毫秒一次），都触发合成逻辑
   - 无论`is_closed`的值是什么，都执行合成
   - 对于每个高级别周期（5m, 30m, 4h, 1d, 1w），执行以下步骤：

4. **确定K线所属周期**：
   - 根据1分钟K线的开始时间，计算它属于哪个高级别周期的K线
   - 使用整数除法向下取整到周期的开始时间
   - 例如，对于5分钟周期，计算公式为：`(timestamp / (5 * 60 * 1000)) * (5 * 60 * 1000)`
   - 这个公式利用整数除法的特性，将时间戳向下取整到最近的5分钟周期开始时间
   - 比如时间戳1609459230000（2020-12-31 23:53:50）会被转换为1609459200000（2020-12-31 23:50:00）
   - **重要**：计算得出的这个时间戳（如1609459200000）就是我们用来查询数据库的主键

5. **获取或创建高级别K线**：
   - 由于每个交易对和时间周期都有独立的表（如k_btc_5m），表名已包含交易对和周期信息
   - 首先计算1分钟K线所属的高级别周期的开始时间(period_start_time)
   - 使用计算得出的周期开始时间(period_start_time)作为主键查询特定表中的K线
   - 在数据库表中，open_time字段是主键，用于唯一标识一根K线
   - 查询SQL示例：`SELECT * FROM k_[symbol]_[interval] WHERE open_time = [period_start_time]`
   - 如果存在，获取该K线；如果不存在，创建新的K线，并将period_start_time作为其open_time值

6. **更新高级别K线数据**：
   - 如果是新创建的K线，使用上一根K线的收盘价作为高级别K线的开盘价
   - 如果是新创建的K线，最高价取开盘价和成交价的最大值
   - 如果是新创建的K线，最低价取开盘价和成交价的最小值
   - 如果是更新现有K线，最高价取当前最高价和新成交价的最大值
   - 如果是更新现有K线，最低价取当前最低价和新成交价的最小值
   - 更新成交量、成交额等累计值
   - 收盘价始终设为最新成交价

7. **判断高级别K线是否完成**：
   - 检查1分钟K线的结束时间是否达到或超过高级别周期的结束时间
   - 如果是，标记高级别K线为已完成，并插入数据库
   - 如果否，更新数据库中的高级别K线

### 时间计算

各周期的时间计算方法：

- 5分钟：`(timestamp / (5 * 60 * 1000)) * (5 * 60 * 1000)`
- 30分钟：`(timestamp / (30 * 60 * 1000)) * (30 * 60 * 1000)`
- 4小时：`(timestamp / (4 * 60 * 60 * 1000)) * (4 * 60 * 60 * 1000)`
- 1天：`(timestamp / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000)`
- 1周：`(timestamp / (7 * 24 * 60 * 60 * 1000)) * (7 * 24 * 60 * 60 * 1000)`

### 特殊情况处理

我们完全信任币安的数据源，不考虑数据源可能出现的错误问题。因此，在处理K线数据时：

1. **数据处理原则**：
   - 直接接受并处理币安WebSocket推送的所有数据
   - 不进行额外的数据验证或异常检测
   - 假设所有接收到的数据都是正确且完整的

2. **数据更新逻辑**：
   - **1分钟K线处理**：
     - 仅根据`is_closed`参数决定是插入新记录还是更新现有记录
     - 当`is_closed=true`时，表示K线已完成，插入新记录
     - 当`is_closed=false`时，表示K线仍在更新，更新现有记录
   - **高级别周期K线处理**：
     - 高级别周期K线（5m, 30m, 4h, 1d, 1w）是通过合成算法生成的
     - 使用计算得出的周期开始时间(period_start_time)作为主键查询是否存在
     - 如果不存在，创建新K线；如果存在，更新现有K线
     - 当1分钟K线的结束时间达到或超过高级别周期的结束时间时，标记高级别K线为已完成

3. **成交量和成交额处理**：
   - 数据库中存储的是品种的成交量
   - 对于1分钟K线，直接使用币安WebSocket推送的成交量(`v`)
   - 对于高级别周期K线，需要计算1分钟K线成交量的增量，然后累加到高级别K线中
   - 增量计算公式：增量 = 当前成交量 - 上次成交量
   - 需要维护一个映射来跟踪每个1分钟K线的上次成交量
   - 当1分钟K线完成时，可以清理对应的记录

## 实现注意事项

1. **Rust语言优势**：
   - 利用Rust的所有权系统自动管理内存，无需手动维护内存缓存
   - 编译时检查避免内存泄漏和数据竞争问题

2. **并发处理**：
   - 使用Rust的并发原语（如Mutex、Arc）确保数据一致性
   - 考虑使用异步编程模型处理高并发情况

3. **系统重启**：
   - 系统重启后，直接从币安WebSocket获取最新数据继续处理
   - 不需要特殊的错误恢复机制，相信数据源的可靠性

4. **性能优化**：
   - 批量处理数据库操作，减少I/O开销
   - 利用Rust的零成本抽象和高效数据结构

## 伪代码示例

```rust
// 定义成交量跟踪结构体
struct VolumeTracker {
    last_volume: f64,
}

// 全局映射，用于跟踪每个1分钟K线的上次成交量
// 键为K线的开始时间，值为上次成交量
static mut VOLUME_TRACKER_MAP: HashMap<String, HashMap<i64, VolumeTracker>> = HashMap::new();

// 获取成交量跟踪映射的安全访问方法
fn get_volume_tracker(symbol: &str) -> &mut HashMap<i64, VolumeTracker> {
    unsafe {
        VOLUME_TRACKER_MAP
            .entry(symbol.to_string())
            .or_insert_with(HashMap::new)
    }
}

// 处理WebSocket推送的1分钟K线
async fn process_kline_data(symbol: &str, kline_data: &KlineData, db: &Database) {
    // 转换为标准K线格式
    let kline_1m = kline_data.to_kline();

    // 获取成交量跟踪映射
    let volume_tracker = get_volume_tracker(symbol);

    // 解析当前成交量
    let current_volume = kline_1m.volume.parse::<f64>().unwrap_or(0.0);

    // 获取上次成交量，如果不存在则为0
    let last_volume = volume_tracker
        .get(&kline_1m.open_time)
        .map(|tracker| tracker.last_volume)
        .unwrap_or(0.0);

    // 计算成交量增量
    let volume_delta = current_volume - last_volume;

    // 更新成交量跟踪映射
    volume_tracker.insert(kline_1m.open_time, VolumeTracker { last_volume: current_volume });

    // 处理1分钟K线
    if kline_data.is_closed {
        // 插入新的1分钟K线
        db.insert_kline(symbol, "1m", &kline_1m);

        // 清理成交量跟踪映射
        volume_tracker.remove(&kline_1m.open_time);
    } else {
        // 更新现有1分钟K线
        db.update_kline(symbol, "1m", &kline_1m);
    }

    // 无论K线是否完成，都触发合成逻辑
    aggregate_higher_timeframes(symbol, &kline_1m, volume_delta, db).await;
}

// 合成高级别周期K线
async fn aggregate_higher_timeframes(symbol: &str, kline_1m: &Kline, volume_delta: f64, db: &Database) {
    // 合成5分钟K线
    aggregate_timeframe(symbol, kline_1m, "5m", 5, volume_delta, db).await;

    // 合成30分钟K线
    aggregate_timeframe(symbol, kline_1m, "30m", 30, volume_delta, db).await;

    // 合成4小时K线
    aggregate_timeframe(symbol, kline_1m, "4h", 240, volume_delta, db).await;

    // 合成1天K线
    aggregate_timeframe(symbol, kline_1m, "1d", 1440, volume_delta, db).await;

    // 合成1周K线
    aggregate_timeframe(symbol, kline_1m, "1w", 10080, volume_delta, db).await;
}

// 合成特定周期的K线
async fn aggregate_timeframe(symbol: &str, kline_1m: &Kline, interval: &str, minutes: i64, volume_delta: f64, db: &Database) {
    // 计算该1分钟K线所属的高级别周期的开始时间
    let interval_ms = minutes * 60 * 1000;
    // 计算period_start_time，这个值将作为数据库查询的主键
    let period_start_time = (kline_1m.open_time / interval_ms) * interval_ms;
    let period_end_time = period_start_time + interval_ms - 1;

    // 获取或创建高级别K线
    // 使用周期开始时间作为主键查询特定表中的K线
    // 表名已包含交易对和时间周期信息，如k_btc_5m
    let higher_kline = match db.get_kline_by_time(symbol, interval, period_start_time) {
        Some(kline) => {
            // 更新现有K线
            let mut updated_kline = kline.clone();

            // 更新最高价和最低价（使用成交价而非K线的高低价）
            updated_kline.high = max(updated_kline.high, kline_1m.close);
            updated_kline.low = min(updated_kline.low, kline_1m.close);

            // 处理成交量（使用计算得出的增量）
            // 只累加成交量的增量，避免重复计算
            updated_kline.volume += volume_delta.to_string();

            // 成交额也可以使用类似的方法计算增量，这里简化处理
            // 实际实现中应该也计算成交额的增量
            updated_kline.quote_asset_volume += kline_1m.quote_asset_volume;

            // 更新收盘价
            updated_kline.close = kline_1m.close;

            updated_kline
        },
        None => {
            // 创建新的高级别K线
            // 获取上一根K线的收盘价作为开盘价
            let prev_kline = db.get_last_kline_before(symbol, interval, period_start_time);
            let open_price = match prev_kline {
                Some(kline) => kline.close.clone(),
                None => kline_1m.open.clone(), // 如果没有上一根K线，使用当前1分钟K线的开盘价
            };

            // 计算最高价和最低价
            let high_price = max(open_price.clone(), kline_1m.close.clone());
            let low_price = min(open_price.clone(), kline_1m.close.clone());

            Kline {
                open_time: period_start_time,
                close_time: period_end_time,
                open: open_price,
                high: high_price,
                low: low_price,
                close: kline_1m.close.clone(),
                volume: kline_1m.volume.clone(),
                quote_asset_volume: kline_1m.quote_asset_volume.clone(),
                number_of_trades: kline_1m.number_of_trades,
                taker_buy_base_asset_volume: kline_1m.taker_buy_base_asset_volume.clone(),
                taker_buy_quote_asset_volume: kline_1m.taker_buy_quote_asset_volume.clone(),
                ignore: "0".to_string(),
            }
        }
    };

    // 判断高级别K线是否完成
    let is_period_closed = kline_1m.close_time >= period_end_time;

    if is_period_closed {
        // 高级别K线已完成，插入数据库
        db.insert_kline(symbol, interval, &higher_kline);
    } else {
        // 高级别K线未完成，更新数据库
        db.update_kline(symbol, interval, &higher_kline);
    }
}
```

## 总结

本算法通过实时处理WebSocket推送的1分钟K线数据，动态合成高级别周期的K线，确保数据的实时性和准确性。算法的关键在于正确处理`is_closed`参数，并根据时间戳计算K线所属的周期。

我们完全信任币安提供的数据源，不考虑数据源可能出现的错误问题。这种信任简化了实现，使我们可以专注于高效处理和合成K线数据，而不需要额外的数据验证和错误处理机制。
