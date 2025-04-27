# K线合成算法实现计划

## 概述

本文档详细描述了如何实现新的K线合成算法，基于更新后的`kline_aggregation_algorithm.md`文档。实现将使用Rust语言，充分利用其性能和安全特性。

## 实现步骤

### 1. 创建基础数据结构

```rust
// 成交量跟踪结构体
struct VolumeTracker {
    last_volume: f64,
}

// 全局映射，用于跟踪每个1分钟K线的上次成交量
// 使用RwLock确保线程安全
lazy_static! {
    static ref VOLUME_TRACKER_MAP: RwLock<HashMap<String, HashMap<i64, VolumeTracker>>> = RwLock::new(HashMap::new());
}

// K线结构体
#[derive(Clone, Debug)]
struct Kline {
    open_time: i64,
    close_time: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    quote_asset_volume: String,
    number_of_trades: i64,
    taker_buy_base_asset_volume: String,
    taker_buy_quote_asset_volume: String,
    ignore: String,
}
```

### 2. 实现WebSocket处理函数

```rust
// 处理WebSocket推送的1分钟K线
async fn process_kline_data(symbol: &str, kline_data: &KlineData, db: &Database) -> Result<(), Error> {
    // 转换为标准K线格式
    let kline_1m = kline_data.to_kline();
    
    // 计算成交量增量
    let volume_delta = calculate_volume_delta(symbol, &kline_1m)?;
    
    // 处理1分钟K线
    if kline_data.is_closed {
        // 插入新的1分钟K线
        db.insert_kline(symbol, "1m", &kline_1m).await?;
        
        // 清理成交量跟踪映射
        clean_volume_tracker(symbol, kline_1m.open_time)?;
    } else {
        // 更新现有1分钟K线
        db.update_kline(symbol, "1m", &kline_1m).await?;
    }
    
    // 无论K线是否完成，都触发合成逻辑
    aggregate_higher_timeframes(symbol, &kline_1m, volume_delta, db).await?;
    
    Ok(())
}
```

### 3. 实现成交量增量计算

```rust
// 计算成交量增量
fn calculate_volume_delta(symbol: &str, kline: &Kline) -> Result<f64, Error> {
    // 解析当前成交量
    let current_volume = kline.volume.parse::<f64>().unwrap_or(0.0);
    
    // 获取上次成交量
    let mut tracker_map = VOLUME_TRACKER_MAP.write().map_err(|_| Error::LockError)?;
    let symbol_map = tracker_map.entry(symbol.to_string()).or_insert_with(HashMap::new);
    
    let last_volume = symbol_map
        .get(&kline.open_time)
        .map(|tracker| tracker.last_volume)
        .unwrap_or(0.0);
    
    // 计算增量
    let volume_delta = current_volume - last_volume;
    
    // 更新跟踪映射
    symbol_map.insert(kline.open_time, VolumeTracker { last_volume: current_volume });
    
    Ok(volume_delta)
}

// 清理成交量跟踪映射
fn clean_volume_tracker(symbol: &str, open_time: i64) -> Result<(), Error> {
    let mut tracker_map = VOLUME_TRACKER_MAP.write().map_err(|_| Error::LockError)?;
    
    if let Some(symbol_map) = tracker_map.get_mut(symbol) {
        symbol_map.remove(&open_time);
    }
    
    Ok(())
}
```

### 4. 实现高级别周期K线合成

```rust
// 合成高级别周期K线
async fn aggregate_higher_timeframes(symbol: &str, kline_1m: &Kline, volume_delta: f64, db: &Database) -> Result<(), Error> {
    // 合成5分钟K线
    aggregate_timeframe(symbol, kline_1m, "5m", 5, volume_delta, db).await?;
    
    // 合成30分钟K线
    aggregate_timeframe(symbol, kline_1m, "30m", 30, volume_delta, db).await?;
    
    // 合成4小时K线
    aggregate_timeframe(symbol, kline_1m, "4h", 240, volume_delta, db).await?;
    
    // 合成1天K线
    aggregate_timeframe(symbol, kline_1m, "1d", 1440, volume_delta, db).await?;
    
    // 合成1周K线
    aggregate_timeframe(symbol, kline_1m, "1w", 10080, volume_delta, db).await?;
    
    Ok(())
}
```

### 5. 实现特定周期K线合成

```rust
// 合成特定周期的K线
async fn aggregate_timeframe(
    symbol: &str, 
    kline_1m: &Kline, 
    interval: &str, 
    minutes: i64, 
    volume_delta: f64, 
    db: &Database
) -> Result<(), Error> {
    // 计算该1分钟K线所属的高级别周期的开始时间
    let interval_ms = minutes * 60 * 1000;
    let period_start_time = (kline_1m.open_time / interval_ms) * interval_ms;
    let period_end_time = period_start_time + interval_ms - 1;
    
    // 查询数据库中是否已存在该高级别周期的K线
    let higher_kline = match db.get_kline_by_time(symbol, interval, period_start_time).await? {
        Some(kline) => {
            // 更新现有K线
            let mut updated_kline = kline.clone();
            
            // 更新最高价和最低价（使用成交价而非K线的高低价）
            updated_kline.high = max(updated_kline.high.parse::<f64>().unwrap_or(0.0), 
                                    kline_1m.close.parse::<f64>().unwrap_or(0.0))
                                    .to_string();
                                    
            updated_kline.low = min(updated_kline.low.parse::<f64>().unwrap_or(f64::MAX), 
                                   kline_1m.close.parse::<f64>().unwrap_or(0.0))
                                   .to_string();
            
            // 处理成交量（使用计算得出的增量）
            let current_volume = updated_kline.volume.parse::<f64>().unwrap_or(0.0);
            updated_kline.volume = (current_volume + volume_delta).to_string();
            
            // 更新收盘价
            updated_kline.close = kline_1m.close.clone();
            
            updated_kline
        },
        None => {
            // 获取上一根K线的收盘价作为开盘价
            let prev_kline = db.get_last_kline_before(symbol, interval, period_start_time).await?;
            let open_price = match prev_kline {
                Some(kline) => kline.close.clone(),
                None => kline_1m.open.clone(), // 如果没有上一根K线，使用当前1分钟K线的开盘价
            };
            
            // 计算最高价和最低价
            let close_price = kline_1m.close.parse::<f64>().unwrap_or(0.0);
            let open_price_f64 = open_price.parse::<f64>().unwrap_or(0.0);
            
            let high_price = max(open_price_f64, close_price).to_string();
            let low_price = min(open_price_f64, close_price).to_string();
            
            // 创建新的高级别K线
            Kline {
                open_time: period_start_time,
                close_time: period_end_time,
                open: open_price,
                high: high_price,
                low: low_price,
                close: kline_1m.close.clone(),
                volume: volume_delta.to_string(),
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
        db.insert_kline(symbol, interval, &higher_kline).await?;
    } else {
        // 高级别K线未完成，更新数据库
        db.update_kline(symbol, interval, &higher_kline).await?;
    }
    
    Ok(())
}
```

### 6. 实现数据库操作

```rust
// 数据库操作接口
trait Database {
    // 插入K线
    async fn insert_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<(), Error>;
    
    // 更新K线
    async fn update_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<(), Error>;
    
    // 根据时间获取K线
    async fn get_kline_by_time(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>, Error>;
    
    // 获取指定时间之前的最后一根K线
    async fn get_last_kline_before(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>, Error>;
}

// SQLite实现
struct SqliteDatabase {
    pool: SqlitePool,
}

impl Database for SqliteDatabase {
    async fn insert_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<(), Error> {
        let table_name = format!("k_{}_{}",symbol.to_lowercase(), interval);
        
        sqlx::query(&format!(
            "INSERT INTO {} (open_time, close_time, open, high, low, close, volume, quote_asset_volume, 
             number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            table_name
        ))
        .bind(kline.open_time)
        .bind(kline.close_time)
        .bind(&kline.open)
        .bind(&kline.high)
        .bind(&kline.low)
        .bind(&kline.close)
        .bind(&kline.volume)
        .bind(&kline.quote_asset_volume)
        .bind(kline.number_of_trades)
        .bind(&kline.taker_buy_base_asset_volume)
        .bind(&kline.taker_buy_quote_asset_volume)
        .bind(&kline.ignore)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;
        
        Ok(())
    }
    
    async fn update_kline(&self, symbol: &str, interval: &str, kline: &Kline) -> Result<(), Error> {
        let table_name = format!("k_{}_{}",symbol.to_lowercase(), interval);
        
        sqlx::query(&format!(
            "UPDATE {} SET close_time = ?, open = ?, high = ?, low = ?, close = ?, 
             volume = ?, quote_asset_volume = ?, number_of_trades = ?, 
             taker_buy_base_asset_volume = ?, taker_buy_quote_asset_volume = ?, ignore = ? 
             WHERE open_time = ?",
            table_name
        ))
        .bind(kline.close_time)
        .bind(&kline.open)
        .bind(&kline.high)
        .bind(&kline.low)
        .bind(&kline.close)
        .bind(&kline.volume)
        .bind(&kline.quote_asset_volume)
        .bind(kline.number_of_trades)
        .bind(&kline.taker_buy_base_asset_volume)
        .bind(&kline.taker_buy_quote_asset_volume)
        .bind(&kline.ignore)
        .bind(kline.open_time)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;
        
        Ok(())
    }
    
    async fn get_kline_by_time(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>, Error> {
        let table_name = format!("k_{}_{}",symbol.to_lowercase(), interval);
        
        let kline = sqlx::query_as::<_, Kline>(&format!(
            "SELECT * FROM {} WHERE open_time = ?",
            table_name
        ))
        .bind(open_time)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;
        
        Ok(kline)
    }
    
    async fn get_last_kline_before(&self, symbol: &str, interval: &str, open_time: i64) -> Result<Option<Kline>, Error> {
        let table_name = format!("k_{}_{}",symbol.to_lowercase(), interval);
        
        let kline = sqlx::query_as::<_, Kline>(&format!(
            "SELECT * FROM {} WHERE open_time < ? ORDER BY open_time DESC LIMIT 1",
            table_name
        ))
        .bind(open_time)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;
        
        Ok(kline)
    }
}
```

### 7. 实现WebSocket连接和处理

```rust
// WebSocket处理
async fn handle_websocket(symbol: &str, db: &Database) -> Result<(), Error> {
    // 构建WebSocket URL
    let ws_url = format!("wss://fstream.binance.com/ws/{}@kline_1m", symbol.to_lowercase());
    
    // 连接WebSocket
    let (ws_stream, _) = connect_async(&ws_url).await.map_err(|e| Error::WebSocketError(e.to_string()))?;
    let (_, read) = ws_stream.split();
    
    // 处理接收到的消息
    let mut read = read.fuse();
    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                // 解析JSON
                let kline_data: KlineData = serde_json::from_str(&text)
                    .map_err(|e| Error::JsonError(e.to_string()))?;
                
                // 处理K线数据
                process_kline_data(symbol, &kline_data, db).await?;
            }
            Ok(Message::Close(_)) => break,
            Err(e) => return Err(Error::WebSocketError(e.to_string())),
            _ => {}
        }
    }
    
    Ok(())
}
```

### 8. 实现主函数

```rust
#[tokio::main]
async fn main() -> Result<(), Error> {
    // 初始化日志
    env_logger::init();
    
    // 连接数据库
    let db = SqliteDatabase::connect("kline.db").await?;
    
    // 获取交易对列表
    let symbols = vec!["btcusdt".to_string()]; // 测试环境下只有一个品种
    
    // 为每个交易对启动一个任务
    let mut handles = Vec::new();
    for symbol in symbols {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Err(e) = handle_websocket(&symbol, &db_clone).await {
                    log::error!("WebSocket error for {}: {:?}", symbol, e);
                    // 等待一段时间后重连
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        handles.push(handle);
    }
    
    // 等待所有任务完成
    for handle in handles {
        handle.await?;
    }
    
    Ok(())
}
```

## 测试计划

1. **单元测试**：
   - 测试成交量增量计算
   - 测试周期开始时间计算
   - 测试K线合成逻辑

2. **集成测试**：
   - 测试WebSocket连接和数据处理
   - 测试数据库操作
   - 测试完整的K线合成流程

3. **性能测试**：
   - 测试高并发情况下的性能
   - 测试长时间运行的稳定性

## 部署计划

1. 编译项目：`cargo build --release`
2. 配置数据库：确保SQLite数据库已正确设置
3. 启动服务：使用批处理文件或系统服务启动
4. 监控日志：确保服务正常运行并处理数据

## 注意事项

- 确保处理WebSocket断连和重连
- 实现适当的错误处理和日志记录
- 考虑添加监控和告警机制
- 定期备份数据库
