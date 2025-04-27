// K线合成模块
use crate::klcommon::{AppError, Database, Kline, KlineData, Result};
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use once_cell::sync::Lazy;

// 成交量跟踪结构体
struct VolumeTracker {
    last_volume: f64,
}

// 全局映射，用于跟踪每个1分钟K线的上次成交量
// 使用RwLock确保线程安全
static VOLUME_TRACKER_MAP: Lazy<RwLock<HashMap<String, HashMap<i64, VolumeTracker>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// 计算成交量增量
fn calculate_volume_delta(symbol: &str, kline: &Kline) -> Result<f64> {
    // 解析当前成交量
    let current_volume = kline.volume.parse::<f64>().unwrap_or(0.0);

    // 获取上次成交量
    let mut tracker_map = VOLUME_TRACKER_MAP.write()
        .map_err(|_| AppError::DataError("获取成交量跟踪映射锁失败".to_string()))?;

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

/// 清理成交量跟踪映射
fn clean_volume_tracker(symbol: &str, open_time: i64) -> Result<()> {
    let mut tracker_map = VOLUME_TRACKER_MAP.write()
        .map_err(|_| AppError::DataError("获取成交量跟踪映射锁失败".to_string()))?;

    if let Some(symbol_map) = tracker_map.get_mut(symbol) {
        symbol_map.remove(&open_time);
    }

    Ok(())
}

/// Process 1-minute kline data from WebSocket
pub async fn process_kline_data(symbol: &str, interval: &str, kline_data: &KlineData, db: &Arc<Database>) -> Result<()> {
    // Output detailed processing information
    info!("aggregator::process_kline_data started: symbol={}, interval={}, is_closed={}",
          symbol, interval, kline_data.is_closed);

    // Convert to standard kline format
    let kline = kline_data.to_kline();
    info!("Converted to standard kline format: open_time={}, close_time={}, open={}, close={}",
          kline.open_time, kline.close_time, kline.open, kline.close);

    // Decide whether to insert a new record or update an existing one based on is_closed
    if kline_data.is_closed {
        // Kline is closed, check if it already exists in the database
        info!("Kline is closed, checking if it already exists: symbol={}, interval={}, open_time={}",
              symbol, interval, kline.open_time);

        let existing_kline = match db.get_kline_by_time(symbol, interval, kline.open_time) {
            Ok(result) => result,
            Err(e) => {
                error!("Failed to check existing kline: {}", e);
                return Err(e);
            }
        };

        if existing_kline.is_some() {
            // Kline already exists, update it
            info!("Kline already exists, updating: symbol={}, interval={}, open_time={}",
                  symbol, interval, kline.open_time);
            if let Err(e) = db.update_kline(symbol, interval, &kline) {
                error!("Failed to update existing kline: {}", e);
                return Err(e);
            }
            info!("Successfully updated existing kline: symbol={}, interval={}, open_time={}",
                  symbol, interval, kline.open_time);
        } else {
            // Kline doesn't exist, insert it
            info!("Kline doesn't exist, inserting new record: symbol={}, interval={}, open_time={}",
                  symbol, interval, kline.open_time);
            if let Err(e) = db.insert_kline(symbol, interval, &kline) {
                error!("Failed to insert kline: {}", e);
                return Err(e);
            }
            info!("Successfully inserted new kline: symbol={}, interval={}, open_time={}",
                  symbol, interval, kline.open_time);
        }

        // If it's a 1-minute kline, clean up the volume tracker
        if interval == "1m" {
            info!("Cleaning volume tracker: symbol={}, open_time={}", symbol, kline.open_time);
            if let Err(e) = clean_volume_tracker(symbol, kline.open_time) {
                error!("Failed to clean volume tracker: {}", e);
            }
        }
    } else {
        // Kline is not closed, update the existing record
        info!("Kline is not closed, updating existing record: symbol={}, interval={}, open_time={}",
              symbol, interval, kline.open_time);
        if let Err(e) = db.update_kline(symbol, interval, &kline) {
            error!("Failed to update kline: {}", e);
            return Err(e);
        }
        info!("Successfully updated kline: symbol={}, interval={}, open_time={}",
              symbol, interval, kline.open_time);
    }

    // If it's a 1-minute kline, trigger the aggregation logic
    if interval == "1m" {
        info!("It's a 1-minute kline, triggering aggregation logic");

        // Calculate volume delta
        info!("Calculating volume delta: symbol={}, open_time={}", symbol, kline.open_time);
        let volume_delta = match calculate_volume_delta(symbol, &kline) {
            Ok(delta) => {
                info!("Volume delta calculation successful: delta={}", delta);
                delta
            },
            Err(e) => {
                error!("Failed to calculate volume delta: {}", e);
                return Err(e);
            }
        };

        // Trigger aggregation logic regardless of whether the kline is complete
        info!("Starting to aggregate higher timeframes: symbol={}, open_time={}, volume_delta={}",
              symbol, kline.open_time, volume_delta);
        if let Err(e) = aggregate_higher_timeframes(symbol, &kline, volume_delta, db).await {
            error!("Failed to aggregate higher timeframes: {}", e);
            return Err(e);
        }
        info!("Successfully aggregated higher timeframes");
    } else {
        info!("Not a 1-minute kline, skipping aggregation logic: interval={}", interval);
    }

    info!("aggregator::process_kline_data completed: symbol={}, interval={}", symbol, interval);
    Ok(())
}

/// 合成高级别周期K线
async fn aggregate_higher_timeframes(symbol: &str, kline_1m: &Kline, volume_delta: f64, db: &Arc<Database>) -> Result<()> {
    info!("开始合成高级别周期K线: symbol={}, open_time={}", symbol, kline_1m.open_time);

    // 合成5分钟K线
    info!("开始合成5分钟K线");
    aggregate_timeframe(symbol, kline_1m, "5m", 5, volume_delta, db).await?;
    info!("5分钟K线合成完成");

    // 合成30分钟K线
    info!("开始合成30分钟K线");
    aggregate_timeframe(symbol, kline_1m, "30m", 30, volume_delta, db).await?;
    info!("30分钟K线合成完成");

    // 合成4小时K线
    info!("开始合成4小时K线");
    aggregate_timeframe(symbol, kline_1m, "4h", 240, volume_delta, db).await?;
    info!("4小时K线合成完成");

    // 合成1天K线
    info!("开始合成1天K线");
    aggregate_timeframe(symbol, kline_1m, "1d", 1440, volume_delta, db).await?;
    info!("1天K线合成完成");

    // 合成1周K线
    info!("开始合成1周K线");
    aggregate_timeframe(symbol, kline_1m, "1w", 10080, volume_delta, db).await?;
    info!("1周K线合成完成");

    info!("所有高级别周期K线合成完成");
    Ok(())
}

/// 合成特定周期的K线
async fn aggregate_timeframe(
    symbol: &str,
    kline_1m: &Kline,
    interval: &str,
    minutes: i64,
    volume_delta: f64,
    db: &Arc<Database>
) -> Result<()> {
    // 使用 target 指定日志目标
    let log_target = &format!("aggregator_{}", interval);
    // log::info!(target: log_target, "开始合成{}周期K线: symbol={}, 1分钟K线open_time={}",
    //       interval, symbol, kline_1m.open_time);

    // 计算该1分钟K线所属的高级别周期的开始时间
    let interval_ms = minutes * 60 * 1000;
    let period_start_time = (kline_1m.open_time / interval_ms) * interval_ms;
    let period_end_time = period_start_time + interval_ms - 1;
    // log::info!(target: log_target, "计算周期时间: interval_ms={}, period_start_time={}, period_end_time={}",
    //       interval_ms, period_start_time, period_end_time);

    // 查询数据库中是否已存在该高级别周期的K线
    // log::info!(target: log_target, "查询数据库中是否已存在该高级别周期的K线: symbol={}, interval={}, period_start_time={}",
    //       symbol, interval, period_start_time);
    let higher_kline = match db.get_kline_by_time(symbol, interval, period_start_time)? {
        Some(kline) => {
            // log::info!(target: log_target, "找到现有K线，进行更新: open_time={}", kline.open_time);
            // 更新现有K线
            let mut updated_kline = kline.clone();

            // 更新最高价和最低价（使用成交价而非K线的高低价）
            let current_high = updated_kline.high.parse::<f64>().unwrap_or(0.0);
            let current_low = updated_kline.low.parse::<f64>().unwrap_or(f64::MAX);
            let close_price = kline_1m.close.parse::<f64>().unwrap_or(0.0);
            // log::info!(target: log_target, "更新最高价和最低价: current_high={}, current_low={}, close_price={}",
            //       current_high, current_low, close_price);

            updated_kline.high = if current_high > close_price { current_high } else { close_price }.to_string();
            updated_kline.low = if current_low < close_price { current_low } else { close_price }.to_string();
            // log::info!(target: log_target, "更新后的最高价和最低价: high={}, low={}", updated_kline.high, updated_kline.low);

            // 处理成交量（使用计算得出的增量）
            let current_volume = updated_kline.volume.parse::<f64>().unwrap_or(0.0);
            updated_kline.volume = (current_volume + volume_delta).to_string();
            log::info!(target: log_target, "成交量更新: current={}, delta={}, new={}",
                  current_volume, volume_delta, updated_kline.volume);

            // 更新收盘价
            updated_kline.close = kline_1m.close.clone();
            // log::info!(target: log_target, "更新收盘价: close={}", updated_kline.close);

            updated_kline
        },
        None => {
            // log::info!(target: log_target, "未找到现有K线，创建新K线");
            // 获取上一根K线的收盘价作为开盘价
            // log::info!(target: log_target, "获取上一根K线的收盘价作为开盘价: symbol={}, interval={}, period_start_time={}",
            //       symbol, interval, period_start_time);
            let prev_kline = db.get_last_kline_before(symbol, interval, period_start_time)?;
            let open_price = match prev_kline {
                Some(kline) => {
                    // log::info!(target: log_target, "找到上一根K线: open_time={}, close={}", kline.open_time, kline.close);
                    kline.close.clone()
                },
                None => {
                    // log::info!(target: log_target, "未找到上一根K线，使用当前1分钟K线的开盘价: open={}", kline_1m.open);
                    kline_1m.open.clone() // 如果没有上一根K线，使用当前1分钟K线的开盘价
                },
            };

            // 计算最高价和最低价
            let close_price = kline_1m.close.parse::<f64>().unwrap_or(0.0);
            let open_price_f64 = open_price.parse::<f64>().unwrap_or(0.0);
            // log::info!(target: log_target, "计算最高价和最低价: open_price_f64={}, close_price={}", open_price_f64, close_price);

            let high_price = if open_price_f64 > close_price { open_price_f64 } else { close_price }.to_string();
            let low_price = if open_price_f64 < close_price { open_price_f64 } else { close_price }.to_string();
            // log::info!(target: log_target, "计算得出的最高价和最低价: high_price={}, low_price={}", high_price, low_price);

            // 创建新的高级别K线
            // log::info!(target: log_target, "创建新的高级别K线: period_start_time={}, period_end_time={}, open={}, volume={}",
            //       period_start_time, period_end_time, open_price, volume_delta);
            let new_kline = Kline {
                open_time: period_start_time,
                close_time: period_end_time,
                open: open_price,
                high: high_price,
                low: low_price,
                close: kline_1m.close.clone(),
                volume: volume_delta.to_string(), // 使用 volume_delta 作为初始成交量
                quote_asset_volume: kline_1m.quote_asset_volume.clone(),
                number_of_trades: kline_1m.number_of_trades,
                taker_buy_base_asset_volume: kline_1m.taker_buy_base_asset_volume.clone(),
                taker_buy_quote_asset_volume: kline_1m.taker_buy_quote_asset_volume.clone(),
                ignore: "0".to_string(),
            };
            // 添加新的成交量设置日志
            log::info!(target: log_target, "成交量设置 (新K线): volume={}", volume_delta);
            new_kline
        }
    };

    // 判断高级别K线是否完成
    let is_period_closed = kline_1m.close_time >= period_end_time;
    log::info!(target: log_target, "判断高级别K线是否完成: kline_1m.close_time={}, period_end_time={}, is_period_closed={}",
          kline_1m.close_time, period_end_time, is_period_closed);

    if is_period_closed {
        // 高级别K线已完成，检查是否已存在
        log::info!(target: log_target, "高级别K线已完成，检查是否已存在: symbol={}, interval={}, open_time={}",
              symbol, interval, higher_kline.open_time);

        // 再次检查数据库中是否已存在该K线（可能在处理过程中被其他线程插入）
        let existing_check = db.get_kline_by_time(symbol, interval, higher_kline.open_time)?;

        if existing_check.is_some() {
            // 已存在，更新
            log::info!(target: log_target, "高级别K线已存在，更新数据库: symbol={}, interval={}, open_time={}",
                  symbol, interval, higher_kline.open_time);
            db.update_kline(symbol, interval, &higher_kline)?;
            log::info!(target: log_target, "更新数据库成功");
        } else {
            // 不存在，插入
            log::info!(target: log_target, "高级别K线不存在，插入数据库: symbol={}, interval={}, open_time={}",
                  symbol, interval, higher_kline.open_time);
            db.insert_kline(symbol, interval, &higher_kline)?;
            log::info!(target: log_target, "插入数据库成功");
        }
    } else {
        // 高级别K线未完成，更新数据库
        log::info!(target: log_target, "高级别K线未完成，更新数据库: symbol={}, interval={}, open_time={}",
              symbol, interval, higher_kline.open_time);
        db.update_kline(symbol, interval, &higher_kline)?;
        log::info!(target: log_target, "更新数据库成功");
    }

    // log::info!(target: log_target, "{}周期K线合成完成", interval);
    Ok(())
}
