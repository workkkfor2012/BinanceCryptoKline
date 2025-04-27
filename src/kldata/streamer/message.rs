use crate::klcommon::{Database, KlineData, Result};
use crate::kldata::aggregator;
use crate::kldata::streamer::WebSocketConnection;
use log::{error, info};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;

/// 处理WebSocket消息
pub async fn process_messages(
    mut rx: mpsc::Receiver<(usize, String)>,
    db: Arc<Database>,
    connections: Arc<TokioMutex<HashMap<usize, WebSocketConnection>>>,
) {
    info!("启动WebSocket消息处理器");

    // 统计信息
    let mut stats: HashMap<String, usize> = HashMap::new();
    let mut last_stats_time = Instant::now();

    // 处理消息
    while let Some((_connection_id, text)) = rx.recv().await {
        // 解析消息
        match parse_message(&text) {
            Ok(Some((symbol, interval, kline_data))) => {
                // 更新统计信息
                let key = format!("{}/{}", symbol, interval);
                *stats.entry(key).or_insert(0) += 1;

                // 处理K线数据
                process_kline_data(&symbol, &interval, &kline_data, &db).await;
            }
            Ok(None) => {
                // 非K线消息，忽略
            }
            Err(e) => {
                error!("解析消息失败: {}", e);
            }
        }

        // 每30秒输出一次统计信息
        let now = Instant::now();
        if now.duration_since(last_stats_time) > Duration::from_secs(30) {
            // 输出统计信息
            info!("WebSocket统计信息:");
            for (key, count) in &stats {
                info!("  {}: {} 条消息", key, count);
            }

            // 输出连接状态
            let connections = connections.lock().await;
            info!("WebSocket连接状态:");
            for (id, conn) in connections.iter() {
                info!("  连接 {}: 状态={}, 流数量={}, 消息数量={}",
                    id, conn.status, conn.streams.len(), conn.message_count);
            }

            // 重置统计信息
            stats.clear();
            last_stats_time = now;
        }
    }

    info!("WebSocket消息处理器已停止");
}

/// 解析WebSocket消息
fn parse_message(text: &str) -> Result<Option<(String, String, KlineData)>> {
    // 输出原始消息内容，便于调试
    info!("收到WebSocket消息: {}", text);

    // 解析JSON
    let json: Value = match serde_json::from_str(text) {
        Ok(json) => json,
        Err(e) => {
            error!("解析JSON失败: {}, 原始消息: {}", e, text);
            return Err(e.into());
        }
    };

    // 检查是否是连续合约K线消息
    let e_value = json.get("e").and_then(|e| e.as_str());
    info!("e字段值: {:?}", e_value);

    // 直接打印整个JSON结构的关键部分，便于调试
    if let Some(k) = json.get("k") {
        info!("k字段: {}", k);
    }

    if e_value == Some("continuous_kline") {
        info!("Found continuous kline message");

        // 获取交易对
        let symbol = json.get("ps").and_then(|s| s.as_str()).unwrap_or("").to_uppercase();
        info!("Symbol: {}", symbol);

        // 获取K线数据
        if let Some(k) = json.get("k") {
            info!("Found k field: {}", k);

            // 获取周期
            let interval = k.get("i").and_then(|i| i.as_str()).unwrap_or("").to_string();
            info!("Interval: {}", interval);

            // 创建KlineData结构体
            let start_time = k.get("t").and_then(|t| t.as_i64()).unwrap_or(0);
            let end_time = k.get("T").and_then(|t| t.as_i64()).unwrap_or(0);
            let is_closed = k.get("x").and_then(|x| x.as_bool()).unwrap_or(false);
            let open = k.get("o").and_then(|o| o.as_str()).unwrap_or("0").to_string();
            let high = k.get("h").and_then(|h| h.as_str()).unwrap_or("0").to_string();
            let low = k.get("l").and_then(|l| l.as_str()).unwrap_or("0").to_string();
            let close = k.get("c").and_then(|c| c.as_str()).unwrap_or("0").to_string();
            let volume = k.get("v").and_then(|v| v.as_str()).unwrap_or("0").to_string();
            let quote_volume = k.get("q").and_then(|q| q.as_str()).unwrap_or("0").to_string();
            let number_of_trades = k.get("n").and_then(|n| n.as_i64()).unwrap_or(0);
            let taker_buy_volume = k.get("V").and_then(|v| v.as_str()).unwrap_or("0").to_string();
            let taker_buy_quote_volume = k.get("Q").and_then(|q| q.as_str()).unwrap_or("0").to_string();
            let ignore = k.get("B").and_then(|b| b.as_str()).unwrap_or("0").to_string();

            let kline_data = KlineData {
                start_time,
                end_time,
                interval: interval.clone(),
                first_trade_id: k.get("f").and_then(|f| f.as_i64()).unwrap_or(0),
                last_trade_id: k.get("L").and_then(|l| l.as_i64()).unwrap_or(0),
                is_closed,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                number_of_trades,
                taker_buy_volume,
                taker_buy_quote_volume,
                ignore,
            };

            info!("Successfully parsed kline data: is_closed={}, start_time={}, end_time={}",
                  kline_data.is_closed, kline_data.start_time, kline_data.end_time);

            return Ok(Some((symbol, interval, kline_data)));
        } else {
            info!("k field not found");
        }
    } else if let Some(data) = json.get("data") {
        // 检查是否是普通K线消息（旧格式）
        info!("Found data field: {}", data);
        if let Some(k) = data.get("k") {
            info!("Found k field: {}", k);
            // 获取交易对和周期
            // 从stream字段中提取交易对和周期
            // 格式：<pair>_perpetual@continuousKline_<interval>
            let stream = json.get("stream").and_then(|s| s.as_str()).unwrap_or("");
            info!("Stream field: {}", stream);
            let parts: Vec<&str> = stream.split('@').collect();
            info!("Stream parts: {:?}", parts);

            if parts.len() >= 2 {
                let pair_parts: Vec<&str> = parts[0].split('_').collect();
                info!("Pair parts: {:?}", pair_parts);
                let symbol = if pair_parts.len() >= 1 {
                    pair_parts[0].to_uppercase()
                } else {
                    data.get("s").and_then(|s| s.as_str()).unwrap_or("").to_uppercase()
                };
                info!("Symbol: {}", symbol);

                let interval_parts: Vec<&str> = parts[1].split('_').collect();
                info!("Interval parts: {:?}", interval_parts);
                let interval = if interval_parts.len() >= 2 {
                    interval_parts[1].to_string()
                } else {
                    k.get("i").and_then(|i| i.as_str()).unwrap_or("").to_string()
                };
                info!("Interval: {}", interval);

                // 解析K线数据
                let kline_data: KlineData = match serde_json::from_value(k.clone()) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to parse kline data: {}, k field: {}", e, k);
                        return Err(e.into());
                    }
                };
                info!("Successfully parsed kline data: is_closed={}, start_time={}, end_time={}",
                      kline_data.is_closed, kline_data.start_time, kline_data.end_time);

                return Ok(Some((symbol, interval, kline_data)));
            } else {
                // 回退到旧的解析方式
                info!("Using old parsing method");
                let symbol = data.get("s").and_then(|s| s.as_str()).unwrap_or("").to_uppercase();
                info!("Symbol from s field: {}", symbol);
                let interval = k.get("i").and_then(|i| i.as_str()).unwrap_or("");
                info!("Interval from i field: {}", interval);

                // 解析K线数据
                let kline_data: KlineData = match serde_json::from_value(k.clone()) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to parse kline data: {}, k field: {}", e, k);
                        return Err(e.into());
                    }
                };
                info!("Successfully parsed kline data: is_closed={}, start_time={}, end_time={}",
                      kline_data.is_closed, kline_data.start_time, kline_data.end_time);

                return Ok(Some((symbol, interval.to_string(), kline_data)));
            }
        } else {
            info!("k field not found");
        }
    } else {
        info!("data field not found or e field is not continuous_kline");
    }

    // 不是K线消息
    info!("Not a kline message");
    Ok(None)
}

/// 处理K线数据
async fn process_kline_data(symbol: &str, interval: &str, kline_data: &KlineData, db: &Arc<Database>) {
    // 输出处理K线数据的详细信息
    info!("开始处理K线数据: symbol={}, interval={}, is_closed={}, start_time={}, end_time={}",
          symbol, interval, kline_data.is_closed, kline_data.start_time, kline_data.end_time);

    // 使用新的K线合成逻辑处理K线数据
    match aggregator::process_kline_data(symbol, interval, kline_data, db).await {
        Ok(_) => {
            info!("成功处理K线数据: symbol={}, interval={}", symbol, interval);
        },
        Err(e) => {
            error!("处理K线数据失败: {}", e);
        }
    }
}
