use crate::klcommon::{Database, KlineData, Result};
use crate::kldata::aggregator::KlineAggregator;
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
    // 解析JSON
    let json: Value = serde_json::from_str(text)?;

    // 检查是否是K线消息
    if let Some(data) = json.get("data") {
        if let Some(k) = data.get("k") {
            // 获取交易对和周期
            let symbol = data.get("s").and_then(|s| s.as_str()).unwrap_or("").to_uppercase();
            let interval = k.get("i").and_then(|i| i.as_str()).unwrap_or("");

            // 解析K线数据
            let kline_data: KlineData = serde_json::from_value(k.clone())?;

            return Ok(Some((symbol, interval.to_string(), kline_data)));
        }
    }

    // 不是K线消息
    Ok(None)
}

/// 处理K线数据
async fn process_kline_data(symbol: &str, interval: &str, kline_data: &KlineData, db: &Arc<Database>) {
    // 转换为标准K线格式
    let kline = kline_data.to_kline();

    // 根据is_closed决定是插入新记录还是更新现有记录
    if kline_data.is_closed {
        // K线已关闭，插入新记录
        if let Err(e) = db.insert_kline(symbol, interval, &kline) {
            error!("插入K线失败: {}", e);
        }

        // 如果是1分钟K线，通知聚合器
        if interval == "1m" {
            if let Some(aggregator) = KlineAggregator::get_instance() {
                if let Err(e) = aggregator.process_kline(symbol, &kline).await {
                    error!("处理K线进行聚合失败: {}", e);
                }
            }
        }
    } else {
        // K线未关闭，更新现有记录
        if let Err(e) = db.update_kline(symbol, interval, &kline) {
            error!("更新K线失败: {}", e);
        }
    }
}
