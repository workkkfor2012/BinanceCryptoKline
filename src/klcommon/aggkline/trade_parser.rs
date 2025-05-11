// 交易解析器 - 解析原始WebSocket消息
use crate::klcommon::models::{BinanceRawAggTrade, AppAggTrade};
use crate::klcommon::{AppError, Result};
use log::{info, error, debug};
use tokio::sync::mpsc;
use serde_json::Value;

/// 运行交易解析任务
///
/// # 参数
/// * `raw_frame_receiver` - 原始帧接收器
/// * `app_trade_sender` - 应用交易发送器
pub async fn run_trade_parser_task(
    mut raw_frame_receiver: mpsc::Receiver<String>,
    app_trade_sender: mpsc::Sender<AppAggTrade>,
) -> Result<()> {
    info!("启动交易解析任务");

    let mut message_count = 0;
    let mut parse_error_count = 0;

    while let Some(raw_frame_json_str) = raw_frame_receiver.recv().await {
        message_count += 1;

        // 每1000条消息输出一次统计信息
        if message_count % 1000 == 0 {
            info!("已处理 {} 条消息，解析错误 {} 条", message_count, parse_error_count);
        }

        // 尝试解析为币安多stream的包裹体
        // {"stream":"<streamName>","data":<raw_payload>}
        // 或者直接是 <raw_payload>
        match serde_json::from_str::<Value>(&raw_frame_json_str) {
            Ok(json_value) => {
                let trade_payload_value = if let (Some(_stream), Some(data)) = (json_value.get("stream"), json_value.get("data")) {
                    // 这是多stream格式
                    data
                } else {
                    // 可能是单stream格式直接是payload
                    &json_value
                };

                match parse_agg_trade(trade_payload_value) {
                    Ok(Some(app_trade)) => {
                        // 发送解析后的交易数据
                        if app_trade_sender.send(app_trade).await.is_err() {
                            error!("[TradeParser] 应用交易接收器已关闭，退出任务");
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        // 不是归集交易消息，忽略
                        debug!("[TradeParser] 非归集交易消息: {}",
                               if raw_frame_json_str.len() > 100 {
                                   format!("{}...", &raw_frame_json_str[..100])
                               } else {
                                   raw_frame_json_str.clone()
                               });
                    }
                    Err(e) => {
                        parse_error_count += 1;
                        error!("[TradeParser] 解析归集交易失败: {}, 原始消息: {}", e,
                               if raw_frame_json_str.len() > 100 {
                                   format!("{}...", &raw_frame_json_str[..100])
                               } else {
                                   raw_frame_json_str
                               });
                    }
                }
            }
            Err(e) => {
                parse_error_count += 1;
                error!("[TradeParser] 解析JSON失败: {}, 原始消息: {}", e,
                       if raw_frame_json_str.len() > 100 {
                           format!("{}...", &raw_frame_json_str[..100])
                       } else {
                           raw_frame_json_str
                       });
            }
        }
    }

    info!("[TradeParser] 原始帧通道已关闭，退出任务");
    Ok(())
}

/// 解析归集交易
///
/// # 参数
/// * `json_value` - JSON值
///
/// # 返回值
/// * `Ok(Some(AppAggTrade))` - 解析成功
/// * `Ok(None)` - 不是归集交易消息
/// * `Err(AppError)` - 解析失败
pub fn parse_agg_trade(json_value: &Value) -> Result<Option<AppAggTrade>> {
    // 检查是否是归集交易消息
    if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
        if event_type == "aggTrade" {
            // 尝试解析为BinanceRawAggTrade
            match serde_json::from_value::<BinanceRawAggTrade>(json_value.clone()) {
                Ok(raw_trade) => {
                    // 转换价格和数量为f64
                    match (raw_trade.price.parse::<f64>(), raw_trade.quantity.parse::<f64>()) {
                        (Ok(price), Ok(quantity)) => {
                            let app_trade = AppAggTrade {
                                symbol: raw_trade.symbol,
                                price,
                                quantity,
                                timestamp_ms: raw_trade.trade_time,
                                is_buyer_maker: raw_trade.is_buyer_maker,
                            };
                            return Ok(Some(app_trade));
                        }
                        _ => {
                            return Err(AppError::ParseError(format!("无法解析价格或数量: price={}, quantity={}",
                                                                   raw_trade.price, raw_trade.quantity)));
                        }
                    }
                }
                Err(e) => {
                    return Err(AppError::ParseError(format!("无法解析BinanceRawAggTrade: {}", e)));
                }
            }
        }
    }

    // 不是归集交易消息
    Ok(None)
}
