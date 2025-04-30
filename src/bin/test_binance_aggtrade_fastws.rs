use std::time::Duration;
use std::error::Error;
use std::str::FromStr;
use std::collections::HashMap;

use async_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use http::Uri;
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;
use serde_json::Value;
use log::{info, error, LevelFilter};
use env_logger::Builder;


// 代理配置
const USE_PROXY: bool = true;
const PROXY_ADDR: &str = "127.0.0.1";
const PROXY_PORT: u16 = 1080;

// 币安WebSocket URL
const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws";

// 测试持续时间（秒）
fn get_test_duration() -> u64 {
    std::env::var("TEST_DURATION")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60) // 默认60秒
}

// 测试交易对 - 将在运行时从币安API获取

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 初始化日志
    init_logger();

    info!("开始测试币安聚合交易WebSocket流 (使用 async_tungstenite 库)");

    // 从币安API获取交易品种
    info!("从币安API获取交易品种...");
    let api = kline_server::klcommon::BinanceApi::new();
    let symbols = match api.get_trading_usdt_perpetual_symbols().await {
        Ok(symbols) => {
            info!("从币安API获取到 {} 个交易品种", symbols.len());
            // 将所有交易品种转换为小写
            symbols.iter().map(|s| s.to_lowercase()).collect::<Vec<String>>()
        },
        Err(e) => {
            error!("从币安API获取交易品种失败: {}", e);
            // 使用默认交易品种
            vec![
                "btcusdt".to_string(), "ethusdt".to_string(), "bnbusdt".to_string(),
                "adausdt".to_string(), "dogeusdt".to_string(), "xrpusdt".to_string(),
                "solusdt".to_string(), "dotusdt".to_string(), "maticusdt".to_string(),
                "ltcusdt".to_string()
            ]
        }
    };

    info!("使用交易对: {:?}", symbols);
    let test_duration = get_test_duration();
    info!("测试持续时间: {}秒", test_duration);

    // 构建WebSocket URL - 使用组合流
    let streams: Vec<String> = symbols.iter()
        .map(|symbol| format!("{}@aggTrade", symbol))
        .collect();

    // 币安组合流URL格式: /stream?streams=<streamName1>/<streamName2>/<streamName3>
    let ws_url = format!("{}stream?streams={}", BINANCE_WS_URL.replace("/ws", "/"), streams.join("/"));
    info!("WebSocket URL: {}", ws_url);

    // 解析URL
    let uri = Uri::from_str(&ws_url)?;
    let host = uri.host().unwrap_or("fstream.binance.com");
    let port = uri.port_u16().unwrap_or(443);

    info!("连接到主机: {}:{}", host, port);

    // 建立TCP连接（通过代理或直接）
    let tcp_stream = if USE_PROXY {
        info!("通过代理 {}:{} 连接", PROXY_ADDR, PROXY_PORT);

        // 连接到代理
        let socks_stream = Socks5Stream::connect(
            (PROXY_ADDR, PROXY_PORT),
            (host, port)
        )
        .await
        .map_err(|e| {
            error!("代理连接失败: {}", e);
            e
        })?;

        // 获取TCP流
        socks_stream.into_inner()
    } else {
        // 直接连接
        let addr = format!("{}:{}", host, port);
        TcpStream::connect(addr).await?
    };

    info!("TCP连接已建立");

    info!("执行WebSocket握手...");

    // 创建WebSocket连接
    let (ws_stream, _) = async_tungstenite::tokio::client_async_tls_with_config(
        ws_url,
        tcp_stream,
        None
    ).await?;

    info!("WebSocket握手成功");

    // 分离读写流
    let (mut write, mut read) = ws_stream.split();

    // 设置结束时间
    let end_time = tokio::time::Instant::now() + Duration::from_secs(test_duration);

    // 消息计数
    let mut message_count = 0;
    let mut message_counts: HashMap<String, usize> = HashMap::new();
    for symbol in &symbols {
        message_counts.insert(symbol.clone(), 0);
    }

    // 接收消息循环
    info!("开始接收消息...");

    // 记录上次打印快照的时间
    let mut last_snapshot_time = tokio::time::Instant::now();
    let snapshot_interval = Duration::from_secs(10); // 每10秒打印一次快照

    while tokio::time::Instant::now() < end_time {
        // 每10秒打印一次快照信息
        let now = tokio::time::Instant::now();
        if now.duration_since(last_snapshot_time) >= snapshot_interval {
            info!("=== 性能快照 ===");
            info!("进程PID: {}", std::process::id());
            info!("总消息数: {}", message_count);
            for (symbol, count) in &message_counts {
                info!("  {} 消息数: {}", symbol, count);
            }
            info!("================");
            last_snapshot_time = now;
        }

        // 不再每100条消息打印一次统计信息，只在10秒快照中显示
        // if message_count > 0 && message_count % 100 == 0 {
        //     info!("统计信息 - 总消息数: {}", message_count);
        //     for (symbol, count) in &message_counts {
        //         info!("  {} 消息数: {}", symbol, count);
        //     }
        // }

        // 设置读取超时
        match tokio::time::timeout(Duration::from_secs(5), read.next()).await {
            Ok(Some(result)) => {
                match result {
                    Ok(msg) => {
                        match msg {
                            Message::Text(text) => {
                                message_count += 1;

                                // 解析JSON
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(json) => {
                                        // 组合流格式: {"stream":"btcusdt@aggTrade","data":{...}}
                                        if let (Some(stream), Some(data)) = (json["stream"].as_str(), json["data"].as_object()) {
                                            // 提取流名称和交易对
                                            let parts: Vec<&str> = stream.split('@').collect();
                                            let symbol = if !parts.is_empty() { parts[0] } else { "unknown" };

                                            // 更新交易对消息计数
                                            *message_counts.entry(symbol.to_string()).or_insert(0) += 1;

                                            // 提取关键信息，但不再使用这些变量
                                            let _event_type = data.get("e").and_then(|v| v.as_str()).unwrap_or("unknown");
                                            let _symbol_from_data = data.get("s").and_then(|v| v.as_str()).unwrap_or("unknown");
                                            let _price = data.get("p").and_then(|v| v.as_str()).unwrap_or("unknown");
                                            let _quantity = data.get("q").and_then(|v| v.as_str()).unwrap_or("unknown");
                                            let _is_buyer_maker = data.get("m").and_then(|v| v.as_bool()).unwrap_or(false);

                                            // 不再打印每条消息的详细信息，只在10秒快照中显示统计
                                            // info!(
                                            //     "收到第{}条消息 - 流: {}, 交易对: {}, 价格: {}, 数量: {}, 买方做市: {}",
                                            //     message_count, stream, symbol_from_data, price, quantity, is_buyer_maker
                                            // );

                                            // 不再每10条消息打印一次完整JSON
                                            // if message_count % 10 == 0 {
                                            //     debug!("完整消息: {}", text);
                                            // }
                                        } else {
                                            // 可能是订阅确认消息，但不再打印详细信息
                                            // if json["result"].is_null() && json["id"].is_number() {
                                            //     info!("收到订阅确认消息: {}", text);
                                            // } else {
                                            //     warn!("收到未知格式的JSON消息: {}", text);
                                            // }
                                        }
                                    },
                                    Err(_) => {
                                        // 不再打印JSON解析错误
                                        // error!("JSON解析错误: {}", e);
                                        // error!("原始消息: {}", text);
                                    }
                                }
                            },
                            Message::Binary(_data) => {
                                // 不再打印二进制消息
                                // debug!("收到二进制消息，长度: {}", data.len());
                            },
                            Message::Ping(data) => {
                                // 不再打印Ping消息
                                // debug!("收到Ping，发送Pong");
                                if let Err(e) = write.send(Message::Pong(data)).await {
                                    error!("发送Pong失败: {}", e);
                                    break;
                                }
                            },
                            Message::Pong(_) => {
                                // 不再打印Pong消息
                                // debug!("收到Pong");
                            },
                            Message::Close(_) => {
                                info!("收到关闭消息，连接将关闭");
                                break;
                            },
                            Message::Frame(_) => {
                                // 不再打印帧消息
                                // debug!("收到原始帧");
                            }
                        }
                    },
                    Err(e) => {
                        error!("WebSocket错误: {}", e);
                        break;
                    }
                }
            },
            Ok(None) => {
                info!("WebSocket流已关闭");
                break;
            },
            Err(_) => {
                // 超时，但不退出，继续尝试读取
                // 不再打印超时信息
                // debug!("读取超时，继续尝试...");

                // 发送ping以保持连接活跃
                // 不再打印Ping信息
                // debug!("发送Ping以保持连接");
                if let Err(e) = write.send(Message::Ping(vec![])).await {
                    error!("发送Ping失败: {}", e);
                    break;
                }
            }
        }
    }

    // 打印最终统计信息
    info!("测试完成，总计收到 {} 条消息", message_count);
    for (symbol, count) in &message_counts {
        info!("  {} 总消息数: {}", symbol, count);
    }

    // 发送关闭消息
    info!("发送关闭消息");
    write.send(Message::Close(None)).await?;

    Ok(())
}

/// 初始化日志系统
fn init_logger() {
    let mut builder = Builder::new();

    // 设置日志级别
    builder.filter_level(LevelFilter::Info);

    // 设置日志格式
    builder.format(|buf, record| {
        use std::io::Write;
        writeln!(
            buf,
            "{} [{}] - [src/bin/test_binance_aggtrade_fastws.rs] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            record.level(),
            record.args()
        )
    });

    // 初始化日志
    builder.init();
}
