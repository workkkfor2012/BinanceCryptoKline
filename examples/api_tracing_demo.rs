//! API模块追踪埋点演示
//! 
//! 展示API模块中新增的追踪功能，包括错误分类、重试循环聚合、决策点追踪等

use kline_server::klcommon::api::BinanceApi;
use kline_server::klcommon::context::{init_tracing_config};
use kline_server::klcommon::models::DownloadTask;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化追踪系统
    init_tracing_config(true);
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();
    
    info!("API模块追踪埋点演示开始");
    
    // 创建API客户端
    let api = BinanceApi::new();
    
    // 演示1：获取交易所信息（包含重试循环追踪）
    info!("=== 演示1：获取交易所信息 ===");
    match api.get_exchange_info().await {
        Ok(exchange_info) => {
            info!("成功获取交易所信息，交易对数量: {}", exchange_info.symbols.len());
        },
        Err(e) => {
            error!("获取交易所信息失败: {}", e);
        }
    }
    
    // 演示2：获取USDT永续合约交易对（包含重试循环和错误分类）
    info!("=== 演示2：获取USDT永续合约交易对 ===");
    match api.get_trading_usdt_perpetual_symbols().await {
        Ok(symbols) => {
            info!("成功获取USDT永续合约交易对，数量: {}", symbols.len());
            if !symbols.is_empty() {
                info!("前5个交易对: {:?}", &symbols[..symbols.len().min(5)]);
            }
        },
        Err(e) => {
            error!("获取USDT永续合约交易对失败: {}", e);
        }
    }
    
    // 演示3：下载K线数据（包含详细的错误处理和决策点追踪）
    info!("=== 演示3：下载K线数据 ===");
    let download_task = DownloadTask {
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        limit: 10,
        start_time: None,
        end_time: None,
    };
    
    match api.download_continuous_klines(&download_task).await {
        Ok(klines) => {
            info!("成功下载K线数据，数量: {}", klines.len());
            if !klines.is_empty() {
                info!("第一条K线: 开盘时间={}, 开盘价={}, 收盘价={}", 
                    klines[0].open_time, klines[0].open, klines[0].close);
            }
        },
        Err(e) => {
            error!("下载K线数据失败: {}", e);
        }
    }
    
    // 演示4：获取服务器时间（包含重试循环追踪）
    info!("=== 演示4：获取服务器时间 ===");
    match api.get_server_time().await {
        Ok(server_time) => {
            info!("成功获取服务器时间: {}", server_time.server_time);
            
            // 转换为可读时间
            use chrono::{DateTime, Utc};
            if let Some(dt) = DateTime::<Utc>::from_timestamp(server_time.server_time / 1000, 0) {
                info!("服务器时间（UTC）: {}", dt.format("%Y-%m-%d %H:%M:%S"));
            }
        },
        Err(e) => {
            error!("获取服务器时间失败: {}", e);
        }
    }
    
    info!("API模块追踪埋点演示结束");
    
    // 等待一下，让所有日志输出完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    Ok(())
}
