// 全市场精简Ticker示例
// 演示如何使用MiniTickerClient接收全市场的精简ticker数据

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use kline_server::klcommon::websocket::{
    MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler, MiniTickerData, WebSocketClient
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    info!("启动全市场精简Ticker示例");

    // 创建数据接收通道
    let (data_sender, mut data_receiver) = mpsc::unbounded_channel::<Vec<MiniTickerData>>();

    // 创建消息处理器
    let message_handler = Arc::new(MiniTickerMessageHandler::new(data_sender));

    // 创建配置
    let config = MiniTickerConfig::default(); // 使用默认配置（包含代理设置）

    // 创建客户端
    let mut client = MiniTickerClient::new(config, message_handler);

    // 启动数据处理任务
    let data_processor = tokio::spawn(async move {
        let mut ticker_count = 0;
        while let Some(tickers) = data_receiver.recv().await {
            ticker_count += tickers.len();
            
            // 打印前几个ticker作为示例
            for (i, ticker) in tickers.iter().take(3).enumerate() {
                info!(
                    "Ticker {}: {} - 价格: {} (开盘: {}, 最高: {}, 最低: {}) 成交量: {}",
                    i + 1,
                    ticker.symbol,
                    ticker.close_price,
                    ticker.open_price,
                    ticker.high_price,
                    ticker.low_price,
                    ticker.total_traded_volume
                );
            }
            
            if tickers.len() > 3 {
                info!("... 还有 {} 个ticker更新", tickers.len() - 3);
            }
            
            info!("累计收到 {} 个ticker更新", ticker_count);
        }
    });

    // 启动WebSocket客户端
    let client_task = tokio::spawn(async move {
        if let Err(e) = client.start().await {
            error!("客户端启动失败: {}", e);
        }
    });

    // 等待任务完成（或者按Ctrl+C退出）
    tokio::select! {
        _ = client_task => {
            info!("客户端任务已完成");
        }
        _ = data_processor => {
            info!("数据处理任务已完成");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("收到退出信号，正在关闭...");
        }
    }

    info!("全市场精简Ticker示例已退出");
    Ok(())
}
