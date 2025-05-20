// 测试双缓冲K线存储
use kline_server::klcommon::{Database, Result};
use kline_server::klcommon::aggkline::{
    DoubleBufferedKlineStore, GlobalSymbolPeriodRegistry, CentralScheduler,
    KlineData, KLINE_PERIODS_MS
};
use std::sync::Arc;
use std::time::Duration;
use log::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();

    info!("启动双缓冲K线存储测试");

    // 初始化数据库
    let db = Arc::new(Database::new("data/klines.db")?);

    // 创建周期映射
    let periods = vec![
        ("1m", 60 * 1000),
        ("5m", 5 * 60 * 1000),
        ("30m", 30 * 60 * 1000),
        ("1h", 60 * 60 * 1000),
        ("4h", 4 * 60 * 60 * 1000),
        ("1d", 24 * 60 * 60 * 1000),
        ("1w", 7 * 24 * 60 * 60 * 1000),
    ];

    // 创建全局品种周期注册表
    let registry = Arc::new(GlobalSymbolPeriodRegistry::new(db.clone(), 1000, &periods));

    // 初始化注册表
    let symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "BNBUSDT".to_string(),
    ];
    registry.initialize(&symbols).await?;

    // 创建双缓冲K线存储
    let kline_store = Arc::new(DoubleBufferedKlineStore::new(1000, periods.len()));

    // 创建中心化调度器
    let scheduler = CentralScheduler::new(kline_store.clone(), 100);
    let running = scheduler.start();

    // 获取品种索引
    let btc_index = registry.get_symbol_index("BTCUSDT").unwrap_or(0);
    let eth_index = registry.get_symbol_index("ETHUSDT").unwrap_or(1);

    // 获取周期索引
    let period_1m_index = registry.get_period_index("1m").unwrap_or(0);
    let period_5m_index = registry.get_period_index("5m").unwrap_or(1);

    info!("品种索引: BTCUSDT={}, ETHUSDT={}", btc_index, eth_index);
    info!("周期索引: 1m={}, 5m={}", period_1m_index, period_5m_index);

    // 写入一些测试数据
    let now = chrono::Utc::now().timestamp_millis();

    // BTC 1m K线
    let btc_1m = KlineData {
        open_time_ms: now - now % 60000,
        open: 50000.0,
        high: 50100.0,
        low: 49900.0,
        close: 50050.0,
        volume: 10.5,
        quote_asset_volume: 525000.0,
    };

    // ETH 1m K线
    let eth_1m = KlineData {
        open_time_ms: now - now % 60000,
        open: 3000.0,
        high: 3050.0,
        low: 2950.0,
        close: 3025.0,
        volume: 50.0,
        quote_asset_volume: 150000.0,
    };

    // 写入数据
    info!("写入 BTC 1m K线: open_time={}, open={}, high={}, low={}, close={}, volume={}, quote_volume={}",
          btc_1m.open_time_ms, btc_1m.open, btc_1m.high, btc_1m.low, btc_1m.close, btc_1m.volume, btc_1m.quote_asset_volume);

    let result1 = kline_store.write_kline(btc_index, period_1m_index, btc_1m);
    let result2 = kline_store.write_kline(eth_index, period_1m_index, eth_1m);

    info!("写入结果: BTC={}, ETH={}", result1, result2);

    info!("写入测试数据完成");

    // 等待一段时间，让调度器交换缓冲区
    info!("等待缓冲区交换...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 读取数据
    let btc_1m_read = kline_store.read_kline(btc_index, period_1m_index);
    let eth_1m_read = kline_store.read_kline(eth_index, period_1m_index);

    if let Some(data) = btc_1m_read {
        info!("读取 BTC 1m K线: open_time={}, open={}, high={}, low={}, close={}, volume={}, quote_volume={}",
              data.open_time_ms, data.open, data.high, data.low, data.close, data.volume, data.quote_asset_volume);
    } else {
        error!("读取 BTC 1m K线失败");
    }

    if let Some(data) = eth_1m_read {
        info!("读取 ETH 1m K线: open_time={}, open={}, high={}, low={}, close={}, volume={}, quote_volume={}",
              data.open_time_ms, data.open, data.high, data.low, data.close, data.volume, data.quote_asset_volume);
    } else {
        error!("读取 ETH 1m K线失败");
    }

    // 停止调度器
    running.store(false, std::sync::atomic::Ordering::SeqCst);

    // 等待一段时间，确保调度器已停止
    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("测试完成");
    Ok(())
}
