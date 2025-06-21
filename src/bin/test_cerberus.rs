//! Cerberus 验证系统测试程序
//! 
//! 用于测试 Cerberus 验证规则是否正常工作

use kline_server::klaggregate::cerberus::{create_default_cerberus_layer, CerberusConfig};
use tracing::{info, warn, error};
use tracing_subscriber::{layer::SubscriberExt, Registry};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建 Cerberus 验证层
    let (cerberus_layer, cerberus_engine) = create_default_cerberus_layer();
    
    // 设置 tracing 订阅器
    let subscriber = Registry::default()
        .with(cerberus_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
        );
    
    tracing::subscriber::set_global_default(subscriber)?;
    
    // 启动 Cerberus 验证引擎
    tokio::spawn(async move {
        cerberus_engine.start().await;
    });
    
    info!("🐕 Cerberus 验证系统测试开始");
    
    // 等待一下让系统初始化
    sleep(Duration::from_millis(100)).await;
    
    // 测试 1: INGESTION_DATA_VALIDITY - 正常数据
    info!(
        target: "MarketDataIngestor",
        event_name = "trade_data_parsed",
        symbol = "BTCUSDT",
        price = 50000.0,
        quantity = 1.5,
        timestamp_ms = chrono::Utc::now().timestamp_millis(),
        "测试正常交易数据解析"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 2: INGESTION_DATA_VALIDITY - 异常价格（应该触发偏差）
    warn!(
        target: "MarketDataIngestor",
        event_name = "trade_data_parsed",
        symbol = "BTCUSDT",
        price = 2000000.0, // 超出合理范围
        quantity = 1.0,
        timestamp_ms = chrono::Utc::now().timestamp_millis(),
        "测试异常价格数据（应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 3: INGESTION_DATA_VALIDITY - 异常时间戳（应该触发偏差）
    warn!(
        target: "MarketDataIngestor",
        event_name = "trade_data_parsed",
        symbol = "ETHUSDT",
        price = 3000.0,
        quantity = 2.0,
        timestamp_ms = chrono::Utc::now().timestamp_millis() + 3600000, // 未来1小时
        "测试异常时间戳数据（应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 4: KLINE_OHLC_CONSISTENCY - 正常K线数据
    info!(
        target: "SymbolKlineAggregator",
        event_name = "kline_updated",
        symbol = "BTCUSDT",
        interval = "1m",
        open = 50000.0,
        high = 50100.0,
        low = 49900.0,
        close = 50050.0,
        volume = 100.0,
        trade_count = 50,
        turnover = 5000000.0,
        taker_buy_volume = 60.0,
        "测试正常K线数据更新"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 5: KLINE_OHLC_CONSISTENCY - 异常K线数据（应该触发偏差）
    error!(
        target: "SymbolKlineAggregator",
        event_name = "kline_updated",
        symbol = "ETHUSDT",
        interval = "5m",
        open = 3000.0,
        high = 2900.0, // high < open，应该触发偏差
        low = 2800.0,
        close = 2950.0,
        volume = 50.0,
        trade_count = 25,
        turnover = 150000.0,
        taker_buy_volume = 30.0,
        "测试异常K线数据（high < open，应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 6: ROUTING_SUCCESS_RATE - 路由成功
    info!(
        target: "TradeEventRouter",
        event_name = "route_success",
        symbol = "BTCUSDT",
        price = 50000.0,
        quantity = 1.0,
        "测试路由成功事件"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 7: ROUTING_SUCCESS_RATE - 路由失败（应该触发偏差）
    error!(
        target: "TradeEventRouter",
        event_name = "route_failure",
        symbol = "INVALIDCOIN",
        error = "aggregator_not_found",
        price = 100.0,
        quantity = 1.0,
        "测试路由失败事件（应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 8: BUFFER_SWAP_INTEGRITY - 正常缓冲区交换
    info!(
        target: "BufferedKlineStore",
        event_name = "buffer_swapped",
        swap_duration_ms = 5.0,
        new_read_buffer_size = 1000,
        swap_count = 1,
        "测试正常缓冲区交换"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 9: BUFFER_SWAP_INTEGRITY - 缓慢的缓冲区交换（应该触发偏差）
    warn!(
        target: "BufferedKlineStore",
        event_name = "buffer_swapped",
        swap_duration_ms = 15.0, // 超过10ms阈值
        new_read_buffer_size = 2000,
        swap_count = 2,
        "测试缓慢的缓冲区交换（应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 10: KLINE_OPEN_TIME_ACCURACY - 正常时间对齐
    info!(
        target: "SymbolKlineAggregator",
        event_name = "kline_generated",
        symbol = "BTCUSDT",
        interval = "1m",
        open_time = 1640995200000i64, // 2022-01-01 00:00:00 UTC，对齐到分钟
        is_final = true,
        "测试正常K线时间对齐"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 11: KLINE_OPEN_TIME_ACCURACY - 时间未对齐（应该触发偏差）
    warn!(
        target: "SymbolKlineAggregator",
        event_name = "kline_generated",
        symbol = "ETHUSDT",
        interval = "1m",
        open_time = 1640995230000i64, // 不对齐到分钟边界
        is_final = true,
        "测试时间未对齐的K线（应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 12: PERSISTENCE_DATA_CONSISTENCY - 正常持久化
    info!(
        target: "KlineDataPersistence",
        event_name = "batch_persisted",
        total_records = 100,
        updated_records = 60,
        inserted_records = 40,
        success_count = 100,
        failed_count = 0,
        "测试正常数据持久化"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 13: PERSISTENCE_DATA_CONSISTENCY - 数据不一致（应该触发偏差）
    error!(
        target: "KlineDataPersistence",
        event_name = "batch_persisted",
        total_records = 100,
        updated_records = 50,
        inserted_records = 30, // 50 + 30 != 100，应该触发偏差
        success_count = 80,
        failed_count = 20,
        "测试数据不一致的持久化（应该触发偏差）"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 14: SYMBOL_INDEX_STABILITY - 正常品种注册
    info!(
        target: "SymbolMetadataRegistry",
        event_name = "symbol_registered",
        symbol = "BTCUSDT",
        symbol_index = 1,
        listing_time = 1609459200000i64, // 2021-01-01 00:00:00 UTC
        "测试正常品种注册"
    );
    
    sleep(Duration::from_millis(50)).await;
    
    // 测试 15: SYMBOL_INDEX_STABILITY - 可疑的上市时间（应该触发偏差）
    warn!(
        target: "SymbolMetadataRegistry",
        event_name = "symbol_registered",
        symbol = "NEWCOIN",
        symbol_index = 999,
        listing_time = chrono::Utc::now().timestamp_millis() - 30000, // 30秒前，可能是默认值
        "测试可疑上市时间的品种注册（应该触发偏差）"
    );
    
    // 等待所有验证任务完成
    sleep(Duration::from_millis(500)).await;
    
    info!("🎯 Cerberus 验证系统测试完成");
    info!("📊 请检查上面的日志输出，应该能看到多个 CERBERUS_DEVIATION 事件");
    
    Ok(())
}
