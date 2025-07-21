use kline_server::{
    klcommon::{api::BinanceApi, config::BinanceKlineConfig, database::Database},
    kldata::backfill::BackfillManager,
};
use tracing::info;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    info!("🧪 测试BackfillManager的网络请求优化...");

    // 加载配置
    let config = BinanceKlineConfig::load()?;
    
    // 创建API客户端和数据库
    let api = BinanceApi::new();
    let db = Database::new(&config.database.path)?;
    
    // 创建BackfillManager（测试模式，只使用少量品种）
    let test_symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    let intervals = vec!["1m".to_string(), "5m".to_string()];
    
    let backfill_manager = BackfillManager::new(
        api,
        db,
        intervals,
        true, // 测试模式
        test_symbols,
    );

    info!("📊 开始执行优化后的run_once方法...");
    let start_time = Instant::now();
    
    match backfill_manager.run_once().await {
        Ok(klines_map) => {
            let elapsed = start_time.elapsed();
            info!(
                "✅ run_once执行成功！",
                elapsed_ms = elapsed.as_millis(),
                klines_count = klines_map.len(),
            );
            
            // 显示获取到的K线数据概览
            for ((symbol, interval), kline) in klines_map.iter().take(5) {
                info!(
                    "📈 K线数据示例: {} {} - 开盘价: {}, 收盘价: {}, 时间: {}",
                    symbol,
                    interval,
                    kline.open_price,
                    kline.close_price,
                    kline.open_time,
                );
            }
        }
        Err(e) => {
            let elapsed = start_time.elapsed();
            info!(
                "❌ run_once执行失败",
                elapsed_ms = elapsed.as_millis(),
                error = %e,
            );
        }
    }

    info!("🎯 测试完成！");
    Ok(())
}
