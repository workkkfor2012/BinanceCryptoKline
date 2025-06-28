//! 数据库模块追踪埋点演示
//! 
//! 展示数据库模块中新增的追踪功能，包括：
//! - 写入队列的上下文传递
//! - K线处理循环的聚合标记
//! - spawn_blocking的上下文传递
//! - 标准化错误处理

use kline_server::klcommon::db::Database;
use kline_server::klcommon::context::init_tracing_config;
use kline_server::klcommon::models::Kline;
use tracing::{info, error};
use std::path::Path;

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
    
    info!("数据库模块追踪埋点演示开始");
    
    // 创建临时数据库文件
    let db_path = "temp_demo.db";
    
    // 演示1：数据库初始化（包含连接池创建、表初始化等）
    info!("=== 演示1：数据库初始化 ===");
    let db = match Database::new(db_path) {
        Ok(database) => {
            info!("数据库初始化成功");
            database
        },
        Err(e) => {
            error!("数据库初始化失败: {}", e);
            return Err(e.into());
        }
    };
    
    // 演示2：确保表存在
    info!("=== 演示2：确保表存在 ===");
    match db.ensure_symbol_table("BTCUSDT", "1m") {
        Ok(_) => {
            info!("表创建或确认存在成功");
        },
        Err(e) => {
            error!("表创建失败: {}", e);
        }
    }
    
    // 演示3：保存符号信息
    info!("=== 演示3：保存符号信息 ===");
    match db.save_symbol("BTCUSDT", "BTC", "USDT", "TRADING") {
        Ok(_) => {
            info!("符号保存成功");
        },
        Err(e) => {
            error!("符号保存失败: {}", e);
        }
    }
    
    // 演示4：创建测试K线数据
    info!("=== 演示4：保存K线数据（写入队列和循环聚合） ===");
    let test_klines = vec![
        Kline {
            open_time: 1640995200000, // 2022-01-01 00:00:00
            open: "47000.00".to_string(),
            high: "47100.00".to_string(),
            low: "46900.00".to_string(),
            close: "47050.00".to_string(),
            volume: "10.5".to_string(),
            close_time: 1640995259999,
            quote_asset_volume: "494025.00".to_string(),
            number_of_trades: 150,
            taker_buy_base_asset_volume: "5.2".to_string(),
            taker_buy_quote_asset_volume: "244812.00".to_string(),
            ignore: "0".to_string(),
        },
        Kline {
            open_time: 1640995260000, // 2022-01-01 00:01:00
            open: "47050.00".to_string(),
            high: "47150.00".to_string(),
            low: "47000.00".to_string(),
            close: "47100.00".to_string(),
            volume: "8.3".to_string(),
            close_time: 1640995319999,
            quote_asset_volume: "390730.00".to_string(),
            number_of_trades: 120,
            taker_buy_base_asset_volume: "4.1".to_string(),
            taker_buy_quote_asset_volume: "193065.00".to_string(),
            ignore: "0".to_string(),
        },
        Kline {
            open_time: 1640995320000, // 2022-01-01 00:02:00
            open: "47100.00".to_string(),
            high: "47200.00".to_string(),
            low: "47050.00".to_string(),
            close: "47180.00".to_string(),
            volume: "12.7".to_string(),
            close_time: 1640995379999,
            quote_asset_volume: "599386.00".to_string(),
            number_of_trades: 180,
            taker_buy_base_asset_volume: "6.8".to_string(),
            taker_buy_quote_asset_volume: "320824.00".to_string(),
            ignore: "0".to_string(),
        },
    ];
    
    // 保存K线数据到数据库（这将触发写入队列、上下文传递、循环聚合等）
    match db.save_klines("BTCUSDT", "1m", &test_klines).await {
        Ok(count) => {
            info!("成功保存 {} 条K线数据", count);
        },
        Err(e) => {
            error!("保存K线数据失败: {}", e);
        }
    }
    
    // 演示5：再次保存相同数据（测试更新逻辑）
    info!("=== 演示5：更新现有K线数据 ===");
    let mut updated_klines = test_klines.clone();
    // 修改第一条K线的收盘价
    updated_klines[0].close = "47080.00".to_string();
    
    match db.save_klines("BTCUSDT", "1m", &updated_klines).await {
        Ok(count) => {
            info!("成功更新 {} 条K线数据", count);
        },
        Err(e) => {
            error!("更新K线数据失败: {}", e);
        }
    }
    
    // 演示6：保存空K线数据（测试决策点）
    info!("=== 演示6：保存空K线数据 ===");
    match db.save_klines("BTCUSDT", "1m", &[]).await {
        Ok(count) => {
            info!("空K线数据处理完成，处理数量: {}", count);
        },
        Err(e) => {
            error!("空K线数据处理失败: {}", e);
        }
    }
    
    info!("数据库模块追踪埋点演示结束");
    
    // 清理临时文件
    if Path::new(db_path).exists() {
        let _ = std::fs::remove_file(db_path);
        info!("临时数据库文件已清理");
    }
    
    // 等待一下，让所有日志输出完成
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    Ok(())
}
