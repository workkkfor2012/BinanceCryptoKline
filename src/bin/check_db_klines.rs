use kline_server::klcommon::db::Database;
use chrono::{TimeZone, Utc, Timelike};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 查询数据库中的K线数据，验证时间对齐");

    let db = Database::new("data/klines.db").await?;
    
    // 查询DOTUSDT的不同周期K线
    let symbols = vec!["DOTUSDT"];
    let intervals = vec!["1h", "4h"];
    
    for symbol in &symbols {
        for interval in &intervals {
            println!("\n📊 {} - {} 周期", symbol, interval);
            println!("{}", "=".repeat(60));
            
            // 获取最近10根K线
            match db.get_latest_klines(symbol, interval, 10).await {
                Ok(klines) => {
                    if klines.is_empty() {
                        println!("❌ 数据库中没有找到 {} {} 的K线数据", symbol, interval);
                        continue;
                    }
                    
                    println!("找到 {} 根K线数据:", klines.len());
                    
                    for (i, kline) in klines.iter().enumerate() {
                        let open_time_dt = Utc.timestamp_millis(kline.open_time);
                        let close_time_dt = Utc.timestamp_millis(kline.close_time);
                        
                        println!("  K线 {}: ", i + 1);
                        println!("    开盘时间: {} ({})", 
                            open_time_dt.format("%Y-%m-%d %H:%M:%S UTC"),
                            kline.open_time
                        );
                        println!("    收盘时间: {} ({})", 
                            close_time_dt.format("%Y-%m-%d %H:%M:%S UTC"),
                            kline.close_time
                        );
                        println!("    开盘价: {}, 收盘价: {}, 成交量: {}", 
                            kline.open, kline.close, kline.volume);
                        
                        // 检查时间对齐
                        if *interval == "4h" {
                            let hour = open_time_dt.hour();
                            let minute = open_time_dt.minute();
                            let second = open_time_dt.second();
                            let is_4h_aligned = hour % 4 == 0 && minute == 0 && second == 0;
                            
                            println!("    4小时对齐检查: {} (时:分:秒 = {}:{}:{})", 
                                if is_4h_aligned { "✅ 正确" } else { "❌ 错误" }, 
                                hour, minute, second
                            );
                            
                            if !is_4h_aligned {
                                println!("      ⚠️  应该对齐到: 0,4,8,12,16,20 小时边界");
                            }
                        } else if interval == "1h" {
                            let minute = open_time_dt.minute();
                            let second = open_time_dt.second();
                            let is_1h_aligned = minute == 0 && second == 0;
                            
                            println!("    1小时对齐检查: {} (分:秒 = {}:{})", 
                                if is_1h_aligned { "✅ 正确" } else { "❌ 错误" }, 
                                minute, second
                            );
                        }
                        println!();
                    }
                    
                    // 统计对齐情况
                    let mut aligned_count = 0;
                    let total_count = klines.len();
                    
                    for kline in &klines {
                        let open_time_dt = Utc.timestamp_millis(kline.open_time);
                        let is_aligned = if interval == "4h" {
                            let hour = open_time_dt.hour();
                            hour % 4 == 0 && open_time_dt.minute() == 0 && open_time_dt.second() == 0
                        } else if interval == "1h" {
                            open_time_dt.minute() == 0 && open_time_dt.second() == 0
                        } else {
                            true // 其他周期暂不检查
                        };
                        
                        if is_aligned {
                            aligned_count += 1;
                        }
                    }
                    
                    println!("📈 对齐统计: {}/{} ({:.1}%) 正确对齐", 
                        aligned_count, total_count, 
                        (aligned_count as f64 / total_count as f64) * 100.0
                    );
                    
                    if aligned_count != total_count {
                        println!("⚠️  发现 {} 根未正确对齐的K线数据", total_count - aligned_count);
                    }
                },
                Err(e) => {
                    println!("❌ 查询失败: {}", e);
                }
            }
        }
    }
    
    // 额外检查：查看是否有其他品种的4h数据
    println!("\n🔍 检查数据库中所有4h周期的品种:");
    println!("{}", "=".repeat(60));
    
    match db.get_all_symbols_with_interval("4h").await {
        Ok(symbols_4h) => {
            if symbols_4h.is_empty() {
                println!("❌ 数据库中没有找到任何4h周期的数据");
            } else {
                println!("找到 {} 个品种有4h数据:", symbols_4h.len());
                for (i, symbol) in symbols_4h.iter().enumerate() {
                    println!("  {}. {}", i + 1, symbol);
                    
                    // 检查每个品种的最新一根4h K线
                    if let Ok(latest_klines) = db.get_latest_klines(symbol, "4h", 1).await {
                        if let Some(kline) = latest_klines.first() {
                            let open_time_dt = Utc.timestamp_millis(kline.open_time);
                            let hour = open_time_dt.hour();
                            let is_aligned = hour % 4 == 0;
                            
                            println!("     最新K线: {} (小时: {}) {}", 
                                open_time_dt.format("%Y-%m-%d %H:%M:%S"),
                                hour,
                                if is_aligned { "✅" } else { "❌" }
                            );
                        }
                    }
                }
            }
        },
        Err(e) => {
            println!("❌ 查询品种列表失败: {}", e);
        }
    }
    
    Ok(())
}
