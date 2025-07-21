use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("测试币安API获取HIVEUSDT和BTCUSDT的1d周期最新3根K线数据");
    
    // 创建HTTP客户端
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    
    let symbols = vec!["HIVEUSDT", "BTCUSDT"];
    
    for symbol in symbols {
        println!("\n=== {} ===", symbol);
        
        // 构建请求URL - 获取最新3根1d周期K线
        let url = format!(
            "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1d&limit=3",
            symbol
        );
        
        println!("请求URL: {}", url);
        
        // 发送请求
        let response = client.get(&url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send()
            .await?;
        
        if !response.status().is_success() {
            println!("请求失败: {}", response.status());
            continue;
        }
        
        // 解析响应
        let response_text = response.text().await?;
        let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)?;
        
        println!("获取到 {} 根K线数据:", raw_klines.len());
        
        for (i, raw_kline) in raw_klines.iter().enumerate() {
            if raw_kline.len() >= 7 {
                let open_time = raw_kline[0].as_i64().unwrap_or(0);
                let close_time = raw_kline[6].as_i64().unwrap_or(0);
                
                // 转换为可读时间格式
                let open_datetime = chrono::Utc.timestamp_millis(open_time)
                    .format("%Y-%m-%d %H:%M:%S").to_string();
                let close_datetime = chrono::Utc.timestamp_millis(close_time)
                    .format("%Y-%m-%d %H:%M:%S").to_string();
                
                println!("  K线 {}: 开盘时间={} ({}), 收盘时间={} ({})", 
                    i + 1, 
                    open_time, 
                    open_datetime,
                    close_time,
                    close_datetime
                );
                println!("    开盘价={}, 最高价={}, 最低价={}, 收盘价={}", 
                    raw_kline[1].as_str().unwrap_or("N/A"),
                    raw_kline[2].as_str().unwrap_or("N/A"),
                    raw_kline[3].as_str().unwrap_or("N/A"),
                    raw_kline[4].as_str().unwrap_or("N/A")
                );
            }
        }
    }
    
    Ok(())
}
