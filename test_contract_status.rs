use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建HTTP客户端
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // 获取交易所信息
    let url = "https://fapi.binance.com/fapi/v1/exchangeInfo";
    let response = client.get(url).send().await?;
    
    if !response.status().is_success() {
        println!("请求失败: {}", response.status());
        return Ok(());
    }

    let json: Value = response.json().await?;
    
    // 查找一些示例合约，特别关注状态字段
    if let Some(symbols) = json["symbols"].as_array() {
        println!("找到 {} 个合约", symbols.len());
        
        // 查找不同状态的合约示例
        let mut status_examples = std::collections::HashMap::new();
        
        for symbol in symbols.iter().take(50) { // 只看前50个
            if let Some(symbol_name) = symbol["symbol"].as_str() {
                if let Some(status) = symbol["status"].as_str() {
                    status_examples.entry(status.to_string()).or_insert_with(Vec::new).push(symbol_name.to_string());
                }
                
                // 打印一个完整的合约信息作为示例
                if symbol_name == "BTCUSDT" {
                    println!("\nBTCUSDT 合约详细信息:");
                    println!("{}", serde_json::to_string_pretty(symbol)?);
                }
            }
        }
        
        println!("\n发现的状态类型:");
        for (status, examples) in status_examples {
            println!("  {}: {} 个合约 (示例: {:?})", status, examples.len(), examples.iter().take(3).collect::<Vec<_>>());
        }
        
        // 查找是否有 contractStatus 字段
        println!("\n检查是否有 contractStatus 字段:");
        for symbol in symbols.iter().take(10) {
            if let Some(symbol_name) = symbol["symbol"].as_str() {
                if let Some(contract_status) = symbol["contractStatus"].as_str() {
                    println!("  {}: contractStatus = {}", symbol_name, contract_status);
                } else {
                    println!("  {}: 没有 contractStatus 字段", symbol_name);
                }
            }
        }
    }

    Ok(())
}
