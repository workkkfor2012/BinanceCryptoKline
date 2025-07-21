use kline_server::klcommon::{BinanceApi, Result};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    info!("🔍 提取SETTLING状态的品种...");

    // 创建API客户端
    let api = BinanceApi::new();
    
    // 获取原始交易所信息
    let exchange_info = api.get_exchange_info().await?;
    
    // 提取所有SETTLING状态的USDT永续合约品种
    let settling_symbols: Vec<String> = exchange_info.symbols
        .iter()
        .filter(|symbol| {
            symbol.symbol.ends_with("USDT") 
            && symbol.contract_type == "PERPETUAL" 
            && symbol.status == "SETTLING"
        })
        .map(|symbol| symbol.symbol.clone())
        .collect();

    info!("📊 找到 {} 个SETTLING状态的USDT永续合约品种:", settling_symbols.len());
    
    for (i, symbol) in settling_symbols.iter().enumerate() {
        info!("  {}. {}", i + 1, symbol);
    }

    // 按字母顺序排序并打印
    let mut sorted_symbols = settling_symbols.clone();
    sorted_symbols.sort();
    
    info!("\n📝 按字母顺序排列的SETTLING品种:");
    for (i, symbol) in sorted_symbols.iter().enumerate() {
        info!("  {}. {}", i + 1, symbol);
    }

    // 保存到文件
    let settling_list = sorted_symbols.join("\n");
    std::fs::write("logs/settling_symbols.txt", settling_list)?;
    info!("💾 SETTLING品种列表已保存到: logs/settling_symbols.txt");

    Ok(())
}
