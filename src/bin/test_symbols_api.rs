use kline_server::klcommon::{BinanceApi, Result};
use std::fs;
use std::path::Path;
use chrono::Utc;
use tracing::{info, error};
use serde_json;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    info!("🚀 开始测试币安API获取品种信息...");

    // 创建logs目录
    let logs_dir = Path::new("logs");
    if !logs_dir.exists() {
        fs::create_dir_all(logs_dir)?;
        info!("📁 创建logs目录: {}", logs_dir.display());
    }

    // 创建API客户端
    let api = BinanceApi::new();
    
    // 获取当前时间戳用于文件命名
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");

    // 首先获取原始的交易所信息
    info!("📡 调用get_exchange_info API获取原始数据...");
    match api.get_exchange_info().await {
        Ok(exchange_info) => {
            info!("✅ 获取原始交易所信息成功!");
            info!("📊 总品种数量: {}", exchange_info.symbols.len());

            // 保存原始交易所信息
            let raw_file = format!("logs/raw_exchange_info_{}.json", timestamp);
            let raw_json = serde_json::to_string_pretty(&exchange_info)?;
            fs::write(&raw_file, raw_json)?;
            info!("💾 原始交易所信息已保存到: {}", raw_file);

            // 统计各种状态的品种
            let mut status_counts = std::collections::HashMap::new();
            for symbol in &exchange_info.symbols {
                if symbol.symbol.ends_with("USDT") && symbol.contract_type == "PERPETUAL" {
                    *status_counts.entry(symbol.status.clone()).or_insert(0) += 1;
                }
            }

            info!("📈 USDT永续合约品种状态统计:");
            for (status, count) in &status_counts {
                info!("  {}: {} 个", status, count);
            }

            // 保存状态统计
            let stats_file = format!("logs/status_statistics_{}.json", timestamp);
            let stats_json = serde_json::to_string_pretty(&status_counts)?;
            fs::write(&stats_file, stats_json)?;
            info!("💾 状态统计已保存到: {}", stats_file);
        }
        Err(e) => {
            error!("❌ 获取原始交易所信息失败: {}", e);
        }
    }

    info!("📡 调用get_trading_usdt_perpetual_symbols API...");

    match api.get_trading_usdt_perpetual_symbols().await {
        Ok((trading_symbols, delisted_symbols)) => {
            info!("✅ API调用成功!");
            info!("📊 正常交易品种数量: {}", trading_symbols.len());
            info!("🗑️ 已下架品种数量: {}", delisted_symbols.len());

            // 保存正常交易品种
            let trading_file = format!("logs/trading_symbols_{}.json", timestamp);
            let trading_json = serde_json::to_string_pretty(&trading_symbols)?;
            fs::write(&trading_file, trading_json)?;
            info!("💾 正常交易品种已保存到: {}", trading_file);

            // 保存已下架品种
            let delisted_file = format!("logs/delisted_symbols_{}.json", timestamp);
            let delisted_json = serde_json::to_string_pretty(&delisted_symbols)?;
            fs::write(&delisted_file, delisted_json)?;
            info!("💾 已下架品种已保存到: {}", delisted_file);

            // 保存汇总信息
            let summary = serde_json::json!({
                "timestamp": timestamp.to_string(),
                "trading_symbols_count": trading_symbols.len(),
                "delisted_symbols_count": delisted_symbols.len(),
                "trading_symbols": trading_symbols,
                "delisted_symbols": delisted_symbols
            });
            
            let summary_file = format!("logs/symbols_summary_{}.json", timestamp);
            let summary_json = serde_json::to_string_pretty(&summary)?;
            fs::write(&summary_file, summary_json)?;
            info!("📋 汇总信息已保存到: {}", summary_file);

            // 打印前10个交易品种作为示例
            info!("🔍 前10个正常交易品种:");
            for (i, symbol) in trading_symbols.iter().take(10).enumerate() {
                info!("  {}. {}", i + 1, symbol);
            }

            // 如果有已下架品种，打印它们
            if !delisted_symbols.is_empty() {
                info!("⚠️ 已下架品种:");
                for (i, symbol) in delisted_symbols.iter().enumerate() {
                    info!("  {}. {}", i + 1, symbol);
                }
            } else {
                info!("✅ 没有发现已下架品种");
            }
        }
        Err(e) => {
            error!("❌ API调用失败: {}", e);
            
            // 保存错误信息
            let error_file = format!("logs/api_error_{}.txt", timestamp);
            let error_content = format!("API调用失败\n时间: {}\n错误: {:#}", timestamp, e);
            fs::write(&error_file, error_content)?;
            error!("💾 错误信息已保存到: {}", error_file);
            
            return Err(e);
        }
    }

    info!("🎉 测试完成!");
    Ok(())
}
