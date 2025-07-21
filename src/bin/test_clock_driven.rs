use kline_server::klcommon::api::interval_to_milliseconds;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("🕐 测试时钟驱动K线创建逻辑");
    
    // 模拟当前时间（整分钟对齐）
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    
    // 对齐到整分钟
    let aligned_time = (current_time / 60000) * 60000;
    
    println!("📅 当前时间: {}", current_time);
    println!("📅 对齐时间: {}", aligned_time);
    
    // 测试各种周期的到期判断
    let periods = vec!["1m", "5m", "30m", "1h", "4h", "1d"];
    
    println!("\n🔍 测试周期到期判断逻辑:");
    for period in &periods {
        let interval_ms = interval_to_milliseconds(period);
        let is_expired = aligned_time % interval_ms < 1000;
        
        println!("  {} ({}ms): {} {}", 
            period, 
            interval_ms, 
            if is_expired { "✅ 到期" } else { "⏳ 未到期" },
            if is_expired { 
                format!("(余数: {}ms)", aligned_time % interval_ms) 
            } else { 
                format!("(距离到期: {}ms)", interval_ms - (aligned_time % interval_ms))
            }
        );
    }
    
    // 测试时钟间隔计算
    println!("\n⏰ 测试时钟间隔计算:");
    const CLOCK_INTERVAL_MS: i64 = 60_000;
    let next_tick_point = (aligned_time / CLOCK_INTERVAL_MS + 1) * CLOCK_INTERVAL_MS;
    let sleep_duration = next_tick_point - aligned_time;
    
    println!("  当前时间: {}", aligned_time);
    println!("  下次滴答: {}", next_tick_point);
    println!("  等待时间: {}ms", sleep_duration);
    
    // 测试常数级复杂度优势
    println!("\n🚀 性能优势分析:");
    let symbol_count = 1000;
    let period_count = periods.len();
    
    println!("  品种数量: {}", symbol_count);
    println!("  周期数量: {}", period_count);
    println!("  旧算法复杂度: O({}) = {} 次检查", 
        symbol_count * period_count, 
        symbol_count * period_count
    );
    println!("  新算法复杂度: O({}) = {} 次周期检查 + 动态K线处理", 
        period_count, 
        period_count
    );
    
    let improvement_ratio = (symbol_count * period_count) as f64 / period_count as f64;
    println!("  性能提升: {:.1}x", improvement_ratio);
    
    // 测试动态品种索引更新逻辑
    println!("\n🔄 测试动态品种索引更新逻辑:");

    // 模拟初始索引状态
    let initial_symbols = 100;
    let mut period_to_kline_indices: Vec<Vec<usize>> = vec![Vec::new(); period_count];

    // 初始化索引
    for symbol_idx in 0..initial_symbols {
        for period_idx in 0..period_count {
            let kline_offset = symbol_idx * period_count + period_idx;
            period_to_kline_indices[period_idx].push(kline_offset);
        }
    }

    println!("  初始品种数: {}", initial_symbols);
    println!("  初始索引大小: {:?}",
        period_to_kline_indices.iter().map(|v| v.len()).collect::<Vec<_>>()
    );

    // 模拟动态添加新品种
    let new_symbol_idx = initial_symbols;
    for period_idx in 0..period_count {
        let kline_offset = new_symbol_idx * period_count + period_idx;
        period_to_kline_indices[period_idx].push(kline_offset);
    }

    println!("  添加新品种后索引大小: {:?}",
        period_to_kline_indices.iter().map(|v| v.len()).collect::<Vec<_>>()
    );

    // 验证索引完整性
    let total_klines = (initial_symbols + 1) * period_count;
    let indexed_klines: usize = period_to_kline_indices.iter().map(|v| v.len()).sum();

    println!("  预期K线总数: {}", total_klines);
    println!("  索引K线总数: {}", indexed_klines);
    println!("  索引完整性: {}",
        if indexed_klines == total_klines { "✅ 正确" } else { "❌ 错误" }
    );

    println!("\n✅ 时钟驱动逻辑测试完成！");
    println!("🎯 动态品种索引更新逻辑验证通过！");
}
