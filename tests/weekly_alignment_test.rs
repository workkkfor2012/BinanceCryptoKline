use chrono::{DateTime, Datelike, TimeZone, Utc};

/// 测试周线对齐逻辑的修复
#[cfg(test)]
mod tests {
    use super::*;

    /// 计算周线对齐时间戳（修复后的逻辑）
    fn calculate_weekly_alignment_fixed(timestamp_ms: i64) -> i64 {
        const WEEK_MS: i64 = 7 * 24 * 60 * 60 * 1000; // 一周的毫秒数
        const MONDAY_ALIGNMENT_OFFSET_MS: i64 = 3 * 24 * 60 * 60 * 1000; // 3天偏移量
        
        ((timestamp_ms + MONDAY_ALIGNMENT_OFFSET_MS) / WEEK_MS) * WEEK_MS - MONDAY_ALIGNMENT_OFFSET_MS
    }

    /// 计算周线对齐时间戳（修复前的逻辑）
    fn calculate_weekly_alignment_old(timestamp_ms: i64) -> i64 {
        const WEEK_MS: i64 = 7 * 24 * 60 * 60 * 1000; // 一周的毫秒数
        (timestamp_ms / WEEK_MS) * WEEK_MS
    }

    /// 获取时间戳对应的星期几（0=周日，1=周一，...，6=周六）
    fn get_weekday(timestamp_ms: i64) -> u32 {
        let dt = Utc.timestamp_millis_opt(timestamp_ms).unwrap();
        dt.weekday().num_days_from_sunday()
    }

    /// 格式化时间戳为可读字符串
    fn format_timestamp(timestamp_ms: i64) -> String {
        let dt = Utc.timestamp_millis_opt(timestamp_ms).unwrap();
        dt.format("%Y-%m-%d %H:%M:%S UTC (周%w)").to_string()
            .replace("周0", "周日")
            .replace("周1", "周一")
            .replace("周2", "周二")
            .replace("周3", "周三")
            .replace("周4", "周四")
            .replace("周5", "周五")
            .replace("周6", "周六")
    }

    #[test]
    fn test_weekly_alignment_fix() {
        println!("=== 周线对齐逻辑测试 ===");
        
        // 测试用例：不同时间点的周线对齐
        let test_cases = vec![
            // Unix纪元开始时间（1970-01-01 00:00:00 UTC，星期四）
            0,
            // 2024年某个周一的开始时间
            1704067200000, // 2024-01-01 00:00:00 UTC (周一)
            // 2024年某个周三的时间
            1704240000000, // 2024-01-03 00:00:00 UTC (周三)
            // 2024年某个周五的时间
            1704412800000, // 2024-01-05 00:00:00 UTC (周五)
            // 当前时间附近的一个时间点
            1735689600000, // 2025-01-01 00:00:00 UTC (周三)
        ];

        for timestamp in test_cases {
            println!("\n--- 测试时间戳: {} ---", timestamp);
            println!("原始时间: {}", format_timestamp(timestamp));
            
            let old_aligned = calculate_weekly_alignment_old(timestamp);
            let new_aligned = calculate_weekly_alignment_fixed(timestamp);
            
            println!("修复前对齐: {} ({})", old_aligned, format_timestamp(old_aligned));
            println!("修复后对齐: {} ({})", new_aligned, format_timestamp(new_aligned));
            
            let old_weekday = get_weekday(old_aligned);
            let new_weekday = get_weekday(new_aligned);
            
            println!("修复前星期: {}", old_weekday);
            println!("修复后星期: {}", new_weekday);
            
            // 验证修复后的对齐时间应该是周一（weekday = 1）
            assert_eq!(new_weekday, 1, "修复后的对齐时间应该是周一");
            
            // 验证修复前的对齐时间是周四（weekday = 4）
            assert_eq!(old_weekday, 4, "修复前的对齐时间应该是周四");
        }
        
        println!("\n=== 测试通过！周线对齐逻辑修复成功 ===");
    }

    #[test]
    fn test_specific_binance_weekly_case() {
        println!("=== 币安周线具体案例测试 ===");
        
        // 模拟一个具体的币安交易时间戳
        // 假设这是2024年1月3日（周三）的某个交易
        let trade_timestamp = 1704283200000; // 2024-01-03 12:00:00 UTC (周三)
        
        println!("交易时间: {}", format_timestamp(trade_timestamp));
        
        let old_aligned = calculate_weekly_alignment_old(trade_timestamp);
        let new_aligned = calculate_weekly_alignment_fixed(trade_timestamp);
        
        println!("修复前K线开始时间: {}", format_timestamp(old_aligned));
        println!("修复后K线开始时间: {}", format_timestamp(new_aligned));
        
        // 验证修复后的K线开始时间是周一
        let new_weekday = get_weekday(new_aligned);
        assert_eq!(new_weekday, 1, "修复后的K线应该从周一开始");
        
        // 验证修复前的K线开始时间是周四
        let old_weekday = get_weekday(old_aligned);
        assert_eq!(old_weekday, 4, "修复前的K线从周四开始（错误）");
        
        println!("✅ 币安周线案例测试通过！");
    }
}
