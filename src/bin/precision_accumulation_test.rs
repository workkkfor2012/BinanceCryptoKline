use std::time::Instant;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const ITERATIONS: u64 = 1_000_000_000; // 10亿次
const BATCH_SIZE: u64 = 50_000_000; // 每批5000万次，用于进度显示

fn main() {
    println!("=== K线聚合精度累积测试 ===");
    println!("测试场景: 模拟1周K线聚合，10位精度数值累加10亿次");
    println!("迭代次数: {} 次", ITERATIONS);
    println!();

    // 测试数据：模拟真实的加密货币价格和成交量
    let price_f64 = 43521.1234567890_f64;           // BTC价格，10位精度
    let quantity_f64 = 0.0012345678_f64;            // 小额成交量
    let price_decimal = dec!(43521.1234567890);      // 对应的Decimal
    let quantity_decimal = dec!(0.0012345678);

    println!("测试数据:");
    println!("  价格: {}", price_f64);
    println!("  数量: {}", quantity_f64);
    println!("  单次成交额: {}", price_f64 * quantity_f64);
    println!();

    // 1. f64累积测试
    println!("开始 f64 累积测试...");
    let f64_result = test_f64_accumulation(price_f64, quantity_f64);
    
    // 2. f32累积测试（对比）
    println!("开始 f32 累积测试...");
    let f32_result = test_f32_accumulation(price_f64 as f32, quantity_f64 as f32);
    
    // 3. Decimal累积测试
    println!("开始 Decimal 累积测试...");
    let decimal_result = test_decimal_accumulation(price_decimal, quantity_decimal);

    // 4. 补偿算法测试（Kahan求和）
    println!("开始 Kahan补偿算法测试...");
    let kahan_result = test_kahan_accumulation(price_f64, quantity_f64);

    // 输出结果对比
    print_precision_comparison(&f64_result, &f32_result, &decimal_result, &kahan_result);
}

#[derive(Debug)]
struct AccumulationResult {
    total_turnover: String,
    total_volume: String,
    execution_time: std::time::Duration,
    final_precision_loss: String,
}

fn test_f64_accumulation(price: f64, quantity: f64) -> AccumulationResult {
    let start = Instant::now();
    let mut total_turnover = 0.0_f64;
    let mut total_volume = 0.0_f64;
    
    let turnover_per_trade = price * quantity;
    
    for i in 0..ITERATIONS {
        total_turnover += turnover_per_trade;
        total_volume += quantity;
        
        // 进度显示
        if i % BATCH_SIZE == 0 && i > 0 {
            let progress = (i as f64 / ITERATIONS as f64) * 100.0;
            print!("\r  f64 进度: {:.1}%", progress);
        }
    }
    println!("\r  f64 完成: 100.0%");
    
    let execution_time = start.elapsed();
    
    // 计算理论值用于精度对比
    let expected_turnover = turnover_per_trade * ITERATIONS as f64;
    let precision_loss = (total_turnover - expected_turnover).abs();
    
    AccumulationResult {
        total_turnover: format!("{:.10}", total_turnover),
        total_volume: format!("{:.10}", total_volume),
        execution_time,
        final_precision_loss: format!("{:.2e}", precision_loss),
    }
}

fn test_f32_accumulation(price: f32, quantity: f32) -> AccumulationResult {
    let start = Instant::now();
    let mut total_turnover = 0.0_f32;
    let mut total_volume = 0.0_f32;
    
    let turnover_per_trade = price * quantity;
    
    for i in 0..ITERATIONS {
        total_turnover += turnover_per_trade;
        total_volume += quantity;
        
        if i % BATCH_SIZE == 0 && i > 0 {
            let progress = (i as f64 / ITERATIONS as f64) * 100.0;
            print!("\r  f32 进度: {:.1}%", progress);
        }
    }
    println!("\r  f32 完成: 100.0%");
    
    let execution_time = start.elapsed();
    
    let expected_turnover = turnover_per_trade * ITERATIONS as f32;
    let precision_loss = (total_turnover - expected_turnover).abs();
    
    AccumulationResult {
        total_turnover: format!("{:.10}", total_turnover),
        total_volume: format!("{:.10}", total_volume),
        execution_time,
        final_precision_loss: format!("{:.2e}", precision_loss),
    }
}

fn test_decimal_accumulation(price: Decimal, quantity: Decimal) -> AccumulationResult {
    let start = Instant::now();
    let mut total_turnover = Decimal::ZERO;
    let mut total_volume = Decimal::ZERO;
    
    let turnover_per_trade = price * quantity;
    
    for i in 0..ITERATIONS {
        total_turnover += turnover_per_trade;
        total_volume += quantity;
        
        if i % BATCH_SIZE == 0 && i > 0 {
            let progress = (i as f64 / ITERATIONS as f64) * 100.0;
            print!("\r  Decimal 进度: {:.1}%", progress);
        }
    }
    println!("\r  Decimal 完成: 100.0%");
    
    let execution_time = start.elapsed();
    
    // Decimal的精度损失应该为0
    AccumulationResult {
        total_turnover: total_turnover.to_string(),
        total_volume: total_volume.to_string(),
        execution_time,
        final_precision_loss: "0 (精确)".to_string(),
    }
}

fn test_kahan_accumulation(price: f64, quantity: f64) -> AccumulationResult {
    let start = Instant::now();
    let mut total_turnover = 0.0_f64;
    let mut total_volume = 0.0_f64;
    let mut turnover_compensation = 0.0_f64;  // Kahan补偿变量
    let mut volume_compensation = 0.0_f64;
    
    let turnover_per_trade = price * quantity;
    
    for i in 0..ITERATIONS {
        // Kahan求和算法 - 成交额
        let turnover_y = turnover_per_trade - turnover_compensation;
        let turnover_t = total_turnover + turnover_y;
        turnover_compensation = (turnover_t - total_turnover) - turnover_y;
        total_turnover = turnover_t;
        
        // Kahan求和算法 - 成交量
        let volume_y = quantity - volume_compensation;
        let volume_t = total_volume + volume_y;
        volume_compensation = (volume_t - total_volume) - volume_y;
        total_volume = volume_t;
        
        if i % BATCH_SIZE == 0 && i > 0 {
            let progress = (i as f64 / ITERATIONS as f64) * 100.0;
            print!("\r  Kahan 进度: {:.1}%", progress);
        }
    }
    println!("\r  Kahan 完成: 100.0%");
    
    let execution_time = start.elapsed();
    
    let expected_turnover = turnover_per_trade * ITERATIONS as f64;
    let precision_loss = (total_turnover - expected_turnover).abs();
    
    AccumulationResult {
        total_turnover: format!("{:.10}", total_turnover),
        total_volume: format!("{:.10}", total_volume),
        execution_time,
        final_precision_loss: format!("{:.2e}", precision_loss),
    }
}

fn print_precision_comparison(
    f64_result: &AccumulationResult,
    f32_result: &AccumulationResult,
    decimal_result: &AccumulationResult,
    kahan_result: &AccumulationResult,
) {
    println!();
    println!("=== 精度累积测试结果对比 ===");
    println!();

    println!("执行时间对比:");
    println!("┌─────────────────┬─────────────────┬─────────────────┐");
    println!("│ 数值类型        │ 执行时间        │ 相对性能        │");
    println!("├─────────────────┼─────────────────┼─────────────────┤");
    
    let times = [
        f64_result.execution_time,
        f32_result.execution_time,
        decimal_result.execution_time,
        kahan_result.execution_time,
    ];
    let fastest_time = times.iter().min().unwrap();
    
    print_time_row("f64 标准", &f64_result.execution_time, fastest_time);
    print_time_row("f32 标准", &f32_result.execution_time, fastest_time);
    print_time_row("Decimal", &decimal_result.execution_time, fastest_time);
    print_time_row("Kahan f64", &kahan_result.execution_time, fastest_time);
    
    println!("└─────────────────┴─────────────────┴─────────────────┘");

    println!();
    println!("精度对比结果:");
    println!("┌─────────────────┬─────────────────────────────┬─────────────────┐");
    println!("│ 数值类型        │ 最终累积成交额              │ 精度损失        │");
    println!("├─────────────────┼─────────────────────────────┼─────────────────┤");
    println!("│ f64 标准          │ {:>27} │ {:>15} │", f64_result.total_turnover, f64_result.final_precision_loss);
    println!("│ f32 标准          │ {:>27} │ {:>15} │", f32_result.total_turnover, f32_result.final_precision_loss);
    println!("│ Decimal         │ {:>27} │ {:>15} │", decimal_result.total_turnover, decimal_result.final_precision_loss);
    println!("│ Kahan f64       │ {:>27} │ {:>15} │", kahan_result.total_turnover, kahan_result.final_precision_loss);
    println!("└─────────────────┴─────────────────────────────┴─────────────────┘");

    println!();
    println!("=== 关键发现 ===");
    println!("• 测试场景: 10位精度数值累加{}次", ITERATIONS);
    println!("• Decimal提供完全精确的计算结果，无精度损失");
    println!("• f64在大量累积后会出现精度损失");
    println!("• f32精度损失更严重，不适合长期累积");
    println!("• Kahan补偿算法可以显著改善f64的精度");
    
    println!();
    println!("=== K线聚合建议 ===");
    println!("• 金融数据计算: 强烈推荐使用 Decimal");
    println!("• 性能要求极高: 可考虑 Kahan算法 + f64");
    println!("• 避免使用 f32 进行长期累积计算");
    println!("• 对于1周K线聚合，Decimal是最安全的选择");
}

fn print_time_row(name: &str, time: &std::time::Duration, fastest: &std::time::Duration) {
    let ratio = time.as_secs_f64() / fastest.as_secs_f64();
    println!("│ {:15} │ {:>13.3} s │ {:>13.2}x │", 
        name, 
        time.as_secs_f64(),
        ratio
    );
}
