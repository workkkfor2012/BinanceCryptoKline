use std::time::Instant;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const ITERATIONS: usize = 10_000;

fn main() {
    println!("=== 浮点数 vs Decimal 性能对比测试 ===");
    println!("测试迭代次数: {} 次", ITERATIONS);
    println!();

    // 测试数据
    let f64_a = 12.34567890_f64;
    let f64_b = 9.87654321_f64;
    
    let decimal_a = dec!(12.34567890);
    let decimal_b = dec!(9.87654321);

    println!("测试数据:");
    println!("  数值A: {}", f64_a);
    println!("  数值B: {}", f64_b);
    println!();

    // f64 性能测试
    println!("开始 f64 性能测试...");
    let f64_results = test_f64_performance(f64_a, f64_b);
    
    // Decimal 性能测试
    println!("开始 Decimal 性能测试...");
    let decimal_results = test_decimal_performance(decimal_a, decimal_b);

    // 输出结果对比
    print_comparison_results(&f64_results, &decimal_results);
}

#[derive(Debug)]
struct PerformanceResults {
    add_time: std::time::Duration,
    sub_time: std::time::Duration,
    mul_time: std::time::Duration,
    div_time: std::time::Duration,
    total_time: std::time::Duration,
}

fn test_f64_performance(a: f64, b: f64) -> PerformanceResults {
    // 加法测试
    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a + b;
    }
    let add_time = start.elapsed();
    println!("  f64 加法完成，结果样本: {}", result / ITERATIONS as f64);

    // 减法测试
    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a - b;
    }
    let sub_time = start.elapsed();
    println!("  f64 减法完成，结果样本: {}", result / ITERATIONS as f64);

    // 乘法测试
    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a * b;
    }
    let mul_time = start.elapsed();
    println!("  f64 乘法完成，结果样本: {}", result / ITERATIONS as f64);

    // 除法测试
    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a / b;
    }
    let div_time = start.elapsed();
    println!("  f64 除法完成，结果样本: {}", result / ITERATIONS as f64);

    let total_time = add_time + sub_time + mul_time + div_time;

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time,
    }
}

fn test_decimal_performance(a: Decimal, b: Decimal) -> PerformanceResults {
    // 加法测试
    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a + b;
    }
    let add_time = start.elapsed();
    println!("  Decimal 加法完成，结果样本: {}", result / Decimal::from(ITERATIONS));

    // 减法测试
    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a - b;
    }
    let sub_time = start.elapsed();
    println!("  Decimal 减法完成，结果样本: {}", result / Decimal::from(ITERATIONS));

    // 乘法测试
    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a * b;
    }
    let mul_time = start.elapsed();
    println!("  Decimal 乘法完成，结果样本: {}", result / Decimal::from(ITERATIONS));

    // 除法测试
    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a / b;
    }
    let div_time = start.elapsed();
    println!("  Decimal 除法完成，结果样本: {}", result / Decimal::from(ITERATIONS));

    let total_time = add_time + sub_time + mul_time + div_time;

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time,
    }
}

fn print_comparison_results(f64_results: &PerformanceResults, decimal_results: &PerformanceResults) {
    println!();
    println!("=== 性能对比结果 ===");
    println!();

    // 详细时间对比
    println!("详细执行时间对比:");
    println!("┌─────────────┬─────────────────┬─────────────────┬─────────────────┐");
    println!("│ 运算类型    │ f64 耗时        │ Decimal 耗时    │ 性能差异        │");
    println!("├─────────────┼─────────────────┼─────────────────┼─────────────────┤");
    
    print_operation_comparison("加法", f64_results.add_time, decimal_results.add_time);
    print_operation_comparison("减法", f64_results.sub_time, decimal_results.sub_time);
    print_operation_comparison("乘法", f64_results.mul_time, decimal_results.mul_time);
    print_operation_comparison("除法", f64_results.div_time, decimal_results.div_time);
    
    println!("├─────────────┼─────────────────┼─────────────────┼─────────────────┤");
    print_operation_comparison("总计", f64_results.total_time, decimal_results.total_time);
    println!("└─────────────┴─────────────────┴─────────────────┴─────────────────┘");

    println!();
    println!("平均单次运算耗时:");
    println!("┌─────────────┬─────────────────┬─────────────────┐");
    println!("│ 运算类型    │ f64 (纳秒)      │ Decimal (纳秒)  │");
    println!("├─────────────┼─────────────────┼─────────────────┤");
    
    print_average_time("加法", f64_results.add_time, decimal_results.add_time);
    print_average_time("减法", f64_results.sub_time, decimal_results.sub_time);
    print_average_time("乘法", f64_results.mul_time, decimal_results.mul_time);
    print_average_time("除法", f64_results.div_time, decimal_results.div_time);
    
    println!("└─────────────┴─────────────────┴─────────────────┘");

    // 总体性能摘要
    let performance_ratio = decimal_results.total_time.as_nanos() as f64 / f64_results.total_time.as_nanos() as f64;
    println!();
    println!("=== 性能摘要 ===");
    println!("f64 总耗时:     {:>12.3} ms", f64_results.total_time.as_secs_f64() * 1000.0);
    println!("Decimal 总耗时: {:>12.3} ms", decimal_results.total_time.as_secs_f64() * 1000.0);
    println!("性能比率:       {:>12.2}x (Decimal相对于f64)", performance_ratio);
    
    if performance_ratio > 1.0 {
        println!("结论: f64 比 Decimal 快 {:.1}%", (performance_ratio - 1.0) * 100.0);
    } else {
        println!("结论: Decimal 比 f64 快 {:.1}%", (1.0 / performance_ratio - 1.0) * 100.0);
    }
}

fn print_operation_comparison(op_name: &str, f64_time: std::time::Duration, decimal_time: std::time::Duration) {
    let ratio = decimal_time.as_nanos() as f64 / f64_time.as_nanos() as f64;
    let diff_percent = if ratio > 1.0 {
        format!("Decimal慢{:.1}%", (ratio - 1.0) * 100.0)
    } else {
        format!("f64慢{:.1}%", (1.0 / ratio - 1.0) * 100.0)
    };
    
    println!("│ {:11} │ {:>13.3} ms │ {:>13.3} ms │ {:15} │", 
        op_name,
        f64_time.as_secs_f64() * 1000.0,
        decimal_time.as_secs_f64() * 1000.0,
        diff_percent
    );
}

fn print_average_time(op_name: &str, f64_time: std::time::Duration, decimal_time: std::time::Duration) {
    let f64_avg_ns = f64_time.as_nanos() / ITERATIONS as u128;
    let decimal_avg_ns = decimal_time.as_nanos() / ITERATIONS as u128;
    
    println!("│ {:11} │ {:>15} │ {:>15} │", 
        op_name,
        f64_avg_ns,
        decimal_avg_ns
    );
}
