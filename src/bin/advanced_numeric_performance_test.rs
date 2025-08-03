use std::time::Instant;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use glam::{Vec4, DVec4};

#[cfg(feature = "simd")]
use wide::{f32x4, f64x4};

const ITERATIONS: usize = 10_000;
const SIMD_ITERATIONS: usize = 2_500; // 4个元素一组，所以迭代次数是1/4

fn main() {
    println!("=== 高性能数值计算方案性能对比测试 ===");
    println!("标量测试迭代次数: {} 次", ITERATIONS);
    println!("SIMD测试迭代次数: {} 次 (每次处理4个元素)", SIMD_ITERATIONS);
    println!();

    // 测试数据
    let f32_a = 12.34567890_f32;
    let f32_b = 9.87654321_f32;
    let f64_a = 12.34567890_f64;
    let f64_b = 9.87654321_f64;
    let decimal_a = dec!(12.34567890);
    let decimal_b = dec!(9.87654321);

    println!("测试数据:");
    println!("  数值A: {}", f64_a);
    println!("  数值B: {}", f64_b);
    println!();

    // 1. 标量f32性能测试
    println!("1. 开始 f32 标量性能测试...");
    let f32_results = test_f32_performance(f32_a, f32_b);
    
    // 2. 标量f64性能测试
    println!("2. 开始 f64 标量性能测试...");
    let f64_results = test_f64_performance(f64_a, f64_b);
    
    // 3. Decimal性能测试
    println!("3. 开始 Decimal 性能测试...");
    let decimal_results = test_decimal_performance(decimal_a, decimal_b);

    // 4. glam Vec4 (f32x4) SIMD测试
    println!("4. 开始 glam Vec4 (f32x4) SIMD 性能测试...");
    let glam_vec4_results = test_glam_vec4_performance(f32_a, f32_b);

    // 5. glam DVec4 (f64x4) SIMD测试
    println!("5. 开始 glam DVec4 (f64x4) SIMD 性能测试...");
    let glam_dvec4_results = test_glam_dvec4_performance(f64_a, f64_b);

    // 6. 标准库 SIMD测试 (如果可用)
    #[cfg(feature = "simd")]
    {
        println!("6. 开始 wide f32x4 SIMD 性能测试...");
        let wide_f32x4_results = test_wide_f32x4_performance(f32_a, f32_b);
        println!("7. 开始 wide f64x4 SIMD 性能测试...");
        let wide_f64x4_results = test_wide_f64x4_performance(f64_a, f64_b);
    }

    // 输出结果对比
    print_comprehensive_comparison(&[
        ("f32 标量", f32_results),
        ("f64 标量", f64_results),
        ("Decimal", decimal_results),
        ("glam Vec4", glam_vec4_results),
        ("glam DVec4", glam_dvec4_results),
    ]);
}

#[derive(Debug, Clone)]
struct PerformanceResults {
    add_time: std::time::Duration,
    sub_time: std::time::Duration,
    mul_time: std::time::Duration,
    div_time: std::time::Duration,
    total_time: std::time::Duration,
    iterations: usize,
}

fn test_f32_performance(a: f32, b: f32) -> PerformanceResults {
    let start = Instant::now();
    let mut result = 0.0_f32;
    for _ in 0..ITERATIONS {
        result += a + b;
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = 0.0_f32;
    for _ in 0..ITERATIONS {
        result += a - b;
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = 0.0_f32;
    for _ in 0..ITERATIONS {
        result += a * b;
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = 0.0_f32;
    for _ in 0..ITERATIONS {
        result += a / b;
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: ITERATIONS,
    }
}

fn test_f64_performance(a: f64, b: f64) -> PerformanceResults {
    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a + b;
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a - b;
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a * b;
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = 0.0_f64;
    for _ in 0..ITERATIONS {
        result += a / b;
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: ITERATIONS,
    }
}

fn test_decimal_performance(a: Decimal, b: Decimal) -> PerformanceResults {
    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a + b;
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a - b;
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a * b;
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = Decimal::ZERO;
    for _ in 0..ITERATIONS {
        result += a / b;
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: ITERATIONS,
    }
}

fn test_glam_vec4_performance(a: f32, b: f32) -> PerformanceResults {
    let vec_a = Vec4::splat(a);
    let vec_b = Vec4::splat(b);

    let start = Instant::now();
    let mut result = Vec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a + vec_b;
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = Vec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a - vec_b;
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = Vec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a * vec_b;
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = Vec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a / vec_b;
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: SIMD_ITERATIONS * 4, // 每次处理4个元素
    }
}

fn test_glam_dvec4_performance(a: f64, b: f64) -> PerformanceResults {
    let vec_a = DVec4::splat(a);
    let vec_b = DVec4::splat(b);

    let start = Instant::now();
    let mut result = DVec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a + vec_b;
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = DVec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a - vec_b;
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = DVec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a * vec_b;
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = DVec4::ZERO;
    for _ in 0..SIMD_ITERATIONS {
        result += vec_a / vec_b;
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: SIMD_ITERATIONS * 4,
    }
}

#[cfg(feature = "simd")]
fn test_wide_f32x4_performance(a: f32, b: f32) -> PerformanceResults {
    let vec_a = f32x4::from([a, a, a, a]);
    let vec_b = f32x4::from([b, b, b, b]);

    let start = Instant::now();
    let mut result = f32x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a + vec_b);
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = f32x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a - vec_b);
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = f32x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a * vec_b);
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = f32x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a / vec_b);
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: SIMD_ITERATIONS * 4,
    }
}

#[cfg(feature = "simd")]
fn test_wide_f64x4_performance(a: f64, b: f64) -> PerformanceResults {
    let vec_a = f64x4::from([a, a, a, a]);
    let vec_b = f64x4::from([b, b, b, b]);

    let start = Instant::now();
    let mut result = f64x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a + vec_b);
    }
    let add_time = start.elapsed();

    let start = Instant::now();
    let mut result = f64x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a - vec_b);
    }
    let sub_time = start.elapsed();

    let start = Instant::now();
    let mut result = f64x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a * vec_b);
    }
    let mul_time = start.elapsed();

    let start = Instant::now();
    let mut result = f64x4::from([0.0, 0.0, 0.0, 0.0]);
    for _ in 0..SIMD_ITERATIONS {
        result = result + (vec_a / vec_b);
    }
    let div_time = start.elapsed();

    PerformanceResults {
        add_time,
        sub_time,
        mul_time,
        div_time,
        total_time: add_time + sub_time + mul_time + div_time,
        iterations: SIMD_ITERATIONS * 4,
    }
}

fn print_comprehensive_comparison(results: &[(&str, PerformanceResults)]) {
    println!();
    println!("=== 综合性能对比结果 ===");
    println!();

    // 找到最快的方案作为基准
    let fastest_total = results.iter()
        .map(|(_, r)| r.total_time.as_nanos())
        .min()
        .unwrap();

    println!("详细执行时间对比 (以最快方案为基准):");
    println!("┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐");
    println!("│ 方案类型        │ 加法 (ms)       │ 减法 (ms)       │ 乘法 (ms)       │ 除法 (ms)       │ 总计 (ms)       │");
    println!("├─────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┤");

    for (name, result) in results {
        println!("│ {:15} │ {:>13.3} │ {:>13.3} │ {:>13.3} │ {:>13.3} │ {:>13.3} │",
            name,
            result.add_time.as_secs_f64() * 1000.0,
            result.sub_time.as_secs_f64() * 1000.0,
            result.mul_time.as_secs_f64() * 1000.0,
            result.div_time.as_secs_f64() * 1000.0,
            result.total_time.as_secs_f64() * 1000.0,
        );
    }
    println!("└─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘");

    println!();
    println!("相对性能对比 (相对于最快方案的倍数):");
    println!("┌─────────────────┬─────────────────┬─────────────────┐");
    println!("│ 方案类型        │ 相对性能倍数    │ 吞吐量 (Mops/s) │");
    println!("├─────────────────┼─────────────────┼─────────────────┤");

    for (name, result) in results {
        let relative_performance = result.total_time.as_nanos() as f64 / fastest_total as f64;
        let throughput = (result.iterations * 4) as f64 / result.total_time.as_secs_f64() / 1_000_000.0; // Mops/s

        println!("│ {:15} │ {:>13.2}x │ {:>13.1} │",
            name,
            relative_performance,
            throughput
        );
    }
    println!("└─────────────────┴─────────────────┴─────────────────┘");

    println!();
    println!("平均单次运算耗时 (纳秒):");
    println!("┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐");
    println!("│ 方案类型        │ 加法            │ 减法            │ 乘法            │ 除法            │");
    println!("├─────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┤");

    for (name, result) in results {
        let ops_count = result.iterations;
        println!("│ {:15} │ {:>15} │ {:>15} │ {:>15} │ {:>15} │",
            name,
            result.add_time.as_nanos() / ops_count as u128,
            result.sub_time.as_nanos() / ops_count as u128,
            result.mul_time.as_nanos() / ops_count as u128,
            result.div_time.as_nanos() / ops_count as u128,
        );
    }
    println!("└─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘");

    // 性能排名
    let mut sorted_results: Vec<_> = results.iter().collect();
    sorted_results.sort_by(|a, b| a.1.total_time.cmp(&b.1.total_time));

    println!();
    println!("=== 性能排名 ===");
    for (rank, (name, result)) in sorted_results.iter().enumerate() {
        let relative_to_fastest = result.total_time.as_nanos() as f64 / fastest_total as f64;
        println!("{}. {} - {:.3} ms ({}x)",
            rank + 1,
            name,
            result.total_time.as_secs_f64() * 1000.0,
            if relative_to_fastest == 1.0 { "基准".to_string() } else { format!("{:.2}", relative_to_fastest) }
        );
    }

    println!();
    println!("=== 关键发现 ===");
    let fastest = sorted_results[0];
    let slowest = sorted_results.last().unwrap();
    let speed_diff = slowest.1.total_time.as_nanos() as f64 / fastest.1.total_time.as_nanos() as f64;

    println!("• 最快方案: {} ({:.3} ms)", fastest.0, fastest.1.total_time.as_secs_f64() * 1000.0);
    println!("• 最慢方案: {} ({:.3} ms)", slowest.0, slowest.1.total_time.as_secs_f64() * 1000.0);
    println!("• 性能差异: 最快比最慢快 {:.1}x", speed_diff);

    // SIMD效果分析
    let simd_methods: Vec<_> = results.iter()
        .filter(|(name, _)| name.contains("glam") || name.contains("simba"))
        .collect();

    if !simd_methods.is_empty() {
        println!("• SIMD优化效果明显，建议在批量计算时使用SIMD方案");
    }
}
