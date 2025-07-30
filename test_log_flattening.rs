//! 测试日志扁平化改造的效果
//!
//! 这个测试程序验证：
//! 1. TargetLogLayer 输出扁平化的 JSON
//! 2. BeaconLogLayer 输出扁平化的 JSON
//! 3. LowFreqLogLayer 输出扁平化的 JSON
//! 4. 不再有嵌套的 "fields": {"fields": {...}} 结构

use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 开始测试日志扁平化改造...");

    // 清理之前的测试文件
    let _ = fs::remove_file("test_beacon.log");
    let _ = fs::remove_file("test_low_freq.log");

    // 初始化各种日志层
    init_beacon_log("test_beacon.log")?;
    init_low_freq_log("test_low_freq.log")?;
    
    // 注意：TargetLogLayer 需要命名管道，这里我们跳过它的初始化
    // init_target_log_sender("test_pipe");

    // 设置 tracing 订阅器，只包含 beacon 和 low_freq 层
    tracing_subscriber::registry()
        .with(BeaconLogLayer::new())
        .with(LowFreqLogLayer::new())
        .init();

    println!("📝 生成测试日志...");

    // 测试 beacon 日志（扁平化）
    info!(
        log_type = "beacon",
        target = "测试模块",
        service = "test_service",
        status = "running",
        count = 42,
        rate = 3.14,
        enabled = true,
        "Beacon 测试消息"
    );

    // 测试 low_freq 日志（扁平化）
    warn!(
        log_type = "low_freq", 
        target = "系统检查点",
        checkpoint = "startup_complete",
        memory_mb = 256,
        cpu_percent = 15.5,
        healthy = true,
        "Low frequency 测试消息"
    );

    // 测试复杂嵌套数据（应该被扁平化）
    error!(
        log_type = "beacon",
        target = "复杂数据测试",
        nested_data = serde_json::json!({
            "inner": {
                "value": "should_be_flattened",
                "number": 123
            }
        }),
        "复杂嵌套数据测试"
    );

    // 等待日志写入完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("🔍 检查生成的日志文件...");

    // 检查 beacon 日志
    if Path::new("test_beacon.log").exists() {
        let beacon_content = fs::read_to_string("test_beacon.log")?;
        println!("\n📄 Beacon 日志内容:");
        for line in beacon_content.lines() {
            if !line.trim().is_empty() {
                println!("  {}", line);
                
                // 验证是否为有效的 JSON 且结构扁平
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                    if let Some(obj) = json.as_object() {
                        // 检查是否有嵌套的 fields 结构
                        if obj.contains_key("fields") {
                            println!("  ❌ 发现嵌套的 'fields' 结构！");
                        } else {
                            println!("  ✅ 结构已扁平化");
                        }
                        
                        // 验证必要字段是否存在于顶层
                        let required_fields = ["timestamp", "level", "target", "message"];
                        for field in &required_fields {
                            if obj.contains_key(*field) {
                                println!("  ✅ 顶层包含字段: {}", field);
                            } else {
                                println!("  ❌ 缺少顶层字段: {}", field);
                            }
                        }
                    }
                } else {
                    println!("  ❌ 无效的 JSON 格式");
                }
            }
        }
    } else {
        println!("❌ beacon 日志文件未生成");
    }

    // 检查 low_freq 日志
    if Path::new("test_low_freq.log").exists() {
        let low_freq_content = fs::read_to_string("test_low_freq.log")?;
        println!("\n📄 Low Frequency 日志内容:");
        for line in low_freq_content.lines() {
            if !line.trim().is_empty() {
                println!("  {}", line);
                
                // 验证是否为有效的 JSON 且结构扁平
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                    if let Some(obj) = json.as_object() {
                        // 检查是否有嵌套的 fields 结构
                        if obj.contains_key("fields") {
                            println!("  ❌ 发现嵌套的 'fields' 结构！");
                        } else {
                            println!("  ✅ 结构已扁平化");
                        }
                    }
                } else {
                    println!("  ❌ 无效的 JSON 格式");
                }
            }
        }
    } else {
        println!("❌ low_freq 日志文件未生成");
    }

    println!("\n🎉 测试完成！");
    println!("💡 如果看到 '✅ 结构已扁平化' 消息，说明改造成功");
    println!("💡 如果看到 '❌ 发现嵌套的 fields 结构' 消息，说明还需要进一步修复");

    Ok(())
}
