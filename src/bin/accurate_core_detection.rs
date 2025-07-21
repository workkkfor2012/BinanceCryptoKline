//! 更准确的物理核心检测方法

use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
struct CoreTopology {
    logical_id: usize,
    physical_id: Option<usize>,
    core_id: Option<usize>,
    is_physical_core: bool,
}

/// 通过多种方法检测真正的物理核心
fn detect_physical_cores_advanced() -> Vec<core_affinity::CoreId> {
    let all_cores = core_affinity::get_core_ids().unwrap_or_default();
    
    println!("=== 高级物理核心检测 ===");
    println!("总逻辑核心数: {}", all_cores.len());
    
    // 方法1: Windows - 使用wmic获取详细拓扑信息
    #[cfg(target_os = "windows")]
    {
        if let Some(physical_cores) = detect_windows_physical_cores(&all_cores) {
            return physical_cores;
        }
    }
    
    // 方法2: Linux - 解析/proc/cpuinfo
    #[cfg(target_os = "linux")]
    {
        if let Some(physical_cores) = detect_linux_physical_cores(&all_cores) {
            return physical_cores;
        }
    }
    
    // 方法3: 性能测试法 - 通过实际性能测试识别物理核心
    if let Some(physical_cores) = detect_by_performance_test(&all_cores) {
        return physical_cores;
    }
    
    // 方法4: 启发式方法 (最后的备选方案)
    println!("⚠️ 使用启发式方法检测物理核心");
    detect_by_heuristic(&all_cores)
}

#[cfg(target_os = "windows")]
fn detect_windows_physical_cores(all_cores: &[core_affinity::CoreId]) -> Option<Vec<core_affinity::CoreId>> {
    use std::process::Command;
    
    println!("🔍 方法1: Windows WMI检测");
    
    // 获取处理器拓扑信息
    let output = Command::new("wmic")
        .args(&["path", "Win32_Processor", "get", "NumberOfCores,NumberOfLogicalProcessors", "/format:csv"])
        .output()
        .ok()?;
    
    let output_str = String::from_utf8_lossy(&output.stdout);
    println!("WMI输出: {}", output_str);
    
    // 解析CSV输出
    for line in output_str.lines().skip(1) { // 跳过标题行
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() >= 3 {
            if let (Ok(cores), Ok(logical)) = (
                fields[1].trim().parse::<usize>(),
                fields[2].trim().parse::<usize>()
            ) {
                println!("检测到: {} 物理核心, {} 逻辑核心", cores, logical);
                
                if logical > cores {
                    // 有超线程，选择前N个偶数索引的核心
                    let physical_cores: Vec<_> = all_cores
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| i % 2 == 0)
                        .take(cores)
                        .map(|(_, &core)| core)
                        .collect();
                    
                    println!("✅ 选择的物理核心: {:?}", 
                        physical_cores.iter().map(|c| c.id).collect::<Vec<_>>());
                    return Some(physical_cores);
                }
            }
        }
    }
    
    None
}

#[cfg(target_os = "linux")]
fn detect_linux_physical_cores(all_cores: &[core_affinity::CoreId]) -> Option<Vec<core_affinity::CoreId>> {
    use std::fs;
    
    println!("🔍 方法2: Linux /proc/cpuinfo检测");
    
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").ok()?;
    let mut topology = HashMap::new();
    let mut current_processor = None;
    let mut current_physical_id = None;
    let mut current_core_id = None;
    
    for line in cpuinfo.lines() {
        if line.starts_with("processor") {
            if let Some(proc_str) = line.split(':').nth(1) {
                current_processor = proc_str.trim().parse::<usize>().ok();
            }
        } else if line.starts_with("physical id") {
            if let Some(phys_str) = line.split(':').nth(1) {
                current_physical_id = phys_str.trim().parse::<usize>().ok();
            }
        } else if line.starts_with("core id") {
            if let Some(core_str) = line.split(':').nth(1) {
                current_core_id = core_str.trim().parse::<usize>().ok();
            }
        } else if line.trim().is_empty() {
            // 处理器信息结束
            if let (Some(proc), Some(phys), Some(core)) = (current_processor, current_physical_id, current_core_id) {
                topology.insert(proc, (phys, core));
            }
            current_processor = None;
            current_physical_id = None;
            current_core_id = None;
        }
    }
    
    // 找出每个物理核心的第一个逻辑核心
    let mut physical_cores_map = HashMap::new();
    for (logical_id, (physical_id, core_id)) in topology {
        let key = (physical_id, core_id);
        physical_cores_map.entry(key).or_insert(logical_id);
    }
    
    let mut physical_logical_ids: Vec<_> = physical_cores_map.values().cloned().collect();
    physical_logical_ids.sort();
    
    let physical_cores: Vec<_> = physical_logical_ids
        .into_iter()
        .filter_map(|logical_id| all_cores.get(logical_id).copied())
        .collect();
    
    println!("✅ Linux检测到的物理核心: {:?}", 
        physical_cores.iter().map(|c| c.id).collect::<Vec<_>>());
    
    Some(physical_cores)
}

/// 通过性能测试检测物理核心
fn detect_by_performance_test(all_cores: &[core_affinity::CoreId]) -> Option<Vec<core_affinity::CoreId>> {
    println!("🔍 方法3: 性能测试检测 (跳过，需要较长时间)");
    // 这里可以实现CPU密集型测试来识别真正的物理核心
    // 物理核心通常有更好的独立性能表现
    None
}

/// 启发式方法检测
fn detect_by_heuristic(all_cores: &[core_affinity::CoreId]) -> Vec<core_affinity::CoreId> {
    println!("🔍 方法4: 启发式检测");
    
    let total_cores = all_cores.len();
    
    // 常见的超线程比例
    let possible_ratios = [2, 4]; // 2:1 或 4:1 超线程
    
    for ratio in possible_ratios {
        if total_cores % ratio == 0 && total_cores >= ratio {
            let physical_count = total_cores / ratio;
            println!("假设超线程比例 {}:1, 物理核心数: {}", ratio, physical_count);
            
            // 选择均匀分布的核心
            let physical_cores: Vec<_> = (0..physical_count)
                .map(|i| i * ratio)
                .filter_map(|idx| all_cores.get(idx).copied())
                .collect();
            
            println!("启发式选择的核心: {:?}", 
                physical_cores.iter().map(|c| c.id).collect::<Vec<_>>());
            
            return physical_cores;
        }
    }
    
    // 如果都不匹配，返回所有核心
    println!("无法确定超线程模式，使用所有核心");
    all_cores.to_vec()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 高级物理核心检测测试 ===\n");
    
    let physical_cores = detect_physical_cores_advanced();
    
    println!("\n=== 最终结果 ===");
    println!("检测到的物理核心: {:?}", 
        physical_cores.iter().map(|c| c.id).collect::<Vec<_>>());
    
    // 验证绑定
    println!("\n=== 绑定验证 ===");
    for (i, &core) in physical_cores.iter().take(4).enumerate() {
        if core_affinity::set_for_current(core) {
            println!("✅ Worker {} -> 物理核心 {}", i, core.id);
        } else {
            println!("❌ Worker {} -> 物理核心 {} 绑定失败", i, core.id);
        }
    }
    
    println!("\n=== 建议 ===");
    println!("1. 在生产环境中，建议使用系统特定的API进行检测");
    println!("2. 可以通过性能基准测试验证核心选择的正确性");
    println!("3. 监控系统性能指标来确认绑定效果");
    
    Ok(())
}
