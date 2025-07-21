//! 测试物理核心检测逻辑

/// 获取系统的物理核心数量
fn get_physical_core_count() -> usize {
    #[cfg(target_os = "windows")]
    {
        use std::process::Command;
        
        // 使用wmic命令获取物理核心数
        if let Ok(output) = Command::new("wmic")
            .args(&["cpu", "get", "NumberOfCores", "/format:list"])
            .output()
        {
            let output_str = String::from_utf8_lossy(&output.stdout);
            for line in output_str.lines() {
                if line.starts_with("NumberOfCores=") {
                    if let Some(cores_str) = line.split('=').nth(1) {
                        if let Ok(cores) = cores_str.trim().parse::<usize>() {
                            return cores;
                        }
                    }
                }
            }
        }
    }
    
    // 如果无法通过系统API获取，使用启发式方法
    let total_logical_cores = core_affinity::get_core_ids().unwrap_or_default().len();
    
    // 假设如果逻辑核心数是偶数且大于4，可能启用了超线程
    if total_logical_cores >= 4 && total_logical_cores % 2 == 0 {
        // 常见情况：超线程比例为2:1
        total_logical_cores / 2
    } else {
        // 保守估计：使用所有逻辑核心
        total_logical_cores
    }
}

/// 获取物理核心ID，跳过超线程核心
fn get_physical_core_ids() -> Vec<core_affinity::CoreId> {
    let all_cores = core_affinity::get_core_ids().unwrap_or_default();
    let total_logical_cores = all_cores.len();
    
    if total_logical_cores == 0 {
        println!("⚠️ 无法获取CPU核心信息");
        return Vec::new();
    }
    
    // 尝试通过系统信息获取物理核心数
    let physical_core_count = get_physical_core_count();
    
    println!("🔍 CPU拓扑检测结果:");
    println!("   总逻辑核心数: {}", total_logical_cores);
    println!("   检测到的物理核心数: {}", physical_core_count);
    
    // 如果检测到超线程（逻辑核心数 > 物理核心数）
    if total_logical_cores > physical_core_count {
        println!("✅ 检测到超线程技术，将选择物理核心进行绑定");
        
        // 选择偶数索引的核心作为物理核心
        // 这是基于Intel和AMD处理器的常见拓扑结构
        let physical_cores: Vec<_> = all_cores
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0) // 只选择偶数索引
            .take(physical_core_count) // 限制为检测到的物理核心数
            .map(|(_, core)| core)
            .collect();
            
        println!("🎯 已选择的物理核心ID: {:?}", 
            physical_cores.iter().map(|c| c.id).collect::<Vec<_>>()
        );
        
        physical_cores
    } else {
        // 没有超线程，直接使用所有核心
        println!("ℹ️ 未检测到超线程，使用所有核心");
        all_cores
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 物理核心检测测试 ===\n");
    
    // 1. 显示所有逻辑核心
    let all_cores = core_affinity::get_core_ids().unwrap_or_default();
    println!("🖥️ 所有逻辑核心: {:?}", 
        all_cores.iter().map(|c| c.id).collect::<Vec<_>>()
    );
    
    // 2. 检测物理核心
    let physical_cores = get_physical_core_ids();
    
    // 3. 模拟4个Worker的绑定
    const NUM_WORKERS: usize = 4;
    println!("\n=== Worker核心绑定模拟 ===");
    
    for worker_id in 0..NUM_WORKERS {
        if let Some(core) = physical_cores.get(worker_id) {
            println!("Worker {} -> 物理核心 {}", worker_id, core.id);
        } else {
            println!("⚠️ Worker {} -> 无可用物理核心", worker_id);
        }
    }
    
    // 4. 测试实际绑定
    println!("\n=== 实际绑定测试 ===");
    for worker_id in 0..std::cmp::min(NUM_WORKERS, physical_cores.len()) {
        let core = physical_cores[worker_id];
        if core_affinity::set_for_current(core) {
            println!("✅ Worker {} 成功绑定到物理核心 {}", worker_id, core.id);
        } else {
            println!("❌ Worker {} 绑定到物理核心 {} 失败", worker_id, core.id);
        }
    }
    
    println!("\n=== 建议 ===");
    if physical_cores.len() >= NUM_WORKERS {
        println!("✅ 物理核心数量充足，可以为每个Worker分配独立的物理核心");
        println!("🚀 这将最大化缓存效率并避免超线程竞争");
    } else {
        println!("⚠️ 物理核心数量不足，部分Worker可能需要共享核心或使用超线程");
        println!("💡 建议减少Worker数量或接受性能折衷");
    }
    
    Ok(())
}
