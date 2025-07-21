//! 测试详细的物理核心检测日志
//! 
//! 这个程序模拟klagg_sub_threads中的核心检测逻辑，展示详细的日志输出

use tracing::{info, warn, error};

// 复制核心检测函数 (简化版)
fn get_physical_core_count_with_logs() -> usize {
    #[cfg(target_os = "windows")]
    {
        use std::process::Command;
        
        info!("🔍 Windows环境: 开始WMI查询CPU拓扑信息");
        
        // 方法1: 使用wmic获取详细的CSV格式数据
        info!("📊 尝试方法1: WMI详细查询 (CSV格式)");
        match Command::new("wmic")
            .args(&["path", "Win32_Processor", "get", "NumberOfCores,NumberOfLogicalProcessors", "/format:csv"])
            .output()
        {
            Ok(output) => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                info!(wmi_raw_output = %output_str.trim(), "WMI原始输出");
                
                // 解析CSV输出 (跳过标题行)
                for (line_num, line) in output_str.lines().enumerate() {
                    if line_num == 0 {
                        info!(csv_header = %line, "CSV标题行");
                        continue;
                    }
                    
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    let fields: Vec<&str> = line.split(',').collect();
                    info!(line_num, fields = ?fields, field_count = fields.len(), "解析CSV行");
                    
                    if fields.len() >= 3 {
                        match (fields[1].trim().parse::<usize>(), fields[2].trim().parse::<usize>()) {
                            (Ok(cores), Ok(logical)) => {
                                info!(physical_cores = cores, logical_cores = logical, method = "WMI_CSV", 
                                    "✅ WMI方法1成功: 检测到CPU拓扑信息");
                                return cores;
                            }
                            _ => {
                                warn!(cores_field = fields[1], logical_field = fields[2], "解析数值失败");
                            }
                        }
                    }
                }
                warn!("WMI方法1: 未找到有效的CPU信息行");
            }
            Err(e) => {
                warn!(error = %e, "❌ WMI方法1失败: 无法执行wmic命令");
            }
        }
        
        error!("❌ 所有Windows WMI方法都失败了");
    }
    
    // 启发式方法
    error!("❌ 所有系统API方法都失败，使用启发式方法");
    
    let total_logical_cores = core_affinity::get_core_ids().unwrap_or_default().len();
    info!(total_logical_cores, "🔧 启发式方法: 基于逻辑核心数推测物理核心数");
    
    if total_logical_cores >= 4 && total_logical_cores % 2 == 0 {
        let estimated_physical = total_logical_cores / 2;
        warn!(total_logical_cores, estimated_physical_cores = estimated_physical, 
            assumption = "2:1超线程", "⚠️ 启发式推测: 假设2:1超线程比例");
        estimated_physical
    } else {
        warn!(total_logical_cores, assumption = "无超线程", 
            "⚠️ 启发式推测: 假设无超线程，使用所有逻辑核心");
        total_logical_cores
    }
}

fn get_physical_core_ids_with_logs() -> Vec<core_affinity::CoreId> {
    info!("🔍 开始CPU物理核心检测流程");
    
    let all_cores = core_affinity::get_core_ids().unwrap_or_default();
    let total_logical_cores = all_cores.len();
    
    info!(all_logical_cores = ?all_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
        total_count = total_logical_cores, "📊 检测到的所有逻辑核心");
    
    if total_logical_cores == 0 {
        error!("❌ 无法获取任何CPU核心信息，系统可能存在问题");
        return Vec::new();
    }
    
    // 尝试通过系统信息获取物理核心数
    info!("🔎 正在通过系统API检测物理核心数量...");
    let physical_core_count = get_physical_core_count_with_logs();
    
    info!(total_logical_cores, detected_physical_cores = physical_core_count,
        hyperthreading_ratio = if physical_core_count > 0 { 
            format!("{}:1", total_logical_cores / physical_core_count) 
        } else { 
            "未知".to_string() 
        }, "📈 CPU拓扑检测结果");
    
    // 分析超线程情况并选择物理核心
    if total_logical_cores > physical_core_count && physical_core_count > 0 {
        info!(logical_cores = total_logical_cores, physical_cores = physical_core_count,
            "✅ 检测到超线程技术，逻辑核心数 > 物理核心数");
        
        // 选择偶数索引的核心作为物理核心
        let physical_cores: Vec<_> = all_cores
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .take(physical_core_count)
            .map(|(_, &core)| core)
            .collect();
        
        info!(selected_cores = ?physical_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
            expected_count = physical_core_count, actual_count = physical_cores.len(),
            "✅ 物理核心选择成功");
        
        // 显示超线程核心映射关系
        info!("📋 超线程核心映射关系:");
        let physical_set: std::collections::HashSet<_> = physical_cores.iter().map(|c| c.id).collect();
        
        for (i, core) in all_cores.iter().enumerate() {
            let core_type = if physical_set.contains(&core.id) {
                "物理核心"
            } else {
                "超线程核心"
            };
            
            info!(logical_index = i, core_id = core.id, core_type = core_type,
                "   逻辑索引{} -> 核心ID{} ({})", i, core.id, core_type);
        }
        
        physical_cores
    } else {
        info!(total_cores = total_logical_cores,
            "ℹ️ 未检测到超线程技术，逻辑核心数 = 物理核心数");
        info!(selected_cores = ?all_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
            "✅ 使用所有核心作为物理核心");
        all_cores
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化简单的日志系统
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_thread_ids(true)
        .init();
    
    println!("=== 详细物理核心检测日志测试 ===\n");
    
    // 执行检测
    let physical_cores = get_physical_core_ids_with_logs();
    
    println!("\n=== Worker绑定模拟 ===");
    const NUM_WORKERS: usize = 4;
    
    for worker_id in 0..NUM_WORKERS {
        if let Some(core) = physical_cores.get(worker_id) {
            info!(worker_id, assigned_core_id = core.id, total_physical_cores = physical_cores.len(),
                "🎯 Worker {} 将绑定到物理核心 {}", worker_id, core.id);
        } else {
            error!(worker_id, available_cores = physical_cores.len(),
                "❌ Worker {} 无可用物理核心进行绑定！", worker_id);
        }
    }
    
    println!("\n=== 总结 ===");
    println!("✅ 详细日志系统已就绪");
    println!("🚀 在云服务器上运行时，你将看到完整的检测过程");
    println!("🔍 任何异常情况都会被详细记录");
    
    Ok(())
}
