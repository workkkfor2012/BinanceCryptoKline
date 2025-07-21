//! 启动“完全分区模型”K线聚合服务。
//!
//! ## 核心执行模型
//! - main函数手动创建一个多线程的 `io_runtime`，用于处理所有I/O密集型任务。
//! - 为每个计算Worker创建独立的、绑核的物理线程。
//! - 在每个绑核线程内创建单线程的 `computation_runtime`，专门运行K线聚合计算。
//! - 计算与I/O任务通过MPSC通道解耦。
//! - 实现基于JoinHandle的健壮关闭流程。

// ==================== 运行模式配置 ====================
// 修改这些常量来控制程序运行模式，无需设置环境变量

/// 可视化测试模式开关
/// - true: 启动Web服务器进行K线数据可视化验证，禁用数据库持久化
/// - false: 正常生产模式，启用数据库持久化，禁用Web服务器
const VISUAL_TEST_MODE: bool = false;

/// 测试模式开关（影响数据源）
/// - true: 使用少量测试品种（BTCUSDT等8个品种）
/// - false: 从币安API获取所有U本位永续合约品种
const TEST_MODE: bool = false;

use anyhow::Result;
use chrono;
use futures::{stream, StreamExt};
use kline_server::klagg_sub_threads::{self as klagg, InitialKlineData, WorkerCmd};
use kline_server::kldata::KlineBackfiller;
use kline_server::klcommon::{
    api::{self, BinanceApi},
    db::Database,
    error::AppError,
    log::{self, init_ai_logging, shutdown_target_log_sender}, // 确保导入了 shutdown_target_log_sender
    server_time_sync::ServerTimeSyncManager,
    websocket::{MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler, WebSocketClient},
    WatchdogV2, // 引入 WatchdogV2
    AggregateConfig,
};
use kline_server::soft_assert;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, watch, Notify, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{error, info, instrument, span, warn, trace, Instrument, Level, Span};
// use uuid; // 移除未使用的导入

// --- 常量定义 ---
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";
const NUM_WORKERS: usize = 4;
const CLOCK_SAFETY_MARGIN_MS: u64 = 10;
const MIN_SLEEP_MS: u64 = 10;
const HEALTH_CHECK_INTERVAL_S: u64 = 10; // 新的监控间隔

/// 获取物理核心ID，跳过超线程核心
///
/// 在超线程系统中，物理核心通常对应偶数索引的逻辑核心ID (0, 2, 4, 6...)
/// 超线程核心对应奇数索引的逻辑核心ID (1, 3, 5, 7...)
///
/// 这个函数实现了一个通用的物理核心检测策略：
/// 1. 首先尝试通过系统API获取真实的物理核心数
/// 2. 如果无法获取，则使用启发式方法：选择偶数索引的核心
fn get_physical_core_ids() -> Vec<core_affinity::CoreId> {
    info!(target: "应用生命周期", "🔍 开始CPU物理核心检测流程");

    let all_cores = core_affinity::get_core_ids().unwrap_or_default();
    let total_logical_cores = all_cores.len();

    info!(target: "应用生命周期",
        all_logical_cores = ?all_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
        total_count = total_logical_cores,
        "📊 检测到的所有逻辑核心"
    );

    if total_logical_cores == 0 {
        error!(target: "应用生命周期", "❌ 无法获取任何CPU核心信息，系统可能存在问题");
        return Vec::new();
    }

    // 尝试通过系统信息获取物理核心数
    info!(target: "应用生命周期", "🔎 正在通过系统API检测物理核心数量...");
    let physical_core_count = get_physical_core_count();

    info!(target: "应用生命周期",
        total_logical_cores,
        detected_physical_cores = physical_core_count,
        hyperthreading_ratio = if physical_core_count > 0 {
            format!("{}:1", total_logical_cores / physical_core_count)
        } else {
            "未知".to_string()
        },
        "📈 CPU拓扑检测结果"
    );

    // 分析超线程情况并选择物理核心
    if total_logical_cores > physical_core_count && physical_core_count > 0 {
        info!(target: "应用生命周期",
            logical_cores = total_logical_cores,
            physical_cores = physical_core_count,
            "✅ 检测到超线程技术，逻辑核心数 > 物理核心数"
        );

        // 选择物理核心的策略
        info!(target: "应用生命周期", "🎯 开始物理核心选择流程...");
        let physical_cores = select_physical_cores(&all_cores, physical_core_count);

        // 验证选择的正确性
        if physical_cores.len() == physical_core_count {
            info!(target: "应用生命周期",
                selected_cores = ?physical_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
                expected_count = physical_core_count,
                actual_count = physical_cores.len(),
                "✅ 物理核心选择成功"
            );
        } else {
            error!(target: "应用生命周期",
                expected = physical_core_count,
                actual = physical_cores.len(),
                selected_cores = ?physical_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
                "❌ 物理核心选择数量不匹配！可能存在CPU拓扑异常"
            );
        }

        // 显示超线程核心映射关系
        log_hyperthread_mapping(&all_cores, &physical_cores);

        physical_cores
    } else if physical_core_count == 0 {
        warn!(target: "应用生命周期",
            "⚠️ 无法通过系统API检测物理核心数，使用启发式方法"
        );
        let heuristic_cores = select_cores_heuristic(&all_cores);
        info!(target: "应用生命周期",
            selected_cores = ?heuristic_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
            method = "启发式",
            "🔧 启发式物理核心选择完成"
        );
        heuristic_cores
    } else {
        // 没有超线程，直接使用所有核心
        info!(target: "应用生命周期",
            total_cores = total_logical_cores,
            "ℹ️ 未检测到超线程技术，逻辑核心数 = 物理核心数"
        );
        info!(target: "应用生命周期",
            selected_cores = ?all_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
            "✅ 使用所有核心作为物理核心"
        );
        all_cores
    }
}

/// 显示超线程核心映射关系
fn log_hyperthread_mapping(all_cores: &[core_affinity::CoreId], physical_cores: &[core_affinity::CoreId]) {
    info!(target: "应用生命周期", "📋 超线程核心映射关系:");

    let physical_set: std::collections::HashSet<_> = physical_cores.iter().map(|c| c.id).collect();

    for (i, core) in all_cores.iter().enumerate() {
        let core_type = if physical_set.contains(&core.id) {
            "物理核心"
        } else {
            "超线程核心"
        };

        info!(target: "应用生命周期",
            logical_index = i,
            core_id = core.id,
            core_type = core_type,
            "   逻辑索引{} -> 核心ID{} ({})",
            i, core.id, core_type
        );
    }
}

/// 启发式方法选择核心
fn select_cores_heuristic(all_cores: &[core_affinity::CoreId]) -> Vec<core_affinity::CoreId> {
    let total_cores = all_cores.len();

    info!(target: "应用生命周期",
        total_cores,
        "🔧 启发式检测：分析可能的超线程配置"
    );

    // 常见的超线程比例
    let possible_ratios = [2, 4]; // 2:1 或 4:1 超线程

    for ratio in possible_ratios {
        if total_cores % ratio == 0 && total_cores >= ratio {
            let physical_count = total_cores / ratio;
            info!(target: "应用生命周期",
                ratio,
                physical_count,
                "🎯 假设超线程比例 {}:1, 推测物理核心数: {}",
                ratio, physical_count
            );

            // 选择均匀分布的核心
            let selected_cores: Vec<_> = (0..physical_count)
                .map(|i| i * ratio)
                .filter_map(|idx| all_cores.get(idx).copied())
                .collect();

            if selected_cores.len() == physical_count {
                info!(target: "应用生命周期",
                    selected_cores = ?selected_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
                    "✅ 启发式选择完成"
                );
                return selected_cores;
            }
        }
    }

    // 如果都不匹配，返回所有核心
    warn!(target: "应用生命周期",
        "⚠️ 无法确定超线程模式，使用所有核心"
    );
    all_cores.to_vec()
}

/// 选择物理核心的具体策略
///
/// 这个函数实现了多种策略来选择物理核心，并包含详细的日志记录
fn select_physical_cores(all_cores: &[core_affinity::CoreId], target_count: usize) -> Vec<core_affinity::CoreId> {
    info!(target: "应用生命周期",
        target_count,
        available_cores = all_cores.len(),
        "🎯 开始物理核心选择，目标数量: {}", target_count
    );

    // 策略1: Intel常见模式 - 偶数索引对应物理核心
    info!(target: "应用生命周期", "📝 尝试策略1: Intel常见拓扑模式 (偶数索引)");
    let even_indexed_cores: Vec<_> = all_cores
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 0)
        .take(target_count)
        .map(|(_, &core)| core)
        .collect();

    info!(target: "应用生命周期",
        strategy = "偶数索引",
        selected = ?even_indexed_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
        count = even_indexed_cores.len(),
        "策略1结果: 选择了{}个核心", even_indexed_cores.len()
    );

    if even_indexed_cores.len() == target_count {
        info!(target: "应用生命周期",
            "✅ 策略1成功: 使用Intel常见拓扑模式"
        );
        return even_indexed_cores;
    }

    // 策略2: 均匀分布策略 - 在所有核心中均匀选择
    info!(target: "应用生命周期", "📝 尝试策略2: 均匀分布策略");
    let step = all_cores.len() / target_count;
    if step > 0 {
        let distributed_cores: Vec<_> = (0..target_count)
            .map(|i| i * step)
            .filter_map(|idx| all_cores.get(idx).copied())
            .collect();

        info!(target: "应用生命周期",
            strategy = "均匀分布",
            step_size = step,
            selected = ?distributed_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
            count = distributed_cores.len(),
            "策略2结果: 步长{}, 选择了{}个核心", step, distributed_cores.len()
        );

        if distributed_cores.len() == target_count {
            warn!(target: "应用生命周期",
                "⚠️ 策略2成功: 使用均匀分布策略 (可能不是最优)"
            );
            return distributed_cores;
        }
    }

    // 策略3: 前N个核心 (最保守的策略)
    warn!(target: "应用生命周期",
        "📝 使用策略3: 保守策略 - 选择前{}个核心", target_count
    );
    let conservative_cores: Vec<_> = all_cores.iter().take(target_count).copied().collect();

    error!(target: "应用生命周期",
        strategy = "保守策略",
        selected = ?conservative_cores.iter().map(|c| c.id).collect::<Vec<_>>(),
        count = conservative_cores.len(),
        "⚠️ 使用保守策略，可能包含超线程核心，性能可能受影响"
    );

    conservative_cores
}

/// 获取系统的物理核心数量
///
/// 在不同操作系统上使用不同的方法来获取真实的物理核心数
fn get_physical_core_count() -> usize {
    #[cfg(target_os = "windows")]
    {
        use std::process::Command;

        info!(target: "应用生命周期", "🔍 Windows环境: 开始WMI查询CPU拓扑信息");

        // 方法1: 使用wmic获取详细的CSV格式数据
        info!(target: "应用生命周期", "📊 尝试方法1: WMI详细查询 (CSV格式)");
        match Command::new("wmic")
            .args(&["path", "Win32_Processor", "get", "NumberOfCores,NumberOfLogicalProcessors", "/format:csv"])
            .output()
        {
            Ok(output) => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                info!(target: "应用生命周期",
                    wmi_raw_output = %output_str.trim(),
                    "WMI原始输出"
                );

                // 解析CSV输出 (跳过标题行)
                for (line_num, line) in output_str.lines().enumerate() {
                    if line_num == 0 {
                        info!(target: "应用生命周期", csv_header = %line, "CSV标题行");
                        continue;
                    }

                    if line.trim().is_empty() {
                        continue;
                    }

                    let fields: Vec<&str> = line.split(',').collect();
                    info!(target: "应用生命周期",
                        line_num,
                        fields = ?fields,
                        field_count = fields.len(),
                        "解析CSV行"
                    );

                    if fields.len() >= 3 {
                        match (fields[1].trim().parse::<usize>(), fields[2].trim().parse::<usize>()) {
                            (Ok(cores), Ok(logical)) => {
                                info!(target: "应用生命周期",
                                    physical_cores = cores,
                                    logical_cores = logical,
                                    method = "WMI_CSV",
                                    "✅ WMI方法1成功: 检测到CPU拓扑信息"
                                );
                                return cores;
                            }
                            (Err(e1), Err(e2)) => {
                                warn!(target: "应用生命周期",
                                    cores_field = fields[1],
                                    logical_field = fields[2],
                                    cores_error = %e1,
                                    logical_error = %e2,
                                    "解析数值失败"
                                );
                            }
                            (Err(e), Ok(logical)) => {
                                warn!(target: "应用生命周期",
                                    cores_field = fields[1],
                                    logical_cores = logical,
                                    error = %e,
                                    "物理核心数解析失败"
                                );
                            }
                            (Ok(cores), Err(e)) => {
                                warn!(target: "应用生命周期",
                                    physical_cores = cores,
                                    logical_field = fields[2],
                                    error = %e,
                                    "逻辑核心数解析失败，但已获得物理核心数"
                                );
                                return cores;
                            }
                        }
                    }
                }
                warn!(target: "应用生命周期", "WMI方法1: 未找到有效的CPU信息行");
            }
            Err(e) => {
                warn!(target: "应用生命周期",
                    error = %e,
                    "❌ WMI方法1失败: 无法执行wmic命令"
                );
            }
        }

        // 方法2: 备用的简单格式查询
        info!(target: "应用生命周期", "📊 尝试方法2: WMI简单查询 (LIST格式)");
        match Command::new("wmic")
            .args(&["cpu", "get", "NumberOfCores", "/format:list"])
            .output()
        {
            Ok(output) => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                info!(target: "应用生命周期",
                    wmi_backup_output = %output_str.trim(),
                    "WMI备用方法原始输出"
                );

                for line in output_str.lines() {
                    if line.starts_with("NumberOfCores=") {
                        if let Some(cores_str) = line.split('=').nth(1) {
                            match cores_str.trim().parse::<usize>() {
                                Ok(cores) => {
                                    info!(target: "应用生命周期",
                                        physical_cores = cores,
                                        method = "WMI_LIST",
                                        "✅ WMI方法2成功: 检测到物理核心数"
                                    );
                                    return cores;
                                }
                                Err(e) => {
                                    warn!(target: "应用生命周期",
                                        raw_value = cores_str,
                                        error = %e,
                                        "WMI方法2: 数值解析失败"
                                    );
                                }
                            }
                        }
                    }
                }
                warn!(target: "应用生命周期", "WMI方法2: 未找到NumberOfCores字段");
            }
            Err(e) => {
                warn!(target: "应用生命周期",
                    error = %e,
                    "❌ WMI方法2失败: 无法执行备用wmic命令"
                );
            }
        }

        error!(target: "应用生命周期", "❌ 所有Windows WMI方法都失败了");
    }

    #[cfg(target_os = "linux")]
    {
        use std::fs;
        use std::collections::HashSet;

        info!(target: "应用生命周期", "🐧 Linux环境: 开始解析/proc/cpuinfo");

        // 读取/proc/cpuinfo获取物理核心数
        match fs::read_to_string("/proc/cpuinfo") {
            Ok(cpuinfo) => {
                info!(target: "应用生命周期",
                    cpuinfo_size = cpuinfo.len(),
                    "✅ 成功读取/proc/cpuinfo文件"
                );

                let mut physical_cores = HashSet::new();
                let mut current_processor = None;
                let mut current_physical_id = None;
                let mut current_core_id = None;
                let mut processor_count = 0;

                for (line_num, line) in cpuinfo.lines().enumerate() {
                    if line.starts_with("processor") {
                        if let Some(proc_str) = line.split(':').nth(1) {
                            current_processor = proc_str.trim().parse::<usize>().ok();
                            processor_count += 1;
                        }
                    } else if line.starts_with("physical id") {
                        if let Some(id) = line.split(':').nth(1) {
                            current_physical_id = Some(id.trim().to_string());
                        }
                    } else if line.starts_with("core id") {
                        if let Some(id) = line.split(':').nth(1) {
                            current_core_id = Some(id.trim().to_string());
                        }
                    } else if line.trim().is_empty() {
                        // 处理器信息结束，记录这个物理核心
                        if let (Some(proc), Some(ref phys_id), Some(ref core_id)) =
                            (current_processor, &current_physical_id, &current_core_id) {
                            let core_key = format!("{}:{}", phys_id, core_id);
                            physical_cores.insert(core_key.clone());

                            if line_num < 50 { // 只记录前几个处理器的详细信息，避免日志过多
                                info!(target: "应用生命周期",
                                    processor = proc,
                                    physical_id = phys_id,
                                    core_id = core_id,
                                    core_key = %core_key,
                                    "处理器{}拓扑信息", proc
                                );
                            }
                        }
                        current_processor = None;
                        current_physical_id = None;
                        current_core_id = None;
                    }
                }

                info!(target: "应用生命周期",
                    total_processors = processor_count,
                    unique_physical_cores = physical_cores.len(),
                    physical_core_keys = ?physical_cores.iter().collect::<Vec<_>>(),
                    "Linux /proc/cpuinfo解析结果"
                );

                if !physical_cores.is_empty() {
                    info!(target: "应用生命周期",
                        physical_cores = physical_cores.len(),
                        method = "PROC_CPUINFO",
                        "✅ Linux方法成功: 通过/proc/cpuinfo检测到物理核心数"
                    );
                    return physical_cores.len();
                } else {
                    warn!(target: "应用生命周期",
                        "⚠️ /proc/cpuinfo中未找到physical id和core id信息"
                    );
                }
            }
            Err(e) => {
                error!(target: "应用生命周期",
                    error = %e,
                    "❌ Linux方法失败: 无法读取/proc/cpuinfo文件"
                );
            }
        }
    }

    // 如果无法通过系统API获取，使用启发式方法
    error!(target: "应用生命周期", "❌ 所有系统API方法都失败，使用启发式方法");

    let total_logical_cores = core_affinity::get_core_ids().unwrap_or_default().len();

    info!(target: "应用生命周期",
        total_logical_cores,
        "🔧 启发式方法: 基于逻辑核心数推测物理核心数"
    );

    // 假设如果逻辑核心数是偶数且大于4，可能启用了超线程
    if total_logical_cores >= 4 && total_logical_cores % 2 == 0 {
        let estimated_physical = total_logical_cores / 2;
        warn!(target: "应用生命周期",
            total_logical_cores,
            estimated_physical_cores = estimated_physical,
            assumption = "2:1超线程",
            "⚠️ 启发式推测: 假设2:1超线程比例"
        );
        estimated_physical
    } else {
        warn!(target: "应用生命周期",
            total_logical_cores,
            assumption = "无超线程",
            "⚠️ 启发式推测: 假设无超线程，使用所有逻辑核心"
        );
        total_logical_cores
    }
}

fn main() -> Result<()> {
    // 1. ==================== 日志系统必须最先初始化 ====================
    // 使用 block_on 是因为 init_ai_logging 是 async 的
    // guard 的生命周期将决定性能日志何时被刷新
    let _guard = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(init_ai_logging())?;

    // 设置一个 panic hook 来捕获未处理的 panic
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(target: "应用生命周期", panic_info = %panic_info, "程序发生未捕获的Panic，即将退出");
        original_hook(panic_info);
        std::process::exit(1);
    }));

    // 2. 手动创建主 I/O 运行时
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4) // 可根据I/O密集程度调整
        .thread_name("io-worker")
        .build()?;

    // 3. 在 I/O 运行时上下文中执行应用启动和管理逻辑
    // 使用 instrument 将 main_span 附加到整个应用生命周期
    let main_span = span!(target: "应用生命周期", Level::INFO, "klagg_app_lifecycle");
    let result = io_runtime.block_on(run_app(&io_runtime).instrument(main_span));

    if let Err(e) = &result {
        // 现在我们有日志系统了
        error!(target: "应用生命周期", error = ?e, "应用因顶层错误而异常退出");
    } else {
        info!(target: "应用生命周期", log_type = "low_freq", "应用程序正常关闭");
    }

    // 4. 优雅关闭
    info!(target: "应用生命周期", "主IO运行时开始关闭...");
    io_runtime.shutdown_timeout(Duration::from_secs(5));
    info!(target: "应用生命周期", "主IO运行时已关闭");

    // 5. [关键] 关闭日志系统，确保所有缓冲的日志都被处理
    shutdown_target_log_sender(); // [启用] 新的关闭函数
    // shutdown_log_sender();     // [禁用] 旧的关闭函数

    result
}

// 使用 `instrument` 宏自动创建和进入一个 Span
#[instrument(target = "应用生命周期", skip_all, name = "run_app")]
async fn run_app(io_runtime: &Runtime) -> Result<()> {
    info!(target: "应用生命周期",log_type = "low_freq", "K线聚合服务启动中...");
    // trace!(log_type = "low_freq", "K线聚合服务启动中...");
    // trace!("🔍 开始初始化全局资源...");

    // 1. ==================== 初始化全局资源 ====================
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    info!(
        target: "应用生命周期",
        log_type = "low_freq",
        path = DEFAULT_CONFIG_PATH,
        persistence_ms = config.persistence_interval_ms,
        "配置文件加载成功"
    );
    trace!(target: "应用生命周期", config_details = ?config, "📋 详细配置信息");

    // 使用编译时常量而不是环境变量
    let enable_test_mode = TEST_MODE;
    let visual_test_mode = VISUAL_TEST_MODE;
    info!(target: "应用生命周期", log_type = "low_freq", test_mode = enable_test_mode, visual_test_mode = visual_test_mode, "运行模式确定");
    trace!(target: "应用生命周期", test_mode = enable_test_mode, visual_test_mode = visual_test_mode, "🧪 测试模式详细信息");

    if visual_test_mode {
        warn!(target: "应用生命周期", log_type="low_freq", "警告：程序运行在可视化测试模式，数据库持久化已禁用！");
    }

    let api_client = Arc::new(BinanceApi::new());

    let db = Arc::new(Database::new(&config.database.database_path)?);
    info!(target: "应用生命周期", log_type = "low_freq", path = %config.database.database_path, "数据库连接成功");

    let time_sync_manager = Arc::new(ServerTimeSyncManager::new());
    let periods = Arc::new(config.supported_intervals.clone());
    info!(target: "应用生命周期",log_type = "low_freq", ?periods, "支持的K线周期已加载");

    // ==================== 纯DB驱动的四步骤启动流程 ====================
    let startup_data_prep_start = std::time::Instant::now();
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程] 开始执行纯DB驱动的数据准备流程...");

    let backfiller = KlineBackfiller::new(db.clone(), periods.iter().cloned().collect());

    // --- 阶段一: 历史补齐 ---
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 1/4] 开始历史数据补齐...");
    let stage1_start = std::time::Instant::now();
    backfiller.run_once().await?;
    let stage1_duration = stage1_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage1_duration.as_millis(),
        duration_s = stage1_duration.as_secs_f64(),
        "✅ [启动流程 | 1/4] 历史数据补齐完成"
    );

    // --- 阶段二: 延迟追赶 ---
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 2/4] 开始延迟追赶补齐（高并发模式）...");
    let stage2_start = std::time::Instant::now();
    backfiller.run_once_with_round(2).await?;
    let stage2_duration = stage2_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage2_duration.as_millis(),
        duration_s = stage2_duration.as_secs_f64(),
        "✅ [启动流程 | 2/4] 延迟追赶补齐完成"
    );

    // --- 阶段三: 加载状态 ---
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 3/4] 开始从数据库加载最新K线状态...");
    let stage3_start = std::time::Instant::now();
    let mut initial_klines = backfiller.load_latest_klines_from_db().await?;
    let stage3_duration = stage3_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage3_duration.as_millis(),
        duration_s = stage3_duration.as_secs_f64(),
        klines_count = initial_klines.len(),
        "✅ [启动流程 | 3/4] 数据库状态加载完成"
    );
    if stage3_duration > std::time::Duration::from_secs(5) {
        warn!(target: "应用生命周期", log_type="performance_alert",
            duration_ms = stage3_duration.as_millis(),
            "⚠️ 性能警告：DB状态加载超过5秒"
        );
    }

    // --- 阶段四: 微型补齐 ---
    info!(target: "应用生命周期", log_type="startup", "➡️ [启动流程 | 4/4] 开始进行微型补齐...");
    let stage4_start = std::time::Instant::now();
    time_sync_manager.sync_time_once().await?; // 获取精确的结束时间
    run_micro_backfill(&api_client, &time_sync_manager, &mut initial_klines, &periods).await?;
    let stage4_duration = stage4_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        duration_ms = stage4_duration.as_millis(),
        duration_s = stage4_duration.as_secs_f64(),
        "✅ [启动流程 | 4/4] 微型补齐完成"
    );

    let total_startup_duration = startup_data_prep_start.elapsed();
    info!(target: "应用生命周期", log_type="startup",
        total_duration_ms = total_startup_duration.as_millis(),
        total_duration_s = total_startup_duration.as_secs_f64(),
        stage1_ms = stage1_duration.as_millis(),
        stage2_ms = stage2_duration.as_millis(),
        stage3_ms = stage3_duration.as_millis(),
        stage4_ms = stage4_duration.as_millis(),
        final_klines_count = initial_klines.len(),
        "✅ [启动流程] 所有数据准备阶段完成 - 性能统计"
    );
    let initial_klines_arc = Arc::new(initial_klines);

    // 2. ==================== 初始化通信设施 ====================
    let (clock_tx, _) = watch::channel(0i64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let internal_shutdown_notify = Arc::new(Notify::new());
    let (w3_cmd_tx, w3_cmd_rx) = mpsc::channel::<WorkerCmd>(128);

    // 3. ==================== 在 I/O 运行时启动核心后台服务 ====================
    info!(target: "应用生命周期", "正在执行首次服务器时间同步...");
    time_sync_manager.sync_time_once().await?;
    info!(target: "应用生命周期",
        offset_ms = time_sync_manager.get_time_diff(),
        "首次服务器时间同步完成"
    );

    // [修改逻辑] 使用封装好的 spawn_instrumented_on 来确保上下文传播
    log::context::spawn_instrumented_on(
        run_clock_task(
            config.clone(),
            time_sync_manager.clone(),
            clock_tx.clone(),
            internal_shutdown_notify.clone(),
        ),
        io_runtime,
    );

    // 4. ============ 获取并建立全局品种索引 (G_Index*) ============
    let (all_symbols_sorted, symbol_to_index_map) =
        initialize_symbol_indexing(&api_client, &db, enable_test_mode).await?;
    let symbol_count = all_symbols_sorted.len();

    // [修改逻辑] 使用 soft_assert! 进行业务断言
    soft_assert!(
        symbol_count > 0 || enable_test_mode,
        message = "没有可处理的交易品种",
        actual_count = symbol_count,
        test_mode = enable_test_mode,
    );

    if symbol_count == 0 && !enable_test_mode {
        let err_msg = "没有可处理的交易品种，服务退出";
        // 使用 error! 记录致命错误
        error!(target: "应用生命周期", reason = err_msg);
        return Err(AppError::InitializationError(err_msg.into()).into());
    }
    info!(target: "应用生命周期", log_type = "low_freq", symbol_count, "全局品种索引初始化完成");

    let symbol_to_global_index = Arc::new(RwLock::new(symbol_to_index_map));
    let global_index_to_symbol = Arc::new(RwLock::new(all_symbols_sorted));
    let global_symbol_count = Arc::new(AtomicUsize::new(symbol_count));

    // ==================== 初始化健康监控中枢 ====================
    let watchdog = Arc::new(WatchdogV2::new());

    // 5. ================ 创建并启动绑核的计算线程和独立的I/O任务 ================
    let creation_span = span!(Level::INFO, "workers_creation");
    let _enter = creation_span.enter(); // 手动进入 Span，覆盖整个循环

    let chunks: Vec<_> = global_index_to_symbol
        .read()
        .await
        .chunks((symbol_count + NUM_WORKERS - 1) / NUM_WORKERS)
        .map(|s| s.to_vec())
        .collect();

    let mut worker_read_handles = Vec::with_capacity(NUM_WORKERS);
    let mut computation_thread_handles = Vec::new();

    // 获取物理核心ID，跳过超线程核心
    let physical_cores = get_physical_core_ids();

    info!(target: "应用生命周期",
        log_type = "low_freq",
        total_logical_cores = core_affinity::get_core_ids().unwrap_or_default().len(),
        physical_cores = physical_cores.len(),
        required_workers = NUM_WORKERS,
        "CPU核心拓扑分析完成"
    );

    if physical_cores.len() < NUM_WORKERS {
        warn!(
            physical_cores = physical_cores.len(),
            required = NUM_WORKERS,
            "物理CPU核心数不足，可能影响性能，将不会进行线程绑定"
        );
    }

    let mut current_start_index = 0;
    let mut w3_cmd_rx_option = Some(w3_cmd_rx);

    for worker_id in 0..NUM_WORKERS {
        let assigned_symbols = chunks.get(worker_id).cloned().unwrap_or_default();
        let cmd_rx = if worker_id == NUM_WORKERS - 1 { w3_cmd_rx_option.take() } else { None };

        info!(
            target: "应用生命周期",
            log_type = "low_freq",
            worker_id,
            assigned_symbols = assigned_symbols.len(),
            "计算Worker正在创建"
        );
        let (mut worker, ws_cmd_rx, trade_rx) = klagg::Worker::new(
            worker_id,
            current_start_index,
            &assigned_symbols,
            symbol_to_global_index.clone(),
            periods.clone(),
            cmd_rx,
            clock_tx.subscribe(),
            initial_klines_arc.clone(),
        )
        .await?;

        worker_read_handles.push(worker.get_read_handle());

        // [修改逻辑] 使用 spawn_instrumented_on
        log::context::spawn_instrumented_on(
            klagg::run_io_loop(
                worker_id,
                assigned_symbols.clone(),
                config.clone(),
                shutdown_rx.clone(),
                ws_cmd_rx,
                worker.get_trade_sender(),
                watchdog.clone(), // 传递 watchdog
            ),
            io_runtime,
        );

        let core_to_bind = physical_cores.get(worker_id).copied();

        // 详细记录Worker核心分配情况
        if let Some(core_id) = core_to_bind {
            info!(target: "应用生命周期",
                worker_id,
                assigned_core_id = core_id.id,
                total_physical_cores = physical_cores.len(),
                "🎯 Worker {} 将绑定到物理核心 {}", worker_id, core_id.id
            );
        } else {
            error!(target: "应用生命周期",
                worker_id,
                available_cores = physical_cores.len(),
                "❌ Worker {} 无可用物理核心进行绑定！", worker_id
            );
        }

        let comp_shutdown_rx = shutdown_rx.clone();
        let computation_watchdog = watchdog.clone(); // 为计算线程克隆

        let computation_handle = std::thread::Builder::new()
            .name(format!("computation-worker-{}", worker_id))
            .spawn({
                // [修改逻辑] 捕获当前 Span 以便在 OS 线程中恢复
                let parent_span = Span::current();
                move || {
                    // [修改逻辑] 在新线程中恢复 tracing 上下文
                    parent_span.in_scope(|| {
                        if let Some(core_id) = core_to_bind {
                            info!(target: "计算核心",
                                worker_id,
                                core_id = core_id.id,
                                "🔧 Worker {} 开始尝试绑定到物理核心 {}", worker_id, core_id.id
                            );

                            if core_affinity::set_for_current(core_id) {
                                info!(target: "计算核心",
                                    worker_id,
                                    core_id = core_id.id,
                                    "✅ Worker {} 成功绑定到物理核心 {} - 独占计算资源",
                                    worker_id, core_id.id
                                );
                            } else {
                                error!(target: "计算核心",
                                    worker_id,
                                    core_id = core_id.id,
                                    "❌ Worker {} 绑定到物理核心 {} 失败 - 性能可能受影响",
                                    worker_id, core_id.id
                                );
                            }
                        } else {
                            error!(target: "计算核心",
                                worker_id,
                                "❌ Worker {} 没有分配到物理核心 - 将使用默认调度", worker_id
                            );
                        }

                        let computation_runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();

                        // [修改逻辑] 将 worker 的生命周期也 instrument
                        let worker_span = span!(target: "计算核心", Level::INFO, "computation_worker_runtime", worker_id);
                        computation_runtime.block_on(
                            worker.run_computation_loop(
                                comp_shutdown_rx,
                                trade_rx,
                                computation_watchdog, // [修改] 传递 watchdog
                            )
                            .instrument(worker_span),
                        );
                    })
                }
            })?;

        computation_thread_handles.push(computation_handle);
        current_start_index += assigned_symbols.len();
    }
    drop(_enter); // 退出 apen
    let worker_handles = Arc::new(worker_read_handles);

    // 6. ================= 在 I/O 运行时启动依赖 Worker 的后台任务 ================

    // [核心修改] 根据模式条件性地启动任务
    let persistence_handle = if visual_test_mode {
        info!(target: "应用生命周期", "启动可视化测试Web服务器...");
        // 【修改】调用时不再传递 config
        log::context::spawn_instrumented_on(
            klagg::web_server::run_visual_test_server(
                worker_handles.clone(),
                global_index_to_symbol.clone(),
                periods.clone(),
                shutdown_rx.clone(),
            ),
            io_runtime,
        );
        None
    } else {
        // 在生产模式下，启动持久化任务 (这部分逻辑保持不变)
        Some(log::context::spawn_instrumented_on(
            klagg::persistence_task(
                db.clone(),
                worker_handles.clone(),
                global_index_to_symbol.clone(),
                periods.clone(),
                config.clone(),
                shutdown_rx.clone(),
                watchdog.clone(), // 传递 watchdog
            ),
            io_runtime,
        ))
    };

    // [修改逻辑] 使用 spawn_instrumented_on
    log::context::spawn_instrumented_on(
        run_symbol_manager(
            config.clone(),
            symbol_to_global_index,
            global_index_to_symbol,
            global_symbol_count,
            w3_cmd_tx,
        ),
        io_runtime,
    );

    // [替换] 启动新的 WatchdogV2 监控中枢
    io_runtime.spawn(
        watchdog.run(
            Duration::from_secs(HEALTH_CHECK_INTERVAL_S),
            internal_shutdown_notify.clone(),
        )
    );

    // 7. ==================== 等待并处理关闭信号 ====================
    info!(target: "应用生命周期", "所有服务已启动，等待关闭信号 (Ctrl+C)...");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(target: "应用生命周期", log_type = "low_freq", reason = "received_ctrl_c", "接收到关闭信号，开始优雅关闭");
        },
        _ = internal_shutdown_notify.notified() => {
            info!(target: "应用生命周期", log_type = "low_freq", reason = "internal_shutdown", "接收到内部关闭通知，开始优雅关闭");
        }
    }

    let _ = shutdown_tx.send(true);

    for (worker_id, handle) in computation_thread_handles.into_iter().enumerate() {
        if let Err(e) = handle.join() {
            error!(target: "应用生命周期", worker_id, panic = ?e, "计算线程在退出时发生 panic");
        }
    }

    // [修改] 条件性地等待持久化任务
    if let Some(handle) = persistence_handle {
        if let Err(e) = handle.await {
            error!(target: "应用生命周期", task = "persistence", panic = ?e, "持久化任务在退出时发生 panic");
        }
    }

    Ok(())
}

/// 全局时钟任务
#[instrument(target = "全局时钟", skip_all, name="run_clock_task")]
async fn run_clock_task(
    _config: Arc<AggregateConfig>, // config 不再需要，但保留参数以减少函数签名变动
    time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    // 【核心修改】时钟任务的目标是严格对齐到服务器时间的"整分钟"，不再依赖任何K线周期。
    const CLOCK_INTERVAL_MS: i64 = 60_000; // 60秒
    info!(target: "全局时钟", log_type="low_freq", interval_ms = CLOCK_INTERVAL_MS, "全局时钟任务已启动，将按整分钟对齐");

    // 时间同步重试计数器
    let mut time_sync_retry_count = 0;
    const MAX_TIME_SYNC_RETRIES: u32 = 10;

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            time_sync_retry_count += 1;
            warn!(target: "全局时钟", log_type="retry",
                  retry_count = time_sync_retry_count,
                  max_retries = MAX_TIME_SYNC_RETRIES,
                  "时间同步失效，尝试重试");

            if time_sync_retry_count >= MAX_TIME_SYNC_RETRIES {
                error!(target: "全局时钟", log_type="assertion", reason="time_sync_invalid",
                       retry_count = time_sync_retry_count,
                       "时间同步失效，已达到最大重试次数，服务将关闭");
                shutdown_notify.notify_one();
                break;
            }

            // 等待一段时间后重试
            sleep(Duration::from_millis(1000)).await;
            continue;
        } else {
            // 时间同步恢复正常，重置重试计数器
            if time_sync_retry_count > 0 {
                info!(target: "全局时钟", log_type="recovery",
                      previous_retry_count = time_sync_retry_count,
                      "时间同步已恢复正常");
                time_sync_retry_count = 0;
            }
        }

        let now = time_sync_manager.get_calibrated_server_time();
        if now == 0 {
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        // 【核心修改】计算下一个服务器时间整分钟点
        let next_tick_point = (now / CLOCK_INTERVAL_MS + 1) * CLOCK_INTERVAL_MS;
        let wakeup_time = next_tick_point + CLOCK_SAFETY_MARGIN_MS as i64;
        let sleep_duration_ms = (wakeup_time - now).max(MIN_SLEEP_MS as i64) as u64;

        trace!(target: "全局时钟",
            now,
            next_tick_point,
            wakeup_time,
            sleep_duration_ms,
            "计算下一次唤醒时间"
        );
        sleep(Duration::from_millis(sleep_duration_ms)).await;

        let final_time = time_sync_manager.get_calibrated_server_time();
        if clock_tx.send(final_time).is_err() {
            warn!(target: "全局时钟", "主时钟通道已关闭，任务退出");
            break;
        }
    }
    warn!(target: "全局时钟", "全局时钟任务已退出");
}

/// 初始化品种索引
#[instrument(target = "应用生命周期", skip_all, name = "initialize_symbol_indexing")]
async fn initialize_symbol_indexing(
    api: &BinanceApi,
    db: &Database,
    enable_test_mode: bool,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    info!(target: "应用生命周期",test_mode = enable_test_mode, "开始初始化品种索引");
    let symbols = if enable_test_mode {
        vec!["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "BNBUSDT", "LTCUSDT"]
            .into_iter()
            .map(String::from)
            .collect()
    } else {
        info!(target: "应用生命周期", "正在从币安API获取所有U本位永续合约品种...");
        let (trading_symbols, delisted_symbols) = api.get_trading_usdt_perpetual_symbols().await?;

        // 处理已下架的品种
        if !delisted_symbols.is_empty() {
            info!(
                target: "应用生命周期",
                "发现已下架品种: {}，这些品种将不会被包含在索引中",
                delisted_symbols.join(", ")
            );
        }

        trading_symbols
    };
    info!(target: "应用生命周期", count = symbols.len(), "品种列表获取成功");

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    soft_assert!(!sorted_symbols_with_time.is_empty() || !enable_test_mode,
        message = "未能找到任何带有历史数据的品种",
        enable_test_mode = enable_test_mode,
    );

    if sorted_symbols_with_time.is_empty() && !enable_test_mode {
        return Err(AppError::DataError("No symbols with historical data found.".to_string()).into());
    }

    // 使用 sort_by 和元组比较来实现主次双重排序
    // 1. 主要按时间戳 (time) 升序排序
    // 2. 如果时间戳相同，则次要按品种名 (symbol) 的字母序升序排序
    // 这确保了每次启动时的排序结果都是确定和稳定的。
    sorted_symbols_with_time.sort_by(|(symbol_a, time_a), (symbol_b, time_b)| {
        (time_a, symbol_a).cmp(&(time_b, symbol_b))
    });

    // 打印排序后的序列，显示品种名称、时间戳和全局索引
    // 构建汇总的品种序列字符串（显示所有品种）
    let symbols_summary = sorted_symbols_with_time
        .iter()
        .enumerate()
        .map(|(index, (symbol, _))| format!("{}:{}", index, symbol))
        .collect::<Vec<_>>()
        .join(", ");

    info!(target: "应用生命周期",
        symbols_count = sorted_symbols_with_time.len(),
        symbols_summary = symbols_summary,
        "排序后的品种序列（所有品种）"
    );

    let all_sorted_symbols: Vec<String> =
        sorted_symbols_with_time.into_iter().map(|(s, _)| s).collect();

    let symbol_to_index: HashMap<String, usize> = all_sorted_symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| (symbol.clone(), index))
        .collect();

    info!(target: "应用生命周期", count = all_sorted_symbols.len(), "品种索引构建完成，并按上市时间排序");
    Ok((all_sorted_symbols, symbol_to_index))
}

/// 负责发现新品种并以原子方式发送指令给 Worker 3 的任务
#[instrument(target = "品种管理器", skip_all, name = "run_symbol_manager")]
async fn run_symbol_manager(
    config: Arc<AggregateConfig>,
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    let enable_test_mode = TEST_MODE;

    if enable_test_mode {
        info!(target: "品种管理器", log_type = "low_freq", "品种管理器启动（测试模式）- 每60秒模拟添加一个新品种");
        run_test_symbol_manager(symbol_to_global_index, global_index_to_symbol, global_symbol_count, cmd_tx).await
    } else {
        info!(target: "品种管理器", log_type = "low_freq", "品种管理器启动（生产模式）- 基于MiniTicker实时发现新品种");
        run_production_symbol_manager(config, symbol_to_global_index, global_index_to_symbol, global_symbol_count, cmd_tx).await
    }
}

/// 测试模式的品种管理器 - 每60秒模拟添加一个新品种
#[instrument(target = "品种管理器", skip_all, name = "run_test_symbol_manager")]
async fn run_test_symbol_manager(
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    // +++ 新增: 简单的启动延迟 +++
    info!(target: "品种管理器", log_type = "low_freq", "测试模式启动，等待2秒以确保I/O核心初始化...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 测试用的额外品种列表（除了初始的8个品种）
    let test_symbols = vec![
        "AVAXUSDT", "MATICUSDT", "LINKUSDT", "UNIUSDT", "ATOMUSDT",
        "DOTUSDT", "FILUSDT", "TRXUSDT", "ETCUSDT", "XLMUSDT",
        "VETUSDT", "ICPUSDT", "FTMUSDT", "HBARUSDT", "NEARUSDT",
        "ALGOUSDT", "MANAUSDT", "SANDUSDT", "AXSUSDT", "THETAUSDT"
    ];

    let mut symbol_index = 0;
    let mut interval = tokio::time::interval(Duration::from_secs(60)); // 每60秒添加一个
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        if symbol_index >= test_symbols.len() {
            info!(target: "品种管理器", log_type = "low_freq", "测试模式：所有模拟品种已添加完毕，品种管理器进入等待状态");
            // 继续运行但不再添加新品种
            tokio::time::sleep(Duration::from_secs(3600)).await;
            continue;
        }

        let symbol = test_symbols[symbol_index].to_string();
        symbol_index += 1;

        // 检查品种是否已存在
        let read_guard = symbol_to_global_index.read().await;
        if read_guard.contains_key(&symbol) {
            drop(read_guard);
            continue;
        }
        drop(read_guard);

        info!(target: "品种管理器", log_type = "low_freq", symbol = %symbol, "测试模式：模拟发现新品种");

        let new_global_index = global_symbol_count.fetch_add(1, Ordering::SeqCst);
        let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<(), String>>();

        // 创建一个模拟的 InitialKlineData
        let initial_data = InitialKlineData {
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 10.0,
        };

        let cmd = WorkerCmd::AddSymbol {
            symbol: symbol.clone(),
            global_index: new_global_index,
            initial_data,
            ack: ack_tx,
        };

        if cmd_tx.send(cmd).await.is_err() {
            warn!(target: "品种管理器", symbol=%symbol, "向Worker 3发送AddSymbol命令失败，通道可能已关闭");
            global_symbol_count.fetch_sub(1, Ordering::SeqCst);
            return Ok(());
        }

        match ack_rx.await {
            Ok(Ok(_)) => {
                let mut write_guard_map = symbol_to_global_index.write().await;
                let mut write_guard_vec = global_index_to_symbol.write().await;

                if !write_guard_map.contains_key(&symbol) {
                    write_guard_map.insert(symbol.clone(), new_global_index);
                    if new_global_index == write_guard_vec.len() {
                        write_guard_vec.push(symbol.clone());
                    } else {
                        error!(
                            log_type = "assertion",
                            symbol = %symbol,
                            new_global_index,
                            vec_len = write_guard_vec.len(),
                            "全局索引与向量长度不一致，发生严重逻辑错误！"
                        );
                    }
                    info!(target: "品种管理器", log_type = "low_freq", symbol = %symbol, new_global_index, "测试模式：成功添加模拟品种到全局索引和Worker {}", symbol);
                }
            }
            Ok(Err(e)) => {
                warn!(target: "品种管理器", symbol = %symbol, reason = %e, "添加新品种失败，Worker拒绝");
                global_symbol_count.fetch_sub(1, Ordering::SeqCst);
            }
            Err(_) => {
                warn!(target: "品种管理器", symbol = %symbol, reason = "ack_channel_closed", "添加新品种失败，与Worker的确认通道已关闭");
                global_symbol_count.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}

/// 生产模式的品种管理器 - 基于MiniTicker实时发现新品种
#[instrument(target = "品种管理器", skip_all, name = "run_production_symbol_manager")]
async fn run_production_symbol_manager(
    config: Arc<AggregateConfig>,
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let handler = Arc::new(MiniTickerMessageHandler::new(tx));
    let mini_ticker_config = MiniTickerConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
    };
    let mut client = MiniTickerClient::new(mini_ticker_config, handler);
    info!(target: "品种管理器", log_type = "low_freq", "正在连接MiniTicker WebSocket...");
    tokio::spawn(async move {
        if let Err(e) = client.start().await {
            warn!(target: "品种管理器", error = ?e, "MiniTicker WebSocket 客户端启动失败");
        }
    });

    // 辅助函数，用于安全地解析浮点数字符串
    let parse_or_zero = |s: &str, field_name: &str, symbol: &str| -> f64 {
        s.parse::<f64>().unwrap_or_else(|_| {
            warn!(target: "品种管理器", %symbol, field_name, value = %s, "无法解析新品种的初始数据，将使用0.0");
            0.0
        })
    };

    while let Some(tickers) = rx.recv().await {
        let read_guard = symbol_to_global_index.read().await;
        let new_symbols: Vec<_> = tickers
            .into_iter()
            .filter(|t| t.symbol.ends_with("USDT"))
            .filter(|t| !read_guard.contains_key(&t.symbol))
            .collect();
        drop(read_guard);

        if !new_symbols.is_empty() {
            info!(target: "品种管理器", count = new_symbols.len(), "发现新品种，开始处理");
            for ticker in new_symbols {
                let new_global_index = global_symbol_count.fetch_add(1, Ordering::SeqCst);
                let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<(), String>>();

                // 从 ticker 数据中解析完整的 OHLCV 数据
                let initial_data = InitialKlineData {
                    open: parse_or_zero(&ticker.open_price, "open", &ticker.symbol),
                    high: parse_or_zero(&ticker.high_price, "high", &ticker.symbol),
                    low: parse_or_zero(&ticker.low_price, "low", &ticker.symbol),
                    close: parse_or_zero(&ticker.close_price, "close", &ticker.symbol),
                    volume: parse_or_zero(&ticker.total_traded_volume, "volume", &ticker.symbol),
                };

                let cmd = WorkerCmd::AddSymbol {
                    symbol: ticker.symbol.clone(),
                    global_index: new_global_index,
                    initial_data,
                    ack: ack_tx,
                };

                if cmd_tx.send(cmd).await.is_err() {
                    warn!(target: "品种管理器", symbol=%ticker.symbol, "向Worker 3发送AddSymbol命令失败，通道可能已关闭");
                    global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                    return Ok(());
                }

                match ack_rx.await {
                    Ok(Ok(_)) => {
                        let mut write_guard_map = symbol_to_global_index.write().await;
                        let mut write_guard_vec = global_index_to_symbol.write().await;

                        if !write_guard_map.contains_key(&ticker.symbol) {
                            write_guard_map.insert(ticker.symbol.clone(), new_global_index);
                            if new_global_index == write_guard_vec.len() {
                                write_guard_vec.push(ticker.symbol.clone());
                            } else {
                                error!(
                                    log_type = "assertion",
                                    symbol = %ticker.symbol,
                                    new_global_index,
                                    vec_len = write_guard_vec.len(),
                                    "全局索引与向量长度不一致，发生严重逻辑错误！"
                                );
                            }
                            info!(target: "品种管理器", symbol = %ticker.symbol, new_global_index, "成功添加新品种到全局索引和Worker");
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = %e, "添加新品种失败，Worker拒绝");
                        global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = "ack_channel_closed", "添加新品种失败，与Worker的确认通道已关闭");
                        global_symbol_count.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            }
        }
    }
    warn!(target: "品种管理器", "品种管理器任务已退出");
    Ok(())
}

/// [启动流程-阶段四] 使用 aggTrades API 对内存中的K线进行微型补齐
///
/// 逻辑说明：
/// 1. 对每个品种每个周期的最新K线，从其close_time+1开始获取aggTrades
/// 2. 将这些交易数据聚合到对应的K线中，实现实时补齐
/// 3. 确保每个K线都被独立处理，避免使用全局时间导致的遗漏
#[instrument(target="应用生命周期", skip_all, name="run_micro_backfill")]
async fn run_micro_backfill(
    api: &Arc<BinanceApi>,
    time_sync: &Arc<ServerTimeSyncManager>,
    klines: &mut HashMap<(String, String), kline_server::klcommon::models::Kline>,
    _periods: &Arc<Vec<String>>,
) -> Result<()> {
    if klines.is_empty() {
        info!(target: "应用生命周期", log_type="startup", "没有K线数据需要微型补齐");
        return Ok(());
    }

    let current_time = time_sync.get_calibrated_server_time();
    let mut updated_count = 0;
    let mut skipped_count = 0;

    // 按品种分组，减少API调用次数
    let mut symbols_to_process: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut symbol_time_ranges: HashMap<String, (i64, i64)> = HashMap::new();

    // 第一步：分析每个品种需要补齐的时间范围
    for ((symbol, _period), kline) in klines.iter() {
        let start_time = kline.close_time + 1;
        if current_time > start_time {
            symbols_to_process.insert(symbol.clone());
            let (existing_start, existing_end) = symbol_time_ranges.get(symbol).unwrap_or(&(i64::MAX, 0));
            symbol_time_ranges.insert(
                symbol.clone(),
                (start_time.min(*existing_start), current_time.max(*existing_end))
            );
        }
    }

    if symbols_to_process.is_empty() {
        info!(target: "应用生命周期", log_type="startup", "所有K线都已是最新状态，无需微型补齐");
        return Ok(());
    }

    info!(target: "应用生命周期", log_type="startup",
        symbols_to_process = symbols_to_process.len(),
        "开始为 {} 个品种获取aggTrades进行微型补齐", symbols_to_process.len()
    );

    // 第二步：并发获取每个品种的aggTrades
    let trades_by_symbol: HashMap<String, Vec<api::AggTrade>> = stream::iter(symbols_to_process)
        .map(|symbol| async {
            let (start_time, end_time) = symbol_time_ranges.get(&symbol).unwrap();
            let trades = api.get_agg_trades(symbol.clone(), Some(*start_time), Some(*end_time), None).await;
            (symbol, trades)
        })
        .buffer_unordered(20)
        .filter_map(|(symbol, result)| async {
            match result {
                Ok(trades) => {
                    if !trades.is_empty() {
                        Some((symbol, trades))
                    } else {
                        None
                    }
                },
                Err(e) => {
                    warn!(target: "应用生命周期", log_type="startup", %symbol, error = %e, "微型补齐时获取aggTrades失败");
                    None
                }
            }
        })
        .collect::<HashMap<_, _>>()
        .await;

    // 第三步：将交易数据聚合到对应的K线中
    let mut symbol_period_stats: HashMap<String, HashMap<String, (u32, u32, f64)>> = HashMap::new(); // symbol -> period -> (trades_count, updates_count, volume_added)

    for ((symbol, period), kline) in klines.iter_mut() {
        let kline_start_time = kline.open_time;
        let kline_end_time = kline.close_time;
        let original_close = kline.close.clone();
        let original_volume: f64 = kline.volume.parse().unwrap_or(0.0);

        let mut trades_processed = 0u32;
        let mut volume_added = 0.0f64;
        let mut has_updates = false;

        if let Some(trades) = trades_by_symbol.get(symbol) {
            for trade in trades {
                // 根据K线是否已结束，采用不同的时间范围判断逻辑
                let should_process_trade = if current_time <= kline_end_time {
                    // K线还未结束，处理从数据库中最新记录时间之后的所有交易
                    trade.timestamp_ms > kline_end_time && trade.timestamp_ms <= current_time
                } else {
                    // K线已结束，处理K线结束时间之后的交易
                    trade.timestamp_ms > kline_end_time && trade.timestamp_ms <= current_time
                };

                if should_process_trade {
                    let trade_period_start = api::get_aligned_time(trade.timestamp_ms, period);

                    // 如果交易属于当前K线的时间周期，则更新当前K线
                    if trade_period_start == kline_start_time {
                        let price: f64 = trade.price.parse().unwrap_or(0.0);
                        let qty: f64 = trade.quantity.parse().unwrap_or(0.0);
                        let high: f64 = kline.high.parse().unwrap_or(0.0);
                        let low: f64 = kline.low.parse().unwrap_or(f64::MAX);

                        kline.high = high.max(price).to_string();
                        kline.low = low.min(price).to_string();
                        kline.close = trade.price.clone();
                        kline.volume = (kline.volume.parse::<f64>().unwrap_or(0.0) + qty).to_string();
                        kline.quote_asset_volume = (kline.quote_asset_volume.parse::<f64>().unwrap_or(0.0) + price * qty).to_string();

                        trades_processed += 1;
                        volume_added += qty;
                        has_updates = true;
                    }
                }
            }
        }

        // 记录每个品种每个周期的处理结果
        if has_updates {
            updated_count += 1;
            let new_volume: f64 = kline.volume.parse().unwrap_or(0.0);
            info!(target: "应用生命周期", log_type="startup",
                symbol = %symbol,
                period = %period,
                trades_processed = trades_processed,
                volume_added = volume_added,
                original_close = %original_close,
                new_close = %kline.close,
                original_volume = original_volume,
                new_volume = new_volume,
                "✅ 微型补齐成功"
            );
        } else {
            skipped_count += 1;
            let has_symbol_trades = trades_by_symbol.contains_key(symbol);
            let trades_count = if has_symbol_trades {
                trades_by_symbol.get(symbol).map(|t| t.len()).unwrap_or(0)
            } else { 0 };

            info!(target: "应用生命周期", log_type="startup",
                symbol = %symbol,
                period = %period,
                kline_start_time = kline_start_time,
                kline_end_time = kline_end_time,
                current_time = current_time,
                gap_ms = current_time - kline_end_time,
                has_symbol_trades = has_symbol_trades,
                trades_count = trades_count,
                "⏭️ 微型补齐跳过（无新交易或时间范围外）"
            );

            // 如果有交易数据但没有更新，打印交易详情用于调试
            if has_symbol_trades && trades_count > 0 {
                if let Some(trades) = trades_by_symbol.get(symbol) {
                    for (i, trade) in trades.iter().take(3).enumerate() { // 只打印前3个交易
                        let trade_period_start = api::get_aligned_time(trade.timestamp_ms, period);
                        info!(target: "应用生命周期", log_type="startup",
                            symbol = %symbol,
                            period = %period,
                            trade_index = i,
                            trade_timestamp = trade.timestamp_ms,
                            trade_period_start = trade_period_start,
                            kline_start_time = kline_start_time,
                            trade_in_range = trade.timestamp_ms > kline_end_time && trade.timestamp_ms <= current_time,
                            period_match = trade_period_start == kline_start_time,
                            trade_price = %trade.price,
                            "🔍 交易详情调试"
                        );
                    }
                }
            }
        }

        // 统计数据收集
        symbol_period_stats
            .entry(symbol.clone())
            .or_insert_with(HashMap::new)
            .insert(period.clone(), (trades_processed, if has_updates { 1 } else { 0 }, volume_added));
    }

    // 第四步：生成详细的汇总统计
    let mut total_trades_processed = 0u32;
    let mut total_volume_added = 0.0f64;
    let mut symbols_with_updates = 0u32;
    let mut periods_with_updates = 0u32;

    for (symbol, period_stats) in &symbol_period_stats {
        let mut symbol_has_updates = false;
        let mut symbol_trades = 0u32;
        let mut symbol_volume = 0.0f64;

        for (_period, (trades, updates, volume)) in period_stats {
            total_trades_processed += trades;
            total_volume_added += volume;
            symbol_trades += trades;
            symbol_volume += volume;

            if *updates > 0 {
                periods_with_updates += 1;
                symbol_has_updates = true;
            }
        }

        if symbol_has_updates {
            symbols_with_updates += 1;
            info!(target: "应用生命周期", log_type="startup",
                symbol = %symbol,
                periods_updated = period_stats.values().filter(|(_, updates, _)| *updates > 0).count(),
                total_periods = period_stats.len(),
                symbol_trades = symbol_trades,
                symbol_volume = symbol_volume,
                "📊 品种微型补齐汇总"
            );
        }
    }

    // 最终汇总统计
    info!(target: "应用生命周期", log_type="startup",
        total_klines = klines.len(),
        updated_klines = updated_count,
        skipped_klines = skipped_count,
        symbols_with_updates = symbols_with_updates,
        periods_with_updates = periods_with_updates,
        total_trades_processed = total_trades_processed,
        total_volume_added = total_volume_added,
        update_rate = format!("{:.1}%", (updated_count as f64 / klines.len() as f64) * 100.0),
        "🎯 微型补齐最终统计汇总"
    );

    if updated_count == 0 {
        info!(target: "应用生命周期", log_type="startup", "ℹ️ 所有K线均为最新状态，无需微型补齐");
    } else {
        info!(target: "应用生命周期", log_type="startup",
            "✅ 微型补齐完成，成功更新了 {} 个品种的 {} 条K线记录",
            symbols_with_updates, updated_count
        );
    }

    Ok(())
}

