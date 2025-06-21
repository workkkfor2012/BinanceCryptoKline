//! K线聚合服务启动文件
//!
//! 启动完整的K线聚合系统，包括数据接入、聚合、存储和持久化。

use kline_server::klaggregate::{KlineAggregateSystem, AggregateConfig};
use kline_server::klcommon::log::{ModuleLayer, NamedPipeLogManager, TraceVisualizationLayer};
use std::sync::Arc;
use kline_server::klaggregate::cerberus::create_default_cerberus_layer;
use kline_server::klcommon::{Result, AppError};
use std::path::Path;
use tokio::signal;
use tokio::time::{Duration};
use tracing::{instrument, info, error, warn, debug, trace, Instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
use chrono;

/// 默认配置文件路径
const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";

/// 日志目标常量
const LOG_TARGET: &str = "KlineAggregateService";

/// K线数据倾倒开关 - 设置为 true 启用2分钟的高频K线数据记录
const ENABLE_KLINE_DUMP: bool = true;

fn main() -> Result<()> {
    // 处理命令行参数
    if !handle_args() {
        return Ok(());
    }

    // 初始化可观察性系统
    let cerberus_engine = init_observability_system()?;

    // 创建Tokio运行时
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| AppError::ConfigError(format!("创建Tokio运行时失败: {}", e)))?;

    // 创建应用程序的根Span，代表整个应用生命周期
    let root_span = tracing::info_span!(
        "kline_aggregate_app",
        service = "kline_aggregate_service",
        version = env!("CARGO_PKG_VERSION")
    );

    // 在根Span的上下文中运行整个应用
    runtime.block_on(run_app(cerberus_engine).instrument(root_span))
}

/// 应用程序的核心业务逻辑
#[instrument(name = "run_app", skip_all)]
async fn run_app(cerberus_engine: Option<kline_server::klaggregate::cerberus::CerberusEngine>) -> Result<()> {
    // 首先打印当前的日志级别配置
    let current_log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "未设置".to_string());
    info!(target: LOG_TARGET, event_name = "日志级别确认", current_rust_log = %current_log_level, "📊 当前日志级别: {}", current_log_level);

    trace!(target: LOG_TARGET, event_name = "服务启动", message = "启动K线聚合服务");
    debug!(target: LOG_TARGET, event_name = "服务启动", message = "启动K线聚合服务");
    info!(target: LOG_TARGET, event_name = "服务启动", message = "启动K线聚合服务");

    // 启动 Cerberus 验证引擎（如果存在）
    if let Some(engine) = cerberus_engine {
        tokio::spawn(async move {
            engine.start().await;
        }.instrument(tracing::info_span!("cerberus_engine_task")));
        info!(target: LOG_TARGET, event_name = "Cerberus验证引擎启动", "🐕 Cerberus 验证引擎已启动");
    }

    // 加载配置
    let config = load_config().await?;
    info!(target: LOG_TARGET, event_name = "配置加载完成123", "配置加载完成456");

    // 创建K线聚合系统
    let system = match KlineAggregateSystem::new(config).await {
        Ok(system) => {
            info!(target: LOG_TARGET, event_name = "系统创建成功", "K线聚合系统创建成功");
            system
        }
        Err(e) => {
            error!(target: LOG_TARGET, event_name = "系统创建失败", error = %e, "创建K线聚合系统失败");
            return Err(e);
        }
    };

    // 启动系统
    if let Err(e) = system.start().await {
        error!(target: LOG_TARGET, event_name = "系统启动失败", error = %e, "启动K线聚合系统失败");
        return Err(e);
    }

    info!(target: LOG_TARGET, event_name = "服务启动完成", "K线聚合服务启动完成");

    // 启动状态监控任务
    start_status_monitor(system.clone()).await;

    // 启动测试日志任务
    start_test_logging().await;

    // 启动K线数据倾倒任务（如果启用）
    if ENABLE_KLINE_DUMP {
        start_kline_dump_task(system.clone()).await;
    }

    // 等待关闭信号
    wait_for_shutdown_signal().await;

    // 优雅关闭
    info!(target: LOG_TARGET, event_name = "收到关闭信号", "收到关闭信号，开始优雅关闭...");
    if let Err(e) = system.stop().await {
        error!(target: LOG_TARGET, event_name = "系统停止失败", error = %e, "关闭K线聚合系统失败");
    } else {
        info!(target: LOG_TARGET, event_name = "服务优雅关闭", "K线聚合服务已优雅关闭");
    }

    Ok(())
}

/// 初始化可观察性系统
fn init_observability_system() -> Result<Option<kline_server::klaggregate::cerberus::CerberusEngine>> {
    use std::sync::{Once, Mutex};

    // 使用更安全的方式存储初始化结果
    static OBSERVABILITY_INIT: Once = Once::new();
    static INIT_RESULT: Mutex<Option<Result<kline_server::klaggregate::cerberus::CerberusEngine>>> = Mutex::new(None);

    let mut init_success = false;

    OBSERVABILITY_INIT.call_once(|| {
        match init_observability_system_inner() {
            Ok(engine) => {
                init_success = true;
                if let Ok(mut result) = INIT_RESULT.lock() {
                    *result = Some(Ok(engine));
                }
            }
            Err(e) => {
                if let Ok(mut result) = INIT_RESULT.lock() {
                    *result = Some(Err(e));
                }
            }
        }
    });

    // 检查初始化结果
    if let Ok(mut result) = INIT_RESULT.lock() {
        match result.take() {
            Some(Ok(engine)) => Ok(Some(engine)),
            Some(Err(e)) => Err(e),
            None => {
                // 如果是第一次调用且在call_once中成功了
                if init_success {
                    Ok(None) // 引擎已经被取走了
                } else {
                    Err(AppError::ConfigError("可观察性系统初始化状态未知".to_string()))
                }
            }
        }
    } else {
        Err(AppError::ConfigError("无法获取初始化状态".to_string()))
    }
}

/// 内部初始化函数，只会被调用一次
fn init_observability_system_inner() -> Result<kline_server::klaggregate::cerberus::CerberusEngine> {
    // 从配置文件读取日志设置，配置文件必须存在
    let (log_level, log_transport, pipe_name) = match load_logging_config() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("加载日志配置失败: {}", e);
            return Err(e);
        }
    };

    // 创建共享的日志管理器
    let log_manager = Arc::new(NamedPipeLogManager::new(pipe_name.clone()));
    log_manager.start_connection_task();

    // 创建两个独立的日志层，共享同一个管理器
    let module_layer = ModuleLayer::new(log_manager.clone());
    let trace_layer = TraceVisualizationLayer::new(log_manager.clone());

    // 创建 Cerberus 验证层
    let (cerberus_layer, cerberus_engine) = create_default_cerberus_layer();

    // 设置tracing订阅器，使用新的双层架构
    let init_result = match log_transport.as_str() {
        "named_pipe" => {
            // 命名管道模式：使用双层架构，职责分离
            Registry::default()
                .with(cerberus_layer)  // Cerberus 验证层
                .with(module_layer)    // 模块日志层（顶层日志）
                .with(trace_layer)     // Trace 可视化层（Span 内日志）
                .with(create_env_filter(&log_level))
                .try_init()
        }
        _ => {
            // 其他模式：回退到双层架构 + 控制台输出
            Registry::default()
                .with(cerberus_layer)  // Cerberus 验证层
                .with(module_layer)    // 模块日志层
                .with(trace_layer)     // Trace 可视化层
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                ) // 添加控制台输出层
                .with(create_env_filter(&log_level))
                .try_init()
        }
    };

    // 检查tracing订阅器初始化结果并决定是否初始化LogTracer
    let _tracing_init_success = match init_result {
        Ok(_) => {
            // tracing订阅器初始化成功，我们是第一个初始化的
            true
        }
        Err(_e) => {
            // 如果已经初始化过，这是正常情况，不需要报错
            false
        }
    };

    // 设置log到tracing的桥接，捕获第三方库的log日志
    // 尝试初始化LogTracer，无论tracing订阅器是否是我们初始化的
    match tracing_log::LogTracer::init_with_filter(log::LevelFilter::Warn) {
        Ok(_) => {
            // 初始化成功，设置了warn级别过滤
            tracing::debug!(target: LOG_TARGET, event_name = "log桥接器初始化成功", "log桥接器初始化成功，第三方库日志过滤级别: warn");
        }
        Err(e) => {
            // 如果失败，尝试不带过滤器的初始化
            match tracing_log::LogTracer::init() {
                Ok(_) => {
                    tracing::debug!(target: LOG_TARGET, event_name = "log桥接器初始化成功", "log桥接器初始化成功（无过滤器）");
                }
                Err(e2) => {
                    // 两种方式都失败，可能已经有其他logger
                    tracing::warn!(target: LOG_TARGET, event_name = "log桥接器初始化失败",
                        error1 = %e, error2 = %e2,
                        "log桥接器初始化失败，可能已有其他logger。第三方库日志可能无法被过滤");
                }
            }
        }
    }

    // 等待一小段时间确保tracing系统完全初始化
    std::thread::sleep(std::time::Duration::from_millis(10));

    tracing::info!(target: LOG_TARGET, event_name = "可观察性系统初始化完成", log_level = %log_level, "🔍 可观察性系统初始化完成，级别: {}", log_level);
    tracing::info!(target: LOG_TARGET, event_name = "规格验证层状态", "📊 规格验证层已禁用，减少日志输出");
    tracing::info!(target: LOG_TARGET, event_name = "日志传输配置", transport = %log_transport, "📡 日志传输方式: {}", log_transport);

    // 显示传输配置信息
    match log_transport.as_str() {
        "named_pipe" => {
            tracing::info!(target: LOG_TARGET, event_name = "日志传输详情", transport_type = "named_pipe", pipe_name = %pipe_name, "📡 使用命名管道传输日志: {}", pipe_name);
        }
        "websocket" => {
            let web_port = std::env::var("WEB_PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse::<u16>()
                .unwrap_or(3000);
            tracing::info!(target: LOG_TARGET, event_name = "日志传输详情", transport_type = "websocket", web_port = web_port, "🌐 使用WebSocket传输日志，端口: {}", web_port);
        }
        _ => {
            tracing::warn!(target: LOG_TARGET, event_name = "未知传输方式", configured_transport = %log_transport, "⚠️ 未知传输方式 '{}', 使用默认命名管道", log_transport);
        }
    }

    // 发送测试日志确保传输工作
    tracing::info!(target: LOG_TARGET, event_name = "可观测性测试日志", test_id = 1, "🧪 测试日志1: 可观察性系统测试");
    tracing::warn!(target: LOG_TARGET, event_name = "可观测性测试日志", test_id = 2, "🧪 测试日志2: 警告级别测试");
    tracing::error!(target: LOG_TARGET, event_name = "可观测性测试日志", test_id = 3, "🧪 测试日志3: 错误级别测试");

    Ok(cerberus_engine)
}

/// 加载日志配置
fn load_logging_config() -> Result<(String, String, String)> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    if Path::new(&config_path).exists() {
        let config = AggregateConfig::from_file(&config_path)?;

        // 确保管道名称格式正确（Windows命名管道需要完整路径）
        let pipe_name = if config.logging.pipe_name.starts_with(r"\\.\pipe\") {
            config.logging.pipe_name
        } else {
            format!(r"\\.\pipe\{}", config.logging.pipe_name)
        };

        Ok((
            config.logging.log_level,
            config.logging.log_transport,
            pipe_name,
        ))
    } else {
        Err(AppError::ConfigError(format!("配置文件不存在: {}，无法加载日志配置", config_path)))
    }
}

/// 加载配置
#[instrument(target = LOG_TARGET, err)]
async fn load_config() -> Result<AggregateConfig> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

    if Path::new(&config_path).exists() {
        info!(target: LOG_TARGET, event_name = "从文件加载配置", path = %config_path, "从文件加载配置: {}", config_path);
        AggregateConfig::from_file(&config_path)
    } else {
        error!(target: LOG_TARGET, event_name = "配置文件不存在", path = %config_path, "配置文件不存在: {}，无法启动服务", config_path);
        return Err(AppError::ConfigError(format!("配置文件不存在: {}，请确保配置文件存在", config_path)));
    }
}

/// 启动状态监控任务
async fn start_status_monitor(system: KlineAggregateSystem) {
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                let status = system.get_status().await;
                info!(
                    target: LOG_TARGET,
                    event_name = "系统状态报告",
                    total_symbols = status.total_symbols,
                    active_connections = status.active_connections,
                    buffer_swap_count = status.buffer_swap_count,
                    persistence_status = %status.persistence_status,
                    "系统状态报告"
                );
            }
        }.instrument(tracing::info_span!("status_monitor_task"))
    );
}

/// 启动测试日志任务（每10秒发送一次测试日志）
async fn start_test_logging() {
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let mut counter = 0;

            loop {
                interval.tick().await;
                counter += 1;

                info!(
                    target: LOG_TARGET,
                    event_name = "定期测试日志",
                    counter = counter,
                    timestamp = %chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
                    "🧪 定期测试日志 #{}: 系统运行正常",
                    counter
                );

                if counter % 3 == 0 {
                    warn!(target: LOG_TARGET, event_name = "定期测试警告", counter = counter, "🧪 警告测试日志 #{}: 这是一个测试警告", counter);
                }
            }
        }.instrument(tracing::info_span!("periodic_test_log_task"))
    );
}

/// 等待关闭信号
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("安装Ctrl+C处理器失败");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("安装SIGTERM处理器失败")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!(target: LOG_TARGET, event_name = "信号接收", signal = "Ctrl+C", "收到Ctrl+C信号");
        },
        _ = terminate => {
            info!(target: LOG_TARGET, event_name = "信号接收", signal = "SIGTERM", "收到SIGTERM信号");
        },
    }
}

/// 显示帮助信息
fn show_help() {
    // 帮助信息已移除，使用日志系统替代
}

/// 显示版本信息
fn show_version() {
    // 版本信息已移除，使用日志系统替代
}

/// 处理命令行参数
fn handle_args() -> bool {
    let args: Vec<String> = std::env::args().collect();

    for arg in &args[1..] {
        match arg.as_str() {
            "-h" | "--help" => {
                show_help();
                return false;
            }
            "-v" | "--version" => {
                show_version();
                return false;
            }
            _ => {
                return false;
            }
        }
    }

    true
}

/// 启动K线数据倾倒任务（记录2分钟的高频K线数据）
async fn start_kline_dump_task(system: KlineAggregateSystem) {
    use std::fs::OpenOptions;
    use std::io::Write;

    tokio::spawn(
        async move {
            // 创建日志文件名（带时间戳）
            let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let log_file_path = format!("logs/kline_dump_{}.log", timestamp);

            warn!(target = "KlineAggregateSystem", event_name = "K线倾倒开始", log_file = %log_file_path, "开始K线数据倾倒，文件: {}", log_file_path);

            // 创建日志文件
            let mut log_file = match OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&log_file_path) {
                Ok(file) => {
                    warn!(target = "KlineAggregateSystem", event_name = "K线倾倒文件创建成功",
                          log_file = %log_file_path, "K线倾倒文件创建成功: {}", log_file_path);
                    file
                },
                Err(e) => {
                    error!(target = "KlineAggregateSystem", event_name = "K线倾倒文件创建失败",
                           log_file = %log_file_path, error = %e, "无法创建K线倾倒文件: {}", e);
                    return;
                }
            };

            // 记录开始时间
            let start_time = std::time::Instant::now();
            let dump_duration = std::time::Duration::from_secs(120); // 2分钟
            let mut dump_count = 0u64;

            // 获取BufferedKlineStore的引用
            let buffered_store = system.get_buffered_store();

            warn!(target = "KlineAggregateSystem", event_name = "K线倾倒循环开始", duration_seconds = 120, "开始2分钟K线数据倾倒循环");

            // 主循环：每次缓冲区交换后倾倒数据
            while start_time.elapsed() < dump_duration {
                // 等待新的快照就绪
                buffered_store.wait_for_snapshot().await;

                // 检查是否超时
                if start_time.elapsed() >= dump_duration {
                    break;
                }

                // 获取当前时间戳
                let current_timestamp = chrono::Utc::now().timestamp_millis();

                // 倾倒所有K线数据
                let dump_start = std::time::Instant::now();
                match dump_all_klines(&buffered_store, &mut log_file, current_timestamp).await {
                    Ok(count) => {
                        dump_count += count;
                        let dump_duration = dump_start.elapsed();

                        // 记录每次倾倒的详细信息
                        warn!(target = "KlineAggregateSystem", event_name = "单次K线倾倒完成",
                               snapshot_records = count, dump_duration_ms = dump_duration.as_millis(),
                               total_dumped = dump_count, elapsed_seconds = start_time.elapsed().as_secs(),
                               "单次倾倒: {} 条记录，耗时 {:.2}ms，总计: {} 条",
                               count, dump_duration.as_secs_f64() * 1000.0, dump_count);

                        if dump_count % 1000 == 0 { // 每1000条记录打印一次进度
                            warn!(target = "KlineAggregateSystem", event_name = "K线倾倒进度",
                                  dumped_count = dump_count, elapsed_seconds = start_time.elapsed().as_secs(),
                                  "K线倾倒进度: {} 条记录", dump_count);
                        }
                    }
                    Err(e) => {
                        error!(target = "KlineAggregateSystem", event_name = "K线倾倒失败", error = %e, "K线数据倾倒失败: {}", e);
                    }
                }
            }

            // 倾倒完成
            let total_duration = start_time.elapsed();
            warn!(target = "KlineAggregateSystem", event_name = "K线倾倒完成",
                  total_records = dump_count, duration_seconds = total_duration.as_secs(),
                  log_file = %log_file_path,
                  "K线数据倾倒完成: {} 条记录，耗时 {:.2} 秒，文件: {}",
                  dump_count, total_duration.as_secs_f64(), log_file_path);

            // 确保数据写入磁盘
            match log_file.flush() {
                Ok(_) => {
                    warn!(target = "KlineAggregateSystem", event_name = "K线倾倒文件刷新成功",
                          log_file = %log_file_path, total_records = dump_count,
                          "K线倾倒文件刷新成功，所有数据已写入磁盘");
                }
                Err(e) => {
                    error!(target = "KlineAggregateSystem", event_name = "K线倾倒文件刷新失败",
                           log_file = %log_file_path, error = %e, "日志文件刷新失败: {}", e);
                }
            }
        }.instrument(tracing::info_span!("kline_dump_task"))
    );
}

/// 倾倒所有K线数据到日志文件
async fn dump_all_klines(
    buffered_store: &kline_server::klaggregate::BufferedKlineStore,
    log_file: &mut std::fs::File,
    timestamp: i64,
) -> Result<u64> {
    use std::io::Write;
    use serde_json;

    let mut dump_count = 0u64;
    let snapshot_start = std::time::Instant::now();

    // 获取所有K线数据的快照
    let snapshot = buffered_store.get_read_buffer_snapshot().await?;
    let snapshot_duration = snapshot_start.elapsed();

    warn!(target = "KlineAggregateSystem", event_name = "K线快照获取完成",
           total_slots = snapshot.len(), snapshot_duration_ms = snapshot_duration.as_millis(),
           "获取K线快照完成: {} 个槽位，耗时 {:.2}ms",
           snapshot.len(), snapshot_duration.as_secs_f64() * 1000.0);

    let write_start = std::time::Instant::now();
    let mut active_klines = 0u64;
    let mut total_klines = 0u64;

    for (flat_index, kline_data) in snapshot.iter().enumerate() {
        total_klines += 1;

        // 只记录有成交量的K线数据，减少日志量
        if kline_data.volume > 0.0 {
            active_klines += 1;

            // 创建结构化日志记录
            let log_record = serde_json::json!({
                "timestamp": timestamp,
                "event_name": "KlineSnapshot",
                "flat_index": flat_index,
                "symbol_index": kline_data.symbol_index,
                "period_index": kline_data.period_index,
                "open_time": kline_data.open_time,
                "open": kline_data.open,
                "high": kline_data.high,
                "low": kline_data.low,
                "close": kline_data.close,
                "volume": kline_data.volume,
                "turnover": kline_data.turnover,
                "trade_count": kline_data.trade_count,
                "taker_buy_volume": kline_data.taker_buy_volume,
                "taker_buy_turnover": kline_data.taker_buy_turnover,
                "is_final": kline_data.is_final
            });

            // 写入日志文件（JSON Lines格式）
            if let Err(e) = writeln!(log_file, "{}", log_record) {
                error!(target = "KlineAggregateSystem", event_name = "K线写入文件失败",
                       flat_index = flat_index, symbol_index = kline_data.symbol_index,
                       period_index = kline_data.period_index, error = %e,
                       "写入K线数据到文件失败");
                return Err(kline_server::klcommon::AppError::IoError(e));
            }

            dump_count += 1;
        }
    }

    let write_duration = write_start.elapsed();

    warn!(target = "KlineAggregateSystem", event_name = "K线数据写入完成",
           total_klines = total_klines, active_klines = active_klines,
           written_records = dump_count, write_duration_ms = write_duration.as_millis(),
           "K线数据写入完成: 总计 {} 个K线，活跃 {} 个，写入 {} 条记录，耗时 {:.2}ms",
           total_klines, active_klines, dump_count, write_duration.as_secs_f64() * 1000.0);

    Ok(dump_count)
}

/// 创建环境过滤器，始终过滤掉第三方库的调试日志
fn create_env_filter(log_level: &str) -> tracing_subscriber::EnvFilter {
    // 无论应用日志级别如何，都过滤掉第三方库的噪音日志
    // 确保第三方库只显示warn和error级别的日志
    let filter_str = format!(
        "{},hyper=warn,reqwest=warn,tokio_tungstenite=warn,tungstenite=warn,rustls=warn,h2=warn,sqlx=warn,rusqlite=warn,tokio=warn,mio=warn,want=warn,tower=warn,fastwebsockets=warn,socks5=warn,webpki_roots=warn,ring=warn",
        log_level
    );

    tracing_subscriber::EnvFilter::new(filter_str)
}
