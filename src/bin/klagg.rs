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
use kline_server::{
    engine::{AppEvent, KlineEngine},
    kldata::KlineBackfiller,
    klcommon::{
        api::BinanceApi,
        db::Database,
        error::AppError,
        log::{init_ai_logging, shutdown_target_log_sender},
        websocket::{
            AggTradeClient, AggTradeConfig, AggTradeMessageHandler,
            MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler, WebSocketClient
        },
        AggregateConfig,
    },
    soft_assert,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{self, Duration};
use tracing::{error, info, instrument, warn};

const DEFAULT_CONFIG_PATH: &str = "config/BinanceKlineConfig.toml";









#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _guard = init_ai_logging().await?;
    std::panic::set_hook(Box::new(|panic_info| {
        error!(target: "应用生命周期", panic_info = %panic_info, "程序发生未捕获的Panic，即将退出");
        std::process::exit(1);
    }));

    if let Err(e) = run_app().await {
        error!(target: "应用生命周期", error = ?e, "应用因顶层错误而异常退出");
    }

    info!(target: "应用生命周期", "应用程序正常关闭");
    shutdown_target_log_sender();
    Ok(())
}

#[instrument(target = "应用生命周期", skip_all, name = "run_app")]
async fn run_app() -> Result<()> {
    info!(target: "应用生命周期", "K线聚合服务启动中 (单线程重构版)...");

    // 1. 初始化资源
    info!(target: "启动流程", "正在加载配置文件...");
    let config = Arc::new(AggregateConfig::from_file(DEFAULT_CONFIG_PATH)?);
    info!(target: "启动流程", "配置文件加载完成");

    info!(target: "启动流程", "正在初始化数据库连接...");
    let db = Arc::new(Database::new_with_config(&config.database.database_path, config.persistence.queue_size)?);
    info!(target: "启动流程", "数据库连接初始化完成");

    // 2. 数据准备 (Backfill)
    info!(target: "启动流程", "正在创建历史数据补齐器...");
    let backfiller = KlineBackfiller::new(db.clone(), config.supported_intervals.clone());
    info!(target: "启动流程", "开始历史数据补齐...");
    backfiller.run_once().await?;
    info!(target: "启动流程", "开始延迟追赶补齐...");
    backfiller.run_once_with_round(2).await?;
    info!(target: "启动流程", "正在加载最新K线数据...");
    let initial_klines = backfiller.load_latest_klines_from_db().await?;
    info!(target: "启动流程", "历史数据补齐完成，清理资源...");
    backfiller.cleanup_after_all_backfill_rounds().await;

    // 3. 品种索引
    let (all_symbols_sorted, _symbol_to_index_map) = initialize_symbol_indexing(&db, TEST_MODE).await?;
    info!(target: "启动流程", count = all_symbols_sorted.len(), "全局品种索引初始化完成");

    // 4. 创建核心引擎和通信通道
    let (event_tx, event_rx) = mpsc::channel(10240);
    let (state_watch_tx, state_watch_rx) = watch::channel(kline_server::engine::events::StateUpdate::default());

    let mut engine = KlineEngine::new(config.clone(), db, initial_klines, &all_symbols_sorted, event_rx, state_watch_tx)?;

    // 5. 启动所有后台异步任务
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(websocket_task(config.clone(), all_symbols_sorted.clone(), event_tx.clone(), shutdown_rx.clone()));

    let symbol_map_arc = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    tokio::spawn(symbol_manager_task(config.clone(), symbol_map_arc, event_tx.clone(), shutdown_rx.clone()));

    if VISUAL_TEST_MODE {
        tokio::spawn(kline_server::engine::web_server::run_visual_test_server(
            state_watch_rx,
            Arc::new(tokio::sync::RwLock::new(all_symbols_sorted)),
            Arc::new(config.supported_intervals.clone()),
            shutdown_rx.clone(),
        ));
    }

    // 6. 运行核心引擎并等待关闭
    info!(target: "应用生命周期", "所有服务已启动，引擎开始运行，等待关闭信号 (Ctrl+C)...");

    tokio::select! {
        _ = engine.run() => {
            warn!(target: "应用生命周期", "KlineEngine主循环意外退出");
        },
        _ = tokio::signal::ctrl_c() => {
            info!(target: "应用生命周期", "接收到Ctrl+C，开始优雅关闭...");
        }
    }

    let _ = shutdown_tx.send(true);
    time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// 初始化品种索引
#[instrument(target = "应用生命周期", skip_all, name = "initialize_symbol_indexing")]
async fn initialize_symbol_indexing(
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
        let temp_client = BinanceApi::create_new_client()?;
        let (trading_symbols, delisted_symbols) = BinanceApi::get_trading_usdt_perpetual_symbols(&temp_client).await?;

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

// websocket_task 的实现
async fn websocket_task(
    config: Arc<AggregateConfig>,
    initial_symbols: Vec<String>,
    event_tx: mpsc::Sender<AppEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!(target: "WebSocket任务", "启动中...");

    let streams: Vec<String> = initial_symbols.iter().map(|s| format!("{}@aggTrade", s.to_lowercase())).collect();
    info!(target: "WebSocket任务", count = streams.len(), "准备订阅 {} 个品种的归集交易流", streams.len());

    // 创建交易数据通道
    let (trade_tx, mut trade_rx) = mpsc::unbounded_channel();

    // 创建消息处理器
    let message_handler = Arc::new(AggTradeMessageHandler::with_trade_sender(
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        trade_tx,
    ));

    // 创建WebSocket配置
    let ws_config = AggTradeConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
        symbols: initial_symbols.clone(),
    };

    // 创建WebSocket客户端
    let mut client = AggTradeClient::new_with_handler(ws_config, message_handler);

    // 启动WebSocket客户端
    let client_handle = tokio::spawn(async move {
        for attempt in 1..=5 {
            info!(target: "WebSocket任务", attempt, "尝试启动WebSocket客户端 (第{}次)", attempt);

            match client.start().await {
                Ok(()) => {
                    info!(target: "WebSocket任务", "WebSocket客户端启动成功");
                    break;
                }
                Err(e) => {
                    error!(target: "WebSocket任务", attempt, error = ?e, "WebSocket客户端启动失败");
                    if attempt < 5 {
                        info!(target: "WebSocket任务", "等待5秒后重试...");
                        time::sleep(Duration::from_secs(5)).await;
                    } else {
                        error!(target: "WebSocket任务", "WebSocket客户端启动失败，已达到最大重试次数");
                        return;
                    }
                }
            }
        }
    });

    // 处理交易数据的任务
    let event_tx_clone = event_tx.clone();
    let trade_processor = tokio::spawn(async move {
        let mut trade_count = 0u64;
        let mut last_log_time = std::time::Instant::now();

        while let Some(trade_data) = trade_rx.recv().await {
            trade_count += 1;

            // 将交易数据转换为AppEvent并发送
            let event = AppEvent::AggTrade(Box::new(trade_data));
            if let Err(e) = event_tx_clone.send(event).await {
                error!(target: "WebSocket任务", error = ?e, "发送交易事件失败，主事件通道可能已关闭");
                break;
            }

            // 每10秒输出一次统计信息
            if last_log_time.elapsed() >= Duration::from_secs(10) {
                info!(target: "WebSocket任务", trade_count, "已处理 {} 笔交易数据", trade_count);
                last_log_time = std::time::Instant::now();
            }
        }

        info!(target: "WebSocket任务", trade_count, "交易数据处理任务结束，总计处理 {} 笔交易", trade_count);
    });

    // 等待关闭信号或任务完成
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => {
            info!(target: "WebSocket任务", "收到关闭信号");
        },
        _ = client_handle => {
            warn!(target: "WebSocket任务", "WebSocket客户端任务意外结束");
        },
        _ = trade_processor => {
            warn!(target: "WebSocket任务", "交易数据处理任务意外结束");
        }
    }

    info!(target: "WebSocket任务", "已关闭");
}

// symbol_manager_task 的骨架实现
async fn symbol_manager_task(
    config: Arc<AggregateConfig>,
    _symbol_to_index: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    event_tx: mpsc::Sender<AppEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    if TEST_MODE {
        info!(target: "品种管理器", "测试模式下跳过品种管理器");
        return;
    }

    info!(target: "品种管理器", "启动中...");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let handler = Arc::new(MiniTickerMessageHandler::new(tx));
    let mini_ticker_config = MiniTickerConfig {
        use_proxy: config.websocket.use_proxy,
        proxy_addr: config.websocket.proxy_host.clone(),
        proxy_port: config.websocket.proxy_port,
    };
    let mut client = MiniTickerClient::new(mini_ticker_config, handler);

    tokio::spawn(async move {
        if let Err(e) = client.start().await {
            warn!(target: "品种管理器", error = ?e, "MiniTicker客户端启动失败");
        }
    });

    loop {
        tokio::select! {
            Some(tickers) = rx.recv() => {
                for ticker in tickers {
                    if ticker.symbol.ends_with("USDT") {
                        info!(target: "品种管理器", symbol = %ticker.symbol, "发现新品种");

                        let event = AppEvent::AddSymbol {
                            symbol: ticker.symbol.clone(),
                            first_kline_open_time: ticker.event_time as i64,
                        };

                        if event_tx.send(event).await.is_err() {
                            error!(target: "品种管理器", "主事件通道关闭，任务退出");
                            return;
                        }
                    }
                }
            },
            _ = shutdown_rx.changed() => break,
        }
    }
    info!(target: "品种管理器", "已关闭");
}









