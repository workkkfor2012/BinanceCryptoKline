//! 品种管理器模块
//!
//! 负责品种索引的初始化和动态品种发现管理。
//! 包含品种索引构建和基于MiniTicker的实时新品种发现功能。

use crate::klcommon::{
    api::BinanceApi,
    db::Database,
    error::AppError,
    websocket::{MiniTickerClient, MiniTickerConfig, MiniTickerMessageHandler, WebSocketClient},
    AggregateConfig,
};
use crate::klagg_sub_threads::{InitialKlineData, WorkerCmd};
use crate::soft_assert;
use anyhow::Result;
use chrono;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{error, info, instrument, warn};

/// 初始化品种索引
/// 
/// 从币安API获取所有交易品种，并根据历史数据的时间戳进行排序，
/// 构建全局品种索引映射。
/// 
/// # 参数
/// - `_api`: BinanceApi实例（保留参数以减少函数签名变动）
/// - `db`: 数据库实例
/// 
/// # 返回值
/// 返回排序后的品种列表和品种到索引的映射
#[instrument(target = "应用生命周期", skip_all, name = "initialize_symbol_indexing")]
pub async fn initialize_symbol_indexing(
    _api: &BinanceApi,
    db: &Database,
) -> Result<(Vec<String>, HashMap<String, usize>)> {
    info!(target: "应用生命周期", "开始初始化品种索引");

    info!(target: "应用生命周期", "正在从币安API获取所有U本位永续合约品种...");
    let (trading_symbols, delisted_symbols) = BinanceApi::get_trading_usdt_perpetual_symbols().await?;

    // 处理已下架的品种
    if !delisted_symbols.is_empty() {
        info!(
            target: "应用生命周期",
            "发现已下架品种: {}，这些品种将不会被包含在索引中",
            delisted_symbols.join(", ")
        );
    }

    let symbols = trading_symbols;
    info!(target: "应用生命周期", count = symbols.len(), "品种列表获取成功");

    let symbol_listing_times = db.batch_get_earliest_kline_timestamps(&symbols, "1d")?;

    let mut sorted_symbols_with_time: Vec<(String, i64)> = symbol_listing_times
        .into_iter()
        .map(|(s, t_opt)| {
            let timestamp = t_opt.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            (s, timestamp)
        })
        .collect();

    soft_assert!(!sorted_symbols_with_time.is_empty(),
        message = "未能找到任何带有历史数据的品种",
        symbols_count = sorted_symbols_with_time.len(),
    );

    if sorted_symbols_with_time.is_empty() {
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

/// 品种管理器任务
/// 
/// 负责发现新品种并以原子方式发送指令给聚合器的任务。
/// 使用MiniTicker WebSocket实时监控新品种的出现。
/// 
/// # 参数
/// - `config`: 应用配置
/// - `symbol_to_global_index`: 品种到全局索引的映射
/// - `global_index_to_symbol`: 全局索引到品种的映射
/// - `global_symbol_count`: 全局品种计数器
/// - `cmd_tx`: 向聚合器发送命令的通道
#[instrument(target = "品种管理器", skip_all, name = "run_symbol_manager")]
pub async fn run_symbol_manager(
    config: Arc<AggregateConfig>,
    symbol_to_global_index: Arc<RwLock<HashMap<String, usize>>>,
    global_index_to_symbol: Arc<RwLock<Vec<String>>>,
    global_symbol_count: Arc<AtomicUsize>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
) -> Result<()> {
    info!(target: "品种管理器", log_type = "low_freq", "品种管理器启动 - 基于MiniTicker实时发现新品种");

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

    // 辅助函数，用于安全地解析 f64 字符串
    let parse_or_zero = |s: &str, field_name: &str, symbol: &str| -> f64 {
        s.parse::<f64>().unwrap_or_else(|_| {
            warn!(target: "品种管理器", %symbol, field_name, value = %s, "无法解析新品种的初始数据，将使用 0.0");
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
                let (ack_tx, ack_rx) = oneshot::channel::<std::result::Result<usize, String>>();

                // 从 ticker 数据中解析完整的 OHLCV 数据
                let initial_data = InitialKlineData {
                    open: parse_or_zero(&ticker.open_price, "open", &ticker.symbol),
                    high: parse_or_zero(&ticker.high_price, "high", &ticker.symbol),
                    low: parse_or_zero(&ticker.low_price, "low", &ticker.symbol),
                    close: parse_or_zero(&ticker.close_price, "close", &ticker.symbol),
                    volume: parse_or_zero(&ticker.total_traded_volume, "volume", &ticker.symbol),
                    turnover: parse_or_zero(&ticker.total_traded_quote_volume, "turnover", &ticker.symbol),
                };

                // 从 ticker 中获取事件时间戳
                // ticker.event_time 是 u64 类型，需要转换为 i64
                let event_time = ticker.event_time as i64;

                let cmd = WorkerCmd::AddSymbol {
                    symbol: ticker.symbol.clone(),
                    initial_data,
                    // [修改] 传递从 miniTicker 事件中获取的精确时间戳
                    first_kline_open_time: event_time,
                    ack: ack_tx,
                };

                if cmd_tx.send(cmd).await.is_err() {
                    warn!(target: "品种管理器", symbol=%ticker.symbol, "向聚合器发送AddSymbol命令失败，通道可能已关闭");
                    return Ok(());
                }

                match ack_rx.await {
                    Ok(Ok(new_global_index)) => {
                        let mut write_guard_map = symbol_to_global_index.write().await;
                        let mut write_guard_vec = global_index_to_symbol.write().await;

                        if !write_guard_map.contains_key(&ticker.symbol) {
                            // 核心同步逻辑：确保索引与向量长度一致
                            if new_global_index == write_guard_vec.len() {
                                write_guard_map.insert(ticker.symbol.clone(), new_global_index);
                                write_guard_vec.push(ticker.symbol.clone());
                                global_symbol_count.store(write_guard_vec.len(), Ordering::SeqCst);
                                info!(target: "品种管理器", symbol = %ticker.symbol, new_global_index, "成功添加新品种到全局索引和聚合器");
                            } else {
                                error!(
                                    log_type = "assertion",
                                    symbol = %ticker.symbol,
                                    new_global_index,
                                    vec_len = write_guard_vec.len(),
                                    "全局索引与向量长度不一致，发生严重逻辑错误！"
                                );
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = %e, "添加新品种失败，聚合器拒绝");
                    }
                    Err(_) => {
                        warn!(target: "品种管理器", symbol = %ticker.symbol, reason = "ack_channel_closed", "添加新品种失败，与聚合器的确认通道已关闭");
                    }
                }
            }
        }
    }
    warn!(target: "品种管理器", "品种管理器任务已退出");
    Ok(())
}
