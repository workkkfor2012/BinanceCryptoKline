use crate::klcommon::{AppError, DownloadTask, ExchangeInfo, Kline, Result, get_proxy_url};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use tracing::{error, info, trace}; // 导入 error, info 和 trace 宏
use kline_macros::perf_profile;
use chrono::TimeZone; // 添加TimeZone导入

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggTrade {
    #[serde(rename = "a")]
    pub agg_trade_id: i64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "T")]
    pub timestamp_ms: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
    // 忽略 f 和 l 字段，因为我们暂时用不到
}

/// 将时间间隔转换为毫秒数
/// 例如: "1m" -> 60000, "1h" -> 3600000
// #[instrument] 移除：这是纯工具函数，快速计算，追踪会产生噪音
pub fn interval_to_milliseconds(interval: &str) -> i64 {
    let last_char = interval.chars().last().unwrap_or('m');
    let value: i64 = interval[..interval.len() - 1].parse().unwrap_or(1);

    match last_char {
        'm' => value * 60 * 1000,        // 分钟
        'h' => value * 60 * 60 * 1000,   // 小时
        'd' => value * 24 * 60 * 60 * 1000, // 天
        'w' => value * 7 * 24 * 60 * 60 * 1000, // 周
        _ => value * 60 * 1000,  // 默认为分钟
    }
}

/// 获取对齐到特定周期的时间戳
///
/// 不同周期的K线有特定的时间对齐要求：
/// - 分钟/小时K线（如 1m, 5m, 1h, 4h）：应该对齐到该周期的整数倍时间。
/// - 日K线（1d）：应该在UTC 00:00:00开始
/// - 周K线（1w）：应该在每周一的UTC 00:00:00开始
// #[instrument] 移除：这是纯工具函数，时间对齐计算，追踪会产生噪音
pub fn get_aligned_time(timestamp_ms: i64, interval: &str) -> i64 {
    use chrono::{DateTime, Datelike, TimeZone, Utc};

    // ✨ [修改] 统一处理所有基于时长的周期 (m, h)，修正4h对齐的BUG
    let last_char = interval.chars().last().unwrap_or(' ');
    if last_char == 'm' || last_char == 'h' {
        let interval_ms = interval_to_milliseconds(interval);
        if interval_ms > 0 {
            return (timestamp_ms / interval_ms) * interval_ms;
        }
    }

    // 保持对d和w的特殊处理
    match interval {
        "1d" => {
            // 对齐到天
            let dt = DateTime::<Utc>::from_timestamp(timestamp_ms / 1000, 0).unwrap();
            let day_start = Utc.ymd(dt.year(), dt.month(), dt.day()).and_hms(0, 0, 0);
            day_start.timestamp() * 1000
        },
        "1w" => {
            // 对齐到周一
            let dt = DateTime::<Utc>::from_timestamp(timestamp_ms / 1000, 0).unwrap();
            let days_from_monday = dt.weekday().num_days_from_monday() as i64;
            let day_start = Utc.ymd(dt.year(), dt.month(), dt.day()).and_hms(0, 0, 0);
            let week_start = day_start - chrono::Duration::days(days_from_monday);
            week_start.timestamp() * 1000
        },
        _ => timestamp_ms // 对于未知或已处理的周期，返回原值
    }
}

/// 币安服务器时间响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerTime {
    /// 服务器时间（毫秒时间戳）
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

/// 币安API客户端 - [重构] 变为无状态的静态工具集
#[derive(Debug, Clone, Copy)]
pub struct BinanceApi; // 不再有任何字段

impl BinanceApi {
    /// 创建一个新的 reqwest::Client 实例（启用连接池）
    pub fn create_new_client() -> Result<Client> {
        let client_builder = Client::builder()
            .timeout(Duration::from_secs(5))        // [优化] 总超时从30秒减少到15秒
            .connect_timeout(Duration::from_secs(2)); // [优化] 连接超时从10秒减少到5秒

        let proxy_url = get_proxy_url();
        match reqwest::Proxy::all(&proxy_url) {
            Ok(proxy) => client_builder.proxy(proxy).build(),
            Err(_) => client_builder.build(),
        }
        .map_err(|e| AppError::ApiError(format!("创建HTTP客户端失败: {}", e)))
    }

    /// [重构] 获取交易所信息 - 接受 client 作为参数
    #[perf_profile]
    pub async fn get_exchange_info(client: &Client) -> Result<ExchangeInfo> {
        let api_url = "https://fapi.binance.com";
        let fapi_url = format!("{}/fapi/v1/exchangeInfo", api_url);

        let request = client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        let response = request.send().await.map_err(AppError::from)?;

        // ✨ [修改] 使用error_for_status()来保留HTTP状态码信息，便于上游进行精细化错误处理
        let response = response.error_for_status().map_err(AppError::from)?;

        let response_text = response.text().await?;
        let exchange_info: ExchangeInfo = serde_json::from_str(&response_text)
            .map_err(AppError::JsonError)?;

        Ok(exchange_info)
    }

    /// [重构] 获取正在交易的U本位永续合约，同时返回需要删除的已下架品种 - 接受 client 作为参数
    #[perf_profile]
    pub async fn get_trading_usdt_perpetual_symbols(client: &Client) -> Result<(Vec<String>, Vec<String>)> {
        const MAX_RETRIES: usize = 5;
        const RETRY_INTERVAL: u64 = 1;

        for retry in 0..MAX_RETRIES {
            match Self::get_exchange_info(client).await {
                Ok(exchange_info) => {
                    let mut trading_symbols = Vec::new();
                    let mut delisted_symbols = Vec::new();

                    for symbol in &exchange_info.symbols {
                        let is_usdt = symbol.symbol.ends_with("USDT");
                        let is_perpetual = symbol.contract_type == "PERPETUAL";

                        // 只处理USDT永续合约
                        if !is_usdt  || !is_perpetual {
                            continue;
                        }

                        // ✨ [修改] 根据状态进行不同处理
                        match symbol.status.as_str() {
                            "TRADING" => {
                                // 正常交易状态，加入交易列表
                                trading_symbols.push(symbol.symbol.clone());
                            },
                            "CLOSE" | "SETTLING" => {
                                // 已下架状态（包括CLOSE和SETTLING），加入删除列表
                                delisted_symbols.push(symbol.symbol.clone());
                                // 注意：这里只是发现已下架品种，具体是否需要删除数据
                                // 要等到backfill模块检查数据库中是否存在相关数据后才能确定
                            },
                            _ => {
                                // 其他未知状态，记录日志
                                info!(
                                    log_type = "low_freq",
                                    message = "发现未知状态的品种",
                                    symbol = %symbol.symbol,
                                    status = %symbol.status,
                                    note = "非TRADING、CLOSE或SETTLING状态，需要关注",
                                );
                            }
                        }
                    }

                    if trading_symbols.is_empty() {
                        if retry == MAX_RETRIES - 1 {
                            return Err(AppError::ApiError("获取U本位永续合约交易对失败，已重试5次但未获取到任何交易对".to_string()));
                        }
                    } else {
                        return Ok((trading_symbols, delisted_symbols));
                    }
                },
                Err(e) => {
                    if retry == MAX_RETRIES - 1 {
                        let final_error = AppError::ApiError(format!("获取交易所信息失败，已重试{}次: {}", MAX_RETRIES, e));
                        // ✨ [新增] 记录决策变量和错误链
                        error!(
                            log_type = "low_freq", // 这是一个低频但关键的失败事件
                            max_retries = MAX_RETRIES,
                            error_chain = format!("{:#}", e),
                            message = "获取交易所信息失败，已达到最大重试次数"
                        );
                        return Err(final_error);
                    }
                }
            }

            if retry < MAX_RETRIES - 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_INTERVAL)).await;
            }
        }

        Err(AppError::ApiError("获取U本位永续合约交易对失败，已达到最大重试次数".to_string()))
    }

    /// [重构] 下载连续合约K线数据 - 接受 client 作为参数
    #[perf_profile(skip_all, fields(symbol = %task.symbol, interval = %task.interval))]
    pub async fn download_continuous_klines(client: &Client, task: &DownloadTask) -> Result<Vec<Kline>> {
        let api_url = "https://fapi.binance.com";
        // 构建URL参数
        let mut url_params = format!(
            "pair={}&contractType=PERPETUAL&interval={}&limit={}",
            task.symbol, task.interval, task.limit
        );

        // 添加可选的起始时间
        if let Some(start_time) = task.start_time {
            url_params.push_str(&format!("&startTime={}", start_time));
        }

        // 添加可选的结束时间
        if let Some(end_time) = task.end_time {
            url_params.push_str(&format!("&endTime={}", end_time));
        }

        // 使用fapi.binance.com
        let fapi_url = format!("{}/fapi/v1/continuousKlines?{}", api_url, url_params);

        // 记录下载URL到日志
        trace!(
            log_type = "low_freq",
            message = "开始下载K线数据",
            symbol = %task.symbol,
            interval = %task.interval,
            download_url = %fapi_url,
            start_time = task.start_time.map(|t| {
                chrono::Utc.timestamp_millis(t).format("%Y-%m-%d %H:%M:%S").to_string()
            }).unwrap_or_else(|| "无限制".to_string()),
            end_time = task.end_time.map(|t| {
                chrono::Utc.timestamp_millis(t).format("%Y-%m-%d %H:%M:%S").to_string()
            }).unwrap_or_else(|| "无限制".to_string()),
            limit = task.limit,
        );

        // 构建请求
        let request = client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        // 发送请求，直接处理结果
        let response = request.send().await.map_err(AppError::from)?;

        // ✨ [修改] 将非 2xx 的HTTP状态码直接转换为错误，这会生成一个 AppError::HttpError，
        // 方便上游根据具体状态码（如 429, 418）进行精细化处理。
        // 这个改动替换了之前手动的状态检查和日志记录，以实现更健壮的错误传递。
        let response = response.error_for_status().map_err(AppError::from)?;

        let response_text = response.text().await?;
        let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)
            .map_err(AppError::JsonError)?;

        if raw_klines.is_empty() {
            let data_error = AppError::DataError(format!("连续合约空结果，原始响应: {}", response_text));
            return Err(data_error);
        }

        let klines = raw_klines.iter().filter_map(|raw| Kline::from_raw_kline(raw)).collect::<Vec<Kline>>();
        Ok(klines)
    }

    /// [重构] 获取币安服务器时间 - 接受 client 作为参数
    #[perf_profile]
    pub async fn get_server_time(client: &Client) -> Result<ServerTime> {
        let api_url = "https://fapi.binance.com";
        let fapi_url = format!("{}/fapi/v1/time", api_url);
        const MAX_RETRIES: usize = 5;
        const RETRY_INTERVAL: u64 = 1;

        for retry in 0..MAX_RETRIES {
            let request = client.get(&fapi_url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<ServerTime>().await {
                            Ok(server_time) => {
                                return Ok(server_time);
                            },
                            Err(e) => {
                                let json_error = AppError::from(e);
                                if retry == MAX_RETRIES - 1 {
                                    let final_error = AppError::ApiError(format!("解析服务器时间响应失败，已重试{}次: {}", MAX_RETRIES, json_error));
                                    return Err(final_error);
                                }
                            }
                        }
                    } else {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_else(|e| format!("无法读取响应内容: {}", e));
                        if retry == MAX_RETRIES - 1 {
                            let api_error = AppError::ApiError(format!("获取服务器时间失败，已重试{}次: {} - {}", MAX_RETRIES, status, text));
                            return Err(api_error);
                        }
                    }
                },
                Err(e) => {
                    let http_error = AppError::from(e);
                    if retry == MAX_RETRIES - 1 {
                        let final_error = AppError::ApiError(format!("获取服务器时间失败，已重试{}次: {}", MAX_RETRIES, http_error));
                        return Err(final_error);
                    }
                }
            }

            if retry < MAX_RETRIES - 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_INTERVAL)).await;
            }
        }

        Err(AppError::ApiError("获取服务器时间失败，已达到最大重试次数".to_string()))
    }

    /// [重构] 获取归集交易记录 (aggTrades) - 接受 client 作为参数
    #[perf_profile(skip_all, fields(symbol = %symbol))]
    pub async fn get_agg_trades(
        client: &Client,
        symbol: String,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<u16>,
    ) -> Result<Vec<AggTrade>> {
        let api_url = "https://fapi.binance.com";
        let mut url_params = format!("symbol={}", symbol);
        if let Some(st) = start_time { url_params.push_str(&format!("&startTime={}", st)); }
        if let Some(et) = end_time { url_params.push_str(&format!("&endTime={}", et)); }
        url_params.push_str(&format!("&limit={}", limit.unwrap_or(1000)));

        let fapi_url = format!("{}/fapi/v1/aggTrades?{}", api_url, url_params);
        let response = client.get(&fapi_url).send().await?;

        if !response.status().is_success() {
            return Err(AppError::ApiError(format!("获取 {} aggTrades 失败: {}", symbol, response.status())));
        }

        let trades: Vec<AggTrade> = response.json().await?;
        Ok(trades)
    }
}
