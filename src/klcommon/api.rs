use crate::klcommon::{AppError, DownloadTask, ExchangeInfo, Kline, Result, get_proxy_url};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use tracing::{warn, error}; // 导入 warn 和 error 宏

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
/// - 分钟K线（1m, 5m, 30m）：应该在每分钟的00秒开始
/// - 小时K线（1h, 4h）：应该在每小时的00分00秒开始
/// - 日K线（1d）：应该在UTC 00:00:00开始
/// - 周K线（1w）：应该在每周一的UTC 00:00:00开始
// #[instrument] 移除：这是纯工具函数，时间对齐计算，追踪会产生噪音
pub fn get_aligned_time(timestamp_ms: i64, interval: &str) -> i64 {
    use chrono::{DateTime, Datelike, TimeZone, Utc};

    match interval {
        "1m" | "5m" | "30m" => {
            // 对齐到分钟
            (timestamp_ms / 60000) * 60000
        },
        "1h" | "4h" => {
            // 对齐到小时
            (timestamp_ms / 3600000) * 3600000
        },
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
        _ => timestamp_ms
    }
}

/// 币安服务器时间响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerTime {
    /// 服务器时间（毫秒时间戳）
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

/// 币安API客户端
#[derive(Clone, Debug)]
pub struct BinanceApi {
    api_url: String,
}

impl BinanceApi {
    /// 创建新的API客户端实例
    pub fn new() -> Self {
        let api_url = "https://fapi.binance.com".to_string();
        Self { api_url }
    }

    /// 创建新的API客户端实例（带自定义URL）
    pub fn new_with_url(api_url: String) -> Self {
        Self { api_url }
    }

    /// 创建一个新的HTTP客户端实例（每次请求都会创建新的连接）
    fn create_client(&self) -> Result<Client> {
        // 创建带有超时设置的HTTP客户端，禁用连接池
        let client_builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(0) // 设置为0，禁用连接池
            .pool_idle_timeout(Duration::from_secs(0)); // 设置空闲超时为0，确保连接不会被重用

        // 添加代理设置
        let proxy_url = get_proxy_url();
        let client = match reqwest::Proxy::all(&proxy_url) {
            Ok(proxy) => {
                client_builder
                    .proxy(proxy)
                    .build()
                    .map_err(|e| AppError::ApiError(format!("创建带代理的HTTP客户端失败: {}", e)))?
            },
            Err(_e) => {
                client_builder
                    .build()
                    .map_err(|e| AppError::ApiError(format!("创建HTTP客户端失败: {}", e)))?
            }
        };

        Ok(client)
    }

    /// 获取交易所信息
    pub async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        let fapi_url = format!("{}/fapi/v1/exchangeInfo", self.api_url);
        let client = self.create_client()?;

        let request = client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        let response = request.send().await.map_err(AppError::from)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            let api_error = AppError::ApiError(format!(
                "从fapi获取交易所信息失败: {} - {}",
                status, text
            ));
            return Err(api_error);
        }

        let response_text = response.text().await?;
        let exchange_info: ExchangeInfo = serde_json::from_str(&response_text)
            .map_err(AppError::JsonError)?;

        Ok(exchange_info)
    }

    /// 获取正在交易的U本位永续合约
    pub async fn get_trading_usdt_perpetual_symbols(&self) -> Result<Vec<String>> {
        const MAX_RETRIES: usize = 5;
        const RETRY_INTERVAL: u64 = 1;

        for retry in 0..MAX_RETRIES {
            match self.get_exchange_info().await {
                Ok(exchange_info) => {
                    let usdt_perpetual_symbols: Vec<String> = exchange_info.symbols
                        .iter()
                        .filter(|symbol| {
                            let is_usdt = symbol.symbol.ends_with("USDT");
                            let is_trading = symbol.status == "TRADING";
                            let is_perpetual = symbol.contract_type == "PERPETUAL";
                            is_usdt && is_trading && is_perpetual
                        })
                        .map(|symbol| symbol.symbol.clone())
                        .collect();

                    if usdt_perpetual_symbols.is_empty() {
                        if retry == MAX_RETRIES - 1 {
                            return Err(AppError::ApiError("获取U本位永续合约交易对失败，已重试5次但未获取到任何交易对".to_string()));
                        }
                    } else {
                        return Ok(usdt_perpetual_symbols);
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

    /// 下载连续合约K线数据
    pub async fn download_continuous_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
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
        let fapi_url = format!("{}/fapi/v1/continuousKlines?{}", self.api_url, url_params);

        // 创建新的HTTP客户端
        let client = self.create_client()?;

        // 构建请求
        let request = client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        // 发送请求
        let response = request.send().await.map_err(AppError::from)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_else(|_| "无法读取响应体".to_string());

            // 修改点：在返回错误前，记录一条结构化的警告日志
            warn!(
                // log_type 会从调用者的Span继承
                http_status = %status,
                response_text = %text,
                message = "API请求返回非成功状态码"
            );

            let api_error = AppError::ApiError(format!(
                "下载 {} 的连续合约K线失败: {} - {}",
                task.symbol, status, text
            ));
            return Err(api_error);
        }

        // 获取原始响应文本
        let response_text = response.text().await?;

        // 尝试解析为JSON
        let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)
            .map_err(AppError::JsonError)?;

        // 检查是否为空结果
        if raw_klines.is_empty() {
            let data_error = AppError::DataError(format!(
                "连续合约空结果，原始响应: {}",
                response_text
            ));
            return Err(data_error);
        }

        let klines = raw_klines
            .iter()
            .filter_map(|raw| Kline::from_raw_kline(raw))
            .collect::<Vec<Kline>>();
        Ok(klines)
    }

    /// 获取币安服务器时间
    pub async fn get_server_time(&self) -> Result<ServerTime> {
        let fapi_url = format!("{}/fapi/v1/time", self.api_url);
        const MAX_RETRIES: usize = 5;
        const RETRY_INTERVAL: u64 = 1;

        let client = self.create_client()?;

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
}
