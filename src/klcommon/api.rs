use crate::klcommon::{AppError, DownloadTask, ExchangeInfo, Kline, Result, get_proxy_url};
use tracing::{debug, error, warn, info, instrument};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use serde::{Deserialize, Serialize};

/// 将时间间隔转换为毫秒数
/// 例如: "1m" -> 60000, "1h" -> 3600000
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
#[derive(Clone)]
pub struct BinanceApi {
    api_url: String,
}

impl BinanceApi {
    /// 创建新的API客户端实例
    pub fn new() -> Self {
        // 使用fapi.binance.com作为API端点
        let api_url = "https://fapi.binance.com".to_string();
        Self { api_url }
    }

    /// 创建新的API客户端实例（带自定义URL）
    pub fn new_with_url(api_url: String) -> Self {
        Self { api_url }
    }

    /// 创建一个新的HTTP客户端实例（每次请求都会创建新的连接）
    #[instrument(target = "klcommon::api", skip(self))]
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
                debug!(target: "api", "使用代理创建HTTP客户端: {}", proxy_url);
                client_builder
                    .proxy(proxy)
                    .build()
                    .map_err(|e| AppError::ApiError(format!("创建带代理的HTTP客户端失败: {}", e)))?
            },
            Err(e) => {
                warn!(target: "api", "设置代理失败，将尝试直接连接: {} - {}", proxy_url, e);
                client_builder
                    .build()
                    .map_err(|e| AppError::ApiError(format!("创建HTTP客户端失败: {}", e)))?
            }
        };

        Ok(client)
    }

    /// 获取交易所信息
    #[instrument(target = "klcommon::api", skip(self))]
    pub async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        // 使用fapi.binance.com
        let fapi_url = format!("{}/fapi/v1/exchangeInfo", self.api_url);

        // 创建新的HTTP客户端
        let client = self.create_client()?;

        // 构建请求
        let request = client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        // 打印完整请求信息（仅调试级别）
        debug!(target: "api", "发送获取交易所信息请求: {}", fapi_url);

        // 发送请求
        let response = match request.send().await {
            Ok(resp) => {
                debug!(target: "api", "获取交易所信息响应: {}", resp.status());
                resp
            },
            Err(e) => {
                error!(target: "api", "获取交易所信息失败: {} - {}", fapi_url, e);
                return Err(e.into());
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            error!(target: "api", "从fapi获取交易所信息失败: {} - {}", status, text);
            return Err(AppError::ApiError(format!(
                "从fapi获取交易所信息失败: {} - {}",
                status, text
            )));
        }

        // 获取响应文本
        let response_text = response.text().await?;

        // 打印响应的前1000个字符（仅调试级别）
        debug!(target: "api", "交易所信息响应前1000个字符: {}", &response_text[..response_text.len().min(1000)]);

        // 解析响应为ExchangeInfo
        let exchange_info: ExchangeInfo = match serde_json::from_str::<ExchangeInfo>(&response_text) {
            Ok(info) => {
                //debug!("成功解析交易所信息JSON，获取到 {} 个交易对", info.symbols.len());
                info
            },
            Err(e) => {
                error!(target: "api", "解析交易所信息JSON失败: {}, 响应前1000个字符: {}",
                    e, &response_text[..response_text.len().min(1000)]);
                return Err(AppError::JsonError(e));
            }
        };

        Ok(exchange_info)
    }

    /// 获取正在交易的U本位永续合约
    ///
    /// 此方法从币安API获取所有正在交易的U本位永续合约交易对。
    /// 如果失败会重试最多5次，如果5次都失败则返回错误。
    ///
    /// # 返回值
    ///
    /// 成功时返回一个包含所有正在交易的U本位永续合约交易对的字符串向量。
    /// 例如：["BTCUSDT", "ETHUSDT", "BNBUSDT", ...]
    ///
    /// # 错误
    ///
    /// 如果无法获取交易所信息或者没有找到任何符合条件的交易对，返回相应的错误。
    ///
    /// # 示例
    ///
    /// ```
    /// let api = BinanceApi::new();
    /// let symbols = api.get_trading_usdt_perpetual_symbols().await?;
    /// println!("获取到 {} 个交易对", symbols.len());
    /// ```
    pub async fn get_trading_usdt_perpetual_symbols(&self) -> Result<Vec<String>> {
        // 最大重试次数
        const MAX_RETRIES: usize = 5;
        // 重试间隔（秒）
        const RETRY_INTERVAL: u64 = 1;

        // 重试逻辑
        for retry in 0..MAX_RETRIES {
            // 获取交易所信息
            match self.get_exchange_info().await {
                Ok(exchange_info) => {
                    // 过滤出U本位永续合约交易对
                    // 条件：
                    // 1. 以USDT结尾（U本位）
                    // 2. 状态为TRADING（正在交易）
                    // 3. 合约类型为PERPETUAL（永续合约）
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

                    // 如果没有找到交易对，只打印信息
                    if usdt_perpetual_symbols.is_empty() {
                        warn!(target: "api", "从API获取不到U本位永续合约交易对 (尝试 {}/{})", retry + 1, MAX_RETRIES);
                        if retry == MAX_RETRIES - 1 {
                            return Err(AppError::ApiError("获取U本位永续合约交易对失败，已重试5次但未获取到任何交易对".to_string()));
                        }
                    } else {
                        // 只输出过滤后的交易对数量
                        info!(target: "api", "获取U本位永续合约交易对成功，获取到 {} 个交易对", usdt_perpetual_symbols.len());
                        return Ok(usdt_perpetual_symbols);
                    }
                },
                Err(e) => {
                    error!(target: "api", "获取交易所信息失败 (尝试 {}/{}): {}", retry + 1, MAX_RETRIES, e);
                    if retry == MAX_RETRIES - 1 {
                        return Err(AppError::ApiError(format!("获取交易所信息失败，已重试{}次: {}", MAX_RETRIES, e)));
                    }
                }
            }

            // 如果不是最后一次重试，等待一段时间后再重试
            if retry < MAX_RETRIES - 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_INTERVAL)).await;
            }
        }

        // 这里理论上不会执行到，因为在最后一次重试失败时已经返回错误
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
        let response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                // 只在错误时记录请求URL
                error!(target: "api", "{}/{}: 连续合约请求失败: URL={}, 错误: {}", task.symbol, task.interval, fapi_url, e);
                return Err(e.into());
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            error!(target: "api",
                "下载 {} 的连续合约K线失败: {} - {}",
                task.symbol, status, text
            );
            return Err(AppError::ApiError(format!(
                "下载 {} 的连续合约K线失败: {} - {}",
                task.symbol, status, text
            )));
        }

        // 获取原始响应文本
        let response_text = response.text().await?;

        // 尝试解析为JSON
        let raw_klines: Vec<Vec<Value>> = match serde_json::from_str(&response_text) {
            Ok(data) => data,
            Err(e) => {
                error!(target: "api", "{}/{}: 连续合约解析JSON失败: {}, 原始响应: {}", task.symbol, task.interval, e, response_text);
                return Err(AppError::JsonError(e));
            }
        };

        // 检查是否为空结果
        if raw_klines.is_empty() {
            error!(target: "api", "{}/{}: 连续合约返回空结果，原始响应: {}", task.symbol, task.interval, response_text);
            return Err(AppError::DataError(format!(
                "连续合约空结果，原始响应: {}",
                response_text
            )));
        }

        let klines = raw_klines
            .iter()
            .filter_map(|raw| Kline::from_raw_kline(raw))
            .collect::<Vec<Kline>>();

        if klines.len() != raw_klines.len() {
            error!(target: "api",
                "解析 {} 的部分连续合约K线失败: 解析了 {}/{} 条K线，原始数据: {}",
                task.symbol,
                klines.len(),
                raw_klines.len(),
                serde_json::to_string(&raw_klines).unwrap_or_else(|_| "无法序列化".to_string())
            );
        }

        Ok(klines)
    }

    /// 获取币安服务器时间
    ///
    /// 调用 /fapi/v1/time 接口获取当前的系统时间
    /// 如果失败会重试最多5次，如果5次都失败则返回错误
    ///
    /// # 返回
    ///
    /// 返回包含服务器时间的 `ServerTime` 结构体
    ///
    /// # 错误
    ///
    /// 如果API请求失败，返回相应的错误
    pub async fn get_server_time(&self) -> Result<ServerTime> {
        // 构建API URL
        let fapi_url = format!("{}/fapi/v1/time", self.api_url);

        // 最大重试次数
        const MAX_RETRIES: usize = 5;
        // 重试间隔（秒）
        const RETRY_INTERVAL: u64 = 1;

        // 创建新的HTTP客户端
        let client = self.create_client()?;

        // 重试逻辑
        for retry in 0..MAX_RETRIES {
            // 构建请求
            let request = client.get(&fapi_url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

            // 发送请求
            match request.send().await {
                Ok(response) => {
                    // 检查响应状态
                    if response.status().is_success() {
                        // 解析响应为ServerTime结构体
                        match response.json::<ServerTime>().await {
                            Ok(server_time) => {
                                if retry > 0 {
                                    info!(target: "api", "获取服务器时间成功，重试次数: {}", retry);
                                }
                                return Ok(server_time);
                            },
                            Err(e) => {
                                error!(target: "api", "解析服务器时间响应失败 (尝试 {}/{}): {}", retry + 1, MAX_RETRIES, e);
                                if retry == MAX_RETRIES - 1 {
                                    return Err(AppError::ApiError(format!("解析服务器时间响应失败，已重试{}次: {}", MAX_RETRIES, e)));
                                }
                            }
                        }
                    } else {
                        let status = response.status();
                        let text = match response.text().await {
                            Ok(t) => t,
                            Err(e) => format!("无法读取响应内容: {}", e),
                        };
                        error!(target: "api", "获取服务器时间失败 (尝试 {}/{}): {} - {}", retry + 1, MAX_RETRIES, status, text);
                        if retry == MAX_RETRIES - 1 {
                            return Err(AppError::ApiError(format!("获取服务器时间失败，已重试{}次: {} - {}", MAX_RETRIES, status, text)));
                        }
                    }
                },
                Err(e) => {
                    error!(target: "api", "获取服务器时间失败 (尝试 {}/{}): URL={}, 错误: {}", retry + 1, MAX_RETRIES, fapi_url, e);
                    if retry == MAX_RETRIES - 1 {
                        return Err(AppError::ApiError(format!("获取服务器时间失败，已重试{}次: {}", MAX_RETRIES, e)));
                    }
                }
            }

            // 如果不是最后一次重试，等待一段时间后再重试
            if retry < MAX_RETRIES - 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_INTERVAL)).await;
            }
        }

        // 这里理论上不会执行到，因为在最后一次重试失败时已经返回错误
        Err(AppError::ApiError("获取服务器时间失败，已达到最大重试次数".to_string()))
    }
}
