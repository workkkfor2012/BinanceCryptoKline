use crate::error::{AppError, Result};
use crate::models::{DownloadTask, ExchangeInfo, Kline};
use log::{debug, error, info, warn};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Binance API client
pub struct BinanceApi {
    direct_client: Client,
    proxy_client: Client,
    data_api_url: String,
    fapi_url: String,
    use_proxy: Arc<AtomicBool>,
}

impl BinanceApi {
    /// Create a new Binance API client
    pub fn new(base_url: String) -> Self {
        // 创建直连客户端
        let direct_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create direct HTTP client");

        // 创建代理客户端
        let proxy_url = "http://127.0.0.1:1080";
        let proxy_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .proxy(reqwest::Proxy::http(proxy_url).expect("Failed to create HTTP proxy"))
            .proxy(reqwest::Proxy::https(proxy_url).expect("Failed to create HTTPS proxy"))
            .build()
            .expect("Failed to create proxy HTTP client");

        info!("Initialized API clients with data-api and fapi endpoints");
        info!("Will use data-api.binance.vision by default, fallback to fapi.binance.com with proxy if needed");

        Self {
            direct_client,
            proxy_client,
            data_api_url: "https://data-api.binance.vision".to_string(),
            fapi_url: "https://fapi.binance.com".to_string(),
            use_proxy: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get exchange information with automatic fallback
    pub async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        // 先尝试使用data-api.binance.vision
        if !self.use_proxy.load(Ordering::Relaxed) {
            let data_api_url = format!("{}/api/v3/exchangeInfo", self.data_api_url);
            debug!("Fetching exchange info from data-api: {}", data_api_url);

            match self.direct_client.get(&data_api_url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                .send()
                .await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<ExchangeInfo>().await {
                                Ok(exchange_info) => {
                                    debug!("Successfully received exchange info from data-api with {} symbols", exchange_info.symbols.len());
                                    return Ok(exchange_info);
                                },
                                Err(e) => {
                                    warn!("Failed to parse exchange info from data-api: {}", e);
                                }
                            }
                        } else {
                            warn!("Failed to get exchange info from data-api: {}", response.status());
                        }
                    },
                    Err(e) => {
                        warn!("Failed to connect to data-api: {}", e);
                    }
            }

            // 如果失败，切换到使用代理的fapi
            info!("Switching to fapi.binance.com with proxy");
            self.use_proxy.store(true, Ordering::Relaxed);
        }

        // 使用fapi.binance.com和代理
        let fapi_url = format!("{}/fapi/v1/exchangeInfo", self.fapi_url);
        debug!("Fetching exchange info from fapi with proxy: {}", fapi_url);

        let response = self.proxy_client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            error!("Failed to get exchange info from fapi: {} - {}", status, text);
            return Err(AppError::ApiError(format!(
                "Failed to get exchange info from fapi: {} - {}",
                status, text
            )));
        }

        let exchange_info: ExchangeInfo = response.json().await?;
        debug!("Received exchange info with {} symbols", exchange_info.symbols.len());

        Ok(exchange_info)
    }

    /// Get U-margined perpetual futures symbols
    pub async fn get_usdt_perpetual_symbols(&self) -> Result<Vec<String>> {
        let exchange_info = self.get_exchange_info().await?;

        // 打印所有交易对的信息，便于调试
        for (i, s) in exchange_info.symbols.iter().enumerate().take(5) {
            debug!("Symbol {}: {} (status: {}, quote_asset: {})", i, s.symbol, s.status, s.quote_asset);
        }

        // 使用与TypeScript代码相同的过滤条件
        let mut symbols = exchange_info
            .symbols
            .iter()
            .filter(|s| {
                // 过滤条件：以USDT结尾且状态为TRADING
                s.symbol.ends_with("USDT") && s.status == "TRADING"
            })
            .map(|s| s.symbol.clone())
            .collect::<Vec<String>>();

        // 不再限制只下载前两个品种
        info!("Found {} USDT-margined perpetual futures symbols", symbols.len());

        // 如果没有找到交易对，手动添加一些常用的
        if symbols.is_empty() {
            info!("No symbols found from API, using default list");
            return Ok(vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "BNBUSDT".to_string(),
                "ADAUSDT".to_string(),
                "DOGEUSDT".to_string(),
                "XRPUSDT".to_string(),
                "SOLUSDT".to_string(),
                "DOTUSDT".to_string(),
            ]);
        }

        info!("Found {} USDT perpetual symbols", symbols.len());
        Ok(symbols)
    }

    /// Download klines for a specific task with automatic fallback
    pub async fn download_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
        // 检查是否需要使用代理
        if !self.use_proxy.load(Ordering::Relaxed) {
            // 先尝试使用data-api.binance.vision
            let data_api_url = format!(
                "{}/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}&timeZone=8:00",
                self.data_api_url, task.symbol, task.interval, task.start_time, task.end_time, task.limit
            );
            debug!("Downloading klines from data-api: {}", data_api_url);

            match self.direct_client.get(&data_api_url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                .send()
                .await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<Vec<Vec<Value>>>().await {
                                Ok(raw_klines) => {
                                    debug!("Successfully received {} klines from data-api for {}", raw_klines.len(), task.symbol);
                                    let klines = raw_klines
                                        .iter()
                                        .filter_map(|raw| Kline::from_raw_kline(raw))
                                        .collect::<Vec<Kline>>();

                                    if klines.len() == raw_klines.len() {
                                        return Ok(klines);
                                    } else {
                                        warn!("Failed to parse some klines from data-api for {}: parsed {}/{} klines",
                                            task.symbol, klines.len(), raw_klines.len());
                                    }
                                },
                                Err(e) => {
                                    warn!("Failed to parse klines from data-api for {}: {}", task.symbol, e);
                                }
                            }
                        } else {
                            warn!("Failed to download klines from data-api for {}: {}", task.symbol, response.status());
                        }
                    },
                    Err(e) => {
                        warn!("Failed to connect to data-api for {}: {}", task.symbol, e);
                    }
            }

            // 如果失败，切换到使用代理的fapi
            info!("Switching to fapi.binance.com with proxy for {}", task.symbol);
            self.use_proxy.store(true, Ordering::Relaxed);
        }

        // 使用fapi.binance.com和代理
        let fapi_url = format!(
            "{}/fapi/v1/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}&timeZone=8:00",
            self.fapi_url, task.symbol, task.interval, task.start_time, task.end_time, task.limit
        );
        debug!("Downloading klines from fapi with proxy: {}", fapi_url);

        let response = self.proxy_client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            error!(
                "Failed to download klines from fapi for {}: {} - {}",
                task.symbol, status, text
            );
            return Err(AppError::ApiError(format!(
                "Failed to download klines from fapi for {}: {} - {}",
                task.symbol, status, text
            )));
        }

        let raw_klines: Vec<Vec<Value>> = response.json().await?;
        debug!(
            "Received {} klines for {}",
            raw_klines.len(),
            task.symbol
        );

        let klines = raw_klines
            .iter()
            .filter_map(|raw| Kline::from_raw_kline(raw))
            .collect::<Vec<Kline>>();

        if klines.len() != raw_klines.len() {
            error!(
                "Failed to parse some klines for {}: parsed {}/{} klines",
                task.symbol,
                klines.len(),
                raw_klines.len()
            );
        }

        Ok(klines)
    }
}
