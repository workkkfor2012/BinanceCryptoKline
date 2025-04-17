use crate::error::{AppError, Result};
use crate::models::{DownloadTask, ExchangeInfo, Kline};
use log::{debug, error, info, warn};
use reqwest::Client;
use serde_json::Value;
// 移除未使用的导入
use std::time::Duration;

/// Binance API client
pub struct BinanceApi {
    client: Client,
    api_url: String,
}

impl BinanceApi {
    /// Create a new Binance API client
    pub fn new(_base_url: String) -> Self {
        // 创建客户端
        let client_builder = Client::builder()
            .timeout(Duration::from_secs(30));

        // 添加代理设置
        let proxy_url = "http://127.0.0.1:1080";
        let client = match reqwest::Proxy::all(proxy_url) {
            Ok(proxy) => {
                info!("Using proxy for all protocols: {}", proxy_url);
                client_builder
                    .proxy(proxy)
                    .build()
                    .expect("Failed to create HTTP client with proxy")
            },
            Err(e) => {
                warn!("Failed to set proxy, will try direct connection: {}", e);
                client_builder
                    .build()
                    .expect("Failed to create HTTP client")
            }
        };

        info!("Initialized API client with fapi endpoint");
        info!("Using https://fapi.binance.com as the only API endpoint");

        Self {
            client,
            api_url: "https://fapi.binance.com".to_string(),
        }
    }

    /// Get exchange information
    pub async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        // 使用fapi.binance.com
        let fapi_url = format!("{}/fapi/v1/exchangeInfo", self.api_url);
        debug!("Fetching exchange info from fapi: {}", fapi_url);

        let response = self.client.get(&fapi_url)
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
        let symbols = exchange_info
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

    /// Download klines for a specific task
    pub async fn download_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
        // 使用fapi.binance.com
        // 使用 fapi/v1/continuousKlines 获取连续合约数据
        let fapi_url = format!(
            "{}/fapi/v1/continuousKlines?pair={}&contractType=PERPETUAL&interval={}&startTime={}&endTime={}&limit={}",
            self.api_url, task.symbol, task.interval, task.start_time, task.end_time, task.limit
        );
        debug!("Downloading continuous klines from fapi: {}", fapi_url);

        let response = self.client.get(&fapi_url)
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
