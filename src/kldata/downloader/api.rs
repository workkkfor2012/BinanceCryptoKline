use crate::klcommon::{AppError, DownloadTask, ExchangeInfo, Kline, Result};
use log::{debug, error};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

/// 币安API客户端
#[derive(Clone)]
pub struct BinanceApi {
    client: Client,
    api_url: String,
}

impl BinanceApi {
    /// 创建新的API客户端实例
    pub fn new() -> Self {
        // 创建带有超时设置的HTTP客户端
        let client_builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10));

        // 添加代理设置
        let proxy_url = "http://127.0.0.1:1080";
        let client = match reqwest::Proxy::all(proxy_url) {
            Ok(proxy) => {
                log::info!("使用代理: {}", proxy_url);
                client_builder
                    .proxy(proxy)
                    .build()
                    .expect("创建带代理的HTTP客户端失败")
            },
            Err(e) => {
                log::warn!("设置代理失败，将尝试直接连接: {}", e);
                client_builder
                    .build()
                    .expect("创建HTTP客户端失败")
            }
        };

        // 使用fapi.binance.com作为API端点
        let api_url = "https://fapi.binance.com".to_string();

        Self { client, api_url }
    }

    /// 获取交易所信息
    pub async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        // 使用fapi.binance.com
        let fapi_url = format!("{}/fapi/v1/exchangeInfo", self.api_url);
        debug!("从fapi获取交易所信息: {}", fapi_url);

        let response = self.client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            error!("从fapi获取交易所信息失败: {} - {}", status, text);
            return Err(AppError::ApiError(format!(
                "从fapi获取交易所信息失败: {} - {}",
                status, text
            )));
        }

        let exchange_info: ExchangeInfo = response.json().await?;
        debug!("获取到 {} 个交易对", exchange_info.symbols.len());

        Ok(exchange_info)
    }

    /// 下载K线数据
    pub async fn download_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
        // 构建URL参数
        let mut url_params = format!(
            "symbol={}&interval={}&limit={}",
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
        let fapi_url = format!("{}/fapi/v1/klines?{}", self.api_url, url_params);
        debug!("从fapi下载K线: {}", fapi_url);

        let response = self.client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            error!(
                "从fapi下载 {} 的K线失败: {} - {}",
                task.symbol, status, text
            );
            return Err(AppError::ApiError(format!(
                "从fapi下载 {} 的K线失败: {} - {}",
                task.symbol, status, text
            )));
        }

        let raw_klines: Vec<Vec<Value>> = response.json().await?;
        debug!(
            "收到 {} 条 {} 的K线",
            raw_klines.len(),
            task.symbol
        );

        let klines = raw_klines
            .iter()
            .filter_map(|raw| Kline::from_raw_kline(raw))
            .collect::<Vec<Kline>>();

        if klines.len() != raw_klines.len() {
            error!(
                "解析 {} 的部分K线失败: 解析了 {}/{} 条K线",
                task.symbol,
                klines.len(),
                raw_klines.len()
            );
        }

        Ok(klines)
    }
}
