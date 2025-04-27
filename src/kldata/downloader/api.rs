use crate::klcommon::{AppError, DownloadTask, ExchangeInfo, Kline, Result};
use log::{debug, error, info};
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

        // 解析响应为ExchangeInfo
        let mut exchange_info: ExchangeInfo = response.json().await?;

        // 过滤出状态为TRADING的交易对
        let total_symbols = exchange_info.symbols.len();
        exchange_info.symbols.retain(|symbol| symbol.status == "TRADING");

        let trading_symbols = exchange_info.symbols.len();
        debug!("获取到 {} 个交易对，其中 {} 个状态为TRADING", total_symbols, trading_symbols);

        Ok(exchange_info)
    }

    /// 获取正在交易的U本位永续合约
    pub async fn get_trading_usdt_perpetual_symbols(&self) -> Result<Vec<String>> {
        // 测试模式标志，设置为true时只返回BTCUSDT一个品种
        let istest = true;

        // 测试模式下，直接返回BTCUSDT
        if istest {
            debug!("测试模式：仅返回BTCUSDT一个品种");
            return Ok(vec!["BTCUSDT".to_string()]);
        }

        // 获取交易所信息
        let exchange_info = self.get_exchange_info().await?;

        // 过滤出以USDT结尾的交易对
        // 注意：get_exchange_info已经过滤了状态为TRADING的交易对
        let usdt_symbols: Vec<String> = exchange_info.symbols
            .iter()
            .filter(|symbol| symbol.symbol.ends_with("USDT"))
            .map(|symbol| symbol.symbol.clone())
            .collect();

        debug!("获取到 {} 个正在交易的U本位永续合约", usdt_symbols.len());

        Ok(usdt_symbols)
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
        let url_for_log = fapi_url.clone();

        let response = self.client.get(&fapi_url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            info!("{} -> 失败: {} - {}", url_for_log, status, text);
            return Err(AppError::ApiError(format!(
                "从fapi下载 {} 的K线失败: {} - {}",
                task.symbol, status, text
            )));
        }

        let raw_klines: Vec<Vec<Value>> = response.json().await?;
        info!("{} -> 成功: 收到 {} 条K线", url_for_log, raw_klines.len());

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
