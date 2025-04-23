#[cfg(test)]
mod tests {
    use kline_server::klcommon::{Database, Result};
    use kline_server::kldata::downloader::{Config, Downloader};
    use kline_server::kldata::streamer::{ContinuousKlineClient, ContinuousKlineConfig};
    use kline_server::kldata::aggregator::KlineAggregator;
    use kline_server::klserver::web::server::start_web_server;
    use std::path::PathBuf;
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use reqwest;
    use serde_json::Value;

    // 创建测试数据库
    fn setup_test_db() -> Arc<Database> {
        // 创建临时数据库文件
        let db_path = PathBuf::from("./target/test_integration.db");
        
        // 如果文件已存在，则删除
        if db_path.exists() {
            fs::remove_file(&db_path).unwrap();
        }
        
        // 创建数据库连接
        let db = Database::new(&db_path).unwrap();
        Arc::new(db)
    }

    #[tokio::test]
    #[ignore] // 忽略此测试，因为它需要网络连接和代理
    async fn test_full_system_integration() -> Result<()> {
        // 创建测试数据库
        let db = setup_test_db();
        let db_clone1 = db.clone();
        let db_clone2 = db.clone();
        let db_clone3 = db.clone();
        
        // 第一步：下载历史数据
        println!("第一步：下载历史数据");
        
        // 创建下载器配置
        let config = Config::new(
            Some("./target".to_string()),
            Some(1),                              // 并发数
            Some(vec!["1m".to_string()]),         // 只下载1分钟K线
            None,                                 // 起始时间（自动计算）
            None,                                 // 结束时间（当前时间）
            Some(vec!["BTCUSDT".to_string()]),    // 只下载BTCUSDT
            Some(true),                           // 使用SQLite文件存储
            Some(100),                            // 每个交易对最多保存100条K线
            Some(false),                          // 不使用更新模式
        );
        
        // 创建下载器
        let downloader = Downloader::new(config)?;
        
        // 运行下载流程
        match downloader.run().await {
            Ok(_) => println!("历史数据下载成功"),
            Err(e) => {
                eprintln!("历史数据下载失败: {}", e);
                return Err(e);
            }
        }
        
        // 验证数据库中是否有K线数据
        let klines = db.get_latest_klines("BTCUSDT", "1m", 10)?;
        assert!(!klines.is_empty(), "数据库中的K线数据为空");
        println!("数据库中有 {} 条K线数据", klines.len());
        
        // 第二步：启动K线聚合器
        println!("第二步：启动K线聚合器");
        
        // 创建K线聚合器
        let aggregator = KlineAggregator::new(db_clone1);
        
        // 启动K线聚合器
        let aggregator_clone = aggregator.clone();
        let aggregator_handle = tokio::spawn(async move {
            if let Err(e) = aggregator_clone.start().await {
                eprintln!("K线聚合器错误: {}", e);
            }
        });
        
        // 第三步：启动WebSocket客户端
        println!("第三步：启动WebSocket客户端");
        
        // 创建WebSocket配置
        let ws_config = ContinuousKlineConfig {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols: vec!["BTCUSDT".to_string()],
            intervals: vec!["1m".to_string()]
        };
        
        // 创建WebSocket客户端
        let mut ws_client = ContinuousKlineClient::new(ws_config, db_clone2);
        
        // 启动WebSocket客户端
        let ws_handle = tokio::spawn(async move {
            match ws_client.start().await {
                Ok(_) => println!("WebSocket客户端正常退出"),
                Err(e) => eprintln!("WebSocket客户端错误: {}", e),
            }
        });
        
        // 第四步：启动Web服务器
        println!("第四步：启动Web服务器");
        
        // 启动Web服务器
        let web_handle = tokio::spawn(async move {
            match start_web_server(db_clone3).await {
                Ok(_) => println!("Web服务器正常退出"),
                Err(e) => eprintln!("Web服务器错误: {}", e),
            }
        });
        
        // 等待服务器启动
        sleep(Duration::from_secs(2)).await;
        
        // 第五步：测试API
        println!("第五步：测试API");
        
        // 测试API
        let client = reqwest::Client::new();
        let response = client.get("http://localhost:3000/api/klines/BTCUSDT/1m")
            .send()
            .await
            .expect("请求失败");
        
        assert!(response.status().is_success(), "API请求失败");
        
        let json: Value = response.json().await.expect("解析JSON失败");
        
        assert_eq!(json["symbol"], "BTCUSDT", "交易对不正确");
        assert_eq!(json["interval"], "1m", "K线周期不正确");
        assert!(json["data"].as_array().unwrap().len() > 0, "K线数据为空");
        
        println!("API测试成功");
        
        // 等待一段时间，让WebSocket接收一些数据
        println!("等待WebSocket接收数据...");
        sleep(Duration::from_secs(30)).await;
        
        // 第六步：测试其他周期的K线是否已合成
        println!("第六步：测试其他周期的K线是否已合成");
        
        // 测试5分钟K线
        let response = client.get("http://localhost:3000/api/klines/BTCUSDT/5m")
            .send()
            .await
            .expect("请求失败");
        
        assert!(response.status().is_success(), "API请求失败");
        
        let json: Value = response.json().await.expect("解析JSON失败");
        
        assert_eq!(json["symbol"], "BTCUSDT", "交易对不正确");
        assert_eq!(json["interval"], "5m", "K线周期不正确");
        
        let data = json["data"].as_array().unwrap();
        println!("5分钟K线数据: {} 条", data.len());
        
        // 终止所有服务
        println!("终止所有服务");
        aggregator_handle.abort();
        ws_handle.abort();
        web_handle.abort();
        
        println!("集成测试完成");
        
        Ok(())
    }
}
