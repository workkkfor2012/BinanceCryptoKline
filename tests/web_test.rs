#[cfg(test)]
mod tests {
    use kline_server::klcommon::{Database, Kline, Result};
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
        let db_path = PathBuf::from("./target/test_web.db");
        
        // 如果文件已存在，则删除
        if db_path.exists() {
            fs::remove_file(&db_path).unwrap();
        }
        
        // 创建数据库连接
        let db = Database::new(&db_path).unwrap();
        
        // 插入一些测试数据
        let symbol = "BTCUSDT";
        let interval = "1m";
        
        // 创建一些测试K线
        for i in 0..10 {
            let open_time = 1672531200000 + i * 60 * 1000; // 2023-01-01 00:00:00 + i分钟
            let close_time = open_time + 59999;
            
            let kline = Kline {
                open_time,
                open: format!("{}", 10000.0 + i as f64 * 10.0),
                high: format!("{}", 10010.0 + i as f64 * 10.0),
                low: format!("{}", 9990.0 + i as f64 * 10.0),
                close: format!("{}", 10005.0 + i as f64 * 10.0),
                volume: format!("{}", 100.0 + i as f64 * 10.0),
                close_time,
                quote_asset_volume: format!("{}", 1000000.0 + i as f64 * 100000.0),
                number_of_trades: 1000 + i * 100,
                taker_buy_base_asset_volume: format!("{}", 50.0 + i as f64 * 5.0),
                taker_buy_quote_asset_volume: format!("{}", 500000.0 + i as f64 * 50000.0),
                ignore: "0".to_string(),
            };
            
            db.insert_kline(symbol, interval, &kline).unwrap();
        }
        
        Arc::new(db)
    }

    #[tokio::test]
    #[ignore] // 忽略此测试，因为它启动了Web服务器
    async fn test_web_server() -> Result<()> {
        // 创建测试数据库
        let db = setup_test_db();
        
        // 启动Web服务器
        let server_handle = tokio::spawn(async move {
            match start_web_server(db).await {
                Ok(_) => println!("Web服务器正常退出"),
                Err(e) => eprintln!("Web服务器错误: {}", e),
            }
        });
        
        // 等待服务器启动
        sleep(Duration::from_secs(2)).await;
        
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
        
        // 终止Web服务器
        server_handle.abort();
        
        Ok(())
    }
}
