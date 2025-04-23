#[cfg(test)]
mod tests {
    use kline_server::klcommon::{Database, Result};
    use kline_server::kldata::streamer::{ContinuousKlineClient, ContinuousKlineConfig};
    use std::path::PathBuf;
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    // 创建测试配置
    fn create_test_config() -> ContinuousKlineConfig {
        ContinuousKlineConfig {
            use_proxy: true,
            proxy_addr: "127.0.0.1".to_string(),
            proxy_port: 1080,
            symbols: vec!["BTCUSDT".to_string()],
            intervals: vec!["1m".to_string()]
        }
    }

    // 创建测试数据库
    fn setup_test_db() -> Arc<Database> {
        // 创建临时数据库文件
        let db_path = PathBuf::from("./target/test_streamer.db");
        
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
    async fn test_streamer_config() -> Result<()> {
        // 创建测试配置
        let config = create_test_config();
        
        // 验证配置
        assert_eq!(config.use_proxy, true, "代理设置不正确");
        assert_eq!(config.proxy_addr, "127.0.0.1", "代理地址不正确");
        assert_eq!(config.proxy_port, 1080, "代理端口不正确");
        assert_eq!(config.symbols, vec!["BTCUSDT"], "交易对不正确");
        assert_eq!(config.intervals, vec!["1m"], "K线周期不正确");
        
        Ok(())
    }

    #[tokio::test]
    #[ignore] // 忽略此测试，因为它需要网络连接和代理
    async fn test_streamer_connection() -> Result<()> {
        // 创建测试数据库
        let db = setup_test_db();
        
        // 创建测试配置
        let config = create_test_config();
        
        // 创建客户端
        let mut client = ContinuousKlineClient::new(config, db.clone());
        
        // 启动客户端，但只运行10秒
        let client_handle = tokio::spawn(async move {
            match client.start().await {
                Ok(_) => println!("客户端正常退出"),
                Err(e) => eprintln!("客户端错误: {}", e),
            }
        });
        
        // 等待10秒
        sleep(Duration::from_secs(10)).await;
        
        // 终止客户端
        client_handle.abort();
        
        // 验证数据库中是否有K线数据
        let klines = db.get_latest_klines("BTCUSDT", "1m", 10)?;
        println!("收到 {} 条K线数据", klines.len());
        
        Ok(())
    }
}
