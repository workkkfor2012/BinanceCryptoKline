#[cfg(test)]
mod tests {
    use kline_server::klcommon::{Database, Result};
    use kline_server::kldata::downloader::{Config, Downloader};
    use std::path::PathBuf;
    use std::fs;
    use std::sync::Arc;

    // 创建测试配置
    fn create_test_config() -> Config {
        // 创建临时数据目录
        let data_dir = "./target/test_data";
        fs::create_dir_all(data_dir).unwrap();
        
        // 创建配置
        Config::new(
            Some(data_dir.to_string()),
            Some(1),                              // 并发数
            Some(vec!["1m".to_string()]),         // 只下载1分钟K线
            None,                                 // 起始时间（自动计算）
            None,                                 // 结束时间（当前时间）
            Some(vec!["BTCUSDT".to_string()]),    // 只下载BTCUSDT
            Some(true),                           // 使用SQLite文件存储
            Some(100),                            // 每个交易对最多保存100条K线
            Some(false),                          // 不使用更新模式
        )
    }

    // 创建测试数据库
    fn setup_test_db() -> Arc<Database> {
        // 创建临时数据库文件
        let db_path = PathBuf::from("./target/test_downloader.db");
        
        // 如果文件已存在，则删除
        if db_path.exists() {
            fs::remove_file(&db_path).unwrap();
        }
        
        // 创建数据库连接
        let db = Database::new(&db_path).unwrap();
        Arc::new(db)
    }

    #[tokio::test]
    async fn test_downloader_config() -> Result<()> {
        // 创建测试配置
        let config = create_test_config();
        
        // 验证配置
        assert_eq!(config.concurrency, 1, "并发数不正确");
        assert_eq!(config.intervals, vec!["1m"], "K线周期不正确");
        assert_eq!(config.symbols, Some(vec!["BTCUSDT".to_string()]), "交易对不正确");
        assert_eq!(config.use_sqlite, true, "SQLite存储设置不正确");
        assert_eq!(config.max_klines_per_symbol, 100, "每个交易对最大K线数不正确");
        
        Ok(())
    }

    #[tokio::test]
    #[ignore] // 忽略此测试，因为它需要网络连接
    async fn test_downloader_run() -> Result<()> {
        // 创建测试配置
        let config = create_test_config();
        
        // 创建下载器
        let downloader = Downloader::new(config)?;
        
        // 运行下载流程
        let results = downloader.run().await?;
        
        // 验证结果
        assert!(!results.is_empty(), "下载结果为空");
        
        // 验证第一个结果
        let first_result = &results[0];
        assert_eq!(first_result.symbol, "BTCUSDT", "交易对不正确");
        assert_eq!(first_result.interval, "1m", "K线周期不正确");
        assert!(!first_result.klines.is_empty(), "K线数据为空");
        
        Ok(())
    }

    #[tokio::test]
    #[ignore] // 忽略此测试，因为它需要网络连接
    async fn test_downloader_with_db() -> Result<()> {
        // 创建测试数据库
        let db = setup_test_db();
        
        // 创建测试配置
        let mut config = create_test_config();
        config.db_path = Some(PathBuf::from("./target/test_downloader.db"));
        
        // 创建下载器
        let downloader = Downloader::new(config)?;
        
        // 运行下载流程
        downloader.run().await?;
        
        // 验证数据库中的K线数据
        let klines = db.get_latest_klines("BTCUSDT", "1m", 100)?;
        assert!(!klines.is_empty(), "数据库中的K线数据为空");
        
        Ok(())
    }
}
