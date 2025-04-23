#[cfg(test)]
mod tests {
    use kline_server::klcommon::{Database, Kline};
    use kline_server::kldata::aggregator::KlineAggregator;
    use std::sync::Arc;
    use std::path::PathBuf;
    use std::fs;
    use chrono::{Utc, TimeZone};

    // 创建测试数据库
    fn setup_test_db() -> Arc<Database> {
        // 创建临时数据库文件
        let db_path = PathBuf::from("./target/test_db.db");
        
        // 如果文件已存在，则删除
        if db_path.exists() {
            fs::remove_file(&db_path).unwrap();
        }
        
        // 创建数据库连接
        let db = Database::new(&db_path).unwrap();
        Arc::new(db)
    }

    // 创建测试K线数据
    fn create_test_klines() -> Vec<Kline> {
        let mut klines = Vec::new();
        
        // 创建一系列1分钟K线，跨越5分钟
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap().timestamp_millis();
        
        for i in 0..5 {
            let open_time = base_time + i * 60 * 1000; // 每分钟一条K线
            let close_time = open_time + 59999; // 开始时间 + 59.999秒
            
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
            
            klines.push(kline);
        }
        
        klines
    }

    #[tokio::test]
    async fn test_kline_aggregation() {
        // 设置测试数据库
        let db = setup_test_db();
        
        // 创建K线聚合器
        let aggregator = KlineAggregator::new(db.clone());
        
        // 创建测试K线数据
        let klines = create_test_klines();
        
        // 将1分钟K线插入数据库
        let symbol = "BTCUSDT";
        let interval = "1m";
        
        for kline in &klines {
            db.insert_kline(symbol, interval, kline).unwrap();
        }
        
        // 处理最后一条K线进行聚合
        let result = aggregator.process_kline(symbol, &klines[4]).await;
        assert!(result.is_ok(), "处理K线进行聚合失败: {:?}", result.err());
        
        // 验证5分钟K线是否已生成
        let five_min_klines = db.get_latest_klines(symbol, "5m", 1).unwrap();
        assert!(!five_min_klines.is_empty(), "未生成5分钟K线");
        
        let five_min_kline = &five_min_klines[0];
        
        // 验证5分钟K线的数据是否正确
        // 开盘价应该是第一条1分钟K线的开盘价
        assert_eq!(five_min_kline.open, klines[0].open, "5分钟K线的开盘价不正确");
        
        // 收盘价应该是最后一条1分钟K线的收盘价
        assert_eq!(five_min_kline.close, klines[4].close, "5分钟K线的收盘价不正确");
        
        // 最高价应该是所有1分钟K线中的最高价
        let expected_high = klines.iter().map(|k| k.high.parse::<f64>().unwrap()).fold(f64::NEG_INFINITY, f64::max).to_string();
        assert_eq!(five_min_kline.high, expected_high, "5分钟K线的最高价不正确");
        
        // 最低价应该是所有1分钟K线中的最低价
        let expected_low = klines.iter().map(|k| k.low.parse::<f64>().unwrap()).fold(f64::INFINITY, f64::min).to_string();
        assert_eq!(five_min_kline.low, expected_low, "5分钟K线的最低价不正确");
        
        // 成交量应该是所有1分钟K线的成交量之和
        let expected_volume = klines.iter().map(|k| k.volume.parse::<f64>().unwrap()).sum::<f64>().to_string();
        assert_eq!(five_min_kline.volume, expected_volume, "5分钟K线的成交量不正确");
        
        // 开始时间应该是第一条1分钟K线的开始时间
        assert_eq!(five_min_kline.open_time, klines[0].open_time, "5分钟K线的开始时间不正确");
        
        // 结束时间应该是最后一条1分钟K线的结束时间
        assert_eq!(five_min_kline.close_time, klines[4].close_time, "5分钟K线的结束时间不正确");
    }
}
