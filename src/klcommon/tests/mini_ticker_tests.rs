#[cfg(test)]
mod tests {
    use crate::klcommon::websocket::{MiniTickerData, MiniTickerConfig, MiniTickerMessageHandler, WebSocketConfig, MessageHandler};
    use tokio::sync::mpsc;
    use std::sync::Arc;

    #[test]
    fn test_mini_ticker_config_default() {
        let config = MiniTickerConfig::default();
        assert!(config.use_proxy);
        assert_eq!(config.proxy_addr, "127.0.0.1");
        assert_eq!(config.proxy_port, 1080);
    }

    #[test]
    fn test_mini_ticker_config_streams() {
        let config = MiniTickerConfig::default();
        let streams = config.get_streams();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0], "!miniTicker@arr");
    }

    #[test]
    fn test_mini_ticker_config_proxy_settings() {
        let config = MiniTickerConfig {
            use_proxy: false,
            proxy_addr: "192.168.1.1".to_string(),
            proxy_port: 8080,
        };
        
        let (use_proxy, addr, port) = config.get_proxy_settings();
        assert!(!use_proxy);
        assert_eq!(addr, "192.168.1.1");
        assert_eq!(port, 8080);
    }

    #[test]
    fn test_mini_ticker_data_deserialization() {
        let json_data = r#"
        {
            "e": "24hrMiniTicker",
            "E": 123456789,
            "s": "BNBUSDT",
            "c": "0.0025",
            "o": "0.0010",
            "h": "0.0025",
            "l": "0.0010",
            "v": "10000",
            "q": "18"
        }
        "#;

        let ticker: Result<MiniTickerData, _> = serde_json::from_str(json_data);
        assert!(ticker.is_ok());
        
        let ticker = ticker.unwrap();
        assert_eq!(ticker.event_type, "24hrMiniTicker");
        assert_eq!(ticker.event_time, 123456789);
        assert_eq!(ticker.symbol, "BNBUSDT");
        assert_eq!(ticker.close_price, "0.0025");
        assert_eq!(ticker.open_price, "0.0010");
        assert_eq!(ticker.high_price, "0.0025");
        assert_eq!(ticker.low_price, "0.0010");
        assert_eq!(ticker.total_traded_volume, "10000");
        assert_eq!(ticker.total_traded_quote_volume, "18");
    }

    #[test]
    fn test_mini_ticker_array_deserialization() {
        let json_array = r#"
        [
            {
                "e": "24hrMiniTicker",
                "E": 123456789,
                "s": "BNBUSDT",
                "c": "0.0025",
                "o": "0.0010",
                "h": "0.0025",
                "l": "0.0010",
                "v": "10000",
                "q": "18"
            },
            {
                "e": "24hrMiniTicker",
                "E": 123456790,
                "s": "BTCUSDT",
                "c": "50000.0",
                "o": "49000.0",
                "h": "51000.0",
                "l": "48000.0",
                "v": "100",
                "q": "5000000"
            }
        ]
        "#;

        let tickers: Result<Vec<MiniTickerData>, _> = serde_json::from_str(json_array);
        assert!(tickers.is_ok());
        
        let tickers = tickers.unwrap();
        assert_eq!(tickers.len(), 2);
        assert_eq!(tickers[0].symbol, "BNBUSDT");
        assert_eq!(tickers[1].symbol, "BTCUSDT");
    }

    #[tokio::test]
    async fn test_mini_ticker_message_handler() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let handler = Arc::new(MiniTickerMessageHandler::new(sender));

        let test_message = r#"
        [
            {
                "e": "24hrMiniTicker",
                "E": 123456789,
                "s": "BNBUSDT",
                "c": "0.0025",
                "o": "0.0010",
                "h": "0.0025",
                "l": "0.0010",
                "v": "10000",
                "q": "18"
            }
        ]
        "#;

        // 模拟处理消息
        let result = handler.handle_message(1, test_message.to_string()).await;
        assert!(result.is_ok());

        // 检查是否收到数据
        let received_data = receiver.try_recv();
        assert!(received_data.is_ok());
        
        let tickers = received_data.unwrap();
        assert_eq!(tickers.len(), 1);
        assert_eq!(tickers[0].symbol, "BNBUSDT");
    }

    #[tokio::test]
    async fn test_mini_ticker_message_handler_invalid_json() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let handler = Arc::new(MiniTickerMessageHandler::new(sender));

        let invalid_message = "invalid json";

        // 模拟处理无效消息
        let result = handler.handle_message(1, invalid_message.to_string()).await;
        assert!(result.is_ok()); // 应该不会失败，只是不会发送数据

        // 检查是否没有收到数据
        let received_data = receiver.try_recv();
        assert!(received_data.is_err()); // 应该没有数据
    }
}
