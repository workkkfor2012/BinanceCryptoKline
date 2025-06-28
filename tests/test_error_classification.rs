//! 测试错误分类系统
//! 
//! 验证AppError的get_error_type_summary()方法是否正确工作

use kline_server::klcommon::error::AppError;

#[test]
fn test_api_error_classification() {
    let error = AppError::ApiError("连接超时".to_string());
    assert_eq!(error.get_error_type_summary(), "api_request_failed");
    assert!(error.is_retryable());
}

#[test]
fn test_database_error_classification() {
    let error = AppError::DatabaseError("插入失败".to_string());
    assert_eq!(error.get_error_type_summary(), "database_operation_failed");
    assert!(!error.is_retryable()); // 一般数据库错误不重试
    
    // 测试可重试的数据库错误
    let locked_error = AppError::DatabaseError("database is locked".to_string());
    assert!(locked_error.is_retryable());
    
    let busy_error = AppError::DatabaseError("database is busy".to_string());
    assert!(busy_error.is_retryable());
}

#[test]
fn test_json_error_classification() {
    // 创建一个简单的JSON解析错误
    let json_str = "{invalid_json";
    let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let error = AppError::JsonError(json_error);
    assert_eq!(error.get_error_type_summary(), "json_parsing_failed");
    assert!(!error.is_retryable()); // 解析错误不应重试
}

#[test]
fn test_http_error_classification() {
    // 创建一个简单的HTTP错误 - 使用ApiError代替复杂的reqwest::Error
    let error = AppError::ApiError("HTTP请求超时".to_string());
    assert_eq!(error.get_error_type_summary(), "api_request_failed");
    assert!(error.is_retryable()); // API错误通常可重试
}

#[test]
fn test_websocket_error_classification() {
    let error = AppError::WebSocketError("连接断开".to_string());
    assert_eq!(error.get_error_type_summary(), "websocket_connection_failed");
    assert!(error.is_retryable());
}

#[test]
fn test_config_error_classification() {
    let error = AppError::ConfigError("配置文件格式错误".to_string());
    assert_eq!(error.get_error_type_summary(), "configuration_error");
    assert!(!error.is_retryable()); // 配置错误不应重试
}

#[test]
fn test_all_error_types_have_classification() {
    // 确保所有错误类型都有对应的分类
    let test_cases = vec![
        (AppError::ApiError("test".to_string()), "api_request_failed"),
        (AppError::DatabaseError("test".to_string()), "database_operation_failed"),
        (AppError::JsonError(serde_json::from_str::<serde_json::Value>("{invalid").unwrap_err()), "json_parsing_failed"),
        (AppError::ConfigError("test".to_string()), "configuration_error"),
        (AppError::DataError("test".to_string()), "data_validation_failed"),
        (AppError::WebSocketError("test".to_string()), "websocket_connection_failed"),
        (AppError::WebSocketProtocolError("test".to_string()), "websocket_protocol_failed"),
        (AppError::ParseError("test".to_string()), "data_parsing_failed"),
        (AppError::ChannelError("test".to_string()), "channel_communication_failed"),
        (AppError::AggregationError("test".to_string()), "aggregation_logic_failed"),
        (AppError::ActorError("test".to_string()), "actor_system_failed"),
        (AppError::WebServerError("test".to_string()), "web_server_failed"),
        (AppError::Unknown("test".to_string()), "unknown_error"),
    ];
    
    for (error, expected_summary) in test_cases {
        assert_eq!(error.get_error_type_summary(), expected_summary);
    }
}
