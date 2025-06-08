//! 日志生成器示例
//! 
//! 生成符合tracing规范的示例日志，用于测试WebLog系统

use chrono::Utc;
use serde_json::json;
use std::thread;
use std::time::Duration;
use rand::Rng;

fn main() {
    println!("🔄 启动日志生成器...");
    
    let targets = vec![
        "app::server",
        "app::database", 
        "app::auth",
        "app::api",
        "app::worker",
    ];
    
    let levels = vec!["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];
    let mut rng = rand::thread_rng();
    let mut trace_id_counter = 1;
    
    loop {
        let target = targets[rng.gen_range(0..targets.len())];
        let level = levels[rng.gen_range(0..levels.len())];
        let timestamp = Utc::now().to_rfc3339();
        
        // 生成不同类型的日志
        match rng.gen_range(0..4) {
            0 => {
                // 普通日志
                let log = json!({
                    "timestamp": timestamp,
                    "level": level,
                    "target": target,
                    "message": generate_message(target, level),
                    "fields": {
                        "user_id": rng.gen_range(1..1000),
                        "request_id": format!("req_{}", rng.gen_range(1000..9999))
                    }
                });
                println!("{}", log);
            }
            1 => {
                // Span开始
                let span_id = format!("span_{}", rng.gen_range(1000..9999));
                let trace_id = format!("trace_{}", trace_id_counter);
                trace_id_counter += 1;
                
                let log = json!({
                    "timestamp": timestamp,
                    "level": "INFO",
                    "target": target,
                    "message": "new",
                    "span": {
                        "id": span_id,
                        "trace_id": trace_id
                    },
                    "fields": {
                        "name": generate_span_name(target),
                        "span.kind": "internal"
                    }
                });
                println!("{}", log);
                
                // 模拟一些span内的事件
                thread::sleep(Duration::from_millis(rng.gen_range(10..100)));
                
                let event_log = json!({
                    "timestamp": Utc::now().to_rfc3339(),
                    "level": "DEBUG",
                    "target": target,
                    "message": "processing request",
                    "span": {
                        "id": span_id,
                        "trace_id": trace_id
                    },
                    "fields": {
                        "step": "validation"
                    }
                });
                println!("{}", event_log);
                
                // Span结束
                thread::sleep(Duration::from_millis(rng.gen_range(10..200)));
                
                let close_log = json!({
                    "timestamp": Utc::now().to_rfc3339(),
                    "level": "INFO",
                    "target": target,
                    "message": "close",
                    "span": {
                        "id": span_id,
                        "trace_id": trace_id
                    },
                    "fields": {
                        "elapsed_milliseconds": rng.gen_range(10..500)
                    }
                });
                println!("{}", close_log);
            }
            2 => {
                // 错误日志
                let log = json!({
                    "timestamp": timestamp,
                    "level": "ERROR",
                    "target": target,
                    "message": generate_error_message(),
                    "fields": {
                        "error": "connection_timeout",
                        "retry_count": rng.gen_range(1..5)
                    }
                });
                println!("{}", log);
            }
            _ => {
                // 性能指标
                let log = json!({
                    "timestamp": timestamp,
                    "level": "INFO",
                    "target": "metrics",
                    "message": "performance_metric",
                    "fields": {
                        "metric_name": "response_time",
                        "value": rng.gen_range(10..1000),
                        "unit": "ms",
                        "endpoint": format!("/api/{}", target.split("::").last().unwrap_or("unknown"))
                    }
                });
                println!("{}", log);
            }
        }
        
        // 随机延迟
        thread::sleep(Duration::from_millis(rng.gen_range(100..2000)));
    }
}

fn generate_message(target: &str, level: &str) -> String {
    let messages = match (target, level) {
        (t, "INFO") if t.contains("server") => vec![
            "Server started successfully",
            "Handling incoming request",
            "Request processed successfully",
            "Health check passed",
        ],
        (t, "INFO") if t.contains("database") => vec![
            "Database connection established",
            "Query executed successfully",
            "Transaction committed",
            "Connection pool status: healthy",
        ],
        (t, "WARN") if t.contains("auth") => vec![
            "Invalid authentication attempt",
            "Token expiring soon",
            "Rate limit approaching",
            "Suspicious login pattern detected",
        ],
        (t, "ERROR") if t.contains("api") => vec![
            "API request failed",
            "Validation error",
            "External service unavailable",
            "Request timeout",
        ],
        _ => vec![
            "Operation completed",
            "Processing data",
            "System status update",
            "Background task finished",
        ],
    };
    
    let mut rng = rand::thread_rng();
    messages[rng.gen_range(0..messages.len())].to_string()
}

fn generate_span_name(target: &str) -> String {
    let names = match target {
        t if t.contains("server") => vec!["handle_request", "process_response", "middleware"],
        t if t.contains("database") => vec!["execute_query", "transaction", "connection"],
        t if t.contains("auth") => vec!["authenticate", "authorize", "validate_token"],
        t if t.contains("api") => vec!["api_call", "validate_input", "serialize_response"],
        _ => vec!["operation", "task", "process"],
    };
    
    let mut rng = rand::thread_rng();
    names[rng.gen_range(0..names.len())].to_string()
}

fn generate_error_message() -> String {
    let errors = vec![
        "Connection timeout",
        "Database query failed",
        "Authentication failed",
        "Invalid input data",
        "External service error",
        "Memory allocation failed",
        "File not found",
        "Permission denied",
    ];
    
    let mut rng = rand::thread_rng();
    errors[rng.gen_range(0..errors.len())].to_string()
}
