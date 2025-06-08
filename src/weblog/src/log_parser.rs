//! 日志解析器模块
//!
//! 负责解析基于Rust tracing规范的JSON格式日志条目，包括：
//! - JSON格式的tracing日志解析
//! - Span事件解析
//! - 结构化字段提取

use crate::types::{LogEntry, SpanInfo};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;

/// 解析JSON格式的tracing日志行
pub fn parse_tracing_log_line(line: &str) -> Option<LogEntry> {
    // 只解析JSON格式的tracing日志
    if let Ok(json_value) = serde_json::from_str::<Value>(line) {
        return parse_tracing_json(&json_value);
    }

    // 不是有效的JSON格式，返回None
    None
}

/// 解析JSON格式的tracing日志（符合WebLog规范）
fn parse_tracing_json(json: &Value) -> Option<LogEntry> {
    // 必需字段
    let timestamp_str = json.get("timestamp")?.as_str()?;
    let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
        .ok()?
        .with_timezone(&Utc);
    let level = json.get("level")?.as_str()?.to_string();
    let target = json.get("target")?.as_str()?.to_string();
    let message = json.get("message")?.as_str()?.to_string();

    // 可选的结构化字段
    let mut fields = HashMap::new();
    if let Some(fields_obj) = json.get("fields").and_then(|f| f.as_object()) {
        for (key, value) in fields_obj {
            fields.insert(key.clone(), value.clone());
        }
    }

    // 可选字段
    let module_path = json.get("module_path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let file = json.get("file")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let line = json.get("line")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);

    // 可选的span信息
    let span = if let Some(span_obj) = json.get("span") {
        Some(SpanInfo {
            name: span_obj.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            target: span_obj.get("target")
                .and_then(|v| v.as_str())
                .unwrap_or(&target)
                .to_string(),
            id: span_obj.get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            parent_id: span_obj.get("parent_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    } else {
        None
    };

    Some(LogEntry {
        timestamp,
        level,
        target,
        message,
        module_path,
        file,
        line,
        fields,
        span,
    })
}





/// 解析Span事件
pub fn parse_span_event(entry: &LogEntry) -> Option<SpanEvent> {
    // 检查是否为span开始事件
    if entry.message.contains("new") || entry.fields.contains_key("span.kind") {
        Some(SpanEvent::Start)
    }
    // 检查是否为span结束事件
    else if entry.message.contains("close") || entry.fields.contains_key("elapsed_milliseconds") {
        Some(SpanEvent::Close)
    }
    // 其他span内事件
    else if entry.span.is_some() {
        Some(SpanEvent::Event)
    } else {
        None
    }
}

/// Span事件类型
#[derive(Debug, Clone, PartialEq)]
pub enum SpanEvent {
    Start,
    Close,
    Event,
}

/// 验证日志条目的完整性
pub fn validate_log_entry(entry: &LogEntry) -> bool {
    // 检查必需字段
    !entry.level.is_empty()
        && !entry.target.is_empty()
        && !entry.message.is_empty()
}

/// 标准化日志级别
pub fn normalize_log_level(level: &str) -> String {
    match level.to_uppercase().as_str() {
        "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR" => level.to_uppercase(),
        _ => "INFO".to_string(),
    }
}

/// 检测是否为有效的JSON格式tracing日志
pub fn is_valid_json_log(line: &str) -> bool {
    // 检查是否为JSON格式的tracing日志
    if line.trim_start().starts_with('{') && line.trim_end().ends_with('}') {
        if let Ok(json) = serde_json::from_str::<Value>(line) {
            return json.get("timestamp").is_some()
                && json.get("level").is_some()
                && json.get("target").is_some();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tracing_json() {
        let json_log = r#"{"timestamp":"2024-01-01T12:00:00.000Z","level":"INFO","target":"test","message":"test message","fields":{"key":"value"}}"#;
        let entry = parse_tracing_log_line(json_log).unwrap();
        assert_eq!(entry.level, "INFO");
        assert_eq!(entry.target, "test");
        assert_eq!(entry.message, "test message");
    }

    #[test]
    fn test_parse_non_json() {
        let text_log = "[2024-01-01T12:00:00.000Z INFO test] test message";
        let entry = parse_tracing_log_line(text_log);
        assert!(entry.is_none());
    }

    #[test]
    fn test_is_valid_json_log() {
        assert!(is_valid_json_log(r#"{"timestamp":"2024-01-01T12:00:00.000Z","level":"INFO","target":"test"}"#));
        assert!(!is_valid_json_log("[2024-01-01 INFO test] message"));
        assert!(!is_valid_json_log("regular log message"));
        assert!(!is_valid_json_log(r#"{"level":"INFO"}"#)); // 缺少timestamp和target
    }

    #[test]
    fn test_validate_log_entry() {
        let valid_entry = LogEntry {
            timestamp: Utc::now(),
            level: "INFO".to_string(),
            target: "test".to_string(),
            message: "test message".to_string(),
            module_path: None,
            file: None,
            line: None,
            fields: HashMap::new(),
            span: None,
        };
        assert!(validate_log_entry(&valid_entry));

        let invalid_entry = LogEntry {
            timestamp: Utc::now(),
            level: "".to_string(),
            target: "test".to_string(),
            message: "test message".to_string(),
            module_path: None,
            file: None,
            line: None,
            fields: HashMap::new(),
            span: None,
        };
        assert!(!validate_log_entry(&invalid_entry));
    }
}
