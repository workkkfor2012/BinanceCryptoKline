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
        // 🏷️ 检查新的 log_type 字段来决定解析方式
        if let Some(log_type) = json_value.get("log_type").and_then(|v| v.as_str()) {
            match log_type {
                "trace" => {
                    // trace 可视化日志：检查 fields.event_type
                    if let Some(trace_event) = parse_trace_visualization_json(&json_value) {
                        return Some(trace_event);
                    }
                }
                "module" => {
                    // 模块化日志：直接解析
                    return parse_module_log_json(&json_value);
                }
                _ => {
                    // 未知类型，尝试通用解析
                }
            }
        }

        // 兼容旧格式：首先检查是否是span事件格式
        if let Some(span_event) = parse_span_event_json(&json_value) {
            return Some(span_event);
        }

        // 然后尝试解析标准tracing格式
        return parse_tracing_json(&json_value);
    }

    // 不是有效的JSON格式，返回None
    None
}

/// 解析 trace 可视化日志（新格式）
fn parse_trace_visualization_json(json: &Value) -> Option<LogEntry> {
    // 检查是否有 fields.event_type
    let event_type = json.get("fields")
        .and_then(|f| f.get("event_type"))
        .and_then(|v| v.as_str())?;

    if !["span_start", "span_end", "log"].contains(&event_type) {
        return None;
    }

    // 必需字段
    let timestamp_str = json.get("timestamp")?.as_str()?;
    let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
        .ok()?
        .with_timezone(&Utc);

    let level = json.get("level")?.as_str()?.to_string();
    let target = json.get("target")?.as_str()?.to_string();
    let message = json.get("message")?.as_str()?.to_string();

    // 提取 fields 对象
    let mut fields = HashMap::new();
    if let Some(fields_obj) = json.get("fields").and_then(|f| f.as_object()) {
        for (key, value) in fields_obj {
            fields.insert(key.clone(), value.clone());
        }
    }

    // 构建span信息（如果是span事件）
    let span = if ["span_start", "span_end"].contains(&event_type) {
        let span_name = fields.get("span_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let span_id = fields.get("span_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let parent_id = fields.get("parent_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Some(SpanInfo {
            name: span_name,
            target: target.clone(),
            id: span_id,
            parent_id,
            hierarchy: None, // trace 日志不包含 hierarchy
        })
    } else {
        None
    };

    Some(LogEntry {
        timestamp,
        level,
        target,
        message,
        module_path: None,
        file: None,
        line: None,
        fields,
        span,
        log_type: Some("trace".to_string()),
    })
}

/// 解析模块化日志（新格式）
fn parse_module_log_json(json: &Value) -> Option<LogEntry> {
    // 必需字段
    let timestamp_str = json.get("timestamp")?.as_str()?;
    let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
        .ok()?
        .with_timezone(&Utc);
    let level = json.get("level")?.as_str()?.to_string();
    let target = json.get("target")?.as_str()?.to_string();
    let message = json.get("message")?.as_str()?.to_string();

    // 提取 fields
    let mut fields = HashMap::new();
    if let Some(fields_obj) = json.get("fields").and_then(|f| f.as_object()) {
        for (key, value) in fields_obj {
            fields.insert(key.clone(), value.clone());
        }
    }

    // 提取 span 信息（包含 hierarchy）
    let span = if let Some(span_obj) = json.get("span") {
        // 提取 hierarchy 数组
        let hierarchy = span_obj.get("hierarchy")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<String>>()
            });

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
            hierarchy,
        })
    } else {
        None
    };

    Some(LogEntry {
        timestamp,
        level,
        target,
        message,
        module_path: None,
        file: None,
        line: None,
        fields,
        span,
        log_type: Some("module".to_string()),
    })
}

/// 解析span事件JSON格式（兼容旧格式）
fn parse_span_event_json(json: &Value) -> Option<LogEntry> {
    // 检查是否是span事件格式
    let event_type = json.get("type")?.as_str()?;
    if !["span_start", "span_end", "log"].contains(&event_type) {
        return None;
    }

    // 必需字段
    let timestamp_str = json.get("timestamp")?.as_str()?;
    let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
        .ok()?
        .with_timezone(&Utc);

    let trace_id = json.get("trace_id")?.as_str()?.to_string();
    let span_id = json.get("span_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown_span")
        .to_string();

    // 根据事件类型构建不同的消息
    let (level, target, message) = match event_type {
        "span_start" => {
            let name = json.get("name")?.as_str()?.to_string();
            let target = json.get("target")?.as_str()?.to_string();
            ("INFO".to_string(), target, format!("Span started: {}", name))
        }
        "span_end" => {
            let duration_ms = json.get("duration_ms")?.as_f64().unwrap_or(0.0);
            ("INFO".to_string(), "span".to_string(), format!("Span ended: {:.2}ms", duration_ms))
        }
        "log" => {
            let level = json.get("level")?.as_str()?.to_string();
            let message = json.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("(empty message)")
                .to_string();
            (level, "log".to_string(), message)
        }
        _ => return None,
    };

    // 构建结构化字段
    let mut fields = HashMap::new();
    fields.insert("event_type".to_string(), serde_json::Value::String(event_type.to_string()));
    fields.insert("trace_id".to_string(), serde_json::Value::String(trace_id.clone()));
    fields.insert("span_id".to_string(), serde_json::Value::String(span_id.clone()));

    if let Some(parent_id) = json.get("parent_id").and_then(|v| v.as_str()) {
        fields.insert("parent_id".to_string(), serde_json::Value::String(parent_id.to_string()));
    }

    if let Some(name) = json.get("name").and_then(|v| v.as_str()) {
        fields.insert("span_name".to_string(), serde_json::Value::String(name.to_string()));
    }

    if let Some(duration_ms) = json.get("duration_ms").and_then(|v| v.as_f64()) {
        fields.insert("duration_ms".to_string(), serde_json::Value::Number(
            serde_json::Number::from_f64(duration_ms).unwrap_or_else(|| serde_json::Number::from(0))
        ));
    }

    // 构建span信息
    let span = Some(SpanInfo {
        name: json.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string(),
        target: json.get("target")
            .and_then(|v| v.as_str())
            .unwrap_or(&target)
            .to_string(),
        id: Some(span_id.clone()),
        parent_id: json.get("parent_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        hierarchy: None, // 兼容旧格式，不包含 hierarchy
    });

    Some(LogEntry {
        timestamp,
        level,
        target,
        message,
        module_path: None,
        file: None,
        line: None,
        fields,
        span,
        log_type: Some("trace".to_string()), // 兼容旧格式，标记为 trace
    })
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

    // 首先从嵌套的 fields 对象中提取字段（自定义格式）
    if let Some(fields_obj) = json.get("fields").and_then(|f| f.as_object()) {
        for (key, value) in fields_obj {
            fields.insert(key.clone(), value.clone());
        }
    }

    // 然后从顶层JSON对象中提取字段（标准tracing格式）
    // 排除已知的系统字段
    let system_fields = ["timestamp", "level", "target", "message", "module_path", "file", "line", "span"];
    if let Some(json_obj) = json.as_object() {
        for (key, value) in json_obj {
            if !system_fields.contains(&key.as_str()) && !fields.contains_key(key) {
                fields.insert(key.clone(), value.clone());
            }
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
            hierarchy: None, // 兼容旧格式，不包含 hierarchy
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
        log_type: None, // 兼容旧格式，不指定类型
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
    // 检查必需字段 - 对于span事件，message可以为空
    !entry.level.is_empty() && !entry.target.is_empty()
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
            log_type: None,
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
            log_type: None,
        };
        assert!(!validate_log_entry(&invalid_entry));
    }
}
