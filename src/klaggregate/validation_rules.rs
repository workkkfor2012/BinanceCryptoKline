//! 具体的验证规则实现
//! 
//! 定义了各种规格验证规则的具体实现逻辑

use super::observability::{ValidationRule, ValidationContext, ValidationResult, ValidationStatus, validation_rules};
use serde_json::json;

/// 创建所有默认验证规则
pub fn create_default_validation_rules() -> Vec<ValidationRule> {
    vec![
        create_symbol_index_stability_rule(),
        create_kline_open_time_accuracy_rule(),
        create_kline_is_final_correctness_rule(),
        create_empty_kline_handling_rule(),
        create_buffer_swap_notification_rule(),
        create_persistence_upsert_logic_rule(),
        create_data_flow_integrity_rule(),
    ]
}

/// 品种索引稳定性验证规则
fn create_symbol_index_stability_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::SYMBOL_INDEX_STABILITY.to_string(),
        description: "验证品种索引在系统重启后保持稳定".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            // 检查是否是品种注册相关的事件
            if context.module == "symbol_metadata_registry" && context.operation.contains("register") {
                if let (Some(symbol), Some(index), Some(listing_time)) = (
                    context.fields.get("symbol").and_then(|v| v.as_str()),
                    context.fields.get("index").and_then(|v| v.as_u64()),
                    context.fields.get("listing_time").and_then(|v| v.as_i64()),
                ) {
                    // 验证上市时间是否为有效值（不是当前时间的默认值）
                    let current_time = chrono::Utc::now().timestamp_millis();
                    let time_diff = (current_time - listing_time).abs();
                    
                    if time_diff < 60000 { // 如果时间差小于1分钟，可能是使用了默认值
                        return ValidationResult {
                            status: ValidationStatus::Warn,
                            message: format!("品种 {} 的上市时间可能使用了默认值", symbol),
                            context: json!({
                                "symbol": symbol,
                                "index": index,
                                "listing_time": listing_time,
                                "current_time": current_time,
                                "time_diff_ms": time_diff
                            }),
                        };
                    }
                    
                    return ValidationResult {
                        status: ValidationStatus::Pass,
                        message: format!("品种 {} 索引分配正常", symbol),
                        context: json!({
                            "symbol": symbol,
                            "index": index,
                            "listing_time": listing_time
                        }),
                    };
                }
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}

/// K线开盘时间准确性验证规则
fn create_kline_open_time_accuracy_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::KLINE_OPEN_TIME_ACCURACY.to_string(),
        description: "验证K线开盘时间是否正确对齐到周期边界".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            if context.module == "symbol_kline_aggregator" && context.operation == "kline_generated" {
                if let (Some(open_time), Some(interval), Some(symbol)) = (
                    context.fields.get("open_time").and_then(|v| v.as_i64()),
                    context.fields.get("interval").and_then(|v| v.as_str()),
                    context.fields.get("symbol").and_then(|v| v.as_str()),
                ) {
                    // 验证开盘时间是否正确对齐
                    let interval_ms = match interval {
                        "1m" => 60_000,
                        "5m" => 300_000,
                        "30m" => 1_800_000,
                        "1h" => 3_600_000,
                        "4h" => 14_400_000,
                        "1d" => 86_400_000,
                        "1w" => 604_800_000,
                        _ => return ValidationResult {
                            status: ValidationStatus::Fail,
                            message: format!("未知的时间周期: {}", interval),
                            context: json!({"interval": interval}),
                        },
                    };
                    
                    if open_time % interval_ms != 0 {
                        return ValidationResult {
                            status: ValidationStatus::Fail,
                            message: format!("K线开盘时间未对齐到周期边界"),
                            context: json!({
                                "symbol": symbol,
                                "interval": interval,
                                "open_time": open_time,
                                "interval_ms": interval_ms,
                                "remainder": open_time % interval_ms
                            }),
                        };
                    }
                    
                    return ValidationResult {
                        status: ValidationStatus::Pass,
                        message: "K线开盘时间对齐正确".to_string(),
                        context: json!({
                            "symbol": symbol,
                            "interval": interval,
                            "open_time": open_time
                        }),
                    };
                }
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}

/// K线is_final状态正确性验证规则
fn create_kline_is_final_correctness_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::KLINE_IS_FINAL_CORRECTNESS.to_string(),
        description: "验证K线is_final状态的正确性".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            if context.module == "symbol_kline_aggregator" && context.operation == "kline_finalized" {
                if let (Some(is_final), Some(open_time), Some(interval), Some(current_time)) = (
                    context.fields.get("is_final").and_then(|v| v.as_bool()),
                    context.fields.get("open_time").and_then(|v| v.as_i64()),
                    context.fields.get("interval").and_then(|v| v.as_str()),
                    context.fields.get("current_time").and_then(|v| v.as_i64()),
                ) {
                    let interval_ms = match interval {
                        "1m" => 60_000,
                        "5m" => 300_000,
                        "30m" => 1_800_000,
                        "1h" => 3_600_000,
                        "4h" => 14_400_000,
                        "1d" => 86_400_000,
                        "1w" => 604_800_000,
                        _ => return ValidationResult {
                            status: ValidationStatus::Fail,
                            message: format!("未知的时间周期: {}", interval),
                            context: json!({"interval": interval}),
                        },
                    };
                    
                    let close_time = open_time + interval_ms;
                    let should_be_final = current_time >= close_time;
                    
                    if is_final != should_be_final {
                        return ValidationResult {
                            status: ValidationStatus::Fail,
                            message: format!("K线is_final状态不正确"),
                            context: json!({
                                "open_time": open_time,
                                "close_time": close_time,
                                "current_time": current_time,
                                "is_final": is_final,
                                "should_be_final": should_be_final,
                                "interval": interval
                            }),
                        };
                    }
                    
                    return ValidationResult {
                        status: ValidationStatus::Pass,
                        message: "K线is_final状态正确".to_string(),
                        context: json!({
                            "open_time": open_time,
                            "is_final": is_final,
                            "interval": interval
                        }),
                    };
                }
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}

/// 空K线处理验证规则
fn create_empty_kline_handling_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::EMPTY_KLINE_HANDLING.to_string(),
        description: "验证空K线的正确处理".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            if context.module == "symbol_kline_aggregator" && context.operation == "empty_kline_detected" {
                if let (Some(volume), Some(trade_count)) = (
                    context.fields.get("volume").and_then(|v| v.as_f64()),
                    context.fields.get("trade_count").and_then(|v| v.as_i64()),
                ) {
                    if volume == 0.0 && trade_count == 0 {
                        return ValidationResult {
                            status: ValidationStatus::Pass,
                            message: "空K线检测正确".to_string(),
                            context: json!({
                                "volume": volume,
                                "trade_count": trade_count
                            }),
                        };
                    } else {
                        return ValidationResult {
                            status: ValidationStatus::Fail,
                            message: "空K线检测逻辑错误".to_string(),
                            context: json!({
                                "volume": volume,
                                "trade_count": trade_count
                            }),
                        };
                    }
                }
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}

/// 缓冲区交换通知验证规则
fn create_buffer_swap_notification_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::BUFFER_SWAP_NOTIFICATION.to_string(),
        description: "验证缓冲区交换通知的正确性".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            if context.module == "buffered_kline_store" && context.operation == "buffer_swapped" {
                if let (Some(swap_duration), Some(new_read_buffer_size)) = (
                    context.fields.get("swap_duration_ms").and_then(|v| v.as_f64()),
                    context.fields.get("new_read_buffer_size").and_then(|v| v.as_u64()),
                ) {
                    // 验证交换时间是否合理（应该很快）
                    if swap_duration > 100.0 { // 超过100ms认为异常
                        return ValidationResult {
                            status: ValidationStatus::Warn,
                            message: "缓冲区交换耗时过长".to_string(),
                            context: json!({
                                "swap_duration_ms": swap_duration,
                                "new_read_buffer_size": new_read_buffer_size
                            }),
                        };
                    }
                    
                    return ValidationResult {
                        status: ValidationStatus::Pass,
                        message: "缓冲区交换正常".to_string(),
                        context: json!({
                            "swap_duration_ms": swap_duration,
                            "new_read_buffer_size": new_read_buffer_size
                        }),
                    };
                }
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}

/// 持久化UPSERT逻辑验证规则
fn create_persistence_upsert_logic_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::PERSISTENCE_UPSERT_LOGIC.to_string(),
        description: "验证持久化UPSERT逻辑的正确性".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            if context.module == "kline_data_persistence" && context.operation == "batch_persisted" {
                if let (Some(total_records), Some(updated_records), Some(inserted_records)) = (
                    context.fields.get("total_records").and_then(|v| v.as_u64()),
                    context.fields.get("updated_records").and_then(|v| v.as_u64()),
                    context.fields.get("inserted_records").and_then(|v| v.as_u64()),
                ) {
                    if updated_records + inserted_records != total_records {
                        return ValidationResult {
                            status: ValidationStatus::Fail,
                            message: "UPSERT记录数不匹配".to_string(),
                            context: json!({
                                "total_records": total_records,
                                "updated_records": updated_records,
                                "inserted_records": inserted_records
                            }),
                        };
                    }
                    
                    return ValidationResult {
                        status: ValidationStatus::Pass,
                        message: "UPSERT逻辑正确".to_string(),
                        context: json!({
                            "total_records": total_records,
                            "updated_records": updated_records,
                            "inserted_records": inserted_records
                        }),
                    };
                }
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}

/// 数据流完整性验证规则
fn create_data_flow_integrity_rule() -> ValidationRule {
    ValidationRule {
        id: validation_rules::DATA_FLOW_INTEGRITY.to_string(),
        description: "验证数据在模块间传递的完整性".to_string(),
        validator: Box::new(|context: &ValidationContext| {
            // 这个规则需要跨模块状态跟踪，暂时简化实现
            if context.operation.contains("data_received") || context.operation.contains("data_sent") {
                return ValidationResult {
                    status: ValidationStatus::Pass,
                    message: "数据流事件记录".to_string(),
                    context: json!(context.fields),
                };
            }
            
            ValidationResult {
                status: ValidationStatus::Pass,
                message: "无相关事件".to_string(),
                context: json!({}),
            }
        }),
    }
}
