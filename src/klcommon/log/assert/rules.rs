//! 运行时断言验证规则实现
//! 
//! 包含 Phase 1 的所有验证规则的具体实现

use super::types::{
    ValidationContext, ValidationResult, ValidationRule, ValidationPriority
};
use std::sync::Arc;
use serde_json::json;

/// 获取所有验证规则
pub fn get_all_rules() -> Vec<Arc<dyn ValidationRule>> {
    vec![
        // Critical 级别规则
        Arc::new(IngestionDataValidityRule),
        Arc::new(KlineOhlcConsistencyRule),
        Arc::new(BufferSwapIntegrityRule),
        
        // Standard 级别规则
        Arc::new(RoutingSuccessRateRule),
        Arc::new(KlineOpenTimeAccuracyRule),
        Arc::new(PersistenceDataConsistencyRule),
        
        // Diagnostic 级别规则
        Arc::new(SymbolIndexStabilityRule),
    ]
}

/// 1. INGESTION_DATA_VALIDITY (Critical)
/// 验证进入系统的每一笔交易数据的合理性
struct IngestionDataValidityRule;

impl ValidationRule for IngestionDataValidityRule {
    fn id(&self) -> &str {
        "INGESTION_DATA_VALIDITY"
    }
    
    fn description(&self) -> &str {
        "验证进入系统的交易数据的合理性，包括价格、数量、时间戳等"
    }
    
    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Critical
    }
    
    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("MarketDataIngestor") && 
        context.event_name == "trade_data_parsed"
    }
    
    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        // 提取交易数据字段
        let price = context.get_number_field("price").unwrap_or(0.0);
        let quantity = context.get_number_field("quantity").unwrap_or(0.0);
        let timestamp_ms = context.get_number_field("timestamp_ms").unwrap_or(0.0) as i64;
        let symbol = context.get_string_field("symbol").unwrap_or_default();
        
        // 价格合理性验证
        if price <= 0.0 || price > 1_000_000.0 {
            return ValidationResult::deviation(
                self.id(),
                "price_out_of_range",
                json!({
                    "received_price": price,
                    "valid_range": {"min": 0.0, "max": 1000000.0},
                    "symbol": symbol,
                    "timestamp": timestamp_ms
                })
            );
        }
        
        // 数量合理性验证
        if quantity <= 0.0 || quantity > 1_000_000_000.0 {
            return ValidationResult::deviation(
                self.id(),
                "quantity_out_of_range",
                json!({
                    "received_quantity": quantity,
                    "valid_range": {"min": 0.0, "max": 1000000000.0},
                    "symbol": symbol,
                    "timestamp": timestamp_ms
                })
            );
        }
        
        // 时间戳合理性验证
        let current_time = chrono::Utc::now().timestamp_millis();
        let time_diff = (current_time - timestamp_ms).abs();
        if time_diff > 300_000 { // 5分钟
            return ValidationResult::deviation(
                self.id(),
                "timestamp_deviation_excessive",
                json!({
                    "trade_timestamp": timestamp_ms,
                    "server_timestamp": current_time,
                    "diff_ms": time_diff,
                    "max_allowed_diff_ms": 300000,
                    "symbol": symbol
                })
            );
        }
        
        // 品种名称格式验证
        if !symbol.ends_with("USDT") || symbol.len() < 5 {
            return ValidationResult::deviation(
                self.id(),
                "invalid_symbol_format",
                json!({
                    "symbol": symbol,
                    "expected_suffix": "USDT",
                    "min_length": 5,
                    "actual_length": symbol.len()
                })
            );
        }
        
        ValidationResult::pass()
    }
}

/// 2. KLINE_OHLC_CONSISTENCY (Critical)
/// 验证K线数据的OHLC逻辑关系正确性
struct KlineOhlcConsistencyRule;

impl ValidationRule for KlineOhlcConsistencyRule {
    fn id(&self) -> &str {
        "KLINE_OHLC_CONSISTENCY"
    }
    
    fn description(&self) -> &str {
        "验证K线数据的OHLC逻辑关系，确保数据内部一致性"
    }
    
    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Critical
    }
    
    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("SymbolKlineAggregator") && 
        context.event_name == "kline_updated"
    }
    
    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        // 提取K线数据
        let open = context.get_number_field("open").unwrap_or(0.0);
        let high = context.get_number_field("high").unwrap_or(0.0);
        let low = context.get_number_field("low").unwrap_or(0.0);
        let close = context.get_number_field("close").unwrap_or(0.0);
        let volume = context.get_number_field("volume").unwrap_or(0.0);
        let trade_count = context.get_number_field("trade_count").unwrap_or(0.0) as i64;
        let turnover = context.get_number_field("turnover").unwrap_or(0.0);
        let taker_buy_volume = context.get_number_field("taker_buy_volume").unwrap_or(0.0);
        
        let symbol = context.get_string_field("symbol").unwrap_or_default();
        let interval = context.get_string_field("interval").unwrap_or_default();
        
        // OHLC 基本关系验证
        if low > open || low > close {
            return ValidationResult::deviation(
                self.id(),
                "low_price_violation",
                json!({
                    "kline_state": {"open": open, "high": high, "low": low, "close": close},
                    "violation": "low > open or low > close",
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }
        
        if high < open || high < close {
            return ValidationResult::deviation(
                self.id(),
                "high_price_violation",
                json!({
                    "kline_state": {"open": open, "high": high, "low": low, "close": close},
                    "violation": "high < open or high < close",
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }
        
        // 成交量和笔数一致性验证
        if volume > 0.0 && trade_count == 0 {
            return ValidationResult::deviation(
                self.id(),
                "volume_trade_count_inconsistency",
                json!({
                    "volume": volume,
                    "trade_count": trade_count,
                    "issue": "volume > 0 but trade_count = 0",
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }
        
        if volume == 0.0 && trade_count > 0 {
            return ValidationResult::deviation(
                self.id(),
                "volume_trade_count_inconsistency",
                json!({
                    "volume": volume,
                    "trade_count": trade_count,
                    "issue": "volume = 0 but trade_count > 0",
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }
        
        // 主动买入量不能超过总成交量
        if taker_buy_volume > volume {
            return ValidationResult::deviation(
                self.id(),
                "taker_buy_volume_excessive",
                json!({
                    "taker_buy_volume": taker_buy_volume,
                    "total_volume": volume,
                    "excess": taker_buy_volume - volume,
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }
        
        // 成交额合理性验证 (简单检查)
        if turnover < 0.0 {
            return ValidationResult::deviation(
                self.id(),
                "negative_turnover",
                json!({
                    "turnover": turnover,
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }
        
        ValidationResult::pass()
    }
}

/// 3. BUFFER_SWAP_INTEGRITY (Critical)
/// 验证双缓冲区交换操作的完整性和性能
struct BufferSwapIntegrityRule;

impl ValidationRule for BufferSwapIntegrityRule {
    fn id(&self) -> &str {
        "BUFFER_SWAP_INTEGRITY"
    }
    
    fn description(&self) -> &str {
        "验证双缓冲区交换操作的完整性和性能"
    }
    
    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Critical
    }
    
    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("BufferedKlineStore") && 
        context.event_name == "buffer_swapped"
    }
    
    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        let swap_duration_ms = context.get_number_field("swap_duration_ms").unwrap_or(0.0);
        let new_read_buffer_size = context.get_number_field("new_read_buffer_size").unwrap_or(0.0) as usize;
        let swap_count = context.get_number_field("swap_count").unwrap_or(0.0) as u64;
        
        // 交换性能验证
        if swap_duration_ms > 10.0 {
            return ValidationResult::deviation(
                self.id(),
                "swap_duration_excessive",
                json!({
                    "swap_duration_ms": swap_duration_ms,
                    "max_allowed_ms": 10.0,
                    "swap_count": swap_count,
                    "buffer_size": new_read_buffer_size
                })
            );
        }
        
        // 缓冲区大小合理性验证
        if new_read_buffer_size == 0 {
            return ValidationResult::deviation(
                self.id(),
                "empty_buffer_after_swap",
                json!({
                    "new_read_buffer_size": new_read_buffer_size,
                    "swap_count": swap_count,
                    "swap_duration_ms": swap_duration_ms
                })
            );
        }
        
        ValidationResult::pass()
    }
}

/// 4. ROUTING_SUCCESS_RATE (Standard, 有状态)
/// 监控交易事件路由的成功率
struct RoutingSuccessRateRule;

impl ValidationRule for RoutingSuccessRateRule {
    fn id(&self) -> &str {
        "ROUTING_SUCCESS_RATE"
    }

    fn description(&self) -> &str {
        "监控交易事件路由的成功率，确保系统健康"
    }

    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Standard
    }

    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("TradeEventRouter") &&
        (context.event_name == "route_success" || context.event_name == "route_failure")
    }

    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        // 这是一个有状态规则的简化实现
        // 在实际实现中，需要通过 StatefulValidationRule trait 来管理状态

        let is_success = context.event_name == "route_success";
        let symbol = context.get_string_field("symbol").unwrap_or_default();

        if !is_success {
            let error = context.get_string_field("error").unwrap_or_default();
            return ValidationResult::deviation(
                self.id(),
                "routing_failure",
                json!({
                    "symbol": symbol,
                    "error": error,
                    "timestamp": context.timestamp
                })
            );
        }

        ValidationResult::pass()
    }
}

/// 5. KLINE_OPEN_TIME_ACCURACY (Standard)
/// 验证K线开盘时间是否正确对齐到周期边界
struct KlineOpenTimeAccuracyRule;

impl ValidationRule for KlineOpenTimeAccuracyRule {
    fn id(&self) -> &str {
        "KLINE_OPEN_TIME_ACCURACY"
    }

    fn description(&self) -> &str {
        "验证K线开盘时间是否正确对齐到周期边界"
    }

    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Standard
    }

    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("SymbolKlineAggregator") &&
        context.event_name == "kline_generated"
    }

    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        let open_time = context.get_number_field("open_time").unwrap_or(0.0) as i64;
        let interval = context.get_string_field("interval").unwrap_or_default();
        let symbol = context.get_string_field("symbol").unwrap_or_default();
        let is_final = context.get_bool_field("is_final").unwrap_or(false);

        // 获取周期毫秒数
        let interval_ms = match interval.as_str() {
            "1m" => 60_000,
            "5m" => 300_000,
            "30m" => 1_800_000,
            "1h" => 3_600_000,
            "4h" => 14_400_000,
            "1d" => 86_400_000,
            "1w" => 604_800_000,
            _ => {
                return ValidationResult::deviation(
                    self.id(),
                    "unknown_interval",
                    json!({
                        "interval": interval,
                        "symbol": symbol,
                        "open_time": open_time
                    })
                );
            }
        };

        // 验证时间对齐
        if open_time % interval_ms != 0 {
            return ValidationResult::deviation(
                self.id(),
                "open_time_misalignment",
                json!({
                    "open_time": open_time,
                    "interval": interval,
                    "interval_ms": interval_ms,
                    "remainder": open_time % interval_ms,
                    "symbol": symbol,
                    "is_final": is_final
                })
            );
        }

        // 验证时间合理性
        let current_time = chrono::Utc::now().timestamp_millis();
        if open_time > current_time {
            return ValidationResult::deviation(
                self.id(),
                "future_open_time",
                json!({
                    "open_time": open_time,
                    "current_time": current_time,
                    "diff_ms": open_time - current_time,
                    "symbol": symbol,
                    "interval": interval
                })
            );
        }

        ValidationResult::pass()
    }
}

/// 6. PERSISTENCE_DATA_CONSISTENCY (Standard)
/// 验证数据持久化操作的一致性
struct PersistenceDataConsistencyRule;

impl ValidationRule for PersistenceDataConsistencyRule {
    fn id(&self) -> &str {
        "PERSISTENCE_DATA_CONSISTENCY"
    }

    fn description(&self) -> &str {
        "验证数据持久化操作的一致性"
    }

    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Standard
    }

    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("KlineDataPersistence") &&
        context.event_name == "batch_persisted"
    }

    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        let total_records = context.get_number_field("total_records").unwrap_or(0.0) as u64;
        let updated_records = context.get_number_field("updated_records").unwrap_or(0.0) as u64;
        let inserted_records = context.get_number_field("inserted_records").unwrap_or(0.0) as u64;
        let success_count = context.get_number_field("success_count").unwrap_or(0.0) as u64;
        let failed_count = context.get_number_field("failed_count").unwrap_or(0.0) as u64;

        // 验证UPSERT记录数一致性
        if updated_records + inserted_records != success_count {
            return ValidationResult::deviation(
                self.id(),
                "upsert_count_mismatch",
                json!({
                    "total_records": total_records,
                    "updated_records": updated_records,
                    "inserted_records": inserted_records,
                    "success_count": success_count,
                    "expected_success": updated_records + inserted_records
                })
            );
        }

        // 验证总记录数一致性
        if success_count + failed_count != total_records {
            return ValidationResult::deviation(
                self.id(),
                "total_count_mismatch",
                json!({
                    "total_records": total_records,
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "calculated_total": success_count + failed_count
                })
            );
        }

        // 验证批次大小合理性
        if total_records == 0 {
            return ValidationResult::deviation(
                self.id(),
                "empty_batch",
                json!({
                    "total_records": total_records,
                    "message": "持久化批次为空"
                })
            );
        }

        if total_records > 10000 {
            return ValidationResult::deviation(
                self.id(),
                "oversized_batch",
                json!({
                    "total_records": total_records,
                    "max_recommended": 10000,
                    "message": "批次大小过大，可能影响性能"
                })
            );
        }

        ValidationResult::pass()
    }
}

/// 7. SYMBOL_INDEX_STABILITY (Diagnostic)
/// 验证品种索引分配的稳定性和一致性
struct SymbolIndexStabilityRule;

impl ValidationRule for SymbolIndexStabilityRule {
    fn id(&self) -> &str {
        "SYMBOL_INDEX_STABILITY"
    }

    fn description(&self) -> &str {
        "验证品种索引分配的稳定性和一致性"
    }

    fn priority(&self) -> ValidationPriority {
        ValidationPriority::Diagnostic
    }

    fn is_applicable(&self, context: &ValidationContext) -> bool {
        context.target.contains("SymbolMetadataRegistry") &&
        context.event_name == "symbol_registered"
    }

    fn validate(&self, context: &ValidationContext) -> ValidationResult {
        let symbol = context.get_string_field("symbol").unwrap_or_default();
        let symbol_index = context.get_number_field("symbol_index").unwrap_or(0.0) as u32;
        let listing_time = context.get_number_field("listing_time").unwrap_or(0.0) as i64;

        // 验证索引合理性
        if symbol_index >= 10000 {
            return ValidationResult::deviation(
                self.id(),
                "symbol_index_too_large",
                json!({
                    "symbol": symbol,
                    "symbol_index": symbol_index,
                    "max_allowed": 10000
                })
            );
        }

        // 验证上市时间合理性
        let current_time = chrono::Utc::now().timestamp_millis();
        let time_diff = current_time - listing_time;

        if listing_time > current_time {
            return ValidationResult::deviation(
                self.id(),
                "future_listing_time",
                json!({
                    "symbol": symbol,
                    "listing_time": listing_time,
                    "current_time": current_time,
                    "diff_ms": time_diff
                })
            );
        }

        // 检查是否使用了默认值 (当前时间)
        if time_diff < 60_000 { // 1分钟内
            return ValidationResult::deviation(
                self.id(),
                "possible_default_listing_time",
                json!({
                    "symbol": symbol,
                    "listing_time": listing_time,
                    "current_time": current_time,
                    "diff_ms": time_diff,
                    "message": "上市时间可能使用了默认值"
                })
            );
        }

        ValidationResult::pass()
    }
}
