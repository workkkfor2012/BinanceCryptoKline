# 📋 Cerberus 验证规则详细说明

## 🎯 验证规则分类

### Critical 级别 (始终开启)
- **INGESTION_DATA_VALIDITY** - 数据摄取有效性验证
- **KLINE_OHLC_CONSISTENCY** - K线OHLC逻辑一致性验证
- **BUFFER_SWAP_INTEGRITY** - 双缓冲交换完整性验证

### Standard 级别 (开发/测试环境)
- **ROUTING_SUCCESS_RATE** - 路由成功率验证
- **KLINE_OPEN_TIME_ACCURACY** - K线开盘时间准确性验证
- **PERSISTENCE_DATA_CONSISTENCY** - 数据持久化一致性验证

### Diagnostic 级别 (问题排查时)
- **SYMBOL_INDEX_STABILITY** - 品种索引稳定性验证

## 📊 详细规则说明

### 1. INGESTION_DATA_VALIDITY (Critical)

**目标**: 确保从WebSocket接收的原始交易数据的有效性和合理性

**触发位置**: `market_data_ingestor.rs` - `process_websocket_message` 函数

**验证逻辑**:
```rust
// 价格验证
if price <= 0.0 || price > 1_000_000.0 {
    return ValidationResult::fail("价格超出合理范围");
}

// 数量验证  
if quantity <= 0.0 || quantity > 1_000_000_000.0 {
    return ValidationResult::fail("数量超出合理范围");
}

// 时间戳验证
let current_time = chrono::Utc::now().timestamp_millis();
let time_diff = (current_time - timestamp_ms).abs();
if time_diff > 3_600_000 { // 1小时
    return ValidationResult::fail("时间戳偏差过大");
}

// 品种名称验证
if !symbol.ends_with("USDT") || symbol.len() < 5 {
    return ValidationResult::fail("品种名称格式不正确");
}
```

**性能要求**: < 5μs per validation
**采样率**: 100% (Critical 级别)

### 2. KLINE_OHLC_CONSISTENCY (Critical)

**目标**: 验证K线数据的OHLC逻辑关系正确性

**触发位置**: `symbol_kline_aggregator.rs` - `update_kline_for_period` 函数

**验证逻辑**:
```rust
// 基本OHLC关系
if kline.low > kline.open || kline.low > kline.close {
    return ValidationResult::fail("最低价不能高于开盘价或收盘价");
}

if kline.high < kline.open || kline.high < kline.close {
    return ValidationResult::fail("最高价不能低于开盘价或收盘价");
}

// 成交量和笔数一致性
if kline.volume > 0.0 && kline.trade_count == 0 {
    return ValidationResult::fail("有成交量但交易笔数为0");
}

if kline.volume == 0.0 && kline.trade_count > 0 {
    return ValidationResult::fail("无成交量但交易笔数大于0");
}

// 成交额合理性
if kline.turnover < 0.0 {
    return ValidationResult::fail("成交额不能为负数");
}

// 主动买入量不能超过总成交量
if kline.taker_buy_volume > kline.volume {
    return ValidationResult::fail("主动买入量超过总成交量");
}
```

**性能要求**: < 3μs per validation
**采样率**: 100% (Critical 级别)

### 3. BUFFER_SWAP_INTEGRITY (Critical)

**目标**: 确保双缓冲区交换操作的完整性和性能

**触发位置**: `buffered_kline_store.rs` - `swap_buffers` 函数

**验证逻辑**:
```rust
// 交换前状态记录
let pre_swap_state = BufferState {
    write_buffer_size: write_buffer.len(),
    read_buffer_size: read_buffer.len(),
    swap_start_time: Instant::now(),
};

// 执行交换...

// 交换后验证
let swap_duration = pre_swap_state.swap_start_time.elapsed();
if swap_duration.as_millis() > 10 {
    return ValidationResult::warn("缓冲区交换耗时过长");
}

// 验证缓冲区大小一致性
if write_buffer.len() != pre_swap_state.read_buffer_size {
    return ValidationResult::fail("缓冲区大小不一致");
}

// 验证数据完整性
let data_integrity_check = verify_buffer_data_integrity(&read_buffer);
if !data_integrity_check {
    return ValidationResult::fail("缓冲区数据完整性检查失败");
}
```

**性能要求**: < 100μs per validation
**采样率**: 100% (Critical 级别)

### 4. ROUTING_SUCCESS_RATE (Standard, 有状态)

**目标**: 监控交易事件路由的成功率，确保系统健康

**触发位置**: `trade_event_router.rs` - `route_trade_event` 函数

**状态结构**:
```rust
struct RoutingState {
    success_count: u64,
    total_count: u64,
    consecutive_failures: u32,
    last_reset_time: i64,
    failure_details: VecDeque<FailureRecord>,
}
```

**验证逻辑**:
```rust
// 更新统计
state.total_count += 1;
if routing_success {
    state.success_count += 1;
    state.consecutive_failures = 0;
} else {
    state.consecutive_failures += 1;
    state.failure_details.push_back(FailureRecord {
        timestamp: Utc::now().timestamp_millis(),
        symbol: context.symbol.clone(),
        error: context.error.clone(),
    });
}

// 计算成功率
let success_rate = state.success_count as f64 / state.total_count as f64;

// 验证阈值
if success_rate < 0.95 && state.total_count > 100 {
    return ValidationResult::fail(format!("路由成功率过低: {:.2}%", success_rate * 100.0));
}

// 连续失败检查
if state.consecutive_failures > 10 {
    return ValidationResult::fail("连续路由失败次数过多");
}

// 定期重置统计 (每小时)
let current_time = Utc::now().timestamp_millis();
if current_time - state.last_reset_time > 3_600_000 {
    state.success_count = 0;
    state.total_count = 0;
    state.last_reset_time = current_time;
    state.failure_details.clear();
}
```

**性能要求**: < 10μs per validation
**采样率**: 100%
**状态清理**: 24小时无活动后清理

### 5. KLINE_OPEN_TIME_ACCURACY (Standard)

**目标**: 验证K线开盘时间是否正确对齐到周期边界

**触发位置**: `symbol_kline_aggregator.rs` - K线生成事件

**验证逻辑**:
```rust
// 获取周期毫秒数
let interval_ms = match interval.as_str() {
    "1m" => 60_000,
    "5m" => 300_000,
    "30m" => 1_800_000,
    "1h" => 3_600_000,
    "4h" => 14_400_000,
    "1d" => 86_400_000,
    "1w" => 604_800_000,
    _ => return ValidationResult::fail("未知的时间周期"),
};

// 验证对齐
if kline.open_time % interval_ms != 0 {
    return ValidationResult::fail(format!(
        "K线开盘时间未对齐到周期边界: open_time={}, interval={}, remainder={}",
        kline.open_time,
        interval,
        kline.open_time % interval_ms
    ));
}

// 验证时间合理性
let current_time = Utc::now().timestamp_millis();
if kline.open_time > current_time {
    return ValidationResult::fail("K线开盘时间不能超过当前时间");
}

// 验证is_final状态
let close_time = kline.open_time + interval_ms;
let should_be_final = current_time >= close_time;
if kline.is_final != should_be_final {
    return ValidationResult::warn(format!(
        "K线is_final状态可能不正确: is_final={}, should_be={}",
        kline.is_final, should_be_final
    ));
}
```

**性能要求**: < 5μs per validation
**采样率**: 50%

### 6. PERSISTENCE_DATA_CONSISTENCY (Standard)

**目标**: 验证数据持久化操作的一致性

**触发位置**: `kline_data_persistence.rs` - `persist_kline_batch` 函数

**验证逻辑**:
```rust
// 验证UPSERT记录数一致性
if updated_records + inserted_records != total_records {
    return ValidationResult::fail(format!(
        "UPSERT记录数不匹配: total={}, updated={}, inserted={}",
        total_records, updated_records, inserted_records
    ));
}

// 验证批次大小合理性
if batch_size == 0 {
    return ValidationResult::fail("批次大小不能为0");
}

if batch_size > 10000 {
    return ValidationResult::warn("批次大小过大，可能影响性能");
}

// 验证持久化性能
let persistence_duration = context.duration_ms;
if persistence_duration > 5000.0 {
    return ValidationResult::warn(format!(
        "持久化耗时过长: {:.2}ms", persistence_duration
    ));
}

// 验证数据完整性
for kline_data in batch {
    if kline_data.symbol_index == 0 && !kline_data.is_empty() {
        return ValidationResult::fail("非空K线数据的品种索引不能为0");
    }
}
```

**性能要求**: < 50μs per validation
**采样率**: 100%

### 7. SYMBOL_INDEX_STABILITY (Diagnostic)

**目标**: 确保品种索引分配的稳定性和一致性

**触发位置**: `symbol_metadata_registry.rs` - `initialize_symbol_info` 函数

**验证逻辑**:
```rust
// 验证索引唯一性
let mut seen_indices = HashSet::new();
for (symbol, index) in symbol_mappings {
    if !seen_indices.insert(index) {
        return ValidationResult::fail(format!("重复的品种索引: {}", index));
    }
}

// 验证索引连续性
let mut indices: Vec<u32> = seen_indices.into_iter().collect();
indices.sort();
for (i, &index) in indices.iter().enumerate() {
    if index != i as u32 {
        return ValidationResult::warn(format!(
            "品种索引不连续: 期望={}, 实际={}", i, index
        ));
    }
}

// 验证上市时间合理性
for symbol_info in symbol_infos {
    let current_time = Utc::now().timestamp_millis();
    let time_diff = current_time - symbol_info.listing_time;
    
    if time_diff < 0 {
        return ValidationResult::fail(format!(
            "品种{}的上市时间不能超过当前时间", symbol_info.symbol
        ));
    }
    
    // 检查是否使用了默认值 (当前时间)
    if time_diff < 60_000 {
        return ValidationResult::warn(format!(
            "品种{}的上市时间可能使用了默认值", symbol_info.symbol
        ));
    }
}
```

**性能要求**: < 100μs per validation
**采样率**: 100%
**触发频率**: 仅在系统启动时

## 🔧 验证规则配置

### 配置示例
```toml
[cerberus.rules.INGESTION_DATA_VALIDITY]
enabled = true
level = "Critical"
sampling_rate = 1.0
thresholds.max_price = 1000000.0
thresholds.max_quantity = 1000000000.0
thresholds.max_time_diff_ms = 3600000

[cerberus.rules.ROUTING_SUCCESS_RATE]
enabled = true
level = "Standard"
sampling_rate = 1.0
thresholds.min_success_rate = 0.95
thresholds.max_consecutive_failures = 10
state_cleanup_hours = 24
```

### 动态配置更新
所有验证规则支持运行时配置更新，无需重启系统。配置变更通过配置管理器推送到各个验证规则实例。

---

**规则扩展**: Phase 2 将添加更多高级验证规则，包括跨模块因果链验证和机器学习驱动的异常检测。
