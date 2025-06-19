# 🐕 Cerberus 超级断言系统 Phase 1 (MVP) 实现计划

## 📋 项目概述

**项目代号**: "Cerberus" (地狱三头犬)  
**核心目标**: 构建动态的、有状态的、能理解跨模块因果关系的运行时验证引擎  
**设计理念**: 从逻辑正确性 (Correctness)、数据一致性 (Consistency)、时序有效性 (Timeliness) 三个维度守护系统

## 🎯 Phase 1 范围定义

### 验证的广度 (Breadth) - 全覆盖断言
1. **数据有效性**: INGESTION_DATA_VALIDITY
2. **路由健康度**: ROUTING_SUCCESS_RATE  
3. **元数据稳定性**: SYMBOL_INDEX_STABILITY
4. **K线内部逻辑**: KLINE_OHLC_CONSISTENCY, KLINE_OPEN_TIME_ACCURACY
5. **双缓冲机制**: BUFFER_SWAP_INTEGRITY
6. **数据持久化**: PERSISTENCE_DATA_CONSISTENCY

### 验证的深度 (Depth) - 有状态验证
- 实现有状态的验证器，支持跨事件状态跟踪
- 线程安全的状态管理，为每个 symbol-interval 组合维护独立状态
- 状态生命周期管理和自动清理机制

### 验证的时序性 (Timeliness) - 性能监控
- 关键操作耗时断言 (如缓冲区交换 < 10ms)
- 端到端延迟监控 (数据摄取到K线更新 < 1ms)
- 周期性任务节拍验证

## 🏗️ 核心架构设计

### 主要组件
```
CerberusEngine (核心验证引擎)
├── ValidationRuleRegistry (验证规则注册表)
├── StatefulValidationManager (有状态验证管理器)  
├── ValidationEventCollector (验证事件收集器)
└── PerformanceMonitor (性能监控器)
```

### 设计原则
- **非侵入式集成**: 通过 `tracing` 事件钩子注入验证逻辑
- **性能优先**: 验证开销 < 1%，支持运行时开关
- **渐进式启用**: 支持按规则级别动态启用/禁用
- **状态隔离**: 每个 symbol-interval 组合独立状态管理

## 📁 文件结构规划

### 新增核心文件
```
src/klaggregate/
├── cerberus_engine.rs      # Cerberus 验证引擎核心实现
├── cerberus_rules.rs       # Phase 1 具体验证规则实现  
├── cerberus_types.rs       # Cerberus 专用数据类型
└── cerberus_config.rs      # 配置管理和运行时控制
```

### 现有文件集成点
```
src/klaggregate/
├── market_data_ingestor.rs     # 数据摄取验证点
├── trade_event_router.rs       # 路由健康度验证点
├── symbol_kline_aggregator.rs  # K线逻辑验证点
├── buffered_kline_store.rs     # 双缓冲验证点
├── kline_data_persistence.rs   # 持久化验证点
├── symbol_metadata_registry.rs # 元数据稳定性验证点
├── mod.rs                      # 系统集成点
└── observability.rs            # tracing 层集成点
```

## 🔧 详细实现计划

### 1. market_data_ingestor.rs 修改点

**验证点 1**: `process_websocket_message` 函数 (约第 120-150 行)
```rust
// 验证规则: INGESTION_DATA_VALIDITY
// 验证内容: 价格、数量、时间戳合理性检查
// 触发时机: WebSocket 消息解析成功后
```

**验证点 2**: `handle_agg_trade` 函数 (约第 180-200 行)  
```rust
// 验证规则: 数据完整性检查
// 验证内容: 数据字段完整性，记录数据流入事件
// 触发时机: 发送到路由器前
```

### 2. trade_event_router.rs 修改点

**验证点 1**: `route_trade_event` 函数 (约第 75-115 行)
```rust
// 验证规则: ROUTING_SUCCESS_RATE  
// 验证内容: 路由成功率统计和阈值检查
// 触发时机: 路由成功/失败后
```

**验证点 2**: `register_aggregator` 函数 (约第 45-65 行)
```rust
// 验证规则: 聚合器注册完整性
// 验证内容: 确保所有必要的聚合器都已注册
// 触发时机: 聚合器注册时
```

### 3. symbol_kline_aggregator.rs 修改点

**验证点 1**: `process_agg_trade` 函数 (约第 130-180 行)
```rust
// 验证规则: KLINE_OHLC_CONSISTENCY
// 验证内容: OHLC 逻辑关系检查 (open <= high, low <= close 等)
// 触发时机: K线数据更新前
```

**验证点 2**: `update_kline_for_period` 函数 (约第 220-280 行)
```rust
// 验证规则: KLINE_OPEN_TIME_ACCURACY
// 验证内容: 开盘时间周期边界对齐检查
// 触发时机: K线生成时
```

**验证点 3**: `finalize_kline` 函数 (约第 300-320 行)
```rust
// 验证规则: KLINE_MONOTONIC_INCREASE (有状态)
// 验证内容: 同一品种同一周期K线时间单调递增
// 触发时机: K线最终化时
```

### 4. buffered_kline_store.rs 修改点

**验证点 1**: `swap_buffers` 函数 (约第 140-180 行)
```rust
// 验证规则: BUFFER_SWAP_INTEGRITY
// 验证内容: 交换操作完整性和性能检查
// 触发时机: 缓冲区交换前后
```

**验证点 2**: `write_kline_data` 函数 (约第 188-214 行)
```rust
// 验证规则: BUFFER_WRITE_ONLY_TO_WRITE_BUFFER
// 验证内容: 确保只向写缓冲区写入数据
// 触发时机: 写入数据时
```

### 5. kline_data_persistence.rs 修改点

**验证点 1**: `persist_kline_batch` 函数 (约第 245-305 行)
```rust
// 验证规则: PERSISTENCE_DATA_CONSISTENCY
// 验证内容: total_records = updated_records + inserted_records
// 触发时机: 批量持久化完成后
```

**验证点 2**: `execute_persistence_task` 函数 (约第 187-229 行)
```rust
// 验证规则: 持久化性能监控
// 验证内容: 持久化延迟和吞吐量监控
// 触发时机: 持久化任务执行时
```

### 6. symbol_metadata_registry.rs 修改点

**验证点 1**: `initialize_symbol_info` 函数 (约第 96-150 行)
```rust
// 验证规则: SYMBOL_INDEX_STABILITY
// 验证内容: 索引分配稳定性和一致性检查
// 触发时机: 品种注册时
```

**验证点 2**: `batch_get_symbol_listing_times` 函数 (约第 154-194 行)
```rust
// 验证规则: 元数据完整性检查
// 验证内容: 上市时间数据合理性验证
// 触发时机: 上市时间查询时
```

## 🎛️ 验证规则优先级

### Critical (始终开启，性能开销 < 0.1%)
- INGESTION_DATA_VALIDITY
- KLINE_OHLC_CONSISTENCY  
- BUFFER_SWAP_INTEGRITY

### Standard (开发/测试环境，性能开销 < 0.5%)
- ROUTING_SUCCESS_RATE
- KLINE_OPEN_TIME_ACCURACY
- PERSISTENCE_DATA_CONSISTENCY

### Diagnostic (问题排查时，性能开销 < 1%)
- SYMBOL_INDEX_STABILITY
- 详细性能监控规则
- 跨模块状态关联验证

## 🚀 实施步骤

### 第一阶段：基础架构 (1-2天)
1. 创建 Cerberus 核心文件和基础架构
2. 实现 StatefulValidator trait 和状态管理
3. 集成到现有的 tracing 系统

### 第二阶段：Critical 规则 (2-3天)  
1. 实现 INGESTION_DATA_VALIDITY
2. 实现 KLINE_OHLC_CONSISTENCY
3. 实现 BUFFER_SWAP_INTEGRITY
4. 集成到关键路径并测试

### 第三阶段：Standard 规则 (2-3天)
1. 实现 ROUTING_SUCCESS_RATE
2. 实现 KLINE_OPEN_TIME_ACCURACY  
3. 实现 PERSISTENCE_DATA_CONSISTENCY
4. 添加性能监控和统计

### 第四阶段：完善和优化 (1-2天)
1. 实现配置管理和运行时控制
2. 添加详细的错误处理和恢复机制
3. 性能调优和压力测试
4. 文档完善

## ⚠️ 风险控制策略

### 性能影响控制
- 每个验证点增加延迟 < 10μs
- 内存使用增加 < 5%
- 支持运行时完全禁用
- 采样验证机制

### 错误隔离机制
- 验证失败不影响主业务逻辑
- 验证器异常自动降级
- 详细的错误日志和恢复机制
- 熔断器模式防止级联失败

### 状态管理安全
- 使用 `DashMap` 实现高并发状态访问
- 基于 TTL 的自动状态清理
- 内存使用监控和限制
- 状态持久化支持 (可选)

## 📊 成功指标

### 功能指标
- [ ] 7个核心验证规则全部实现
- [ ] 有状态验证器正常工作
- [ ] 验证事件正确收集和报告
- [ ] 配置管理功能完整

### 性能指标  
- [ ] 验证开销 < 1% (目标 < 0.5%)
- [ ] 内存增长 < 5%
- [ ] 验证延迟 < 10μs per check
- [ ] 状态管理无内存泄漏

### 质量指标
- [ ] 单元测试覆盖率 > 90%
- [ ] 集成测试通过
- [ ] 压力测试稳定运行 24h+
- [ ] 文档完整且准确

## 🔬 技术实现细节

### 有状态验证器设计
```rust
pub trait StatefulValidator: Send + Sync {
    type State: Send + Sync + Clone;

    fn validate(&self, context: &ValidationContext, state: &mut Self::State) -> ValidationResult;
    fn create_initial_state(&self) -> Self::State;
    fn should_cleanup_state(&self, state: &Self::State) -> bool;
    fn get_state_key(&self, context: &ValidationContext) -> String;
}
```

### 验证上下文结构
```rust
pub struct ValidationContext {
    pub module: String,
    pub operation: String,
    pub timestamp: i64,
    pub fields: HashMap<String, serde_json::Value>,
    pub span_data: Option<HashMap<String, String>>,
    pub trace_id: Option<String>,
}
```

### 性能优化策略
1. **采样验证**: 高频规则支持采样率配置 (默认 10%)
2. **异步验证**: 非关键路径验证异步执行
3. **状态缓存**: 使用 `DashMap<String, Arc<RwLock<T>>>` 实现高并发状态管理
4. **内存池**: 预分配验证上下文对象池，减少内存分配开销
5. **批量处理**: 验证事件批量处理，减少锁竞争

### 配置管理
```rust
pub struct CerberusConfig {
    pub enabled: bool,
    pub sampling_rate: f64,  // 0.0 - 1.0
    pub max_state_entries: usize,
    pub state_cleanup_interval_ms: u64,
    pub performance_threshold_ms: f64,
    pub rule_configs: HashMap<String, RuleConfig>,
}

pub struct RuleConfig {
    pub enabled: bool,
    pub level: ValidationLevel,
    pub sampling_rate: Option<f64>,
    pub custom_thresholds: HashMap<String, f64>,
}
```

## 📋 具体验证规则实现

### 1. INGESTION_DATA_VALIDITY
```rust
// 验证逻辑:
// - 价格 > 0 且 < 1000000
// - 数量 > 0 且 < 1000000000
// - 时间戳在合理范围内 (当前时间 ± 1小时)
// - 品种名称格式正确 (字母数字组合，以USDT结尾)
```

### 2. ROUTING_SUCCESS_RATE (有状态)
```rust
// 状态结构:
struct RoutingState {
    success_count: u64,
    total_count: u64,
    last_reset_time: i64,
    consecutive_failures: u32,
}

// 验证逻辑:
// - 成功率 > 95% (可配置)
// - 连续失败次数 < 10
// - 每小时重置统计
```

### 3. KLINE_OHLC_CONSISTENCY
```rust
// 验证逻辑:
// - low <= open <= high
// - low <= close <= high
// - volume >= 0
// - trade_count >= 0
// - turnover >= 0
// - 如果 volume > 0，则 trade_count > 0
```

### 4. KLINE_MONOTONIC_INCREASE (有状态)
```rust
// 状态结构:
struct KlineTimeState {
    last_open_time: i64,
    symbol: String,
    interval: String,
}

// 验证逻辑:
// - 同一品种同一周期的K线开盘时间严格递增
// - 时间间隔等于周期长度
```

## 🛠️ 集成实现方案

### tracing 层集成
```rust
impl<S> Layer<S> for CerberusValidationLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // 1. 提取验证上下文
        let validation_context = self.extract_context(event, &ctx);

        // 2. 匹配适用的验证规则
        let applicable_rules = self.rule_registry.get_applicable_rules(&validation_context);

        // 3. 执行验证 (异步，不阻塞主线程)
        self.execute_validations_async(validation_context, applicable_rules);
    }
}
```

### 状态管理实现
```rust
pub struct StatefulValidationManager {
    states: DashMap<String, Arc<RwLock<Box<dyn Any + Send + Sync>>>>,
    cleanup_scheduler: tokio::time::Interval,
    max_entries: usize,
}

impl StatefulValidationManager {
    pub async fn get_or_create_state<T>(&self, key: &str, creator: impl Fn() -> T) -> Arc<RwLock<T>>
    where T: Send + Sync + 'static {
        // 高效的状态获取或创建逻辑
        // 包含 LRU 淘汰和内存限制
    }
}
```

## 🧪 测试策略

### 单元测试
- 每个验证规则独立测试
- 状态管理器测试
- 性能基准测试

### 集成测试
- 端到端验证流程测试
- 多线程并发测试
- 内存泄漏测试

### 压力测试
- 高频数据下的性能测试
- 长时间运行稳定性测试
- 异常情况恢复测试

## 📈 监控和可观测性

### 验证指标
- 验证规则执行次数
- 验证失败率
- 验证执行延迟
- 状态管理器性能指标

### 告警规则
- 验证失败率 > 5%
- 验证延迟 > 100μs
- 状态内存使用 > 100MB
- 连续验证异常 > 10次

## 📅 详细实施时间表

### Day 1: 基础架构搭建
**目标**: 建立 Cerberus 核心框架
- [ ] 创建 `cerberus_types.rs` - 核心数据结构定义
- [ ] 创建 `cerberus_engine.rs` - 验证引擎骨架
- [ ] 实现 `StatefulValidator` trait
- [ ] 基础状态管理器实现
- [ ] 单元测试框架搭建

**交付物**: 可编译的基础框架，通过基础单元测试

### Day 2: Critical 规则实现 (第一批)
**目标**: 实现最关键的验证规则
- [ ] 实现 `INGESTION_DATA_VALIDITY` 规则
- [ ] 实现 `KLINE_OHLC_CONSISTENCY` 规则
- [ ] 集成到 `market_data_ingestor.rs`
- [ ] 集成到 `symbol_kline_aggregator.rs`
- [ ] 编写对应的单元测试

**交付物**: 2个 Critical 规则正常工作，验证事件正确生成

### Day 3: Critical 规则完成 + 集成
**目标**: 完成所有 Critical 规则并集成到系统
- [ ] 实现 `BUFFER_SWAP_INTEGRITY` 规则
- [ ] 集成到 `buffered_kline_store.rs`
- [ ] 完善 tracing 层集成
- [ ] 性能基准测试
- [ ] 集成测试编写

**交付物**: 所有 Critical 规则工作正常，性能开销 < 0.1%

### Day 4: Standard 规则实现
**目标**: 实现 Standard 级别验证规则
- [ ] 实现 `ROUTING_SUCCESS_RATE` (有状态)
- [ ] 实现 `KLINE_OPEN_TIME_ACCURACY`
- [ ] 集成到 `trade_event_router.rs`
- [ ] 状态管理器压力测试
- [ ] 内存泄漏检测

**交付物**: Standard 规则正常工作，状态管理稳定

### Day 5: 持久化验证 + 配置管理
**目标**: 完成持久化验证和配置系统
- [ ] 实现 `PERSISTENCE_DATA_CONSISTENCY`
- [ ] 集成到 `kline_data_persistence.rs`
- [ ] 实现配置管理系统
- [ ] 运行时开关功能
- [ ] 采样率配置

**交付物**: 持久化验证工作，支持运行时配置

### Day 6: 元数据验证 + 完善
**目标**: 完成元数据验证和系统完善
- [ ] 实现 `SYMBOL_INDEX_STABILITY`
- [ ] 集成到 `symbol_metadata_registry.rs`
- [ ] 性能监控和告警
- [ ] 错误处理完善
- [ ] 文档更新

**交付物**: 所有验证规则完成，系统稳定可用

### Day 7: 测试和优化
**目标**: 全面测试和性能优化
- [ ] 端到端集成测试
- [ ] 24小时稳定性测试
- [ ] 性能调优
- [ ] 代码审查
- [ ] 部署准备

**交付物**: 生产就绪的 Cerberus Phase 1 系统

## 🎯 里程碑检查点

### Milestone 1: 基础架构完成 (Day 1 结束)
**验收标准**:
- [ ] 所有核心类型定义完成
- [ ] StatefulValidator trait 可用
- [ ] 基础状态管理器工作正常
- [ ] 单元测试覆盖率 > 80%

### Milestone 2: Critical 规则完成 (Day 3 结束)
**验收标准**:
- [ ] 3个 Critical 规则全部实现
- [ ] 集成到关键路径无异常
- [ ] 性能开销 < 0.1%
- [ ] 验证事件正确收集

### Milestone 3: Standard 规则完成 (Day 5 结束)
**验收标准**:
- [ ] 所有 Standard 规则实现
- [ ] 有状态验证器稳定工作
- [ ] 配置管理功能完整
- [ ] 内存使用稳定

### Milestone 4: 系统完成 (Day 7 结束)
**验收标准**:
- [ ] 所有验证规则正常工作
- [ ] 24小时稳定性测试通过
- [ ] 性能指标达标
- [ ] 文档完整

## 🚨 风险缓解计划

### 技术风险
**风险**: 性能开销超标
**缓解**:
- 每日性能基准测试
- 采样率动态调整
- 异步验证机制

**风险**: 状态管理内存泄漏
**缓解**:
- TTL 自动清理机制
- 内存使用监控
- 压力测试验证

**风险**: 验证器异常影响主业务
**缓解**:
- 异常隔离机制
- 自动降级功能
- 熔断器保护

### 进度风险
**风险**: 实现复杂度超预期
**缓解**:
- 分阶段交付
- 每日进度检查
- 及时调整范围

## 📊 质量保证计划

### 代码质量
- [ ] 代码审查 (每个 PR)
- [ ] 静态分析工具检查
- [ ] 单元测试覆盖率 > 90%
- [ ] 集成测试覆盖主要场景

### 性能质量
- [ ] 每日性能基准测试
- [ ] 内存使用监控
- [ ] 延迟分布分析
- [ ] 长时间稳定性测试

### 文档质量
- [ ] API 文档完整
- [ ] 使用示例清晰
- [ ] 故障排查指南
- [ ] 配置参考文档

---

**项目启动**: 等待最终确认后立即开始 Day 1 的基础架构搭建工作。

**联系方式**: 如有任何问题或需要调整计划，请及时沟通。
