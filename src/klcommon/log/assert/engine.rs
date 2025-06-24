//! 运行时断言引擎核心实现
//! 
//! 包含 AssertEngine (后台验证引擎) 和 AssertLayer (tracing 集成层)

use super::types::{ValidationContext, ValidationResult, ValidationRule, PerfStats, AssertConfig};
use super::rules::get_all_rules;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn, trace, Event, Id, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

/// 获取验证规则的中文描述
fn get_rule_chinese_description(rule_id: &str) -> &'static str {
    match rule_id {
        "INGESTION_DATA_VALIDITY" => "数据摄取有效性验证(价格、数量、时间戳合理性)",
        "KLINE_OHLC_CONSISTENCY" => "K线OHLC逻辑一致性验证(开高低收关系)",
        "BUFFER_SWAP_INTEGRITY" => "双缓冲交换完整性验证(交换性能和数据一致性)",
        "ROUTING_SUCCESS_RATE" => "交易事件路由成功率监控(系统健康度)",
        "KLINE_OPEN_TIME_ACCURACY" => "K线开盘时间准确性验证(时间边界对齐)",
        "PERSISTENCE_DATA_CONSISTENCY" => "数据持久化一致性验证(事务完整性)",
        "SYMBOL_INDEX_STABILITY" => "品种索引稳定性验证(元数据完整性)",
        _ => "未知验证规则",
    }
}

/// 状态条目 - 包含状态数据和访问时间
#[derive(Clone)]
struct StateEntry {
    /// 状态数据 (使用 Any 类型擦除)
    state: Arc<tokio::sync::RwLock<Box<dyn std::any::Any + Send + Sync>>>,
    /// 最后访问时间
    last_access: Arc<std::sync::atomic::AtomicI64>,
}

/// 短时状态管理器 - 使用 TTL 自动清理
pub struct ShortTermStateManager {
    /// 状态存储
    states: DashMap<String, StateEntry>,
    /// TTL 配置 (秒)
    ttl_seconds: u64,
}

impl ShortTermStateManager {
    /// 创建新的状态管理器
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            states: DashMap::new(),
            ttl_seconds,
        }
    }

    /// 启动清理任务（需要在 Tokio runtime 中调用）
    pub fn start_cleanup_task(&self) {
        let states = self.states.clone();
        let ttl_seconds = self.ttl_seconds;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // 每分钟清理一次

            loop {
                interval.tick().await;

                let current_time = chrono::Utc::now().timestamp();
                let mut to_remove = Vec::new();

                // 收集过期的键
                for entry in states.iter() {
                    let last_access = entry.last_access.load(std::sync::atomic::Ordering::Relaxed);
                    if current_time - last_access > ttl_seconds as i64 {
                        to_remove.push(entry.key().clone());
                    }
                }

                // 移除过期状态
                for key in to_remove {
                    states.remove(&key);
                    debug!(target: "AssertStateManager", "清理过期状态: key={}", key);
                }
            }
        });
    }
    
    /// 获取或创建状态
    pub async fn get_or_create_state<T>(&self, key: &str, creator: impl FnOnce() -> T) -> Arc<tokio::sync::RwLock<T>>
    where
        T: Send + Sync + 'static,
    {
        let current_time = chrono::Utc::now().timestamp();
        
        // 尝试获取现有状态
        if let Some(entry) = self.states.get(key) {
            entry.last_access.store(current_time, std::sync::atomic::Ordering::Relaxed);

            // 尝试转换类型
            let state_any = entry.state.clone();
            let state_guard = state_any.read().await;
            if let Some(typed_state) = state_guard.downcast_ref::<Arc<tokio::sync::RwLock<T>>>() {
                return typed_state.clone();
            }
        }
        
        // 创建新状态
        let new_state = Arc::new(tokio::sync::RwLock::new(creator()));
        let state_any = Arc::new(tokio::sync::RwLock::new(
            Box::new(new_state.clone()) as Box<dyn std::any::Any + Send + Sync>
        ));
        
        let entry = StateEntry {
            state: state_any,
            last_access: Arc::new(std::sync::atomic::AtomicI64::new(current_time)),
        };
        
        self.states.insert(key.to_string(), entry);
        new_state
    }

    /// 获取当前状态数量
    pub fn state_count(&self) -> usize {
        self.states.len()
    }
}

/// 性能报告器
pub struct PerformanceReporter {
    /// 性能统计数据
    stats: DashMap<String, PerfStats>,
    /// Top N 配置
    top_n: usize,
}

impl PerformanceReporter {
    /// 创建新的性能报告器
    pub fn new(top_n: usize) -> Self {
        Self {
            stats: DashMap::new(),
            top_n,
        }
    }
    
    /// 记录 span 性能数据
    pub fn record_span(&self, span_name: &str, duration: Duration) {
        let duration_ns = duration.as_nanos() as u64;
        
        self.stats.entry(span_name.to_string())
            .or_insert_with(PerfStats::new)
            .add_measurement(duration_ns);
    }
    
    /// 生成 Top N 性能报告
    pub fn generate_report(&self) -> Vec<(String, PerfStats)> {
        let mut entries: Vec<_> = self.stats.iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        
        // 按平均耗时排序
        entries.sort_by(|a, b| b.1.avg_duration_ns().cmp(&a.1.avg_duration_ns()));
        
        // 取 Top N
        entries.into_iter().take(self.top_n).collect()
    }
}

/// 规则覆盖率跟踪器
pub struct RuleCoverageTracker {
    /// 规则最后触发时间
    rule_last_triggered: DashMap<String, i64>,
    /// 规则描述映射
    rule_descriptions: HashMap<String, String>,
    /// 启动时间
    start_time: i64,
}

impl RuleCoverageTracker {
    /// 创建新的覆盖率跟踪器
    pub fn new(rules: &[Arc<dyn ValidationRule>]) -> Self {
        let mut rule_descriptions = HashMap::new();
        for rule in rules {
            rule_descriptions.insert(
                rule.id().to_string(),
                get_rule_chinese_description(rule.id()).to_string(),
            );
        }

        Self {
            rule_last_triggered: DashMap::new(),
            rule_descriptions,
            start_time: chrono::Utc::now().timestamp(),
        }
    }

    /// 记录规则被触发
    pub fn record_rule_triggered(&self, rule_id: &str) {
        let current_time = chrono::Utc::now().timestamp();
        self.rule_last_triggered.insert(rule_id.to_string(), current_time);
    }

    /// 生成覆盖率报告
    pub fn generate_coverage_report(&self) -> String {
        let current_time = chrono::Utc::now().timestamp();
        let uptime_seconds = current_time - self.start_time;

        // 按优先级分组统计
        let mut critical_rules = Vec::new();
        let mut standard_rules = Vec::new();
        let mut diagnostic_rules = Vec::new();
        let mut total_triggered = 0;
        let total_rules = self.rule_descriptions.len();

        for (rule_id, description) in &self.rule_descriptions {
            let last_triggered = self.rule_last_triggered.get(rule_id)
                .map(|entry| *entry.value())
                .unwrap_or(0);

            let (status, is_triggered) = if last_triggered > 0 {
                total_triggered += 1;
                let seconds_ago = current_time - last_triggered;
                if seconds_ago <= 10 {
                    (format!("🟢 活跃({}秒前)", seconds_ago), true)
                } else {
                    (format!("🟡 {}秒前", seconds_ago), true)
                }
            } else {
                ("🔴 未触发".to_string(), false)
            };

            let rule_info = format!("  - {}: {} [{}]", rule_id, description, status);

            // 根据规则ID分类
            if rule_id.contains("INGESTION") || rule_id.contains("OHLC") || rule_id.contains("BUFFER") {
                critical_rules.push((rule_info, is_triggered));
            } else if rule_id.contains("ROUTING") || rule_id.contains("KLINE") || rule_id.contains("PERSISTENCE") {
                standard_rules.push((rule_info, is_triggered));
            } else {
                diagnostic_rules.push((rule_info, is_triggered));
            }
        }

        // 计算覆盖率
        let coverage_percentage = (total_triggered as f64 / total_rules as f64 * 100.0) as u32;
        let health_indicator = if coverage_percentage >= 80 {
            "🟢 健康"
        } else if coverage_percentage >= 50 {
            "🟡 一般"
        } else {
            "🔴 需要关注"
        };

        let mut report = format!(
            "📊 运行时断言规则覆盖率报告 (运行时间: {}秒)\n💡 覆盖率: {}/{} ({}%) - 系统状态: {}\n",
            uptime_seconds, total_triggered, total_rules, coverage_percentage, health_indicator
        );

        // Critical级别统计
        let critical_triggered = critical_rules.iter().filter(|(_, triggered)| *triggered).count();
        report.push_str(&format!("\n🔴 Critical级别规则 ({}/{}):\n", critical_triggered, critical_rules.len()));
        for (rule_info, _) in critical_rules {
            report.push_str(&format!("{}\n", rule_info));
        }

        // Standard级别统计
        let standard_triggered = standard_rules.iter().filter(|(_, triggered)| *triggered).count();
        report.push_str(&format!("\n🟡 Standard级别规则 ({}/{}):\n", standard_triggered, standard_rules.len()));
        for (rule_info, _) in standard_rules {
            report.push_str(&format!("{}\n", rule_info));
        }

        // Diagnostic级别统计
        let diagnostic_triggered = diagnostic_rules.iter().filter(|(_, triggered)| *triggered).count();
        report.push_str(&format!("\n🔵 Diagnostic级别规则 ({}/{}):\n", diagnostic_triggered, diagnostic_rules.len()));
        for (rule_info, _) in diagnostic_rules {
            report.push_str(&format!("{}\n", rule_info));
        }

        report
    }
}

/// 运行时断言验证引擎 - 在后台任务中运行
pub struct AssertEngine {
    /// 配置
    config: AssertConfig,
    /// 验证规则
    rules: Vec<Arc<dyn ValidationRule>>,
    /// 状态管理器
    state_manager: Arc<ShortTermStateManager>,
    /// 性能报告器
    performance_reporter: Arc<PerformanceReporter>,
    /// 验证任务接收器
    validation_receiver: Option<mpsc::UnboundedReceiver<ValidationContext>>,
    /// 规则覆盖率跟踪器
    coverage_tracker: Arc<RuleCoverageTracker>,
}

impl AssertEngine {
    /// 创建新的验证引擎
    pub fn new(config: AssertConfig) -> (Self, mpsc::UnboundedSender<ValidationContext>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let rules = get_all_rules();
        let coverage_tracker = Arc::new(RuleCoverageTracker::new(&rules));

        let engine = Self {
            state_manager: Arc::new(ShortTermStateManager::new(config.state_ttl_seconds)),
            performance_reporter: Arc::new(PerformanceReporter::new(config.performance_top_n)),
            rules,
            config,
            validation_receiver: Some(receiver),
            coverage_tracker,
        };

        (engine, sender)
    }

    /// 启动验证引擎
    pub async fn start(mut self) {
        let receiver = self.validation_receiver.take().expect("验证接收器已被取走");

        info!(target: "AssertEngine", "运行时断言验证引擎启动，规则数量: {}", self.rules.len());

        // 启动状态管理器的清理任务
        self.state_manager.start_cleanup_task();

        // 启动覆盖率报告任务
        self.start_coverage_report_task();

        // 启动验证任务处理
        self.process_validation_tasks(receiver).await;
    }

    /// 启动覆盖率报告任务
    fn start_coverage_report_task(&self) {
        let coverage_tracker = self.coverage_tracker.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10)); // 每10秒报告一次

            loop {
                interval.tick().await;

                let report = coverage_tracker.generate_coverage_report();
                info!(target: "AssertCoverage",
                    event_name = "coverage_report",
                    "\n{}", report
                );
            }
        });
    }

    /// 处理验证任务
    async fn process_validation_tasks(self, mut receiver: mpsc::UnboundedReceiver<ValidationContext>) {
        while let Some(context) = receiver.recv().await {
            self.process_single_validation(context).await;
        }

        info!(target: "AssertEngine", "运行时断言验证引擎停止");
    }

    /// 处理单个验证任务
    async fn process_single_validation(&self, context: ValidationContext) {
        let start_time = Instant::now();

        // 获取适用的规则
        let applicable_rules: Vec<_> = self.rules.iter()
            .filter(|rule| rule.is_applicable(&context))
            .collect();

        if applicable_rules.is_empty() {
            return;
        }

        // 提取关键业务字段用于日志
        let symbol = context.get_string_field("symbol").unwrap_or_default();
        let price = context.get_number_field("price").unwrap_or(0.0);
        let interval = context.get_string_field("interval").unwrap_or_default();

        // 只在有多个规则时显示开始日志，避免冗余
        if applicable_rules.len() > 1 {
            debug!(target: "AssertEngine",
                event_name = "validation_task_start",
                target = %context.target,
                event = %context.event_name,
                symbol = %symbol,
                price = price,
                interval = %interval,
                applicable_rules = applicable_rules.len(),
                "开始验证任务: target={}, event={}, symbol={}, 适用规则数={}",
                context.target, context.event_name, symbol, applicable_rules.len()
            );
        }

        // 执行验证
        let mut pass_count = 0;
        let mut deviation_count = 0;
        let mut skip_count = 0;

        for rule in applicable_rules {
            // 记录规则被触发
            self.coverage_tracker.record_rule_triggered(rule.id());

            // 获取规则的中文描述
            let rule_description = get_rule_chinese_description(rule.id());

            match rule.validate(&context) {
                ValidationResult::Pass => {
                    pass_count += 1;
                    trace!(target: "AssertEngine",
                        event_name = "validation_pass",
                        rule_id = %rule.id(),
                        rule_description = %rule_description,
                        symbol = %symbol,
                        price = price,
                        "✅ 验证通过: {} - {} (品种: {}, 价格: {})",
                        rule.id(), rule_description, symbol, price
                    );
                }
                ValidationResult::Deviation(deviation) => {
                    deviation_count += 1;
                    // 记录偏差事件 - 使用tracing结构化日志格式
                    warn!(
                        target: "AssertDeviation",
                        event_name = "validation_deviation",
                        event_type = "ASSERT_DEVIATION",
                        rule_id = %deviation.rule_id,
                        rule_description = %rule_description,
                        deviation_type = %deviation.deviation_type,
                        symbol = %symbol,
                        timestamp = deviation.timestamp,
                        timestamp_iso = %chrono::DateTime::from_timestamp_millis(deviation.timestamp)
                            .unwrap_or_default()
                            .format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                        evidence = %serde_json::to_string(&deviation.evidence).unwrap_or_default(),
                        "🚨 检测到验证偏差: {} - {} (品种: {}, 偏差类型: {})",
                        deviation.rule_id, rule_description, symbol, deviation.deviation_type
                    );
                }
                ValidationResult::Skip(reason) => {
                    skip_count += 1;
                    debug!(target: "AssertEngine",
                        event_name = "validation_skip",
                        rule_id = %rule.id(),
                        rule_description = %rule_description,
                        symbol = %symbol,
                        reason = %reason,
                        "⏭️ 验证跳过: {} - {} (品种: {}, 原因: {})",
                        rule.id(), rule_description, symbol, reason
                    );
                }
            }
        }

        // 记录验证任务完成统计
        if deviation_count > 0 || skip_count > 0 {
            info!(target: "AssertEngine",
                event_name = "validation_task_complete",
                target = %context.target,
                event = %context.event_name,
                symbol = %symbol,
                pass_count = pass_count,
                deviation_count = deviation_count,
                skip_count = skip_count,
                "验证任务完成: symbol={}, 通过={}, 偏差={}, 跳过={}",
                symbol, pass_count, deviation_count, skip_count
            );
        }

        let duration = start_time.elapsed();
        if duration.as_millis() > 10 {
            warn!(target: "AssertEngine", "验证任务耗时过长: {}ms", duration.as_millis());
        }
    }

    /// 获取性能报告
    pub fn get_performance_report(&self) -> Vec<(String, PerfStats)> {
        self.performance_reporter.generate_report()
    }

    /// 获取状态管理器统计
    pub fn get_state_stats(&self) -> usize {
        self.state_manager.state_count()
    }
}

/// 运行时断言 Tracing 层 - 集成到 tracing 系统
pub struct AssertLayer {
    /// 验证任务发送器
    validation_sender: mpsc::UnboundedSender<ValidationContext>,
    /// 性能报告器
    performance_reporter: Arc<PerformanceReporter>,
    /// Span 开始时间记录
    span_timings: DashMap<Id, Instant>,
}

impl AssertLayer {
    /// 创建新的断言层（不启动引擎）
    pub fn new(config: AssertConfig) -> (Self, AssertEngine) {
        let (engine, sender) = AssertEngine::new(config.clone());
        let performance_reporter = Arc::new(PerformanceReporter::new(config.performance_top_n));

        let layer = Self {
            validation_sender: sender,
            performance_reporter,
            span_timings: DashMap::new(),
        };

        (layer, engine)
    }
}

impl<S> Layer<S> for AssertLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // 提取事件的基本信息
        let metadata = event.metadata();
        let target = metadata.target().to_string();

        // 创建字段访问器来提取事件数据
        let mut field_visitor = FieldVisitor::new();
        event.record(&mut field_visitor);

        // 获取事件名称
        let event_name = field_visitor.fields.get("event_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown_event")
            .to_string();

        // 创建验证上下文
        let mut context = ValidationContext::new(target, event_name);
        context = context.with_fields(field_visitor.fields);

        // 获取当前 span 的信息
        if let Some(span) = ctx.lookup_current() {
            let mut span_data = HashMap::new();
            span_data.insert("span_name".to_string(), span.name().to_string());

            // 获取 trace_id (如果存在)
            if let Some(trace_id) = span.extensions().get::<String>() {
                context = context.with_trace_id(trace_id.clone());
            }

            context.span_data = Some(span_data);
        }

        // 发送验证任务（非阻塞）
        if let Err(_) = self.validation_sender.send(context) {
            // 验证引擎可能已停止，静默忽略
        }
    }
}

/// 字段访问器 - 用于提取事件字段
struct FieldVisitor {
    fields: HashMap<String, serde_json::Value>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }
}

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(format!("{:?}", value)),
        );
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Bool(value),
        );
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(number) = serde_json::Number::from_f64(value) {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::Number(number),
            );
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::String(format!("{}", value)),
            );
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }
}
