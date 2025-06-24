//! 轨迹提炼器模块
//!
//! 负责在内存中构建完整的调用树，并按需生成对大模型友好的文本摘要。
//! 这个模块与现有的程序员可视化系统并行工作，专注于为AI分析提供简洁的执行路径。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::{Id, span, event, Subscriber, Level};
use tracing_subscriber::{layer::Context, Layer};
use std::fmt;

// --- 1. 定义核心数据结构 ---

/// 表示调用树中的一个节点 (Node)
#[derive(Debug)]
pub struct DistilledTraceNode {
    pub name: String,
    pub fields: HashMap<String, String>,
    pub start_time: Instant,
    pub duration_ms: Option<f64>,
    pub self_time_ms: Option<f64>,
    pub children: Vec<Arc<RwLock<DistilledTraceNode>>>,
    pub has_error: bool,
    pub error_messages: Vec<String>,
    is_critical_path: bool, // 内部状态，用于渲染
}

impl DistilledTraceNode {
    fn new(name: String, fields: HashMap<String, String>) -> Self {
        Self {
            name,
            fields,
            start_time: Instant::now(),
            duration_ms: None,
            self_time_ms: None,
            children: Vec::new(),
            has_error: false,
            error_messages: Vec::new(),
            is_critical_path: false,
        }
    }
}

/// 字段访问器，用于从 event/span 中提取字段到 HashMap
struct FieldExtractor<'a> {
    fields: &'a mut HashMap<String, String>,
}

impl<'a> FieldExtractor<'a> {
    fn new(fields: &'a mut HashMap<String, String>) -> Self { 
        Self { fields } 
    }
}

impl<'a> tracing::field::Visit for FieldExtractor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.fields.insert(field.name().to_string(), format!("{:?}", value));
    }
    
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }
    
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }
    
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }
    
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }
    
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }
}

/// 全局的、线程安全的"轨迹仓库"(Store)
/// Key: trace_id (根Span的ID), Value: 根节点的TraceNode
#[derive(Clone, Default)]
pub struct TraceDistillerStore(Arc<RwLock<HashMap<u64, Arc<RwLock<DistilledTraceNode>>>>>);

impl TraceDistillerStore {
    /// 获取所有已完成的trace列表
    pub fn get_completed_traces(&self) -> Vec<(u64, String)> {
        let store = self.0.read().unwrap();
        store.iter()
            .filter_map(|(trace_id, node_arc)| {
                let node = node_arc.read().unwrap();
                if node.duration_ms.is_some() {
                    Some((*trace_id, node.name.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// 获取指定trace的根节点
    pub fn get_trace(&self, trace_id: u64) -> Option<Arc<RwLock<DistilledTraceNode>>> {
        let store = self.0.read().unwrap();
        store.get(&trace_id).cloned()
    }
    
    /// 清理旧的已完成trace，保持内存使用合理
    /// ✨ 关键修复：只清理已完成的 Trace，保护正在运行的主流程
    pub fn cleanup_old_traces(&self, max_traces: usize) {
        let mut store = self.0.write().unwrap();

        // ✨ 关键修复 1: 筛选出所有已完成的 Trace
        let mut completed_trace_ids: Vec<u64> = store.iter()
            .filter(|(_, node_arc)| {
                // 只有当 duration_ms 有值时，才算完成
                node_arc.read().unwrap().duration_ms.is_some()
            })
            .map(|(trace_id, _)| *trace_id)
            .collect();

        // ✨ 关键修复 2: 只在已完成的 Trace 数量超过上限时才清理
        if completed_trace_ids.len() > max_traces {
            // 按 ID 排序，ID 小的更旧
            completed_trace_ids.sort_unstable();

            let to_remove_count = completed_trace_ids.len() - max_traces;
            for &trace_id_to_remove in completed_trace_ids.iter().take(to_remove_count) {
                store.remove(&trace_id_to_remove);
            }

            tracing::debug!(
                target: "trace_distiller",
                "清理了 {} 个已完成的旧 Trace，保留 {} 个最新的已完成 Trace，当前总 Trace 数: {}",
                to_remove_count,
                max_traces,
                store.len()
            );
        }
    }
}

/// `Layer`，负责实时构建和更新内存中的调用树
#[derive(Clone)]
pub struct TraceDistillerLayer {
    store: TraceDistillerStore,
}

impl TraceDistillerLayer {
    pub fn new(store: TraceDistillerStore) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for TraceDistillerLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = if let Some(span) = ctx.span(id) { span } else { return };

        // 如果这个span已经有我们的节点了，说明是重复调用，直接返回
        if span.extensions().get::<Arc<RwLock<DistilledTraceNode>>>().is_some() {
            return;
        }

        let mut fields = HashMap::new();
        let mut visitor = FieldExtractor::new(&mut fields);
        attrs.record(&mut visitor);

        let node = Arc::new(RwLock::new(DistilledTraceNode::new(
            span.metadata().name().to_string(),
            fields,
        )));

        // ✨ 简化：只把节点存入当前span的extensions
        // 不在这里尝试建立父子关系，避免时序问题
        span.extensions_mut().insert(node);
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let span = if let Some(span) = ctx.span(id) { span } else { return };

        // ✨ 强化：在on_enter时建立父子关系
        // 此时所有父span都已经entered，它们的extensions中必然有对应的node
        match span.parent() {
            Some(parent_span) => {
                // 这是子span，需要链接到父节点
                let parent_node_arc = if let Some(p_node) = parent_span.extensions().get::<Arc<RwLock<DistilledTraceNode>>>() {
                    p_node.clone()
                } else {
                    // 父节点没有node，这通常不应该发生
                    tracing::warn!("父节点缺少DistilledTraceNode: {}", parent_span.metadata().name());
                    return;
                };

                let current_node_arc = if let Some(c_node) = span.extensions().get::<Arc<RwLock<DistilledTraceNode>>>() {
                    c_node.clone()
                } else {
                    tracing::warn!("当前节点缺少DistilledTraceNode: {}", span.metadata().name());
                    return;
                };

                // 检查是否已经链接过，避免重复添加
                let is_already_linked = parent_node_arc.read().unwrap().children.iter()
                    .any(|child| Arc::ptr_eq(child, &current_node_arc));

                if !is_already_linked {
                    parent_node_arc.write().unwrap().children.push(current_node_arc);
                }
            }
            None => {
                // 这是根span，将它添加到store中
                if let Some(node_arc) = span.extensions().get::<Arc<RwLock<DistilledTraceNode>>>() {
                    self.store.0.write().unwrap().entry(id.into_u64()).or_insert_with(|| node_arc.clone());
                }
            }
        }
    }

    fn on_event(&self, event: &event::Event<'_>, ctx: Context<'_, S>) {
        // 捕获错误和警告信息
        if *event.metadata().level() <= Level::WARN {
            if let Some(span) = ctx.lookup_current() {
                if let Some(node) = span.extensions().get::<Arc<RwLock<DistilledTraceNode>>>() {
                    let mut node_guard = node.write().unwrap();
                    
                    if *event.metadata().level() <= Level::ERROR {
                        node_guard.has_error = true;
                    }
                    
                    // 提取错误消息
                    let mut fields = HashMap::new();
                    let mut visitor = FieldExtractor::new(&mut fields);
                    event.record(&mut visitor);
                    
                    if let Some(message) = fields.get("message") {
                        node_guard.error_messages.push(message.clone());
                    }
                }
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = if let Some(span) = ctx.span(&id) { span } else { return };

        // 先获取node_arc的克隆，避免生命周期问题
        let node_arc = if let Some(node_arc) = span.extensions().get::<Arc<RwLock<DistilledTraceNode>>>() {
            node_arc.clone()
        } else {
            return;
        };

        // 检查是否为根节点（在使用span之前）
        let is_root = span.parent().is_none();

        // 计算耗时
        {
            let mut node = node_arc.write().unwrap();
            let duration = node.start_time.elapsed();
            node.duration_ms = Some(duration.as_secs_f64() * 1000.0);

            let children_duration: f64 = node.children.iter()
                .map(|child_arc| child_arc.read().unwrap().duration_ms.unwrap_or(0.0))
                .sum();
            node.self_time_ms = Some(node.duration_ms.unwrap() - children_duration);
        }

        // 如果是根节点关闭，说明整个Trace已完成，可以进行后处理
        if is_root {
            // 计算关键路径
            Self::calculate_critical_path(&node_arc);

            // 定期清理旧trace (测试期间设置为超大值，避免清理)
            self.store.cleanup_old_traces(999999);
        }
    }
}

impl TraceDistillerLayer {
    /// 递归计算并标记关键路径（最耗时的执行路径）
    fn calculate_critical_path(node_arc: &Arc<RwLock<DistilledTraceNode>>) {
        // 找到最耗时的子节点
        let longest_child = {
            let node = node_arc.read().unwrap();
            node.children.iter()
                .max_by(|a, b| {
                    let dur_a = a.read().unwrap().duration_ms.unwrap_or(0.0);
                    let dur_b = b.read().unwrap().duration_ms.unwrap_or(0.0);
                    dur_a.partial_cmp(&dur_b).unwrap_or(std::cmp::Ordering::Equal)
                })
                .cloned()
        };

        if let Some(longest_child) = longest_child {
            // 标记最耗时的子节点为关键路径
            longest_child.write().unwrap().is_critical_path = true;
            Self::calculate_critical_path(&longest_child);
        }
    }
}

// --- 3. 为大模型生成文本摘要的生成器 ---

/// 【新增】将Store中所有已完成的Trace提炼成一份统一的文本报告
pub fn distill_all_completed_traces_to_text(store: &TraceDistillerStore) -> String {
    let mut report = String::new();
    use std::fmt::Write;

    let store_lock = store.0.read().unwrap();

    // 筛选出所有已完成的Trace
    let completed_traces: Vec<_> = store_lock.iter()
        .filter(|(_, node_arc)| node_arc.read().unwrap().duration_ms.is_some())
        .collect();

    if completed_traces.is_empty() {
        return "No completed traces found in this snapshot.".to_string();
    }

    writeln!(report, "========== Trace Snapshot Report ==========").unwrap();
    writeln!(report, "Timestamp: {}", chrono::Utc::now().to_rfc3339()).unwrap();
    writeln!(report, "Completed Traces Found: {}", completed_traces.len()).unwrap();
    writeln!(report, "========================================\n").unwrap();

    for (trace_id, root_node_arc) in completed_traces {
        // 复用我们已有的单个Trace提炼函数
        let single_summary = distill_trace_to_text(*trace_id, root_node_arc);
        writeln!(report, "{}", single_summary).unwrap();
        writeln!(report, "\n------------------ End of Trace {:#x} ------------------\n", trace_id).unwrap();
    }

    report
}

/// 主函数，生成一个Trace的完整文本摘要
pub fn distill_trace_to_text(trace_id: u64, root_node_arc: &Arc<RwLock<DistilledTraceNode>>) -> String {
    let mut summary = String::new();
    let root_node = root_node_arc.read().unwrap();

    // 写入头部信息
    use std::fmt::Write;
    writeln!(summary, "=== 函数执行路径分析报告 ===").unwrap();
    writeln!(summary, "Trace ID: {:#x}", trace_id).unwrap();
    writeln!(summary, "根函数: {}", root_node.name).unwrap();
    
    if let Some(duration) = root_node.duration_ms {
        writeln!(summary, "总耗时: {:.2}ms", duration).unwrap();
    }
    
    let status = if root_node.has_error { "❌ 执行失败" } else { "✅ 执行成功" };
    writeln!(summary, "执行状态: {}", status).unwrap();
    writeln!(summary, "").unwrap();
    
    writeln!(summary, "=== 调用树结构 ===").unwrap();
    writeln!(summary, "格式: 函数名 (总耗时 | 自身耗时) [参数]").unwrap();
    writeln!(summary, "🔥 = 关键路径 (最耗时分支)").unwrap();
    writeln!(summary, "❌ = 包含错误").unwrap();
    writeln!(summary, "").unwrap();

    // 递归生成调用树
    generate_node_text(&root_node, "", true, &mut summary);
    
    // 如果有错误，单独列出错误信息
    if root_node.has_error {
        writeln!(summary, "").unwrap();
        writeln!(summary, "=== 错误信息汇总 ===").unwrap();
        collect_errors(&root_node, &mut summary);
    }

    summary
}

/// 递归函数，生成单个节点及其子节点的文本
fn generate_node_text(
    node: &std::sync::RwLockReadGuard<DistilledTraceNode>, 
    prefix: &str, 
    is_last: bool, 
    summary: &mut String
) {
    use std::fmt::Write;
    
    let connector = if prefix.is_empty() { 
        "" 
    } else if is_last { 
        "└─ " 
    } else { 
        "├─ " 
    };
    
    let critical_marker = if node.is_critical_path { "🔥 " } else { "" };
    let error_marker = if node.has_error { "❌ " } else { "" };
    
    let duration_str = node.duration_ms
        .map(|d| format!("{:.2}ms", d))
        .unwrap_or_else(|| "运行中".to_string());
    
    let self_time_str = node.self_time_ms
        .map(|st| format!(" | {:.2}ms", st))
        .unwrap_or_default();
    
    let fields_str = if !node.fields.is_empty() {
        let content = node.fields.iter()
            .filter(|(k, _)| k.as_str() != "message") // 过滤掉message字段
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        if content.is_empty() { 
            "".to_string() 
        } else { 
            format!(" [{}]", content) 
        }
    } else { 
        "".to_string() 
    };

    writeln!(
        summary, 
        "{}{}{}{}{} ({}{}){}", 
        prefix, connector, critical_marker, error_marker, 
        node.name, duration_str, self_time_str, fields_str
    ).unwrap();

    // 递归处理子节点
    let child_prefix = format!("{}{}", prefix, if is_last { "   " } else { "│  " });
    if let Some((last_child_arc, other_children_arc)) = node.children.split_last() {
        for child_arc in other_children_arc {
            let child_node = child_arc.read().unwrap();
            generate_node_text(&child_node, &child_prefix, false, summary);
        }
        let last_child_node = last_child_arc.read().unwrap();
        generate_node_text(&last_child_node, &child_prefix, true, summary);
    }
}

/// 收集所有错误信息
fn collect_errors(node: &std::sync::RwLockReadGuard<DistilledTraceNode>, summary: &mut String) {
    use std::fmt::Write;
    
    if !node.error_messages.is_empty() {
        writeln!(summary, "函数 {}: ", node.name).unwrap();
        for msg in &node.error_messages {
            writeln!(summary, "  - {}", msg).unwrap();
        }
    }
    
    for child_arc in &node.children {
        let child_node = child_arc.read().unwrap();
        collect_errors(&child_node, summary);
    }
}
