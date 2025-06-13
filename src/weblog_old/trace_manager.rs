//! Trace管理器模块
//! 
//! 负责管理Trace和Span的生命周期，包括：
//! - Trace的创建和更新
//! - Span的关联和层次结构
//! - 内存管理和清理
//! - 查询和统计

use crate::types::*;
use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

/// Trace管理器
#[derive(Debug)]
pub struct TraceManager {
    /// 所有活跃的Trace
    traces: HashMap<String, Trace>,
    /// 所有Span（跨Trace）
    spans: HashMap<String, Span>,
    /// Trace历史记录（LRU）
    trace_history: VecDeque<String>,
    /// 最大保留的Trace数量
    max_traces: usize,
    /// 统计信息
    stats: TraceStats,
}

/// Trace统计信息
#[derive(Debug, Default)]
pub struct TraceStats {
    pub total_traces: u64,
    pub active_traces: usize,
    pub completed_traces: u64,
    pub error_traces: u64,
    pub total_spans: u64,
    pub avg_trace_duration_ms: f64,
    pub max_trace_duration_ms: f64,
}

impl TraceManager {
    /// 创建新的Trace管理器
    pub fn new(max_traces: usize) -> Self {
        Self {
            traces: HashMap::new(),
            spans: HashMap::new(),
            trace_history: VecDeque::new(),
            max_traces,
            stats: TraceStats::default(),
        }
    }

    /// 处理新的日志条目
    pub fn process_log_entry(&mut self, entry: &LogEntry) {
        // 如果有span信息，处理span相关逻辑
        if let Some(span_info) = &entry.span {
            if let Some(span_id) = &span_info.id {
                self.process_span_event(span_id, entry);
            }
        }

        // 如果有span信息，确保trace存在
        if let Some(span_info) = &entry.span {
            if let Some(span_id) = &span_info.id {
                // 使用span_id作为trace_id（简化处理）
                self.ensure_trace_exists(span_id);

                // 将事件添加到对应的span
                if let Some(span) = self.spans.get_mut(span_id) {
                    span.events.push(entry.clone());
                }
            }
        }
    }

    /// 处理Span事件
    fn process_span_event(&mut self, span_id: &str, entry: &LogEntry) {
        // 检查是否为span开始事件
        if entry.message.contains("new") || entry.fields.contains_key("span.kind") {
            self.create_span(span_id, entry);
        }
        // 检查是否为span结束事件
        else if entry.message.contains("close") || entry.fields.contains_key("elapsed_milliseconds") {
            self.close_span(span_id, entry);
        }
        // 其他span事件
        else {
            if let Some(span) = self.spans.get_mut(span_id) {
                span.events.push(entry.clone());
            }
        }
    }

    /// 创建新的Span
    fn create_span(&mut self, span_id: &str, entry: &LogEntry) {
        let trace_id = span_id.to_string(); // 简化：使用span_id作为trace_id

        let span = Span {
            span_id: span_id.to_string(),
            trace_id: trace_id.clone(),
            parent_id: entry.span.as_ref()
                .and_then(|s| s.parent_id.clone()),
            name: entry.span.as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| entry.message.clone()),
            target: entry.target.clone(),
            level: entry.level.clone(),
            start_time: SystemTime::now(),
            end_time: None,
            duration_ms: None,
            fields: entry.fields.clone(),
            events: vec![entry.clone()],
        };

        self.spans.insert(span_id.to_string(), span);
        self.stats.total_spans += 1;

        // 确保对应的Trace存在
        self.ensure_trace_exists(&trace_id);
        
        // 更新Trace中的span计数
        if let Some(trace) = self.traces.get_mut(&trace_id) {
            trace.span_count += 1;
            trace.spans.insert(span_id.to_string(), self.spans[span_id].clone());
            
            // 如果这是第一个span，设置为root span
            if trace.root_span_id.is_none() {
                trace.root_span_id = Some(span_id.to_string());
                trace.root_span_name = Some(self.spans[span_id].name.clone());
            }
        }
    }

    /// 关闭Span
    fn close_span(&mut self, span_id: &str, entry: &LogEntry) {
        let trace_id = if let Some(span) = self.spans.get_mut(span_id) {
            span.end_time = Some(SystemTime::now());
            span.events.push(entry.clone());

            // 计算持续时间
            if let Ok(duration) = span.end_time.unwrap().duration_since(span.start_time) {
                span.duration_ms = Some(duration.as_millis() as f64);
            }

            // 从字段中提取持续时间（如果有）
            if let Some(elapsed) = entry.fields.get("elapsed_milliseconds") {
                if let Some(ms) = elapsed.as_f64() {
                    span.duration_ms = Some(ms);
                }
            }

            span.trace_id.clone()
        } else {
            return;
        };

        // 更新Trace状态
        if let Some(trace) = self.traces.get_mut(&trace_id) {
            if let Some(span) = self.spans.get(span_id) {
                trace.spans.insert(span_id.to_string(), span.clone());
            }
        }

        self.update_trace_status(&trace_id);
    }

    /// 确保Trace存在
    fn ensure_trace_exists(&mut self, trace_id: &str) {
        if !self.traces.contains_key(trace_id) {
            let trace = Trace {
                trace_id: trace_id.to_string(),
                root_span_id: None,
                root_span_name: None,
                start_time: SystemTime::now(),
                end_time: None,
                duration_ms: None,
                span_count: 0,
                spans: HashMap::new(),
                status: TraceStatus::Active,
            };

            self.traces.insert(trace_id.to_string(), trace);
            self.trace_history.push_back(trace_id.to_string());
            self.stats.total_traces += 1;

            // 清理旧的Trace
            self.cleanup_old_traces();
        }
    }

    /// 更新Trace状态
    fn update_trace_status(&mut self, trace_id: &str) {
        if let Some(trace) = self.traces.get_mut(trace_id) {
            let all_spans_closed = trace.spans.values().all(|span| span.end_time.is_some());
            let has_error = trace.spans.values().any(|span| span.level == "ERROR");

            if all_spans_closed && trace.span_count > 0 {
                trace.status = if has_error {
                    TraceStatus::Error
                } else {
                    TraceStatus::Completed
                };
                trace.end_time = Some(SystemTime::now());

                // 计算总持续时间
                if let Ok(duration) = trace.end_time.unwrap().duration_since(trace.start_time) {
                    trace.duration_ms = Some(duration.as_millis() as f64);
                }

                // 更新统计
                match trace.status {
                    TraceStatus::Completed => self.stats.completed_traces += 1,
                    TraceStatus::Error => self.stats.error_traces += 1,
                    _ => {}
                }

                // 更新平均持续时间
                if let Some(duration) = trace.duration_ms {
                    self.stats.avg_trace_duration_ms = 
                        (self.stats.avg_trace_duration_ms * (self.stats.completed_traces + self.stats.error_traces - 1) as f64 + duration) 
                        / (self.stats.completed_traces + self.stats.error_traces) as f64;
                    
                    if duration > self.stats.max_trace_duration_ms {
                        self.stats.max_trace_duration_ms = duration;
                    }
                }
            }
        }
    }

    /// 清理旧的Trace
    fn cleanup_old_traces(&mut self) {
        while self.traces.len() > self.max_traces {
            if let Some(old_trace_id) = self.trace_history.pop_front() {
                if let Some(trace) = self.traces.remove(&old_trace_id) {
                    // 清理相关的spans
                    for span_id in trace.spans.keys() {
                        self.spans.remove(span_id);
                    }
                }
            }
        }
        
        self.stats.active_traces = self.traces.len();
    }

    /// 获取所有Trace
    pub fn get_traces(&self) -> Vec<Trace> {
        self.traces.values().cloned().collect()
    }

    /// 获取指定Trace
    pub fn get_trace(&self, trace_id: &str) -> Option<&Trace> {
        self.traces.get(trace_id)
    }

    /// 获取指定Span
    pub fn get_span(&self, span_id: &str) -> Option<&Span> {
        self.spans.get(span_id)
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> &TraceStats {
        &self.stats
    }

    /// 获取活跃的Trace数量
    pub fn active_trace_count(&self) -> usize {
        self.traces.values()
            .filter(|trace| matches!(trace.status, TraceStatus::Active))
            .count()
    }

    /// 清理所有数据
    pub fn clear(&mut self) {
        self.traces.clear();
        self.spans.clear();
        self.trace_history.clear();
        self.stats = TraceStats::default();
    }
}
