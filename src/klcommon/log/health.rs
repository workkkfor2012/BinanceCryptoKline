//! src/klcommon/health.rs
//! 系统健康状态监控框架 (Watchdog V2)

use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::time::Duration;
use tracing::{info, error, warn};

/// 组件的健康状态枚举
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub enum ComponentStatus {
    Ok,
    Warning,
    Error,
}

/// 单个组件的健康报告
#[derive(Debug, Serialize, Clone)]
pub struct HealthReport {
    pub component_name: String,
    pub status: ComponentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub details: HashMap<String, serde_json::Value>,
}

/// 健康报告者必须实现的 Trait (接口)
#[async_trait::async_trait]
pub trait HealthReporter: Send + Sync {
    /// 返回组件的唯一名称
    fn name(&self) -> String;
    /// 异步生成健康报告
    async fn report(&self) -> HealthReport;
}

/// 系统状态监控中枢 (Watchdog V2)
#[derive(Clone)]
pub struct WatchdogV2 {
    reporters: Arc<RwLock<Vec<Arc<dyn HealthReporter>>>>,
}

impl WatchdogV2 {
    pub fn new() -> Self {
        Self {
            reporters: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 注册一个新的健康报告者
    pub fn register(&self, reporter: Arc<dyn HealthReporter>) {
        info!(target = "状态监控", "健康报告者已注册: {}", reporter.name());
        self.reporters.write().unwrap().push(reporter);
    }

    /// 启动看门狗的监控循环
    #[tracing::instrument(target = "健康监控", skip_all)]
    pub async fn run(
        self: Arc<Self>,
        check_interval: Duration,
        shutdown_notify: Arc<tokio::sync::Notify>,
    ) {
        info!(target = "状态监控", log_type = "low_freq", interval_s = check_interval.as_secs(), "WatchdogV2 监控中枢已启动");
        let mut interval = tokio::time::interval(check_interval);

        loop {
            interval.tick().await;

            // 克隆 reporters 以避免跨 await 点持有锁
            let reporters = {
                let reporters_guard = self.reporters.read().unwrap();
                if reporters_guard.is_empty() {
                    continue;
                }
                reporters_guard.clone()
            };

            let mut reports = Vec::with_capacity(reporters.len());
            let mut has_error = false;

            for reporter in reporters.iter() {
                let report = reporter.report().await;
                if report.status == ComponentStatus::Error {
                    has_error = true;
                }
                reports.push(report);
            }
            
            // 将所有报告聚合到一条 beacon 日志中
            info!(
                target = "状态监控",
                log_type = "beacon",
                message = "系统健康状态巡检",
                status_reports = ?reports
            );

            if has_error {
                error!(
                    target = "状态监控",
                    log_type = "assertion",
                    reason = "critical_component_failure",
                    status_reports = ?reports,
                    "WatchdogV2 检测到严重组件故障，触发系统关闭"
                );
                shutdown_notify.notify_one();
                break;
            }
        }
        warn!(target = "状态监控", "WatchdogV2 监控中枢任务已退出");
    }
}