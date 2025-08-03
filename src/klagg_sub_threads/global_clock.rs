//! 全局时钟模块
//!
//! 负责提供精确的整分钟时钟节拍，用于驱动K线聚合系统的时间对齐。
//! 采用动态等待窗口机制，根据网络延迟自动调整时钟精度。

use crate::klcommon::{server_time_sync::ServerTimeSyncManager, AggregateConfig};
use std::sync::Arc;
use tokio::sync::{watch, Notify};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, instrument, trace, warn};

// --- 常量定义 ---
const MIN_SLEEP_MS: u64 = 10;

/// 全局时钟任务
/// 
/// 负责提供精确的整分钟时钟节拍，用于驱动K线聚合系统。
/// 
/// # 参数
/// - `_config`: 配置对象（保留参数以减少函数签名变动）
/// - `time_sync_manager`: 服务器时间同步管理器
/// - `clock_tx`: 时钟信号发送器
/// - `shutdown_notify`: 关闭通知器
#[instrument(target = "全局时钟", skip_all, name="run_clock_task")]
pub async fn run_clock_task(
    _config: Arc<AggregateConfig>, // config 不再需要，但保留参数以减少函数签名变动
    time_sync_manager: Arc<ServerTimeSyncManager>,
    clock_tx: watch::Sender<i64>,
    shutdown_notify: Arc<Notify>,
) {
    // 【核心修改】时钟任务的目标是严格对齐到服务器时间的"整分钟"，不再依赖任何K线周期。
    const CLOCK_INTERVAL_MS: i64 = 60_000; // 60秒
    info!(target: "全局时钟", log_type="low_freq", interval_ms = CLOCK_INTERVAL_MS, "全局时钟任务已启动，将按整分钟对齐");

    // 时间同步重试计数器
    let mut time_sync_retry_count = 0;
    const MAX_TIME_SYNC_RETRIES: u32 = 10;

    loop {
        if !time_sync_manager.is_time_sync_valid() {
            time_sync_retry_count += 1;
            warn!(target: "全局时钟", log_type="retry",
                  retry_count = time_sync_retry_count,
                  max_retries = MAX_TIME_SYNC_RETRIES,
                  "时间同步失效，正在主动重试同步...");

            // 主动尝试进行一次同步，并捕获具体错误
            match time_sync_manager.sync_time_once().await {
                Ok((diff, delay)) => {
                    // 如果意外成功了，就重置计数器并继续
                    info!(target: "全局时钟", log_type="recovery",
                          diff_ms = diff,
                          delay_ms = delay,
                          "在重试期间，时间同步成功恢复");
                    time_sync_retry_count = 0;
                    continue;
                }
                Err(e) => {
                    // 打印出底层的、具体的网络错误
                    error!(target: "全局时钟", log_type="retry_failure",
                           retry_count = time_sync_retry_count,
                           error = ?e, // <-- 这是关键的补充信息
                           "时间同步重试失败");
                }
            }

            if time_sync_retry_count >= MAX_TIME_SYNC_RETRIES {
                error!(target: "全局时钟", log_type="assertion", reason="time_sync_invalid",
                       retry_count = time_sync_retry_count,
                       "时间同步失效，已达到最大重试次数，服务将关闭");
                shutdown_notify.notify_one();
                break;
            }

            // 等待一段时间后再次尝试 (可以使用逐渐增长的等待时间)
            sleep(Duration::from_millis(1000 * time_sync_retry_count as u64)).await;
            continue;
        } else {
            // 时间同步恢复正常，重置重试计数器
            if time_sync_retry_count > 0 {
                info!(target: "全局时钟", log_type="recovery",
                      previous_retry_count = time_sync_retry_count,
                      "时间同步已恢复正常");
                time_sync_retry_count = 0;
            }
        }

        let now = time_sync_manager.get_calibrated_server_time();
        if now == 0 {
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        // 【核心修改】计算动态等待窗口：平均网络延迟 * 2 + 100ms
        let avg_delay = time_sync_manager.get_avg_network_delay();
        let dynamic_grace_period_ms = (avg_delay * 2 + 100).max(100); // 两倍网络延迟+100ms，最小值100ms

        // 计算下一个服务器时间整分钟点
        let next_tick_point = (now / CLOCK_INTERVAL_MS + 1) * CLOCK_INTERVAL_MS;
        let wakeup_time = next_tick_point + dynamic_grace_period_ms;
        let sleep_duration_ms = (wakeup_time - now).max(MIN_SLEEP_MS as i64) as u64;

        trace!(target: "全局时钟",
            now,
            avg_network_delay = avg_delay,
            dynamic_grace_period_ms,
            next_tick_point,
            wakeup_time,
            sleep_duration_ms,
            "计算下一次唤醒时间（动态等待窗口）"
        );
        sleep(Duration::from_millis(sleep_duration_ms)).await;

        let final_time = time_sync_manager.get_calibrated_server_time();
        // [日志增强] 在发送时钟信号前，记录关键决策信息
        debug!(
            target: "全局时钟",
            sent_time = final_time,
            used_grace_period_ms = dynamic_grace_period_ms,
            avg_network_delay = avg_delay,
            "发送时钟滴答信号"
        );
        if clock_tx.send(final_time).is_err() {
            warn!(target: "全局时钟", "主时钟通道已关闭，任务退出");
            break;
        }
    }
    warn!(target: "全局时钟", "全局时钟任务已退出");
}

/// [新增] 一个专门用于周期性打印服务器时间的日志任务
///
/// 这个任务完全独立于系统时钟信号，只负责定期输出校准后的服务器时间，
/// 用于调试和监控时间同步状态。
///
/// # 参数
/// - `time_sync_manager`: 服务器时间同步管理器
/// - `shutdown_rx`: 关闭信号接收器
#[instrument(target = "周期时间日志", skip_all, name="run_periodic_time_logger")]
pub async fn run_periodic_time_logger(
    time_sync_manager: Arc<ServerTimeSyncManager>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!(target: "周期时间日志", "周期性时间日志任务已启动，将每5秒输出一次校准后的服务器时间。");
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip); // 如果处理慢了，跳过积压的tick

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            },
            _ = interval.tick() => {
                let calibrated_time_ms = time_sync_manager.get_calibrated_server_time();
                if calibrated_time_ms > 0 {
                    // 将毫秒时间戳转换为可读格式
                    if let Some(datetime) = chrono::DateTime::from_timestamp_millis(calibrated_time_ms) {
                        let formatted_time = datetime.format("%Y-%m-%d %H:%M:%S.%3f UTC").to_string();

                        info!(
                            target: "周期时间日志",
                            log_type="checkpoint", // 使用一个特定的log_type方便过滤
                            server_time_ms = calibrated_time_ms,
                            offset_ms = time_sync_manager.get_time_diff(),
                            avg_network_delay_ms = time_sync_manager.get_avg_network_delay(),
                            is_sync_valid = time_sync_manager.is_time_sync_valid(),
                            "校准后的服务器时间: {}",
                            formatted_time
                        );
                    } else {
                        warn!(target: "周期时间日志",
                              server_time_ms = calibrated_time_ms,
                              "无法解析时间戳为可读格式");
                    }
                } else {
                    warn!(target: "周期时间日志", "时间同步尚未完成，无法获取校准后的服务器时间");
                }
            }
        }
    }
    warn!(target: "周期时间日志", "周期性时间日志任务已退出。");
}
