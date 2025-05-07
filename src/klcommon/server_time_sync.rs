use crate::klcommon::{BinanceApi, Result};
use log::{debug, error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use chrono::Utc;

/// 服务器时间同步管理器
///
/// 负责两个主要任务：
/// 1. 每分钟的第30秒与服务器通信，获取服务器时间，计算时间差值
/// 2. 测量获取服务器时间的延迟
pub struct ServerTimeSyncManager {
    /// 币安API客户端
    api: BinanceApi,
    /// 本地时间与服务器时间的差值（毫秒）
    time_diff: Arc<AtomicI64>,
    /// 网络延迟（毫秒）
    network_delay: Arc<AtomicI64>,
    /// 最后一次同步时间（毫秒时间戳）
    last_sync_time: Arc<AtomicI64>,
}

impl ServerTimeSyncManager {
    /// 创建新的服务器时间同步管理器
    pub fn new() -> Self {
        Self {
            api: BinanceApi::new(),
            time_diff: Arc::new(AtomicI64::new(0)), // 初始时间差为0
            network_delay: Arc::new(AtomicI64::new(0)), // 初始网络延迟为0
            last_sync_time: Arc::new(AtomicI64::new(0)), // 初始最后同步时间为0
        }
    }

    /// 获取当前的时间差值
    pub fn get_time_diff(&self) -> i64 {
        self.time_diff.load(Ordering::SeqCst)
    }

    /// 获取当前的网络延迟
    pub fn get_network_delay(&self) -> i64 {
        self.network_delay.load(Ordering::SeqCst)
    }

    /// 获取最后一次同步时间
    pub fn get_last_sync_time(&self) -> i64 {
        self.last_sync_time.load(Ordering::SeqCst)
    }

    /// 计算最优请求发送时间
    ///
    /// 基于当前的时间差值和网络延迟，计算下一分钟开始前的最优请求发送时间
    pub fn calculate_optimal_request_time(&self) -> i64 {
        // 获取当前本地时间
        let local_time = Utc::now().timestamp_millis();

        // 计算下一分钟的开始时间（本地时间）
        let next_minute_local = ((local_time / 60000) + 1) * 60000;

        // 获取当前的时间差值和网络延迟
        let time_diff = self.get_time_diff();
        let network_delay = self.get_network_delay();

        // 安全边际（毫秒）
        let safety_margin = 30;

        // 计算最优发送时间
        let optimal_send_time = next_minute_local - time_diff - network_delay - safety_margin;

        optimal_send_time
    }

    /// 只进行一次服务器时间同步，不启动定时任务
    pub async fn sync_time_once(&self) -> Result<(i64, i64)> {
        info!("开始与币安服务器进行时间同步");

        // 记录开始时间，用于计算网络延迟
        let start_time = Instant::now();

        // 与币安服务器时间同步
        let server_time = self.api.get_server_time().await?;

        // 计算网络延迟（往返时间的一半）
        let network_delay = start_time.elapsed().as_millis() as i64 / 2;

        // 更新网络延迟
        self.network_delay.store(network_delay, Ordering::SeqCst);

        info!("币安服务器时间: {}, 网络延迟: {}毫秒", server_time.server_time, network_delay);

        // 计算本地时间与服务器时间的差值
        let local_time = Utc::now().timestamp_millis();
        let time_diff = server_time.server_time - local_time;

        // 更新共享的时间差值
        self.time_diff.store(time_diff, Ordering::SeqCst);

        // 更新最后同步时间
        self.last_sync_time.store(local_time, Ordering::SeqCst);

        info!("本地时间与服务器时间差值: {}毫秒", time_diff);

        // 返回时间差值和网络延迟
        Ok((time_diff, network_delay))
    }

    /// 启动服务器时间同步任务
    pub async fn start(&self) -> Result<()> {
        info!("启动服务器时间同步管理器");

        // 首先进行一次时间同步
        let (time_diff, network_delay) = self.sync_time_once().await?;
        info!("初始时间同步完成，时间差值: {}毫秒，网络延迟: {}毫秒", time_diff, network_delay);

        // 启动定时同步任务
        let time_sync_handle = self.start_time_sync_task().await?;

        // 等待任务完成（实际上不会完成，除非发生错误）
        if let Err(e) = time_sync_handle.await {
            error!("时间同步任务异常终止: {}", e);
        }

        Ok(())
    }

    /// 启动独立的时间同步任务（每分钟的第30秒运行）
    async fn start_time_sync_task(&self) -> Result<tokio::task::JoinHandle<()>> {
        let api = self.api.clone();
        let time_diff = self.time_diff.clone();
        let network_delay = self.network_delay.clone();
        let last_sync_time = self.last_sync_time.clone();

        info!("启动独立的时间同步任务，将在每分钟的第30秒运行");

        let handle = tokio::spawn(async move {
            loop {
                // 获取当前时间
                let now = Utc::now();
                let seconds = now.timestamp() % 60; // 获取当前秒数

                // 计算到下一个30秒的等待时间
                let wait_seconds = if seconds < 30 {
                    30 - seconds
                } else {
                    90 - seconds // 等待到下一分钟的30秒
                };

                debug!("时间同步任务: 等待 {}秒 到下一个30秒点", wait_seconds);

                // 等待到30秒
                sleep(Duration::from_secs(wait_seconds as u64)).await;

                // 记录开始时间，用于计算网络延迟
                let start_time = Instant::now();

                // 获取服务器时间
                match api.get_server_time().await {
                    Ok(server_time) => {
                        // 计算网络延迟（往返时间的一半）
                        let new_network_delay = start_time.elapsed().as_millis() as i64 / 2;

                        // 更新网络延迟
                        let old_network_delay = network_delay.swap(new_network_delay, Ordering::SeqCst);

                        // 计算时间差值
                        let local_time = Utc::now().timestamp_millis();
                        let new_time_diff = server_time.server_time - local_time;

                        // 更新共享的时间差值
                        let old_time_diff = time_diff.swap(new_time_diff, Ordering::SeqCst);

                        // 更新最后同步时间
                        last_sync_time.store(local_time, Ordering::SeqCst);

                        info!("时间同步任务: 更新时间差值: {}毫秒 (原差值: {}毫秒), 网络延迟: {}毫秒 (原延迟: {}毫秒)",
                            new_time_diff, old_time_diff, new_network_delay, old_network_delay);
                    },
                    Err(e) => {
                        error!("时间同步任务: 获取服务器时间失败: {}", e);
                        // 如果获取失败，等待5秒后重试
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(handle)
    }
}
