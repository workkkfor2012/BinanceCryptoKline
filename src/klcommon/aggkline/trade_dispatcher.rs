// 交易分发器 - 将解析后的交易数据分发给对应的KlineActor
use crate::klcommon::models::AppAggTrade;
use crate::klcommon::{Result};
use log::{info, error, warn};
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{Duration, Instant};

/// 运行应用交易分发器任务
///
/// # 参数
/// * `app_trade_receiver` - 应用交易接收器
/// * `actor_senders` - Actor发送器映射表
pub async fn run_app_trade_dispatcher_task(
    mut app_trade_receiver: mpsc::Receiver<AppAggTrade>,
    actor_senders: HashMap<String, mpsc::Sender<AppAggTrade>>,
) -> Result<()> {
    info!("启动应用交易分发器任务");

    // 将actor_senders包装在Arc<Mutex<>>中，以便在任务中共享
    let actor_senders = Arc::new(Mutex::new(actor_senders));

    // 统计信息
    let mut message_count = 0;
    let mut dispatch_error_count = 0;
    let mut last_stats_time = Instant::now();
    let stats_interval = Duration::from_secs(30);

    // 符号统计
    let mut symbol_stats: HashMap<String, usize> = HashMap::new();

    while let Some(app_trade) = app_trade_receiver.recv().await {
        message_count += 1;

        // 更新符号统计
        *symbol_stats.entry(app_trade.symbol.clone()).or_insert(0) += 1;

        // 每30秒输出一次统计信息
        if last_stats_time.elapsed() >= stats_interval {
            info!("已分发 {} 条交易数据，分发错误 {} 条", message_count, dispatch_error_count);

            // 输出每个符号的统计信息
            let mut stats = symbol_stats.iter().collect::<Vec<_>>();
            stats.sort_by_key(|(_, count)| std::cmp::Reverse(*count));

            for (symbol, count) in stats.iter().take(10) { // 只显示前10个
                info!("  {}: {} 条", symbol, count);
            }

            if stats.len() > 10 {
                info!("  ... 以及 {} 个其他符号", stats.len() - 10);
            }

            // 重置统计
            last_stats_time = Instant::now();
            symbol_stats.clear();
        }

        // 获取对应的Actor发送器
        let mut senders = actor_senders.lock().await;

        if let Some(actor_sender) = senders.get_mut(&app_trade.symbol) {
            // 发送交易数据到对应的Actor
            if let Err(e) = actor_sender.send(app_trade.clone()).await {
                dispatch_error_count += 1;
                error!("[Dispatcher] 发送交易数据到Actor失败: {}, symbol: {}", e, app_trade.symbol);

                // 可以选择移除这个sender
                // senders.remove(&app_trade.symbol);
            }
        } else {
            // 收到一个没有对应Actor的交易对的数据
            warn!("[Dispatcher] 收到未知交易对的交易数据: {}", app_trade.symbol);
        }
    }

    info!("[Dispatcher] 应用交易通道已关闭，退出任务");
    Ok(())
}
