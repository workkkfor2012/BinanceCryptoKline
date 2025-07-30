use std::sync::Arc;
use kline_server::klcommon::server_time_sync::ServerTimeSyncManager;

#[tokio::main]
async fn main() {
    println!("测试EMA网络延迟计算");
    
    let sync_manager = Arc::new(ServerTimeSyncManager::new());
    
    // 模拟几次网络延迟更新
    let delays = vec![50, 60, 45, 70, 55, 65, 40, 80];
    
    println!("初始平均延迟: {}", sync_manager.get_avg_network_delay());
    
    for (i, delay) in delays.iter().enumerate() {
        // 模拟sync_time_once的调用
        sync_manager.network_delay.store(*delay, std::sync::atomic::Ordering::SeqCst);
        
        // 手动调用EMA更新（这里我们需要访问私有方法，所以直接模拟）
        let old_avg = sync_manager.avg_network_delay.load(std::sync::atomic::Ordering::Relaxed);
        let new_avg = if old_avg == 0 {
            *delay
        } else {
            (0.2 * (*delay as f64) + 0.8 * (old_avg as f64)).round() as i64
        };
        sync_manager.avg_network_delay.store(new_avg, std::sync::atomic::Ordering::SeqCst);
        
        println!("第{}次更新: 瞬时延迟={}ms, 平均延迟={}ms, 动态等待窗口={}ms", 
                 i+1, delay, new_avg, new_avg * 2 + 100);
    }
    
    let final_avg = sync_manager.get_avg_network_delay();
    println!("\n最终平均延迟: {}ms", final_avg);
    println!("最终动态等待窗口: {}ms", final_avg * 2 + 100);
}
