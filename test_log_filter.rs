use tracing::{trace, info, warn, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// 引入我们的日志层
use kline_server::klcommon::log::target_log::{TargetLogLayer, init_target_log_sender};

fn main() {
    // 初始化日志发送器
    init_target_log_sender("test_log_filter");

    // 设置订阅器
    tracing_subscriber::registry()
        .with(TargetLogLayer::new())
        .init();

    println!("开始测试日志过滤...");

    // 这些日志应该被过滤掉（trace级别 + target="log"）
    trace!(target: "log", "这条trace日志应该被过滤");
    trace!(target: "log", message = "这条trace日志也应该被过滤", field1 = "value1");

    // 这些日志应该正常显示（不同级别或不同target）
    trace!(target: "other", "这条trace日志应该显示（不同target）");
    info!(target: "log", "这条info日志应该显示（不同级别）");
    warn!(target: "log", "这条warn日志应该显示");
    error!(target: "log", "这条error日志应该显示");

    // 正常的业务日志
    info!("正常的业务日志");
    trace!("正常的trace日志（默认target）");

    println!("测试完成，请检查日志输出");
    
    // 等待一下让日志发送完成
    std::thread::sleep(std::time::Duration::from_millis(100));
}
