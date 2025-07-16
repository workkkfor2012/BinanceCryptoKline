// K线数据服务主程序 - 专注于K线补齐功能
use kline_server::klcommon::{Database, Result};
use kline_server::kldata::KlineBackfiller;
use kline_macros::perf_profile;

use std::sync::Arc;

// 导入统一日志设置模块
use kline_server::klcommon::log::{shutdown_log_sender, init_ai_logging};

// 导入tracing宏
use tracing::info;



// ========================================
// 🔧 测试开关配置
// ========================================
/// 是否启用测试模式（限制为只处理 BTCUSDT）
const TEST_MODE: bool = false;

/// 测试模式下使用的交易对
const TEST_SYMBOLS: &[&str] = &["BTCUSDT"];




// ========================================

#[tokio::main]
async fn main() -> Result<()> {
    // 持有 guard，直到 main 函数结束，确保文件被正确写入
    let _log_guard = init_ai_logging().await?;

    let result = run_app().await;

    // ✨ [修改] 使用确定性的关闭逻辑替换 sleep
    shutdown_log_sender();

    result
}

/// 应用程序的核心业务逻辑
#[perf_profile]
async fn run_app() -> Result<()> {
    // 在 run_app 开始时增加低频日志，标记核心业务逻辑的开始
    info!(log_type = "low_freq", message = "核心应用逻辑开始执行");

    let intervals = "1m,5m,30m,1h,4h,1d,1w".to_string();
    let interval_list = intervals.split(',').map(|s| s.trim().to_string()).collect::<Vec<String>>();

    let db_path = std::path::PathBuf::from("./data/klines.db");
    let db = Arc::new(Database::new(&db_path)?);

    let backfiller = if TEST_MODE {
        KlineBackfiller::new_test_mode(
            db.clone(),
            interval_list.clone(),
            TEST_SYMBOLS.iter().map(|s| s.to_string()).collect()
        )
    } else {
        KlineBackfiller::new(db.clone(), interval_list.clone())
    };

    backfiller.run_once().await?;

    // ✨ [新增] 低频日志：标记核心业务逻辑的成功结束
    info!(log_type = "low_freq", message = "核心应用逻辑成功完成");

    Ok(())
}






