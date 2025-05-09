// K线比对工具 - 比对合成的K线和API获取的K线
use kline_server::klcommon::{AppError, Result, BinanceApi, Database, Kline};
use log::{info, error, warn};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;

// 测试的交易对和周期
const SYMBOL: &str = "BTCUSDT";
const INTERVAL: &str = "1m"; // 1分钟K线
const CHECK_INTERVAL_SECS: u64 = 30; // 每30秒检查一次

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();

    info!("启动K线比对工具");
    info!("交易对: {}, 周期: {}", SYMBOL, INTERVAL);
    info!("检查间隔: {}秒", CHECK_INTERVAL_SECS);

    // 初始化数据库
    let db = Arc::new(Database::new("data/klines.db")?);
    info!("数据库连接成功");

    // 检查数据库表是否存在
    check_database_tables(&db, SYMBOL, INTERVAL).await?;

    // 初始化API客户端
    let api = BinanceApi::new();
    info!("API客户端初始化成功");

    // 创建定时器
    let mut interval = time::interval(Duration::from_secs(CHECK_INTERVAL_SECS));

    // 循环比对
    loop {
        // 等待下一个时间点
        interval.tick().await;

        // 获取当前时间
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        info!("开始比对K线数据, 当前时间: {}", now);

        // 获取上一根已完成的K线时间
        let last_kline_time = get_last_completed_kline_time(now, INTERVAL);
        info!("上一根已完成K线的开始时间: {}", last_kline_time);

        // 从数据库获取K线
        match db.get_kline_by_time(SYMBOL, INTERVAL, last_kline_time) {
            Ok(Some(db_kline)) => {
                info!("从数据库获取到K线: open_time={}", db_kline.open_time);

                // 从API获取相同时间段的K线
                match get_kline_from_api(&api, SYMBOL, INTERVAL, last_kline_time).await {
                    Ok(api_kline) => {
                        // 比对K线数据
                        compare_klines(&db_kline, &api_kline);
                    }
                    Err(e) => {
                        error!("从API获取K线失败: {}", e);
                    }
                }
            }
            Ok(None) => {
                warn!("数据库中未找到时间为 {} 的K线", last_kline_time);
            }
            Err(e) => {
                error!("从数据库获取K线失败: {}", e);
            }
        }
    }
}

/// 获取上一根已完成K线的开始时间
fn get_last_completed_kline_time(now: i64, interval: &str) -> i64 {
    // 获取周期的毫秒数
    let interval_ms = match interval {
        "1m" => 60 * 1000,
        "5m" => 5 * 60 * 1000,
        "15m" => 15 * 60 * 1000,
        "30m" => 30 * 60 * 1000,
        "1h" => 60 * 60 * 1000,
        "4h" => 4 * 60 * 60 * 1000,
        "1d" => 24 * 60 * 60 * 1000,
        "1w" => 7 * 24 * 60 * 60 * 1000,
        _ => 60 * 1000, // 默认1分钟
    };

    // 计算上一根已完成K线的开始时间
    // 例如，当前时间是10:15，对于1分钟K线，上一根已完成的K线是10:14开始的
    let current_kline_start = now - (now % interval_ms);
    let last_kline_start = current_kline_start - interval_ms;

    last_kline_start
}

/// 从API获取指定时间的K线
async fn get_kline_from_api(api: &BinanceApi, symbol: &str, interval: &str, open_time: i64) -> Result<Kline> {
    // 创建下载任务
    let task = kline_server::klcommon::DownloadTask {
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        limit: 1,
        start_time: Some(open_time),
        end_time: Some(open_time + 60000), // 结束时间设为开始时间+1分钟
    };

    // 下载K线数据
    let klines = api.download_klines(&task).await?;

    // 检查是否获取到K线
    if klines.is_empty() {
        return Err(AppError::DataError(format!(
            "未获取到K线数据: symbol={}, interval={}, open_time={}",
            symbol, interval, open_time
        )));
    }

    // 返回第一条K线
    Ok(klines[0].clone())
}

/// 检查数据库表是否存在
async fn check_database_tables(db: &Arc<Database>, symbol: &str, interval: &str) -> Result<()> {
    // 使用Database的ensure_symbol_table方法确保表存在
    db.ensure_symbol_table(symbol, interval)?;

    // 获取表名
    let symbol_lower = symbol.to_lowercase().replace("usdt", "");
    let interval_lower = interval.to_lowercase();
    let table_name = format!("k_{symbol_lower}_{interval_lower}");

    // 获取K线数量
    let count = db.get_kline_count(symbol, interval)?;

    info!("表 {} 已存在，包含 {} 条K线数据", table_name, count);

    // 获取最新K线时间戳
    if let Ok(Some(timestamp)) = db.get_latest_kline_timestamp(symbol, interval) {
        info!("最新K线时间戳: {}", timestamp);
    } else {
        info!("表中没有K线数据");
    }

    Ok(())
}

/// 比对两个K线数据
fn compare_klines(db_kline: &Kline, api_kline: &Kline) {
    // 检查开盘时间是否一致
    if db_kline.open_time != api_kline.open_time {
        warn!(
            "开盘时间不一致: 数据库={}, API={}",
            db_kline.open_time, api_kline.open_time
        );
    }

    // 比对OHLC
    let open_diff = (db_kline.open.parse::<f64>().unwrap_or(0.0) - api_kline.open.parse::<f64>().unwrap_or(0.0)).abs();
    let high_diff = (db_kline.high.parse::<f64>().unwrap_or(0.0) - api_kline.high.parse::<f64>().unwrap_or(0.0)).abs();
    let low_diff = (db_kline.low.parse::<f64>().unwrap_or(0.0) - api_kline.low.parse::<f64>().unwrap_or(0.0)).abs();
    let close_diff = (db_kline.close.parse::<f64>().unwrap_or(0.0) - api_kline.close.parse::<f64>().unwrap_or(0.0)).abs();

    // 比对成交量
    let volume_diff = (db_kline.volume.parse::<f64>().unwrap_or(0.0) - api_kline.volume.parse::<f64>().unwrap_or(0.0)).abs();
    let quote_volume_diff = (db_kline.quote_asset_volume.parse::<f64>().unwrap_or(0.0) - api_kline.quote_asset_volume.parse::<f64>().unwrap_or(0.0)).abs();

    // 计算相对误差（百分比）
    let calc_rel_error = |diff: f64, val: f64| -> f64 {
        if val.abs() < 1e-10 {
            0.0
        } else {
            (diff / val) * 100.0
        }
    };

    let open_rel_error = calc_rel_error(open_diff, api_kline.open.parse::<f64>().unwrap_or(1.0));
    let high_rel_error = calc_rel_error(high_diff, api_kline.high.parse::<f64>().unwrap_or(1.0));
    let low_rel_error = calc_rel_error(low_diff, api_kline.low.parse::<f64>().unwrap_or(1.0));
    let close_rel_error = calc_rel_error(close_diff, api_kline.close.parse::<f64>().unwrap_or(1.0));
    let volume_rel_error = calc_rel_error(volume_diff, api_kline.volume.parse::<f64>().unwrap_or(1.0));
    let quote_volume_rel_error = calc_rel_error(quote_volume_diff, api_kline.quote_asset_volume.parse::<f64>().unwrap_or(1.0));

    // 输出比对结果
    info!("K线比对结果 (open_time={})", db_kline.open_time);
    info!("开盘价: 数据库={}, API={}, 差异={:.8}, 相对误差={:.4}%",
          db_kline.open, api_kline.open, open_diff, open_rel_error);
    info!("最高价: 数据库={}, API={}, 差异={:.8}, 相对误差={:.4}%",
          db_kline.high, api_kline.high, high_diff, high_rel_error);
    info!("最低价: 数据库={}, API={}, 差异={:.8}, 相对误差={:.4}%",
          db_kline.low, api_kline.low, low_diff, low_rel_error);
    info!("收盘价: 数据库={}, API={}, 差异={:.8}, 相对误差={:.4}%",
          db_kline.close, api_kline.close, close_diff, close_rel_error);
    info!("成交量: 数据库={}, API={}, 差异={:.8}, 相对误差={:.4}%",
          db_kline.volume, api_kline.volume, volume_diff, volume_rel_error);
    info!("成交额: 数据库={}, API={}, 差异={:.8}, 相对误差={:.4}%",
          db_kline.quote_asset_volume, api_kline.quote_asset_volume, quote_volume_diff, quote_volume_rel_error);

    // 判断是否有显著差异
    const ERROR_THRESHOLD: f64 = 1.0; // 1%的相对误差阈值
    if open_rel_error > ERROR_THRESHOLD || high_rel_error > ERROR_THRESHOLD ||
       low_rel_error > ERROR_THRESHOLD || close_rel_error > ERROR_THRESHOLD ||
       volume_rel_error > ERROR_THRESHOLD || quote_volume_rel_error > ERROR_THRESHOLD {
        warn!("K线数据存在显著差异！相对误差超过{}%", ERROR_THRESHOLD);
    } else {
        info!("K线数据比对一致，相对误差在{}%以内", ERROR_THRESHOLD);
    }
}
