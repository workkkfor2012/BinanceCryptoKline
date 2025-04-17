use log::{info, error, warn};
use rusqlite::{Connection, Result as SqliteResult, params};
use std::path::PathBuf;
use std::collections::HashMap;

// 定义常量
/// 所有周期列表
const ALL_INTERVALS: [&str; 6] = ["1m", "5m", "30m", "4h", "1d", "1w"];

/// K线数量乘数因子
const KLINE_MULTIPLIER: i64 = 10;
/// 每个周期的预期K线数量
const EXPECTED_COUNTS: [i64; 6] = [
    50 * KLINE_MULTIPLIER, 50 * KLINE_MULTIPLIER, 30 * KLINE_MULTIPLIER,
    30 * KLINE_MULTIPLIER, 20 * KLINE_MULTIPLIER, 10 * KLINE_MULTIPLIER
];

fn init_logging() {
    // 设置环境变量，强制使用 info 级别
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
}

/// 获取指定交易对和周期的K线数量
fn get_kline_count(conn: &Connection, symbol: &str, interval: &str) -> SqliteResult<i64> {
    // 去掉交易对名称中的"USDT"后缀
    let symbol_lower = symbol.to_lowercase().replace("usdt", "");
    let interval_lower = interval.to_lowercase();

    // 统一使用 k_ 前缀
    let table_name = format!("k_{symbol_lower}_{interval_lower}");

    // 检查表是否存在
    let table_exists: bool = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
        params![table_name],
        |row| row.get::<_, i64>(0).map(|count| count > 0),
    ).unwrap_or(false);

    if !table_exists {
        return Ok(0);
    }

    let query = format!("SELECT COUNT(*) FROM {}", table_name);
    conn.query_row(
        &query,
        [],
        |row| row.get(0),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    init_logging();

    info!("开始统计K线数据...");

    // 连接数据库
    let db_path = PathBuf::from("./data/klines.db");
    if !db_path.exists() {
        error!("数据库文件不存在: {}", db_path.display());
        return Err("数据库文件不存在".into());
    }

    // 直接使用 SQLite 连接
    let conn = Connection::open(&db_path)?;

    // 获取所有表名
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'symbols'")?;
    let table_names: SqliteResult<Vec<String>> = stmt.query_map([], |row| row.get(0))?.collect();
    let table_names = table_names?;

    // 解析表名，提取交易对和周期信息
    let mut symbols = HashMap::new();
    for table_name in &table_names {
        // 表名格式: symbol_interval 或 k_symbol_interval (对于数字开头的交易对)
        let parts: Vec<&str> = table_name.split('_').collect();

        let (symbol, interval) = if parts[0] == "k" && parts.len() >= 3 {
            // 处理 k_symbol_interval 格式
            (parts[1].to_string(), parts[2].to_string())
        } else if parts.len() >= 2 {
            // 处理 symbol_interval 格式
            (parts[0].to_string(), parts[1].to_string())
        } else {
            warn!("无法解析表名: {}", table_name);
            continue;
        };

        // 将交易对名称转换为大写并添加USDT后缀
        let symbol_upper = format!("{}USDT", symbol.to_uppercase());

        // 将交易对添加到集合中
        symbols.entry(symbol_upper).or_insert_with(Vec::new).push(interval);
    }

    // 输出统计结果
    info!("合约总数: {}", symbols.len());
    info!("");

    // 按字母顺序排序交易对
    let mut symbol_names: Vec<String> = symbols.keys().cloned().collect();
    symbol_names.sort();

    // 遍历所有交易对
    for symbol in symbol_names {
        // 获取该交易对的所有周期的K线数量
        let mut counts = HashMap::new();
        let mut all_intervals_exist = true;

        for interval in ALL_INTERVALS.iter() {
            let count = get_kline_count(&conn, &symbol, interval)?;
            counts.insert(*interval, count);

            // 检查是否所有周期都存在
            if count == 0 {
                all_intervals_exist = false;
            }
        }

        // 只输出至少有一个周期有数据的交易对
        if all_intervals_exist {
            // 构建统计信息字符串
            let mut stats_str = format!("{}: ", symbol);

            for (i, interval) in ALL_INTERVALS.iter().enumerate() {
                let count = counts.get(interval).unwrap_or(&0);
                let expected = EXPECTED_COUNTS[i];
                let percentage = if expected > 0 { (*count as f64 / expected as f64) * 100.0 } else { 0.0 };

                stats_str.push_str(&format!("{}={} ({:.1}%), ", interval, count, percentage));
            }

            // 移除最后的逗号和空格
            if stats_str.ends_with(", ") {
                stats_str.truncate(stats_str.len() - 2);
            }

            // 输出统计信息
            info!("{}", stats_str);
        }
    }

    info!("");
    info!("K线数据统计完成");

    Ok(())
}
