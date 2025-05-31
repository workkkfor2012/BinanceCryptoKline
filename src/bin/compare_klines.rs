// K线比对工具 - 比对合成的K线和API获取的K线
use kline_server::klcommon::{AppError, Result, BinanceApi, Database, Kline};
use log::{info, error, warn, debug};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;
use std::env;
use std::fs::File;
use std::io::Write;
use log::LevelFilter;
use env_logger::Builder;
use std::path::Path;
use chrono::{Utc, TimeZone};

// 默认设置
const DEFAULT_INTERVAL: &str = "1m"; // 默认1分钟K线
const CHECK_INTERVAL_SECS: u64 = 1; // 每秒检查一次，但只在每分钟的第30秒执行比对

// 默认要比对的交易对列表（如果API获取失败时使用）
// 模式1：只测试BTC一个品种
const BTC_ONLY_SYMBOLS: [&str; 1] = [
    "BTCUSDT",
];

// 模式2：测试6个主要品种
const MAIN_SYMBOLS: [&str; 6] = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "XRPUSDT"
];

#[tokio::main]
async fn main() -> Result<()> {
    // 解析命令行参数
    let args: Vec<String> = env::args().collect();

    // 获取周期参数
    let interval = if args.len() > 1 {
        args[1].clone()
    } else {
        DEFAULT_INTERVAL.to_string()
    };

    // 获取测试模式参数
    let test_mode = if args.len() > 2 {
        args[2].parse::<u8>().unwrap_or(3) // 默认模式3：所有品种
    } else {
        3 // 默认模式3：所有品种
    };

    // 获取输出文件参数
    let output_file = if args.len() > 3 {
        Some(args[3].clone())
    } else {
        None
    };

    // 配置日志输出
    let mut builder = Builder::from_env(env_logger::Env::default().default_filter_or("info"));

    // 设置更详细的日志格式
    builder.format(|buf, record| {
        use std::io::Write;
        let level_style = buf.default_level_style(record.level());
        writeln!(
            buf,
            "[{}] {} [{}]: {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            level_style.value(record.level()),
            record.target(),
            record.args()
        )
    });

    // 设置日志级别
    builder.filter_module("compare_klines", LevelFilter::Info);
    builder.filter_module("kline_server::klcommon::api", LevelFilter::Info);
    builder.filter_module("kline_server::klcommon::db", LevelFilter::Info);

    // 如果指定了输出文件，则同时输出到文件
    if let Some(file_path) = &output_file {
        // 确保目录存在
        if let Some(parent) = Path::new(file_path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // 创建或截断文件
        let file = File::create(file_path)?;

        // 创建一个写入器，同时写入到标准输出和文件
        let file_writer = std::io::BufWriter::new(file);

        // 使用自定义日志初始化
        builder.target(env_logger::Target::Pipe(Box::new(file_writer))).init();

        info!("日志将输出到文件: {}", file_path);
    } else {
        builder.init();
    }

    info!("启动K线比对工具 - 只显示不一致的比对结果");
    info!("周期: {}", interval);
    info!("测试模式: {}", match test_mode {
        1 => "模式1 - 只测试BTC一个品种",
        2 => "模式2 - 测试6个主要品种",
        _ => "模式3 - 测试所有品种",
    });
    info!("使用方法: compare_klines.exe [周期] [测试模式] [输出文件]");
    info!("  周期: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w (默认: 1m)");
    info!("  测试模式: 1=只测试BTC, 2=测试6个主要品种, 3=测试所有品种 (默认: 3)");
    info!("  输出文件: 可选，指定输出报告的文件路径");
    info!("每分钟的第30秒执行比对");

    // 初始化数据库
    let db = Arc::new(Database::new("data/klines.db")?);
    info!("数据库连接成功");

    // 初始化API客户端
    let api = BinanceApi::new();
    info!("API客户端初始化成功");

    // 根据测试模式选择交易对列表
    let symbols = match test_mode {
        1 => {
            // 模式1：只测试BTC一个品种
            info!("测试模式1：只测试BTC一个品种");
            BTC_ONLY_SYMBOLS.iter().map(|s| s.to_string()).collect()
        },
        2 => {
            // 模式2：测试6个主要品种
            info!("测试模式2：测试6个主要品种");
            MAIN_SYMBOLS.iter().map(|s| s.to_string()).collect()
        },
        _ => {
            // 模式3：测试所有品种
            info!("测试模式3：测试所有品种");
            match api.get_trading_usdt_perpetual_symbols().await {
                Ok(symbols) => {
                    info!("从API获取到 {} 个交易对", symbols.len());
                    symbols
                },
                Err(e) => {
                    warn!("从API获取交易对失败，将使用主要交易对列表: {}", e);
                    MAIN_SYMBOLS.iter().map(|s| s.to_string()).collect()
                }
            }
        }
    };

    info!("将对 {} 个交易对进行比对", symbols.len());
    debug!("交易对列表: {:?}", symbols);

    // 检查所有交易对的数据库表是否存在
    for symbol in &symbols {
        check_database_tables(&db, symbol, &interval).await?;
    }

    // 创建定时器
    let mut interval_timer = time::interval(Duration::from_secs(CHECK_INTERVAL_SECS));

    // 循环比对
    loop {
        // 等待下一个时间点
        interval_timer.tick().await;

        // 获取当前时间
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();

        let now_millis = now.as_millis() as i64;
        let seconds = (now_millis / 1000) % 60; // 当前秒数

        // 只在每分钟的第30秒执行比对
        if seconds != 30 {
            continue;
        }

        let current_time_str = format_timestamp(now_millis);
        info!("开始比对K线数据, 当前时间: {} ({})", now_millis, current_time_str);

        // 记录开始时间，用于计算比对耗时
        let start_time = SystemTime::now();

        // 统计变量
        let mut total_consistent = 0;
        let mut total_inconsistent = 0;
        let mut total_errors = 0;
        let mut total_missing = 0;
        let mut total_compared = 0; // 成功比对的数量

        // 记录不一致和错误的交易对
        let mut inconsistent_symbols = Vec::new();
        let mut error_symbols = Vec::new();

        // 对每个交易对进行比对
        for symbol in &symbols {
            info!("比对交易对: {}", symbol);

            // 从API获取最新的两根K线数据（倒数第二根和最新一根）
            // 传入0作为占位符，实际上不会使用这个时间戳
            match get_kline_from_api(&api, symbol, &interval, 0).await {
                Ok(api_kline) => {
                    // api_kline 现在是倒数第二根K线
                    let api_time_str = format_timestamp(api_kline.open_time);
                    info!("{}: 从API获取到倒数第二根K线: open_time={} ({})",
                          symbol, api_kline.open_time, api_time_str);

                    // 使用API返回的倒数第二根K线时间戳获取数据库中的K线
                    match db.get_kline_by_time(symbol, &interval, api_kline.open_time) {
                        Ok(Some(db_kline)) => {
                            let db_time_str = format_timestamp(db_kline.open_time);
                            info!("{}: 从数据库获取到对应时间戳的K线: open_time={} ({})",
                                 symbol, db_kline.open_time, db_time_str);

                            // 比对K线数据
                            info!("比对 {} 的K线数据: 时间戳={} ({})", symbol, db_kline.open_time, db_time_str);
                            let has_diff = compare_kline_with_symbol(symbol, &db_kline, &api_kline);

                            total_compared += 1;

                            if !has_diff {
                                total_consistent += 1;
                                info!("{}: K线数据比对一致 (open_time={} / {})",
                                     symbol, db_kline.open_time, db_time_str);
                            } else {
                                total_inconsistent += 1;
                                inconsistent_symbols.push(symbol.clone());
                            }
                        },
                        Ok(None) => {
                            info!("{}: 数据库中未找到对应API时间戳的K线 {} ({}), 记录为缺失",
                                 symbol, api_kline.open_time, api_time_str);
                            total_missing += 1;
                        },
                        Err(e) => {
                            error!("{}: 从数据库获取K线失败: {} (时间戳: {} / {})",
                                  symbol, e, api_kline.open_time, api_time_str);
                            total_errors += 1;
                            error_symbols.push(symbol.clone());
                        }
                    }
                },
                Err(e) => {
                    error!("{}: 从API获取K线数据失败: {}", symbol, e);
                    total_errors += 1;
                    error_symbols.push(symbol.clone());
                }
            }
        }

        // 计算比对耗时
        let elapsed = SystemTime::now().duration_since(start_time).unwrap_or(Duration::from_secs(0));
        let elapsed_secs = elapsed.as_secs_f64();

        // 输出精简的统计结果
        let status = if total_inconsistent == 0 && total_errors == 0 {
            "✓ 全部一致"
        } else if total_inconsistent > 0 && total_errors == 0 {
            "⚠ 部分不一致"
        } else {
            "✗ 有错误"
        };

        // 计算比对速度（每秒比对的交易对数）
        let speed = if elapsed_secs > 0.0 {
            total_compared as f64 / elapsed_secs
        } else {
            0.0
        };

        // 输出摘要信息
        info!("========================================================");
        info!("周期 {} 比对完成", interval);
        info!("--------------------------------------------------------");
        info!("耗时: {:.2}秒, 速度: {:.1}对/秒", elapsed_secs, speed);
        info!("总交易对数: {}", symbols.len());
        info!("成功比对数: {}", total_compared);
        info!("数据一致数: {}", total_consistent);
        info!("数据不一致数: {}", total_inconsistent);
        info!("数据缺失数: {}", total_missing);
        info!("发生错误数: {}", total_errors);
        info!("比对结果: {}", status);
        info!("========================================================");

        // 如果有不一致或错误，输出提示
        if total_inconsistent > 0 {
            warn!("发现 {} 个交易对数据不一致，详细信息请查看上方日志", total_inconsistent);
            // 输出不一致交易对的列表
            if !inconsistent_symbols.is_empty() {
                warn!("不一致的交易对: {}", inconsistent_symbols.join(", "));
            }
        }

        if total_errors > 0 {
            error!("发现 {} 个交易对出现错误，详细信息请查看上方日志", total_errors);
            // 输出错误交易对的列表
            if !error_symbols.is_empty() {
                error!("出错的交易对: {}", error_symbols.join(", "));
            }
        }

        // 如果指定了输出文件，将结果写入文件
        if let Some(file_path) = &output_file {
            let mut summary = format!(
                "# K线数据比对报告\n\n## 基本信息\n\n- 周期: {}\n- 测试模式: {}\n- 比对时间: {}\n- 耗时: {:.2}秒\n- 比对速度: {:.1}对/秒\n\n## 比对结果\n\n- 状态: {}\n- 总交易对数: {}\n- 成功比对数: {}\n- 数据一致数: {}\n- 数据不一致数: {}\n- 数据缺失数: {}\n- 发生错误数: {}\n\n",
                interval,
                match test_mode {
                    1 => "模式1 - 只测试BTC一个品种",
                    2 => "模式2 - 测试6个主要品种",
                    _ => "模式3 - 测试所有品种",
                },
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                elapsed_secs,
                speed,
                status,
                symbols.len(),
                total_compared,
                total_consistent,
                total_inconsistent,
                total_missing,
                total_errors
            );

            // 添加不一致交易对列表
            if !inconsistent_symbols.is_empty() {
                summary.push_str("## 不一致的交易对\n\n");
                for symbol in &inconsistent_symbols {
                    summary.push_str(&format!("- {}\n", symbol));
                }
                summary.push_str("\n");
            }

            // 添加错误交易对列表
            if !error_symbols.is_empty() {
                summary.push_str("## 出错的交易对\n\n");
                for symbol in &error_symbols {
                    summary.push_str(&format!("- {}\n", symbol));
                }
                summary.push_str("\n");
            }

            // 添加简短摘要（用于测试脚本解析）
            summary.push_str("## 摘要（用于脚本解析）\n\n");
            summary.push_str(&format!(
                "周期: {}\n测试模式: {}\n状态: {}\n总数: {}\n一致: {}\n不一致: {}\n缺失: {}\n错误: {}\n耗时: {:.2}秒\n",
                interval,
                test_mode,
                status,
                symbols.len(),
                total_consistent,
                total_inconsistent,
                total_missing,
                total_errors,
                elapsed_secs
            ));

            if let Ok(mut file) = std::fs::OpenOptions::new().write(true).open(file_path) {
                let _ = file.write_all(summary.as_bytes());
            }
        }
    }
}

/// 获取上一根已完成K线的开始时间
/// 根据当前时间和周期动态计算上一根已完成K线的开始时间
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

    // 输出计算过程的详细信息，便于调试
    debug!("时间戳计算: 当前时间={} ({}), 当前K线开始={} ({}), 上一K线开始={} ({})",
           now, format_timestamp(now),
           current_kline_start, format_timestamp(current_kline_start),
           last_kline_start, format_timestamp(last_kline_start));

    last_kline_start
}

/// 从API获取最新的两根K线（倒数第二根和最新一根），失败时最多重试5次
async fn get_kline_from_api(api: &BinanceApi, symbol: &str, interval: &str, _open_time: i64) -> Result<Kline> {
    // 创建下载任务 - 只需要limit=2，不需要指定时间范围
    let task = kline_server::klcommon::DownloadTask {
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        limit: 2, // 获取最新的两根K线
        start_time: None, // 不指定开始时间
        end_time: None,   // 不指定结束时间
    };

    info!("从API获取最新两根K线数据: symbol={}, interval={}", symbol, interval);

    // 最大重试次数
    const MAX_RETRIES: usize = 5;
    // 重试延迟（毫秒）
    const RETRY_DELAY_MS: u64 = 500;

    // 尝试从连续合约K线接口获取数据
    let mut last_error = None;
    for retry in 0..MAX_RETRIES {
        match api.download_continuous_klines(&task).await {
            Ok(klines) => {
                if klines.len() >= 2 {
                    // 返回倒数第二根K线（索引0）
                    let kline = &klines[0];
                    info!("从连续合约K线接口获取到倒数第二根K线: symbol={}, interval={}, open_time={} ({})",
                          symbol, interval, kline.open_time, format_timestamp(kline.open_time));

                    // 同时记录最新一根K线的信息（索引1）
                    let latest_kline = &klines[1];
                    info!("最新一根K线信息: symbol={}, interval={}, open_time={} ({})",
                          symbol, interval, latest_kline.open_time, format_timestamp(latest_kline.open_time));

                    return Ok(klines[0].clone()); // 返回倒数第二根K线
                } else if klines.len() == 1 {
                    // 如果只有一根K线，则返回它
                    let kline = &klines[0];
                    warn!("从普通K线接口只获取到一根K线: symbol={}, interval={}, open_time={} ({})",
                          symbol, interval, kline.open_time, format_timestamp(kline.open_time));
                    return Ok(klines[0].clone());
                }
                info!("普通K线接口返回空结果，将尝试连续合约K线接口");
                break; // 如果返回空结果，跳出循环尝试连续合约接口
            },
            Err(e) => {
                last_error = Some(e);
                if retry < MAX_RETRIES - 1 {
                    warn!("{}: 从普通K线接口获取数据失败 (重试 {}/{}): {}",
                          symbol, retry + 1, MAX_RETRIES, last_error.as_ref().unwrap());
                    // 等待一段时间后重试
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                }
            }
        }
    }

    // 如果所有尝试都失败，返回最后一个错误
    Err(last_error.unwrap_or_else(|| AppError::DataError(format!(
        "未获取到K线数据: symbol={}, interval={}",
        symbol, interval
    ))))
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
        info!("最新K线时间戳: {} ({})", timestamp, format_timestamp(timestamp));
    } else {
        info!("表中没有K线数据");
    }

    Ok(())
}

/// 比对两个K线数据（带交易对信息）
///
/// 返回值：如果存在显著差异，返回true；否则返回false
fn compare_kline_with_symbol(symbol: &str, db_kline: &Kline, api_kline: &Kline) -> bool {
    // 检查开盘时间是否一致
    if db_kline.open_time != api_kline.open_time {
        warn!(
            "{}: 开盘时间不一致: 数据库={}, API={}",
            symbol, db_kline.open_time, api_kline.open_time
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

    // 不再输出详细比对结果，只在有显著差异时才输出
    // 这些信息将在下面的差异判断中按需输出

    // 判断是否有显著差异
    const ERROR_THRESHOLD: f64 = 1.0; // 1%的相对误差阈值
    let has_significant_diff = open_rel_error > ERROR_THRESHOLD || high_rel_error > ERROR_THRESHOLD ||
       low_rel_error > ERROR_THRESHOLD || close_rel_error > ERROR_THRESHOLD ||
       volume_rel_error > ERROR_THRESHOLD || quote_volume_rel_error > ERROR_THRESHOLD;

    if has_significant_diff {
        // 构建一行显示所有差异信息
        let mut diff_parts = Vec::new();

        if open_rel_error > ERROR_THRESHOLD {
            diff_parts.push(format!("开:{:.2}%", open_rel_error));
        }
        if high_rel_error > ERROR_THRESHOLD {
            diff_parts.push(format!("高:{:.2}%", high_rel_error));
        }
        if low_rel_error > ERROR_THRESHOLD {
            diff_parts.push(format!("低:{:.2}%", low_rel_error));
        }
        if close_rel_error > ERROR_THRESHOLD {
            diff_parts.push(format!("收:{:.2}%", close_rel_error));
        }
        if volume_rel_error > ERROR_THRESHOLD {
            diff_parts.push(format!("量:{:.2}%", volume_rel_error));
        }
        if quote_volume_rel_error > ERROR_THRESHOLD {
            diff_parts.push(format!("额:{:.2}%", quote_volume_rel_error));
        }

        // 在一行中输出所有差异信息
        warn!("不一致: {} [{}] 时间:{} 差异:[{}]",
              symbol,
              db_kline.open_time,
              format_timestamp(db_kline.open_time),
              diff_parts.join(", "));

        // 输出详细的K线数据对比
        warn!("数据库K线: {} [{}] 开:{} 高:{} 低:{} 收:{} 量:{} 额:{}",
              symbol,
              format_timestamp(db_kline.open_time),
              db_kline.open,
              db_kline.high,
              db_kline.low,
              db_kline.close,
              db_kline.volume,
              db_kline.quote_asset_volume);

        warn!("API K线: {} [{}] 开:{} 高:{} 低:{} 收:{} 量:{} 额:{}",
              symbol,
              format_timestamp(api_kline.open_time),
              api_kline.open,
              api_kline.high,
              api_kline.low,
              api_kline.close,
              api_kline.volume,
              api_kline.quote_asset_volume);
    } else {
        // 数据一致时只输出调试信息
        debug!("{}: K线数据比对一致", symbol);
    }

    // 返回是否有显著差异，方便调用者使用
    has_significant_diff
}

/// 比对两个K线数据（兼容旧版本，不带交易对信息）
///
/// 返回值：如果存在显著差异，返回true；否则返回false
#[allow(dead_code)]
fn compare_klines(db_kline: &Kline, api_kline: &Kline) -> bool {
    // 调用新的带交易对信息的函数，使用默认交易对名称
    compare_kline_with_symbol("未知交易对", db_kline, api_kline)
}

/// 将毫秒时间戳格式化为可读的日期时间字符串
fn format_timestamp(timestamp_ms: i64) -> String {
    let seconds = timestamp_ms / 1000;
    let nanoseconds = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match Utc.timestamp_opt(seconds, nanoseconds) {
        chrono::LocalResult::Single(dt) => {
            // 显示更详细的时间信息，包括秒和毫秒
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        },
        _ => format!("无效时间戳: {}", timestamp_ms)
    }
}