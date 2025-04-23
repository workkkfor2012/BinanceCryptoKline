use crate::klcommon::{AppError, Kline, Result};


/// 处理K线数据，生成聚合K线
pub fn process_klines(klines: &[Kline], minutes: i64) -> Result<Vec<Kline>> {
    if klines.is_empty() {
        return Ok(Vec::new());
    }

    // 按时间排序
    let mut sorted_klines = klines.to_vec();
    sorted_klines.sort_by_key(|k| k.open_time);

    // 计算时间间隔（毫秒）
    let interval_ms = minutes * 60 * 1000;

    // 分组
    let mut grouped_klines: Vec<Vec<&Kline>> = Vec::new();
    let mut current_group: Vec<&Kline> = Vec::new();
    let mut current_group_start_time = 0;

    for kline in &sorted_klines {
        // 计算K线应该属于的组的开始时间
        let group_start_time = (kline.open_time / interval_ms) * interval_ms;

        if current_group.is_empty() {
            // 第一个K线
            current_group_start_time = group_start_time;
            current_group.push(kline);
        } else if group_start_time == current_group_start_time {
            // 同一组
            current_group.push(kline);
        } else {
            // 新组
            grouped_klines.push(current_group);
            current_group = vec![kline];
            current_group_start_time = group_start_time;
        }
    }

    // 添加最后一组
    if !current_group.is_empty() {
        grouped_klines.push(current_group);
    }

    // 聚合每组K线
    let mut result = Vec::new();

    for group in grouped_klines {
        if group.is_empty() {
            continue;
        }

        // 聚合
        let aggregated = aggregate_klines(&group, minutes)?;
        result.push(aggregated);
    }

    Ok(result)
}

/// 聚合一组K线
fn aggregate_klines(klines: &[&Kline], minutes: i64) -> Result<Kline> {
    if klines.is_empty() {
        return Err(AppError::DataError("没有K线可聚合".to_string()));
    }

    // 计算开盘时间和收盘时间
    let interval_ms = minutes * 60 * 1000;
    let first = klines.first().unwrap();
    let last = klines.last().unwrap();

    let open_time = (first.open_time / interval_ms) * interval_ms;
    let close_time = open_time + interval_ms - 1;

    // 使用第一个K线的开盘价
    let open = first.open.clone();

    // 使用最后一个K线的收盘价
    let close = last.close.clone();

    // 计算最高价和最低价
    let high = klines
        .iter()
        .map(|k| k.high.parse::<f64>().unwrap_or(0.0))
        .fold(f64::MIN, |a, b| a.max(b))
        .to_string();

    let low = klines
        .iter()
        .map(|k| k.low.parse::<f64>().unwrap_or(f64::MAX))
        .fold(f64::MAX, |a, b| a.min(b))
        .to_string();

    // 计算成交量和成交额
    let volume: f64 = klines
        .iter()
        .map(|k| k.volume.parse::<f64>().unwrap_or(0.0))
        .sum();

    let quote_asset_volume: f64 = klines
        .iter()
        .map(|k| k.quote_asset_volume.parse::<f64>().unwrap_or(0.0))
        .sum();

    // 计算成交笔数
    let number_of_trades: i64 = klines
        .iter()
        .map(|k| k.number_of_trades)
        .sum();

    // 计算主动买入成交量和成交额
    let taker_buy_base_asset_volume: f64 = klines
        .iter()
        .map(|k| k.taker_buy_base_asset_volume.parse::<f64>().unwrap_or(0.0))
        .sum();

    let taker_buy_quote_asset_volume: f64 = klines
        .iter()
        .map(|k| k.taker_buy_quote_asset_volume.parse::<f64>().unwrap_or(0.0))
        .sum();

    // 创建聚合K线
    let aggregated = Kline {
        open_time,
        open,
        high,
        low,
        close,
        volume: volume.to_string(),
        close_time,
        quote_asset_volume: quote_asset_volume.to_string(),
        number_of_trades,
        taker_buy_base_asset_volume: taker_buy_base_asset_volume.to_string(),
        taker_buy_quote_asset_volume: taker_buy_quote_asset_volume.to_string(),
        ignore: "0".to_string(),
    };

    Ok(aggregated)
}
