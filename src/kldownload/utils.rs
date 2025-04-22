use chrono::{DateTime, Duration, TimeZone, Utc};
use log::debug;

/// Convert milliseconds to DateTime
pub fn ms_to_datetime(ms: i64) -> DateTime<Utc> {
    let seconds = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    Utc.timestamp_opt(seconds, nanos).unwrap()
}

/// Convert DateTime to milliseconds
pub fn datetime_to_ms(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

/// Format milliseconds as a human-readable date string
pub fn format_ms(ms: i64) -> String {
    ms_to_datetime(ms).format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Calculate time chunks for downloading data
#[allow(dead_code)]
pub fn calculate_time_chunks(
    start_time: i64,
    end_time: i64,
    interval_ms: i64,
    max_limit: i32,
) -> Vec<(i64, i64)> {
    let max_duration_ms = interval_ms * max_limit as i64;
    let mut chunks = Vec::new();
    let mut current_start = start_time;

    while current_start < end_time {
        let current_end = std::cmp::min(current_start + max_duration_ms, end_time);
        chunks.push((current_start, current_end));
        current_start = current_end;
    }

    debug!(
        "Calculated {} time chunks from {} to {}",
        chunks.len(),
        format_ms(start_time),
        format_ms(end_time)
    );

    chunks
}

/// Get interval duration in milliseconds
#[allow(dead_code)]
pub fn get_interval_ms(interval: &str) -> i64 {
    match interval {
        "1s" => 1_000,
        "1m" => 60_000,
        "3m" => 3 * 60_000,
        "5m" => 5 * 60_000,
        "15m" => 15 * 60_000,
        "30m" => 30 * 60_000,
        "1h" => 60 * 60_000,
        "2h" => 2 * 60 * 60_000,
        "4h" => 4 * 60 * 60_000,
        "6h" => 6 * 60 * 60_000,
        "8h" => 8 * 60 * 60_000,
        "12h" => 12 * 60 * 60_000,
        "1d" => 24 * 60 * 60_000,
        "3d" => 3 * 24 * 60 * 60_000,
        "1w" => 7 * 24 * 60 * 60_000,
        _ => 60 * 60_000, // Default to 1h
    }
}

/// Get default start time (30 days ago)
#[allow(dead_code)]
pub fn get_default_start_time() -> i64 {
    let now = Utc::now();
    let thirty_days_ago = now - Duration::days(30);
    datetime_to_ms(thirty_days_ago)
}

/// Get default end time (now)
pub fn get_default_end_time() -> i64 {
    datetime_to_ms(Utc::now())
}

/// Format duration in milliseconds as a human-readable string
pub fn format_duration_ms(ms: i64) -> String {
    let seconds = ms / 1000;
    let minutes = seconds / 60;
    let hours = minutes / 60;
    let days = hours / 24;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours % 24, minutes % 60, seconds % 60)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes % 60, seconds % 60)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds % 60)
    } else {
        format!("{}s", seconds)
    }
}

/// Format file size in bytes as a human-readable string
#[allow(dead_code)]
pub fn format_file_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if size >= GB {
        format!("{:.2} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.2} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.2} KB", size as f64 / KB as f64)
    } else {
        format!("{} bytes", size)
    }
}

