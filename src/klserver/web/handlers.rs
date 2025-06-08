use axum::{
    extract::{Path, State},
    Json,
};
use tracing::{error, info};
use serde::Serialize;
use std::sync::Arc;

use crate::klcommon::Database;

/// Kline response
#[derive(Serialize)]
pub struct KlineResponse {
    pub symbol: String,
    pub interval: String,
    pub data: Vec<KlineData>,
}

/// Kline data
#[derive(Serialize)]
pub struct KlineData {
    pub time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

/// Kline data handler
pub async fn klines_handler(
    Path((symbol, interval)): Path<(String, String)>,
    State(db): State<Arc<Database>>,
) -> Json<KlineResponse> {
    info!("Getting kline data: symbol={}, interval={}", symbol, interval);

    // Get kline data from database
    let klines = match db.get_latest_klines(&symbol, &interval, 100) {
        Ok(klines) => klines,
        Err(e) => {
            error!("Failed to get kline data: {}", e);
            Vec::new()
        }
    };

    // Convert to response format
    let data = klines.into_iter().map(|k| {
        // Ensure timestamp is in seconds, not milliseconds
        let time_in_seconds = if k.open_time > 10000000000 {
            k.open_time / 1000 // If milliseconds, convert to seconds
        } else {
            k.open_time // Already in seconds
        };

        KlineData {
            time: time_in_seconds,
            open: k.open,
            high: k.high,
            low: k.low,
            close: k.close,
            volume: k.volume,
        }
    }).collect();

    Json(KlineResponse {
        symbol: symbol.to_uppercase(),
        interval,
        data,
    })
}
