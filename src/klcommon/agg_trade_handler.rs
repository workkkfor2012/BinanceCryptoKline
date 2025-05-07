// 归集交易消息处理器
use crate::klcommon::{
    Database, Result, KlineBuilder,
    process_agg_trade, parse_agg_trade_message
};
use crate::klcommon::websocket::MessageHandler;
use log::{info, error, debug};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;

/// 归集交易消息处理器
pub struct AggTradeMessageHandler {
    /// 数据库连接
    pub db: Arc<Database>,
    /// K线构建器映射表 (open_time -> KlineBuilder)
    pub kline_builders: Arc<Mutex<HashMap<i64, KlineBuilder>>>,
    /// 时间周期
    pub interval: String,
}

impl AggTradeMessageHandler {
    /// 创建新的归集交易消息处理器
    pub fn new(db: Arc<Database>, interval: String) -> Self {
        Self {
            db,
            kline_builders: Arc::new(Mutex::new(HashMap::new())),
            interval,
        }
    }
}

impl MessageHandler for AggTradeMessageHandler {
    fn handle_message(&self, _connection_id: usize, text: String) -> impl std::future::Future<Output = Result<()>> + Send {
        let db = self.db.clone();
        let kline_builders = self.kline_builders.clone();
        let interval = self.interval.clone();

        async move {
            // 解析归集交易消息
            match parse_agg_trade_message(&text) {
                Ok(Some(agg_trade)) => {
                    debug!("收到归集交易: symbol={}, price={}, quantity={}, time={}",
                        agg_trade.symbol, agg_trade.price, agg_trade.quantity, agg_trade.trade_time);

                    // 处理归集交易数据
                    let mut builders = kline_builders.lock().await;
                    match process_agg_trade(&agg_trade, &mut builders, &interval) {
                        Ok(completed_klines) => {
                            // 处理已完成的K线
                            for (open_time, _) in completed_klines {
                                if let Some(builder) = builders.remove(&open_time) {
                                    match builder.build() {
                                        Ok(kline) => {
                                            // 保存K线到数据库
                                            match db.save_kline(&agg_trade.symbol, &interval, &kline) {
                                                Ok(_) => {
                                                    info!("保存K线成功: symbol={}, interval={}, open_time={}",
                                                        agg_trade.symbol, interval, open_time);
                                                },
                                                Err(e) => {
                                                    error!("保存K线失败: {}", e);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("构建K线失败: {}", e);
                                        }
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            error!("处理归集交易失败: {}", e);
                        }
                    }
                },
                Ok(None) => {
                    // 非归集交易消息，忽略
                },
                Err(e) => {
                    error!("解析消息失败: {}", e);
                }
            }

            Ok(())
        }
    }
}
