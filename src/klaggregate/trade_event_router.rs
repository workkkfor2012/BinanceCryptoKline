//! 交易事件路由模块
//! 
//! 负责将归集交易数据路由到对应的品种聚合器。

use crate::klaggregate::{AggTradeData, SymbolKlineAggregator};
use crate::klcommon::{Result, AppError};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{debug, warn, error, info, instrument};

/// 交易事件路由器
pub struct TradeEventRouter {
    /// 品种到聚合器的映射
    aggregators: Arc<RwLock<HashMap<String, Arc<SymbolKlineAggregator>>>>,
    
    /// 路由统计信息
    route_count: Arc<RwLock<HashMap<String, u64>>>,
    
    /// 错误计数
    error_count: Arc<RwLock<u64>>,
}

impl TradeEventRouter {
    /// 创建新的交易事件路由器
    #[instrument(target = "TradeEventRouter")]
    pub fn new() -> Self {
        info!(target: "TradeEventRouter", event_name = "路由器创建", "创建交易事件路由器");
        
        Self {
            aggregators: Arc::new(RwLock::new(HashMap::new())),
            route_count: Arc::new(RwLock::new(HashMap::new())),
            error_count: Arc::new(RwLock::new(0)),
        }
    }
    
    /// 注册品种聚合器
    #[instrument(target = "TradeEventRouter", fields(symbol = %symbol), skip(self, aggregator), err)]
    pub async fn register_aggregator(
        &self,
        symbol: String,
        aggregator: Arc<SymbolKlineAggregator>,
    ) -> Result<()> {
        info!(target: "TradeEventRouter", event_name = "聚合器注册开始", symbol = %symbol, "注册品种聚合器: symbol={}", symbol);

        let mut aggregators = self.aggregators.write().await;

        if aggregators.contains_key(&symbol) {
            warn!(target: "TradeEventRouter", event_name = "聚合器替换", symbol = %symbol, "品种聚合器已存在，将被替换: symbol={}", symbol);
        }

        aggregators.insert(symbol.clone(), aggregator);

        // 初始化路由计数
        let mut route_count = self.route_count.write().await;
        route_count.insert(symbol.clone(), 0);

        info!(target: "TradeEventRouter", event_name = "聚合器注册完成", symbol = %symbol, "品种聚合器注册完成: symbol={}", symbol);
        Ok(())
    }
    
    /// 取消注册品种聚合器
    #[instrument(target = "TradeEventRouter", fields(symbol = %symbol), skip(self), err)]
    pub async fn unregister_aggregator(&self, symbol: &str) -> Result<()> {
        debug!(target: "TradeEventRouter", "取消注册品种聚合器: symbol={}", symbol);

        let mut aggregators = self.aggregators.write().await;
        let mut route_count = self.route_count.write().await;

        aggregators.remove(symbol);
        route_count.remove(symbol);

        debug!(target: "TradeEventRouter", "品种聚合器取消注册完成: symbol={}", symbol);
        Ok(())
    }
    
    /// 路由交易事件
    #[instrument(target = "TradeEventRouter", fields(symbol = %trade.symbol, price = %trade.price, quantity = %trade.quantity), skip(self), err)]
    pub async fn route_trade_event(&self, trade: AggTradeData) -> Result<()> {
        // 查找对应的聚合器
        let aggregators = self.aggregators.read().await;
        
        if let Some(aggregator) = aggregators.get(&trade.symbol) {
            // 找到对应的聚合器，处理交易
            match aggregator.process_agg_trade(&trade).await {
                Ok(()) => {
                    // 更新路由计数
                    let mut route_count = self.route_count.write().await;
                    *route_count.entry(trade.symbol.clone()).or_insert(0) += 1;

                    // 发出路由成功验证事件
                    info!(target: "TradeEventRouter",
                        event_name = "route_success",
                        symbol = %trade.symbol,
                        price = trade.price,
                        quantity = trade.quantity,
                        "交易事件路由成功"
                    );
                }
                Err(e) => {
                    error!(target: "TradeEventRouter", event_name = "聚合失败", symbol = %trade.symbol, error = %e, "处理品种交易失败");

                    // 发出路由失败验证事件
                    error!(target: "TradeEventRouter",
                        event_name = "route_failure",
                        symbol = %trade.symbol,
                        error = %e,
                        price = trade.price,
                        quantity = trade.quantity,
                        "交易事件路由失败"
                    );

                    // 更新错误计数
                    let mut error_count = self.error_count.write().await;
                    *error_count += 1;

                    return Err(e);
                }
            }
        } else {
            warn!(target: "TradeEventRouter", event_name = "聚合器未找到", symbol = %trade.symbol, "未找到品种聚合器");

            // 发出路由失败验证事件
            error!(target: "TradeEventRouter",
                event_name = "route_failure",
                symbol = %trade.symbol,
                error = "aggregator_not_found",
                price = trade.price,
                quantity = trade.quantity,
                "未找到品种聚合器"
            );

            // 更新错误计数
            let mut error_count = self.error_count.write().await;
            *error_count += 1;

            return Err(AppError::DataError(format!(
                "未找到品种 {} 的聚合器",
                trade.symbol
            )));
        }
        
        Ok(())
    }
    
    /// 批量路由交易事件
    #[instrument(target = "TradeEventRouter", fields(trades_count = trades.len()), skip(self, trades), err)]
    pub async fn route_trade_events(&self, trades: Vec<AggTradeData>) -> Result<()> {
        let mut success_count = 0;
        let mut error_count = 0;
        
        for trade in trades {
            match self.route_trade_event(trade).await {
                Ok(()) => success_count += 1,
                Err(e) => {
                    error_count += 1;
                    error!(target: "TradeEventRouter", "路由交易事件失败: {}", e);
                }
            }
        }

        if error_count > 0 {
            warn!(target: "TradeEventRouter", "批量路由完成，有错误: success_count={}, error_count={}", success_count, error_count);
        } else {
            info!(target: "TradeEventRouter", "批量路由完成: success_count={}", success_count);
        }
        
        Ok(())
    }
    
    /// 获取已注册的品种列表
    pub async fn get_registered_symbols(&self) -> Vec<String> {
        let aggregators = self.aggregators.read().await;
        aggregators.keys().cloned().collect()
    }
    
    /// 获取指定品种的聚合器
    pub async fn get_aggregator(&self, symbol: &str) -> Option<Arc<SymbolKlineAggregator>> {
        let aggregators = self.aggregators.read().await;
        aggregators.get(symbol).cloned()
    }
    
    /// 获取所有聚合器
    pub async fn get_all_aggregators(&self) -> HashMap<String, Arc<SymbolKlineAggregator>> {
        let aggregators = self.aggregators.read().await;
        aggregators.clone()
    }
    
    /// 强制完成所有聚合器的K线
    #[instrument(target = "TradeEventRouter", skip(self), err)]
    pub async fn finalize_all_aggregators(&self) -> Result<()> {
        info!(target: "TradeEventRouter", event_name = "强制完成所有聚合器开始", "强制完成所有聚合器的K线");

        let aggregators = self.aggregators.read().await;
        let mut success_count = 0;
        let mut error_count = 0;

        for (symbol, aggregator) in aggregators.iter() {
            match aggregator.finalize_all_klines().await {
                Ok(()) => {
                    success_count += 1;
                    debug!(target: "TradeEventRouter", event_name = "聚合器强制完成成功", symbol = %symbol, "品种K线强制完成成功");
                }
                Err(e) => {
                    error_count += 1;
                    error!(target: "TradeEventRouter", event_name = "聚合器强制完成失败", symbol = %symbol, error = %e, "品种K线强制完成失败");
                }
            }
        }

        info!(target: "TradeEventRouter", event_name = "强制完成所有聚合器完成", success_count = success_count, error_count = error_count, "强制完成K线操作完成");
        
        if error_count > 0 {
            Err(AppError::DataError(format!(
                "部分聚合器强制完成失败: {} 个失败",
                error_count
            )))
        } else {
            Ok(())
        }
    }
    
    /// 获取路由统计信息
    pub async fn get_statistics(&self) -> RouterStatistics {
        let aggregators = self.aggregators.read().await;
        let route_count = self.route_count.read().await;
        let error_count = self.error_count.read().await;
        
        let total_routes: u64 = route_count.values().sum();
        let registered_symbols = aggregators.len();
        
        RouterStatistics {
            registered_symbols,
            total_routes,
            error_count: *error_count,
            symbol_route_counts: route_count.clone(),
        }
    }
    
    /// 重置统计信息
    pub async fn reset_statistics(&self) {
        let mut route_count = self.route_count.write().await;
        let mut error_count = self.error_count.write().await;
        
        for count in route_count.values_mut() {
            *count = 0;
        }
        *error_count = 0;
        
        info!(target: "TradeEventRouter", event_name = "路由统计重置", "路由统计信息已重置");
    }
    
    /// 检查路由器健康状态
    pub async fn health_check(&self) -> RouterHealthStatus {
        let aggregators = self.aggregators.read().await;
        let error_count = self.error_count.read().await;
        
        let registered_count = aggregators.len();
        let is_healthy = registered_count > 0 && *error_count < 1000; // 错误数少于1000认为健康
        
        RouterHealthStatus {
            is_healthy,
            registered_aggregators: registered_count,
            total_errors: *error_count,
            status_message: if is_healthy {
                "路由器运行正常".to_string()
            } else if registered_count == 0 {
                "没有注册的聚合器".to_string()
            } else {
                format!("错误数过多: {}", *error_count)
            },
        }
    }
}

impl Default for TradeEventRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// 路由统计信息
#[derive(Debug, Clone)]
pub struct RouterStatistics {
    /// 已注册的品种数量
    pub registered_symbols: usize,
    /// 总路由次数
    pub total_routes: u64,
    /// 错误计数
    pub error_count: u64,
    /// 每个品种的路由次数
    pub symbol_route_counts: HashMap<String, u64>,
}

/// 路由器健康状态
#[derive(Debug, Clone)]
pub struct RouterHealthStatus {
    /// 是否健康
    pub is_healthy: bool,
    /// 已注册的聚合器数量
    pub registered_aggregators: usize,
    /// 总错误数
    pub total_errors: u64,
    /// 状态消息
    pub status_message: String,
}
