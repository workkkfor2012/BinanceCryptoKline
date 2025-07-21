//! 扁平化重构功能测试
//!
//! 测试目标：
//! 1. 验证kline_expirations数组与kline_states的同步性
//! 2. 验证线性扫描的时钟处理逻辑正确性
//! 3. 验证动态添加品种时到期时间设置正确性

#[cfg(test)]
mod tests {
    use crate::klagg_sub_threads::{Worker, WorkerCmd, InitialKlineData, KlineState, KlineData};
    use crate::klcommon::{
        api::interval_to_milliseconds,
        models::Kline as DbKline,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::{mpsc, watch, RwLock, oneshot};

    /// 创建测试用的DbKline
    fn create_test_db_kline(open_time: i64) -> DbKline {
        DbKline {
            open_time,
            open: "100.0".to_string(),
            high: "105.0".to_string(),
            low: "95.0".to_string(),
            close: "102.0".to_string(),
            volume: "1000.0".to_string(),
            close_time: open_time + 60000 - 1,
            quote_asset_volume: "102000.0".to_string(),
            number_of_trades: 50,
            taker_buy_base_asset_volume: "600.0".to_string(),
            taker_buy_quote_asset_volume: "61200.0".to_string(),
            ignore: "0".to_string(),
        }
    }

    /// 基础测试：验证扁平化重构的基本功能
    #[tokio::test]
    async fn test_basic_flattened_structure() {
        // 简单测试：验证kline_expirations数组的基本功能
        let periods = Arc::new(vec!["1m".to_string(), "5m".to_string()]);
        let symbols = vec!["BTCUSDT".to_string()];

        let initial_klines = HashMap::new(); // 空的初始数据

        let mut symbol_map = HashMap::new();
        symbol_map.insert("BTCUSDT".to_string(), 0);
        let symbol_to_global_index = Arc::new(RwLock::new(symbol_map));

        let (clock_tx, clock_rx) = watch::channel(0i64);

        // 创建Worker
        let (worker, _ws_rx, _trade_rx) = Worker::new(
            0,
            0,
            &symbols,
            symbol_to_global_index,
            periods.clone(),
            None,
            clock_rx,
            Arc::new(initial_klines),
        ).await.unwrap();

        // 验证基本结构
        assert_eq!(worker.kline_states.len(), worker.kline_expirations.len(), "kline_states和kline_expirations长度应该相等");
        assert_eq!(worker.kline_states.len(), symbols.len() * periods.len(), "数组长度应该等于品种数×周期数");

        // 验证未初始化的K线有哨兵值
        for expiration in &worker.kline_expirations {
            assert_eq!(*expiration, i64::MAX, "未初始化的K线应该有哨兵值i64::MAX");
        }

        println!("✅ 基础结构测试通过");
    }

    /// 测试Worker初始化时kline_expirations的正确性
    #[tokio::test]
    async fn test_kline_expirations_initialization() {
        let periods = Arc::new(vec!["1m".to_string(), "5m".to_string(), "1h".to_string()]);
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        
        // 创建初始K线数据
        let mut initial_klines = HashMap::new();
        let base_time = 1700000000000i64; // 固定时间戳
        
        for symbol in &symbols {
            for period in periods.iter() {
                let aligned_time = (base_time / interval_to_milliseconds(period)) * interval_to_milliseconds(period);
                initial_klines.insert(
                    (symbol.clone(), period.clone()),
                    create_test_db_kline(aligned_time)
                );
            }
        }

        // 创建symbol_to_global_index映射
        let mut symbol_map = HashMap::new();
        symbol_map.insert("BTCUSDT".to_string(), 0);
        symbol_map.insert("ETHUSDT".to_string(), 1);
        let symbol_to_global_index = Arc::new(RwLock::new(symbol_map));

        let (clock_tx, clock_rx) = watch::channel(0i64);
        
        // 创建Worker
        let (worker, _ws_rx, _trade_rx) = Worker::new(
            0,
            0,
            &symbols,
            symbol_to_global_index,
            periods.clone(),
            None,
            clock_rx,
            Arc::new(initial_klines),
        ).await.unwrap();

        // 验证kline_expirations数组的正确性
        let num_periods = periods.len();
        for (symbol_idx, symbol) in symbols.iter().enumerate() {
            for (period_idx, period) in periods.iter().enumerate() {
                let kline_offset = symbol_idx * num_periods + period_idx;
                let kline = &worker.kline_states[kline_offset];
                let expiration = worker.kline_expirations[kline_offset];
                
                if kline.is_initialized {
                    let expected_expiration = kline.open_time + interval_to_milliseconds(period);
                    assert_eq!(
                        expiration, 
                        expected_expiration,
                        "品种 {} 周期 {} 的到期时间不正确: 期望 {}, 实际 {}",
                        symbol, period, expected_expiration, expiration
                    );
                    println!("✅ {} {} 到期时间正确: {}", symbol, period, expiration);
                } else {
                    assert_eq!(expiration, i64::MAX, "未初始化的K线应该有哨兵值");
                }
            }
        }
    }

    /// 测试时钟处理逻辑的正确性
    #[tokio::test]
    async fn test_clock_tick_processing() {
        let periods = Arc::new(vec!["1m".to_string(), "5m".to_string()]);
        let symbols = vec!["BTCUSDT".to_string()];
        
        let mut initial_klines = HashMap::new();
        let base_time = 1700000000000i64;
        
        // 创建一个1分钟K线，开始时间为整分钟
        let aligned_1m = (base_time / 60000) * 60000;
        initial_klines.insert(
            ("BTCUSDT".to_string(), "1m".to_string()),
            create_test_db_kline(aligned_1m)
        );
        
        // 创建一个5分钟K线
        let aligned_5m = (base_time / 300000) * 300000;
        initial_klines.insert(
            ("BTCUSDT".to_string(), "5m".to_string()),
            create_test_db_kline(aligned_5m)
        );

        let mut symbol_map = HashMap::new();
        symbol_map.insert("BTCUSDT".to_string(), 0);
        let symbol_to_global_index = Arc::new(RwLock::new(symbol_map));

        let (clock_tx, clock_rx) = watch::channel(0i64);
        
        let (mut worker, _ws_rx, _trade_rx) = Worker::new(
            0,
            0,
            &symbols,
            symbol_to_global_index,
            periods.clone(),
            None,
            clock_rx,
            Arc::new(initial_klines),
        ).await.unwrap();

        // 记录初始状态
        let initial_1m_open_time = worker.kline_states[0].open_time; // 1m K线在offset 0
        let initial_5m_open_time = worker.kline_states[1].open_time; // 5m K线在offset 1
        
        println!("初始1m K线开始时间: {}", initial_1m_open_time);
        println!("初始5m K线开始时间: {}", initial_5m_open_time);

        // 模拟时钟滴答：1分钟后
        let tick_time = aligned_1m + 60000;
        worker.process_clock_tick(tick_time);

        // 验证1分钟K线已经更新
        assert_eq!(worker.kline_states[0].open_time, initial_1m_open_time + 60000, "1m K线应该已经更新到下一个周期");
        assert_eq!(worker.kline_states[0].is_final, false, "新K线不应该是final状态");
        assert_eq!(worker.kline_expirations[0], initial_1m_open_time + 120000, "1m K线的新到期时间应该正确");
        
        // 验证5分钟K线还没有更新（因为还没到期）
        assert_eq!(worker.kline_states[1].open_time, initial_5m_open_time, "5m K线还不应该更新");
        
        println!("✅ 时钟处理逻辑测试通过");
    }

    /// 测试动态添加品种的功能
    #[tokio::test]
    async fn test_dynamic_symbol_addition() {
        let periods = Arc::new(vec!["1m".to_string(), "5m".to_string()]);
        let symbols = vec!["BTCUSDT".to_string()];
        
        let initial_klines = HashMap::new(); // 空的初始数据
        
        let mut symbol_map = HashMap::new();
        symbol_map.insert("BTCUSDT".to_string(), 0);
        let symbol_to_global_index = Arc::new(RwLock::new(symbol_map));

        let (clock_tx, clock_rx) = watch::channel(0i64);
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        
        let (mut worker, _ws_rx, _trade_rx) = Worker::new(
            0,
            0,
            &symbols,
            symbol_to_global_index.clone(),
            periods.clone(),
            Some(cmd_rx),
            clock_rx,
            Arc::new(initial_klines),
        ).await.unwrap();

        // 设置当前时钟
        let current_time = 1700000000000i64;
        worker.last_clock_tick = current_time;

        // 动态添加新品种
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let initial_data = InitialKlineData {
            open: 50000.0,
            high: 51000.0,
            low: 49000.0,
            close: 50500.0,
            volume: 100.0,
        };

        // 更新全局索引
        {
            let mut guard = symbol_to_global_index.write().await;
            guard.insert("ETHUSDT".to_string(), 1);
        }

        let cmd = WorkerCmd::AddSymbol {
            symbol: "ETHUSDT".to_string(),
            global_index: 1,
            initial_data,
            ack: ack_tx,
        };

        worker.process_command(cmd).await;
        let result = ack_rx.await.unwrap();
        assert!(result.is_ok(), "动态添加品种应该成功");

        // 验证新品种的K线状态和到期时间
        let num_periods = periods.len();
        let eth_base_offset = 1 * num_periods; // ETHUSDT的本地索引是1

        for (period_idx, period) in periods.iter().enumerate() {
            let kline_offset = eth_base_offset + period_idx;
            let kline = &worker.kline_states[kline_offset];
            let expiration = worker.kline_expirations[kline_offset];
            
            assert!(kline.is_initialized, "新添加品种的K线应该已初始化");
            assert!(!kline.is_final, "新添加品种的K线不应该是final状态");
            
            let expected_expiration = kline.open_time + interval_to_milliseconds(period);
            assert_eq!(
                expiration, 
                expected_expiration,
                "新添加品种 {} 周期 {} 的到期时间不正确",
                "ETHUSDT", period
            );
            
            println!("✅ 新品种 ETHUSDT {} 到期时间正确: {}", period, expiration);
        }
    }

    /// 性能基准测试：对比线性扫描 vs 理论嵌套循环的性能
    #[tokio::test]
    async fn test_performance_benchmark() {
        use std::time::Instant;
        
        let periods = Arc::new(vec!["1m".to_string(), "5m".to_string(), "30m".to_string(), "1h".to_string()]);
        let num_symbols = 1000; // 测试1000个品种
        
        // 创建大量测试数据
        let symbols: Vec<String> = (0..num_symbols)
            .map(|i| format!("TEST{}USDT", i))
            .collect();
            
        let mut initial_klines = HashMap::new();
        let base_time = 1700000000000i64;
        
        for symbol in &symbols {
            for period in periods.iter() {
                let aligned_time = (base_time / interval_to_milliseconds(period)) * interval_to_milliseconds(period);
                initial_klines.insert(
                    (symbol.clone(), period.clone()),
                    create_test_db_kline(aligned_time)
                );
            }
        }

        let mut symbol_map = HashMap::new();
        for (idx, symbol) in symbols.iter().enumerate() {
            symbol_map.insert(symbol.clone(), idx);
        }
        let symbol_to_global_index = Arc::new(RwLock::new(symbol_map));

        let (clock_tx, clock_rx) = watch::channel(0i64);
        
        let (mut worker, _ws_rx, _trade_rx) = Worker::new(
            0,
            0,
            &symbols,
            symbol_to_global_index,
            periods.clone(),
            None,
            clock_rx,
            Arc::new(initial_klines),
        ).await.unwrap();

        // 性能测试：多次运行时钟处理
        let test_iterations = 100;
        let tick_time = base_time + 60000; // 1分钟后
        
        let start = Instant::now();
        for _ in 0..test_iterations {
            worker.process_clock_tick(tick_time);
        }
        let duration = start.elapsed();
        
        println!("🚀 性能测试结果:");
        println!("   品种数量: {}", num_symbols);
        println!("   周期数量: {}", periods.len());
        println!("   总K线数: {}", num_symbols * periods.len());
        println!("   测试迭代: {}", test_iterations);
        println!("   总耗时: {:?}", duration);
        println!("   平均每次: {:?}", duration / test_iterations);
        println!("   每秒处理: {:.0} 次时钟滴答", test_iterations as f64 / duration.as_secs_f64());
        
        // 基本性能断言：每次时钟处理应该在合理时间内完成
        let avg_per_tick = duration / test_iterations;
        assert!(avg_per_tick.as_millis() < 10, "每次时钟处理应该在10ms内完成，实际: {:?}", avg_per_tick);
        
        println!("✅ 性能测试通过");
    }

    /// SIMD vs 标准版本性能对比测试
    #[tokio::test]
    async fn test_simd_vs_standard_performance() {
        use std::time::Instant;

        let periods = Arc::new(vec!["1m".to_string(), "5m".to_string(), "15m".to_string(), "1h".to_string()]);
        let num_symbols = 2000; // 测试2000个品种

        // 创建大量测试数据
        let symbols: Vec<String> = (0..num_symbols)
            .map(|i| format!("TEST{}USDT", i))
            .collect();

        let mut initial_klines = HashMap::new();
        let base_time = 1700000000000i64;

        for symbol in &symbols {
            for period in periods.iter() {
                let aligned_time = (base_time / interval_to_milliseconds(period)) * interval_to_milliseconds(period);
                initial_klines.insert(
                    (symbol.clone(), period.clone()),
                    create_test_db_kline(aligned_time)
                );
            }
        }

        let mut symbol_map = HashMap::new();
        for (idx, symbol) in symbols.iter().enumerate() {
            symbol_map.insert(symbol.clone(), idx);
        }
        let symbol_to_global_index = Arc::new(RwLock::new(symbol_map));

        let (clock_tx, clock_rx) = watch::channel(0i64);

        let (mut worker, _ws_rx, _trade_rx) = Worker::new(
            0,
            0,
            &symbols,
            symbol_to_global_index,
            periods.clone(),
            None,
            clock_rx,
            Arc::new(initial_klines),
        ).await.unwrap();

        // 性能测试：多次运行时钟处理
        let test_iterations = 50;
        let tick_time = base_time + 60000; // 1分钟后，触发1分钟K线到期

        println!("🚀 SIMD vs 标准版本性能对比测试:");
        println!("   品种数量: {}", num_symbols);
        println!("   周期数量: {}", periods.len());
        println!("   总K线数: {}", num_symbols * periods.len());
        println!("   测试迭代: {}", test_iterations);

        #[cfg(feature = "simd")]
        {
            println!("   当前编译版本: SIMD优化版本");
        }
        #[cfg(not(feature = "simd"))]
        {
            println!("   当前编译版本: 标准版本");
        }

        let start = Instant::now();
        let mut total_processed = 0;

        for i in 0..test_iterations {
            // 每次测试前重置一些K线状态，确保有K线需要处理
            if i % 10 == 0 {
                // 每10次迭代重置一些K线的到期时间
                for j in 0..100 {
                    if j < worker.kline_expirations.len() {
                        worker.kline_expirations[j] = tick_time - 1000; // 设置为即将到期
                        worker.kline_states[j].is_final = false; // 重置final状态
                    }
                }
            }

            let processed = {
                #[cfg(feature = "simd")]
                {
                    worker.process_clock_tick_simd(
                        tick_time,
                        HashMap::new(),
                        periods.len(),
                        num_symbols * periods.len()
                    )
                }
                #[cfg(not(feature = "simd"))]
                {
                    worker.process_clock_tick_standard(
                        tick_time,
                        HashMap::new(),
                        periods.len(),
                        num_symbols * periods.len()
                    )
                }
            };

            total_processed += processed;
        }

        let duration = start.elapsed();

        println!("📊 性能测试结果:");
        println!("   总耗时: {:?}", duration);
        println!("   平均每次: {:?}", duration / test_iterations);
        println!("   总处理K线数: {}", total_processed);
        println!("   平均每次处理: {:.1}", total_processed as f64 / test_iterations as f64);
        println!("   每秒处理: {:.0} 次时钟滴答", test_iterations as f64 / duration.as_secs_f64());
        println!("   每微秒扫描K线数: {:.1}", (num_symbols * periods.len()) as f64 / duration.as_micros() as f64 * test_iterations as f64);

        // 基本性能断言：每次时钟处理应该在合理时间内完成
        let avg_per_tick = duration / test_iterations;
        assert!(avg_per_tick.as_millis() < 50, "每次时钟处理应该在50ms内完成，实际: {:?}", avg_per_tick);

        println!("✅ SIMD vs 标准版本性能对比测试完成");
    }
}
