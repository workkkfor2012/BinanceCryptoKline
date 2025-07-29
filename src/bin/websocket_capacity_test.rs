//! WebSocket订阅容量测试程序
//!
//! 测试不同数量品种的WebSocket订阅情况，找出订阅数量的临界点
//! 测试场景：8个品种、100个品种、200个品种、300个品种、400个品种和所有品种

use anyhow::Result;
use kline_server::klcommon::{
    log::{init_ai_logging, shutdown_target_log_sender},
    websocket::{AggTradeClient, AggTradeConfig, AggTradeMessageHandler, WebSocketClient, WsCommand},
};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{error, info};

/// 测试品种数量配置
const TEST_SCENARIOS: &[usize] = &[8]; // 0表示所有品种
//const TEST_SCENARIOS: &[usize] = &[8, 100, 200, 300, 400, 0]; // 0表示所有品种

/// 测试用的8个品种
const TEST_8_SYMBOLS: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "BCHUSDT", "XRPUSDT", 
    "LTCUSDT", "TRXUSDT", "ETCUSDT", "LINKUSDT"
];

/// 从文档中解析所有品种
fn parse_all_symbols() -> Vec<String> {
    // 从docs/所有品种.txt中提取的所有品种
    let all_symbols_raw = r#"["btcusdt@aggTrade","ethusdt@aggTrade","bchusdt@aggTrade","xrpusdt@aggTrade","ltcusdt@aggTrade","trxusdt@aggTrade","etcusdt@aggTrade","linkusdt@aggTrade","xlmusdt@aggTrade","adausdt@aggTrade","xmrusdt@aggTrade","dashusdt@aggTrade","zecusdt@aggTrade","xtzusdt@aggTrade","atomusdt@aggTrade","bnbusdt@aggTrade","ontusdt@aggTrade","iotausdt@aggTrade","batusdt@aggTrade","vetusdt@aggTrade","neousdt@aggTrade","qtumusdt@aggTrade","iostusdt@aggTrade","thetausdt@aggTrade","algousdt@aggTrade","zilusdt@aggTrade","kncusdt@aggTrade","zrxusdt@aggTrade","compusdt@aggTrade","dogeusdt@aggTrade","sxpusdt@aggTrade","kavausdt@aggTrade","bandusdt@aggTrade","rlcusdt@aggTrade","mkrusdt@aggTrade","snxusdt@aggTrade","dotusdt@aggTrade","defiusdt@aggTrade","yfiusdt@aggTrade","crvusdt@aggTrade","trbusdt@aggTrade","runeusdt@aggTrade","sushiusdt@aggTrade","egldusdt@aggTrade","solusdt@aggTrade","icxusdt@aggTrade","storjusdt@aggTrade","uniusdt@aggTrade","avaxusdt@aggTrade","enjusdt@aggTrade","flmusdt@aggTrade","ksmusdt@aggTrade","nearusdt@aggTrade","aaveusdt@aggTrade","filusdt@aggTrade","lrcusdt@aggTrade","rsrusdt@aggTrade","belusdt@aggTrade","axsusdt@aggTrade","alphausdt@aggTrade","zenusdt@aggTrade","sklusdt@aggTrade","grtusdt@aggTrade","1inchusdt@aggTrade","chzusdt@aggTrade","sandusdt@aggTrade","ankrusdt@aggTrade","rvnusdt@aggTrade","sfpusdt@aggTrade","cotiusdt@aggTrade","chrusdt@aggTrade","manausdt@aggTrade","aliceusdt@aggTrade","hbarusdt@aggTrade","oneusdt@aggTrade","dentusdt@aggTrade","celrusdt@aggTrade","hotusdt@aggTrade","mtlusdt@aggTrade","ognusdt@aggTrade","nknusdt@aggTrade","1000shibusdt@aggTrade","icpusdt@aggTrade","bakeusdt@aggTrade","gtcusdt@aggTrade","btcdomusdt@aggTrade","tlmusdt@aggTrade","iotxusdt@aggTrade","c98usdt@aggTrade","maskusdt@aggTrade","atausdt@aggTrade","dydxusdt@aggTrade","1000xecusdt@aggTrade","galausdt@aggTrade","celousdt@aggTrade","arusdt@aggTrade","arpausdt@aggTrade","ctsiusdt@aggTrade","lptusdt@aggTrade","ensusdt@aggTrade","peopleusdt@aggTrade","roseusdt@aggTrade","duskusdt@aggTrade","flowusdt@aggTrade","imxusdt@aggTrade","api3usdt@aggTrade","gmtusdt@aggTrade","apeusdt@aggTrade","woousdt@aggTrade","jasmyusdt@aggTrade","opusdt@aggTrade","injusdt@aggTrade","stgusdt@aggTrade","spellusdt@aggTrade","1000luncusdt@aggTrade","luna2usdt@aggTrade","ldousdt@aggTrade","aptusdt@aggTrade","qntusdt@aggTrade","fetusdt@aggTrade","fxsusdt@aggTrade","hookusdt@aggTrade","magicusdt@aggTrade","tusdt@aggTrade","highusdt@aggTrade","minausdt@aggTrade","astrusdt@aggTrade","phbusdt@aggTrade","gmxusdt@aggTrade","cfxusdt@aggTrade","stxusdt@aggTrade","ssvusdt@aggTrade","ckbusdt@aggTrade","perpusdt@aggTrade","truusdt@aggTrade","lqtyusdt@aggTrade","usdcusdt@aggTrade","arbusdt@aggTrade","idusdt@aggTrade","joeusdt@aggTrade","leverusdt@aggTrade","rdntusdt@aggTrade","hftusdt@aggTrade","xvsusdt@aggTrade","blurusdt@aggTrade","eduusdt@aggTrade","suiusdt@aggTrade","1000pepeusdt@aggTrade","1000flokiusdt@aggTrade","umausdt@aggTrade","nmrusdt@aggTrade","mavusdt@aggTrade","xvgusdt@aggTrade","wldusdt@aggTrade","arkmusdt@aggTrade","pendleusdt@aggTrade","agldusdt@aggTrade","yggusdt@aggTrade","dodoxusdt@aggTrade","bntusdt@aggTrade","oxtusdt@aggTrade","seiusdt@aggTrade","cyberusdt@aggTrade","hifiusdt@aggTrade","arkusdt@aggTrade","bicousdt@aggTrade","bigtimeusdt@aggTrade","waxpusdt@aggTrade","bsvusdt@aggTrade","rifusdt@aggTrade","gasusdt@aggTrade","polyxusdt@aggTrade","powrusdt@aggTrade","tiausdt@aggTrade","cakeusdt@aggTrade","memeusdt@aggTrade","tokenusdt@aggTrade","twtusdt@aggTrade","ordiusdt@aggTrade","steemusdt@aggTrade","ilvusdt@aggTrade","ntrnusdt@aggTrade","beamxusdt@aggTrade","kasusdt@aggTrade","1000bonkusdt@aggTrade","pythusdt@aggTrade","superusdt@aggTrade","ongusdt@aggTrade","ustcusdt@aggTrade","ethwusdt@aggTrade","jtousdt@aggTrade","1000satsusdt@aggTrade","1000ratsusdt@aggTrade","auctionusdt@aggTrade","aceusdt@aggTrade","movrusdt@aggTrade","nfpusdt@aggTrade","aiusdt@aggTrade","xaiusdt@aggTrade","mantausdt@aggTrade","wifusdt@aggTrade","ondousdt@aggTrade","altusdt@aggTrade","lskusdt@aggTrade","jupusdt@aggTrade","zetausdt@aggTrade","roninusdt@aggTrade","dymusdt@aggTrade","omusdt@aggTrade","pixelusdt@aggTrade","strkusdt@aggTrade","glmusdt@aggTrade","portalusdt@aggTrade","axlusdt@aggTrade","tonusdt@aggTrade","myrousdt@aggTrade","metisusdt@aggTrade","aevousdt@aggTrade","vanryusdt@aggTrade","bomeusdt@aggTrade","ethfiusdt@aggTrade","enausdt@aggTrade","wusdt@aggTrade","tnsrusdt@aggTrade","sagausdt@aggTrade","taousdt@aggTrade","omniusdt@aggTrade","rezusdt@aggTrade","bbusdt@aggTrade","notusdt@aggTrade","turbousdt@aggTrade","iousdt@aggTrade","mewusdt@aggTrade","zkusdt@aggTrade","listausdt@aggTrade","zrousdt@aggTrade","renderusdt@aggTrade","bananausdt@aggTrade","gusdt@aggTrade","rareusdt@aggTrade","synusdt@aggTrade","sysusdt@aggTrade","brettusdt@aggTrade","voxelusdt@aggTrade","popcatusdt@aggTrade","sunusdt@aggTrade","dogsusdt@aggTrade","mboxusdt@aggTrade","chessusdt@aggTrade","fluxusdt@aggTrade","bswusdt@aggTrade","neiroethusdt@aggTrade","quickusdt@aggTrade","rplusdt@aggTrade","polusdt@aggTrade","uxlinkusdt@aggTrade","1mbabydogeusdt@aggTrade","neirousdt@aggTrade","kdausdt@aggTrade","fidausdt@aggTrade","catiusdt@aggTrade","fiousdt@aggTrade","ghstusdt@aggTrade","hmstrusdt@aggTrade","reiusdt@aggTrade","cosusdt@aggTrade","eigenusdt@aggTrade","diausdt@aggTrade","1000catusdt@aggTrade","scrusdt@aggTrade","goatusdt@aggTrade","moodengusdt@aggTrade","safeusdt@aggTrade","santosusdt@aggTrade","ponkeusdt@aggTrade","cetususdt@aggTrade","cowusdt@aggTrade","1000000mogusdt@aggTrade","driftusdt@aggTrade","grassusdt@aggTrade","swellusdt@aggTrade","actusdt@aggTrade","pnutusdt@aggTrade","1000xusdt@aggTrade","hippousdt@aggTrade","degenusdt@aggTrade","aktusdt@aggTrade","banusdt@aggTrade","scrtusdt@aggTrade","slerfusdt@aggTrade","1000cheemsusdt@aggTrade","1000whyusdt@aggTrade","chillguyusdt@aggTrade","morphousdt@aggTrade","theusdt@aggTrade","aerousdt@aggTrade","kaiausdt@aggTrade","acxusdt@aggTrade","orcausdt@aggTrade","moveusdt@aggTrade","komausdt@aggTrade","meusdt@aggTrade","raysolusdt@aggTrade","spxusdt@aggTrade","virtualusdt@aggTrade","avausdt@aggTrade","degousdt@aggTrade","velodromeusdt@aggTrade","mocausdt@aggTrade","vanausdt@aggTrade","penguusdt@aggTrade","lumiausdt@aggTrade","usualusdt@aggTrade","aixbtusdt@aggTrade","cgptusdt@aggTrade","fartcoinusdt@aggTrade","kmnousdt@aggTrade","hiveusdt@aggTrade","dexeusdt@aggTrade","dfusdt@aggTrade","phausdt@aggTrade","ai16zusdt@aggTrade","griffainusdt@aggTrade","zerebrousdt@aggTrade","biousdt@aggTrade","alchusdt@aggTrade","cookieusdt@aggTrade","swarmsusdt@aggTrade","sonicusdt@aggTrade","dusdt@aggTrade","promusdt@aggTrade","susdt@aggTrade","arcusdt@aggTrade","avaaiusdt@aggTrade","solvusdt@aggTrade","trumpusdt@aggTrade","melaniausdt@aggTrade","vthousdt@aggTrade","animeusdt@aggTrade","pippinusdt@aggTrade","vineusdt@aggTrade","vvvusdt@aggTrade","berausdt@aggTrade","tstusdt@aggTrade","layerusdt@aggTrade","b3usdt@aggTrade","heiusdt@aggTrade","ipusdt@aggTrade","gpsusdt@aggTrade","shellusdt@aggTrade","kaitousdt@aggTrade","achusdt@aggTrade","redusdt@aggTrade","vicusdt@aggTrade","epicusdt@aggTrade","bmtusdt@aggTrade","mubarakusdt@aggTrade","formusdt@aggTrade","bidusdt@aggTrade","tutusdt@aggTrade","broccoli714usdt@aggTrade","broccolif3busdt@aggTrade","brusdt@aggTrade","plumeusdt@aggTrade","bananas31usdt@aggTrade","sirenusdt@aggTrade","nilusdt@aggTrade","partiusdt@aggTrade","jellyjellyusdt@aggTrade","maviausdt@aggTrade","paxgusdt@aggTrade","walusdt@aggTrade","funusdt@aggTrade","gunusdt@aggTrade","mlnusdt@aggTrade","athusdt@aggTrade","babyusdt@aggTrade","forthusdt@aggTrade","promptusdt@aggTrade","xcnusdt@aggTrade","fheusdt@aggTrade","stousdt@aggTrade","kernelusdt@aggTrade","wctusdt@aggTrade","aergousdt@aggTrade","initusdt@aggTrade","bankusdt@aggTrade","eptusdt@aggTrade","deepusdt@aggTrade","hyperusdt@aggTrade","fisusdt@aggTrade","memefiusdt@aggTrade","jstusdt@aggTrade","signusdt@aggTrade","aiotusdt@aggTrade","ctkusdt@aggTrade","pundixusdt@aggTrade","dolousdt@aggTrade","haedalusdt@aggTrade","sxtusdt@aggTrade","alpineusdt@aggTrade","asrusdt@aggTrade","b2usdt@aggTrade","milkusdt@aggTrade","obolusdt@aggTrade","syrupusdt@aggTrade","doodusdt@aggTrade","ogusdt@aggTrade","skyaiusdt@aggTrade","zkjusdt@aggTrade","nxpcusdt@aggTrade","cvcusdt@aggTrade","agtusdt@aggTrade","aweusdt@aggTrade","busdt@aggTrade","soonusdt@aggTrade","humausdt@aggTrade","ausdt@aggTrade","sophusdt@aggTrade","merlusdt@aggTrade","hypeusdt@aggTrade","bdxnusdt@aggTrade","port3usdt@aggTrade","pufferusdt@aggTrade","1000000bobusdt@aggTrade","lausdt@aggTrade","skateusdt@aggTrade","homeusdt@aggTrade","resolvusdt@aggTrade","sqdusdt@aggTrade","taikousdt@aggTrade","pumpbtcusdt@aggTrade","spkusdt@aggTrade","fusdt@aggTrade","myxusdt@aggTrade","newtusdt@aggTrade","dmcusdt@aggTrade","husdt@aggTrade","olusdt@aggTrade","saharausdt@aggTrade","icntusdt@aggTrade","bullausdt@aggTrade","idolusdt@aggTrade","musdt@aggTrade","tanssiusdt@aggTrade","ainusdt@aggTrade","crossusdt@aggTrade","pumpusdt@aggTrade","cusdt@aggTrade","tacusdt@aggTrade","velvetusdt@aggTrade","erausdt@aggTrade","tausdt@aggTrade","cvxusdt@aggTrade","slpusdt@aggTrade","tagusdt@aggTrade","zorausdt@aggTrade"]"#;
    
    // 解析JSON数组并提取品种名称
    let streams: Vec<String> = serde_json::from_str(all_symbols_raw)
        .expect("解析品种列表失败");
    
    streams.into_iter()
        .map(|stream| {
            // 从 "btcusdt@aggTrade" 提取 "BTCUSDT"
            stream.split('@').next()
                .unwrap_or(&stream)
                .to_uppercase()
        })
        .collect()
}

/// 获取指定数量的测试品种
fn get_test_symbols(count: usize) -> Vec<String> {
    if count == 8 {
        TEST_8_SYMBOLS.iter().map(|s| s.to_string()).collect()
    } else if count == 0 {
        // 0表示所有品种
        parse_all_symbols()
    } else {
        // 取前N个品种
        let all_symbols = parse_all_symbols();
        all_symbols.into_iter().take(count).collect()
    }
}

/// 测试结果统计
#[derive(Debug, Clone)]
struct TestResult {
    symbol_count: usize,
    actual_count: usize,
    total_messages: usize,
    error_count: usize,
    duration_secs: u64,
    avg_rate: f64,
    success: bool,
}

impl TestResult {
    fn new(symbol_count: usize, actual_count: usize, total_messages: usize, error_count: usize, duration_secs: u64) -> Self {
        let avg_rate = if duration_secs > 0 { total_messages as f64 / duration_secs as f64 } else { 0.0 };
        let success = total_messages > 0 && error_count == 0;

        Self {
            symbol_count,
            actual_count,
            total_messages,
            error_count,
            duration_secs,
            avg_rate,
            success,
        }
    }
}

/// 运行单个测试场景
async fn run_test_scenario(symbol_count: usize, test_duration: Duration) -> Result<TestResult> {
    let test_name = if symbol_count == 0 {
        "所有品种".to_string()
    } else {
        format!("{}个品种", symbol_count)
    };

    info!("🚀 开始测试: {}", test_name);

    // 获取测试品种
    let symbols = get_test_symbols(symbol_count);
    let actual_count = symbols.len();

    info!("📊 实际测试品种数量: {}", actual_count);
    info!("📋 前10个品种: {:?}", symbols.iter().take(10).collect::<Vec<_>>());

    // 创建消息处理器
    let message_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    // 创建一个简单的通道来接收交易数据
    let (trade_tx, mut trade_rx) = mpsc::channel(10000);

    // 创建一个空的symbol索引映射用于测试
    let symbol_to_global_index = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    let handler = Arc::new(AggTradeMessageHandler::new(
        trade_tx,
        symbol_to_global_index,
    ));

    // 配置WebSocket客户端
    let config = AggTradeConfig {
        use_proxy: true,
        proxy_addr: "127.0.0.1".to_string(),
        proxy_port: 1080,
        symbols,
    };

    let mut client = AggTradeClient::new_with_handler(config, handler);

    // 获取命令发送端用于动态订阅
    let command_sender = client.get_command_sender()
        .expect("客户端应该有命令发送端");

    // 创建关闭信号
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // 启动客户端
    let client_task = tokio::spawn(async move {
        tokio::select! {
            result = client.start() => {
                if let Err(e) = result {
                    error!("WebSocket客户端错误: {}", e);
                }
            }
            _ = shutdown_rx.changed() => {
                info!("WebSocket客户端收到关闭信号");
            }
        }
    });

    // 启动消息接收任务
    let message_count_clone = message_count.clone();
    let test_name_clone = test_name.clone();
    let mut shutdown_rx_clone = shutdown_tx.subscribe();
    let receive_task = tokio::spawn(async move {
        let mut last_count = 0;
        let mut stats_interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                Some(_trade) = trade_rx.recv() => {
                    // 简单计数，不处理具体数据
                },
                _ = stats_interval.tick() => {
                    let current_count = message_count_clone.load(std::sync::atomic::Ordering::Relaxed);
                    let rate = (current_count - last_count) as f64 / 5.0;
                    info!("📈 [{}] 消息统计: 总计={}, 速率={:.1}/秒", test_name_clone, current_count, rate);
                    last_count = current_count;
                },
                _ = shutdown_rx_clone.changed() => {
                    if *shutdown_rx_clone.borrow() {
                        info!("消息接收任务收到关闭信号");
                        break;
                    }
                }
            }
        }
    });

    // 启动动态添加和删除品种的任务
    let command_sender_clone = command_sender.clone();
    let dynamic_task = tokio::spawn(async move {
        // 等待10秒让WebSocket连接稳定后再动态添加DOGEUSDT
        tokio::time::sleep(Duration::from_secs(10)).await;

        info!("🔄 准备动态添加品种: DOGEUSDT");
        let new_symbols = vec!["DOGEUSDT".to_string()];

        // 检查通道是否还可用
        if command_sender_clone.is_closed() {
            error!("❌ 命令通道已关闭，无法发送动态订阅");
            return;
        }

        match command_sender_clone.send(WsCommand::Subscribe(new_symbols.clone())).await {
            Ok(_) => {
                info!("✅ 动态订阅命令已发送: {:?}", new_symbols);
            }
            Err(e) => {
                error!("❌ 发送动态订阅命令失败: {}", e);
                return;
            }
        }

        // 等待5秒后动态删除DOGEUSDT
        tokio::time::sleep(Duration::from_secs(5)).await;

        info!("🔄 准备动态删除品种: DOGEUSDT");
        let remove_symbols = vec!["DOGEUSDT".to_string()];

        // 检查通道是否还可用
        if command_sender_clone.is_closed() {
            error!("❌ 命令通道已关闭，无法发送动态取消订阅");
            return;
        }

        match command_sender_clone.send(WsCommand::Unsubscribe(remove_symbols.clone())).await {
            Ok(_) => {
                info!("✅ 动态取消订阅命令已发送: {:?}", remove_symbols);
            }
            Err(e) => {
                error!("❌ 发送动态取消订阅命令失败: {}", e);
            }
        }
    });

    // 等待测试时间
    tokio::time::sleep(test_duration).await;

    // 获取最终统计
    let final_message_count = message_count.load(std::sync::atomic::Ordering::Relaxed);
    let final_error_count = error_count.load(std::sync::atomic::Ordering::Relaxed);

    info!("✅ [{}] 测试完成:", test_name);
    info!("   - 品种数量: {}", actual_count);
    info!("   - 总消息数: {}", final_message_count);
    info!("   - 错误数量: {}", final_error_count);
    info!("   - 平均速率: {:.1} 消息/秒", final_message_count as f64 / test_duration.as_secs() as f64);

    // 发送关闭信号
    info!("🔄 [{}] 开始优雅关闭...", test_name);
    let _ = shutdown_tx.send(true);

    // 等待任务完成，设置超时
    let timeout_duration = Duration::from_secs(3);

    match tokio::time::timeout(timeout_duration, client_task).await {
        Ok(_) => info!("✅ [{}] WebSocket客户端已优雅关闭", test_name),
        Err(_) => {
            info!("⚠️ [{}] WebSocket客户端关闭超时，强制终止", test_name);
        }
    }

    match tokio::time::timeout(timeout_duration, receive_task).await {
        Ok(_) => info!("✅ [{}] 消息接收任务已优雅关闭", test_name),
        Err(_) => {
            info!("⚠️ [{}] 消息接收任务关闭超时，强制终止", test_name);
        }
    }

    // 等待动态操作任务完成
    match tokio::time::timeout(timeout_duration, dynamic_task).await {
        Ok(_) => info!("✅ [{}] 动态操作任务已完成", test_name),
        Err(_) => {
            info!("⚠️ [{}] 动态操作任务超时，强制终止", test_name);
        }
    }

    // 额外等待时间确保连接完全关闭
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("🏁 [{}] 测试场景完全结束", test_name);

    // 创建并返回测试结果
    let result = TestResult::new(
        symbol_count,
        actual_count,
        final_message_count,
        final_error_count,
        test_duration.as_secs(),
    );

    Ok(result)
}

fn main() -> Result<()> {
    // 1. 日志系统必须最先初始化 - 采用与klagg_sub_threads.rs相同的模式
    let _guard = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(init_ai_logging())?;

    // 2. 创建主运行时
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // 3. 在运行时中执行异步主逻辑
    let result = runtime.block_on(async_main());

    // 4. 关闭日志系统
    shutdown_target_log_sender();

    result
}

async fn async_main() -> Result<()> {
    info!("🔬 WebSocket订阅容量测试开始");
    info!("📋 测试场景: {:?}", TEST_SCENARIOS);

    // 每个测试运行10秒
    let test_duration = Duration::from_secs(30);
    let mut results = Vec::new();

    for &symbol_count in TEST_SCENARIOS {
        // 运行测试
        match run_test_scenario(symbol_count, test_duration).await {
            Ok(result) => {
                results.push(result);
            }
            Err(e) => {
                error!("测试失败 [{}个品种]: {}", symbol_count, e);
                // 创建失败的测试结果
                let failed_result = TestResult {
                    symbol_count,
                    actual_count: 0,
                    total_messages: 0,
                    error_count: 1,
                    duration_secs: test_duration.as_secs(),
                    avg_rate: 0.0,
                    success: false,
                };
                results.push(failed_result);
            }
        }

        // 测试间隔，让系统恢复
        info!("⏸️  等待5秒后进行下一个测试...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    // 显示汇总结果
    display_summary_results(&results);

    info!("🏁 所有测试完成");
    Ok(())
}

/// 显示测试结果汇总
fn display_summary_results(results: &[TestResult]) {
    info!("📊 ========== 测试结果汇总 ==========");
    info!("| 品种数 | 实际数 | 消息数 | 错误数 | 速率(/s) | 状态 |");
    info!("|--------|--------|--------|--------|----------|------|");

    for result in results {
        let status = if result.success { "✅ 成功" } else { "❌ 失败" };
        let symbol_display = if result.symbol_count == 0 { "全部".to_string() } else { result.symbol_count.to_string() };

        info!("| {:>6} | {:>6} | {:>6} | {:>6} | {:>8.1} | {} |",
            symbol_display,
            result.actual_count,
            result.total_messages,
            result.error_count,
            result.avg_rate,
            status
        );
    }

    info!("=====================================");

    // 分析结果
    let successful_tests: Vec<_> = results.iter().filter(|r| r.success).collect();
    let failed_tests: Vec<_> = results.iter().filter(|r| !r.success).collect();

    info!("🎯 分析结果:");
    info!("   - 成功测试: {}/{}", successful_tests.len(), results.len());
    info!("   - 失败测试: {}/{}", failed_tests.len(), results.len());

    if let Some(max_successful) = successful_tests.iter().max_by_key(|r| r.actual_count) {
        info!("   - 最大成功品种数: {} (消息速率: {:.1}/秒)", max_successful.actual_count, max_successful.avg_rate);
    }

    if let Some(first_failure) = failed_tests.iter().min_by_key(|r| r.actual_count) {
        if first_failure.actual_count > 0 {
            info!("   - 首次失败品种数: {}", first_failure.actual_count);
        }
    }
}
