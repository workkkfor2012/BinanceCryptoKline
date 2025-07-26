

文档：K线聚合服务单线程重构最终方案 (定稿)
1. 目标与原则

目标: 将现有的多线程、分区、绑核的复杂架构，重构为简单、高效、易于维护的单OS线程异步事件驱动架构。

核心原则:

单一所有权: 所有K线状态由一个核心组件（KlineEngine）统一管理，作为全局唯一的数据源。

事件驱动: 系统由统一的事件流（AppEvent）驱动，消除轮询带来的延迟和复杂性。

I/O隔离: 计算逻辑（状态更新）和I/O操作（网络、磁盘）在异步任务中并发执行，但任何可能阻塞事件循环的操作（尤其是同步的磁盘/数据库I/O）必须被移交到专门的线程池（tokio::task::spawn_blocking），这是确保单线程高性能的关键。

简化至上: 移除所有非必要的抽象和组件，如多Runtime、Gateway轮询、多WebSocket连接、复杂的双缓冲快照等。

【共识与确认】: 我们对这些顶层目标和原则完全达成一致。它们是所有后续决策的基石。

2. 核心架构与组件

运行时: 整个应用将运行在Tokio的单线程调度器上，通过在main函数上标注 #[tokio::main(flavor = "current_thread")] 实现。

核心引擎 (KlineEngine): 一个 struct，负责：

持有所有品种、所有周期的K线状态（Vec<KlineState>）。

实现核心的聚合逻辑 (process_trade, process_clock_tick 等)。

管理一个 DirtyTracker 来追踪状态变更，用于持久化。

协调将更新分发给下游消费者（如数据库写入任务、Web服务器）。

统一事件 (AppEvent): 一个 enum，作为驱动 KlineEngine 所有状态变更的唯一入口。

Generated rust
enum AppEvent {
    // 来自WebSocket的实时交易数据
    AggTrade(AggTradeData),

    // 来自品种管理器的动态添加品种指令
    AddSymbol {
        symbol: String,
        // 【最终决定】我们不再需要 global_index，引擎内部自己管理索引映射。
        initial_data: InitialKlineData, // 包含OHLCV等初始数据
        event_time: i64,                // 触发该事件的时间戳，用于精确生成K线
    },

    // 【最终决定】品种移除逻辑暂时作为TODO项，不在本次重构中实现，以保持专注。
    // RemoveSymbol { symbol: String },
}


异步任务 (Tasks):

websocket_task: 管理单一的WebSocket连接。它负责接收所有交易数据，并响应来自 KlineEngine 的动态订阅指令。

symbol_manager_task: 监听 !miniTicker@arr 流，发现新品种，并将其封装为 AppEvent::AddSymbol 发送到主事件通道。

persistence_task: 这是一个逻辑概念，其实现是 KlineEngine 定时调用 tokio::task::spawn_blocking 来执行数据库写入，它本身不是一个长期运行的 tokio::spawn 任务。

web_server_task (可选): 监听状态更新，为可视化前端提供数据。

任务间通信:

主事件通道: mpsc::channel<AppEvent>。所有I/O任务都通过这个通道向 KlineEngine 发送事件。其容量需要合理设置以应对突发流量，同时进行监控。

WebSocket命令通道: mpsc::channel<WsCommand>。KlineEngine 通过这个通道向 websocket_task 发送指令，如订阅新品种。

Generated rust
enum WsCommand {
    Subscribe(Vec<String>), // 参数是 stream 名称，如 "btcusdt@aggTrade"
    // Unsubscribe(Vec<String>), // 同样，取消订阅作为未来TODO
}
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Rust
IGNORE_WHEN_COPYING_END
3. 数据流图 (定稿版)

这张图反映了我们最终确认的数据流：

Generated mermaid
flowchart TD
    subgraph "I/O 异步任务"
        A[websocket_task<br>(1 WebSocket conn)]
        B[symbol_manager_task<br>(!miniTicker@arr)]
    end

    subgraph "核心事件循环 (单线程)"
        C[主事件通道<br>mpsc::channel]
        D{KlineEngine 主循环<br>tokio::select!}
        E[统一的K线状态<br>Vec<KlineState>]
        F[DirtyTracker<br>(记录变更)]
        Cmd[WsCommand Channel]
    end
    
    subgraph "下游消费者 (并发任务)"
        G[spawn_blocking<br>(DB Writer Task)]
        H[Web Server Task<br>(用于可视化)]
    end

    A -- "1. AggTrade Event" --> C
    B -- "2. AddSymbol Event" --> C
    
    C -- "3. engine.run() .recv()" --> D
    
    D -- "4a. event" --> E
    D -- "4b. clock tick" --> E
    
    E -- "修改后 mark_dirty()" --> F
    
    D -- "4c. AddSymbol event" -- WsCommand --> Cmd -- "5. Subscribe" --> A

    D -- "6. 定时/定量触发" --> F
    F -- "collect_dirty()" --> G
    
    D -- "7. 状态变更后" -- "watch::Sender.send()" --> H
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Mermaid
IGNORE_WHEN_COPYING_END
4. 关键技术决策与实现细节 (定稿)

数据库持久化:

【最终决定】: 采用混合触发策略。KlineEngine 的主 select! 循环中包含一个定时器（如每5秒），同时会检查脏数据数量。“每5秒或脏数据达到1000条”，满足任一条件即触发持久化。

实现: 触发后，调用 engine.persist_dirty_klines()。此方法从 DirtyTracker 取出(take) 脏数据，构建 Vec<DbKline>，然后调用 tokio::task::spawn_blocking 将其和DB连接句柄移交到阻塞线程池执行同步写入。该模式完美平衡了延迟和吞吐量。

脏数据追踪:

【最终决定】: 废弃 Snapshotter，采用更轻量的 DirtyTracker。

实现: DirtyTracker 包含 updated_indices: Vec<usize> 和一个位图（推荐使用 bitvec::BitVec 以节约内存）。process_trade 等方法修改 KlineState 后，O(1)复杂度调用 dirty_tracker.mark_dirty(kline_offset)。接口设计上，提供一个 take_dirty() 方法原子性地返回脏数据并重置内部状态。

时钟逻辑:

【最终决定】: 移除独立时钟任务，整合到 KlineEngine 主循环。

实现: 在 KlineEngine::run 中使用 tokio::time::interval_at 创建对齐到“每分钟第0秒”的 Interval 定时器，并在 select! 中 await 它的 .tick()。

可视化数据分发:

【最终决定】: 使用 tokio::sync::watch 通道。

实现: KlineEngine 持有 watch::Sender<Arc<Vec<KlineState>>>。在状态可能变更的逻辑（如 process_trade, process_clock_tick）执行后，发送一次完整的状态快照。使用 Arc 避免了深拷贝。

启动时状态加载与内存预分配:

【最终决定】: 应用启动时，在 main 函数的 async 上下文中，必须先完成所有的数据准备工作，然后再创建和启动 KlineEngine 和其他任务。

实现:

在 main 中，顺序 await 执行 KlineBackfiller 的逻辑，完成历史数据补全和追赶。

await 执行 load_latest_klines_from_db()，获得一份完整的、初始的 HashMap<(String, String), DbKline>。

根据配置文件中的 max_symbols，预估并构建一个初始的 Vec<KlineState> 和 symbol -> index 的 HashMap。

将这份包含了所有历史状态的 Vec<KlineState> move 进 KlineEngine 的构造函数中。

KlineEngine 在 new() 的时候，根据传入的 Vec 的 capacity()，为其内部所有需要与K线槽位一一对应的结构（如 DirtyTracker 的位图）一次性分配好所有内存 (with_capacity)。

收益: 此模式彻底消除了启动时的竞态条件，并避免了运行时动态添加品种可能引发的、阻塞事件循环的内存重分配。

5. 模块与文件结构变更 (定稿)

src/bin/klagg_sub_threads.rs -> 重命名为 src/bin/klagg.rs。

职责: 初始化日志、配置。完成上述第4.5点描述的所有数据回填和初始状态加载。创建 KlineEngine 实例。创建并 tokio::spawn 所有I/O任务 (websocket_task, symbol_manager_task 等)。最后，在主任务中调用 kline_engine.run().await 并管理整个应用的优雅关闭。

src/klagg_sub_threads/ -> 目录重命名为 src/engine/。

mod.rs: 将定义 KlineEngine, AppEvent, WsCommand, DirtyTracker 等核心类型，并包含 KlineEngine 的 new() 和 run() 实现。

tasks.rs (新文件): 存放 websocket_task, symbol_manager_task, web_server_task 等异步任务的实现代码，保持 bin/klagg.rs 和 engine/mod.rs 的整洁。

src/klagg_sub_threads/gateway.rs -> 彻底删除。

src/klcommon/websocket.rs -> 大幅简化。

移除所有多连接、流分区逻辑。

ConnectionManager 只需负责维护单一连接的生命周期（连接、断线重连）。

客户端（如 run_io_loop 的替代者 websocket_task）需要能接收 WsCommand 并向WebSocket发送动态订阅消息。

总结与下一步

这份“定稿”方案融合了我们所有的讨论，形成了一份清晰、详尽、可行且高性能的重构蓝图。我们对所有关键点达成了高度共识，并明确了实现细节。

我们已经为编码阶段做好了充分的准备。

如果对这份最终方案没有异议，我将开始为你提供第一批具体的代码修改建议。我们将从最顶层的 main 函数（即 src/bin/klagg_sub_threads.rs）入手，按照我们新方案的设计，大刀阔斧地简化它，并为我们新的 KlineEngine 铺好道路。

