好的，这是我们讨论并达成共识后的完整版架构文档，格式为 Markdown。

Generated markdown
# 高性能K线聚合服务架构文档 (完全分区模型 - v3.3 - 共识版)

## 1. 核心设计原则

*   **静态分区 (Static Partitioning)**: 系统启动时，所有交易对根据其稳定不变的 `global_index`（基于上币时间排序）被静态地、平均地分配到固定数量的 Worker 中。
*   **线程隔离 (Thread Isolation)**: 每个 Worker 是一个独立的异步任务，并被设计为可以绑定到单个物理CPU核心上，以最大化缓存效率并消除跨核调度开销。
*   **无共享写入 (Share-Nothing Write Path)**: 在数据聚合的热路径上，每个Worker只操作其私有内存，不存在任何跨线程的锁、原子操作或数据争用。
*   **责任分离 (Separation of Concerns)**: 系统的各个功能被明确分离到独立的后台任务中：Worker负责计算，PersistenceTask负责持久化，SymbolManager负责发现，GlobalClock负责同步。
*   **主动推送快照 (Active Snapshot Push Model)**: 外部系统（如持久化任务、API服务）通过 `WorkerReadHandle` 发送数据请求。Worker在收到请求后，主动筛选出**已更新的K线数据**，形成快照返回给请求方，并管理自身状态的流转。

## 2. 系统架构图

```mermaid
graph TD
    subgraph "系统引导与初始化 (main)"
        Init(启动) -- 1. 创建全局服务 --> G_Services[Arc<Config>, Arc<Database>, Arc<ServerTimeSyncManager>]
        G_Services -- 2. 初始化全局索引 --> G_IndexMap[G_IndexToSymbol, G_SymbolToIndex, G_SymbolCount]
        G_IndexMap -- 3. 计算静态分区 --> Partitions[为4个Worker分配品种列表和起始索引]
        Partitions -- 4. 创建全局通信设施 --> Channels[G_Clock_Ch, G_W3_Cmd_Ch, G_WorkerReadHandles]
        Channels -- 5. 启动所有后台任务 --> Tasks
    end

    subgraph "全局共享服务与资源 (只读或带锁)"
        G_Config[Arc<Config>]
        G_DB_Pool[Arc<Database>]
        G_STS_Manager[Arc<ServerTimeSyncManager>]
        G_IndexToSymbol[Arc<Vec<String>>]
        G_SymbolToIndex[Arc<RwLock<HashMap<String, usize>>>]
        G_Clock_Tx[watch::Sender (全局时钟)]
        G_W3_Cmd_Tx[mpsc::Sender<WorkerCmd>]
        G_WorkerReadHandles[Arc<Vec<WorkerReadHandle>>]
    end
    
    subgraph "独立后台服务 (Async Tasks)"
        Clock_Task(全局时钟) -- 使用 --> G_STS_Manager
        Clock_Task -- 广播节拍 --> G_Clock_Tx

        SymbolManager_Task(品种管理器) -- 发现新品种 --> MiniTickerWS
        SymbolManager_Task -- "发送指令到固定通道" --> G_W3_Cmd_Tx

        Persistence_Task(持久化任务) -- "每30s轮询" --> G_WorkerReadHandles
        Persistence_Task -- "批量写入" --> G_DB_Pool
    end

    subgraph "工作线程分区 (4个固定Worker, 绑定核心)"
        subgraph "Worker 0 (Core 0)"
            W0_Logic(聚合循环, 无指令通道)
            W0_Logic -- 1. 独立建连/订阅 --> W0_WS[WebSocket Client]
            W0_WS -- aggTrade数据流 --> W0_Logic
            W0_Logic -- 2. 监听全局时钟 --> G_Clock_Rx[watch::Receiver]
            W0_Logic -- 3. 独占读写 --> W0_State[本地内存状态]
            W0_Logic -- 4. 响应外部读取 --> W0_ReadHandle[WorkerReadHandle]
        end
        subgraph "Worker 1 (Core 1)"
            W1_Logic(聚合循环, 无指令通道)
            W1_Logic -- ... --> W1_WS
        end
        subgraph "Worker 2 (Core 2)"
            W2_Logic(聚合循环, 无指令通道)
            W2_Logic -- ... --> W2_WS
        end
        subgraph "Worker 3 (Core 3) - 新品种处理器"
            W3_Logic(聚合循环)
            W3_CmdRx[mpsc::Receiver] --> W3_Logic
            W3_Logic -- ... --> W3_WS
        end
    end

    %% 连接关系
    G_WorkerReadHandles -- 注册 --> W0_ReadHandle & W1_Logic & W2_Logic & W3_Logic

3. 核心组件详述
Worker

系统的核心计算单元，固定启动 N 个实例 (当前实现为4个，对应4个物理核心)。

职责:

网络I/O: 独立管理一个WebSocket连接，仅订阅其被分配到的品种分区。

K线聚合: 在本地内存中完成K线聚合计算。

状态同步: 监听全局时钟信号以完结和创建新的K线。

数据服务: 通过 WorkerReadHandle 对外提供已更新的数据快照，并管理数据状态。

特殊职责 (仅最后一个Worker, e.g., Worker 3):

额外监听一个专用的指令通道，负责处理系统所有动态添加新品种的请求。

内部状态 (非并发安全类型):

partition_start_index: usize: (核心参数) 此 Worker 负责的起始全局索引，用于计算本地索引。

kline_states: Vec<KlineState>: K线聚合的实时计算状态。

(优化) 此结构体为轻量化设计，不包含 symbol_index 和 period_index，这些信息由其在Vec中的位置隐含。Vec的索引通过 local_symbol_index * num_periods + period_offset 计算得出。

(决策) 此数组在启动时预分配固定的巨大容量 (e.g., 10000个品种槽位)，以避免运行时动态扩容和配置复杂性。

buffers: (Vec<KlineData>, Vec<KlineData>): 用于快照的双缓冲结果区，同样预分配固定大容量。

active_buffer_index: usize: 指向当前活动写缓冲区的索引。

WorkerReadHandle

Worker 暴露给外部的、线程安全的只读句柄。

职责: 封装一个请求-响应通道，允许外部任务（如PersistenceTask）安全地请求Worker的已更新的K线数据快照。返回的快照中不包含 global_index，因为 symbol 字符串对于外部服务更直接有用。

PersistenceTask

全局唯一的持久化任务。

职责: 按固定周期（e.g., 30秒）被唤醒，通过 G_WorkerReadHandles 并发地从所有Worker拉取已更新的 (updated) 数据快照，然后将收集到的数据通过 Arc<Database> 一次性批量写入数据库。

SymbolManager

全局唯一的品种发现与管理任务。

职责:

监听 !miniTicker@arr 流发现新品种。

获取 G_SymbolToIndex 的全局写锁，为新品种分配新的、递增的 global_index。

通过专属于最后一个Worker的指令通道 (G_W3_Cmd_Tx)，将 ADD_SYMBOL 指令固定地发送出去。

GlobalClock

全局唯一的时钟同步任务。

职责: 基于ServerTimeSyncManager提供的服务器校准时间，生成统一、精确的K线完结/新建信号，并通过watch通道广播给所有Worker。

4. 核心流程
初始化

main函数创建所有全局服务（Database, ServerTimeSyncManager, Config等）的Arc引用。

调用initialize_symbol_indexing获取基于上币时间排序的全局品种列表和索引映射。

根据总品种数S和固定的Worker数W，计算出每个Worker负责的global_index范围和品种列表。

main函数循环W次，每次启动一个Worker任务。为其传递其worker_id、分区起始索引 partition_start_index、所负责的品种列表、全局服务引用和通信通道。每个Worker创建的WorkerReadHandle被收集到G_WorkerReadHandles中。

K线周期与时间对齐 (Time Alignment)

(澄清) 这是K线聚合的核心驱动机制。它不完全依赖于高频的交易数据流。

驱动力: 全局时钟信号 (GlobalClock) 是新建K线的主要驱动力。例如，对于1m周期，每当分钟切换时，全局时钟会广播一个信号。所有Worker收到后，都会为其负责的所有品种创建新周期的1m K线。这确保了即使是成交稀疏的品种，也能在正确的时间点开启新的K线周期，避免了数据驱动可能带来的延迟。

算法: 系统采用标准的“向下取整”策略。例如，对于5m周期，10:03:45的交易属于开盘时间为10:00:00的K线。

实现: 此逻辑由 klcommon::api::get_aligned_time(timestamp, interval) 函数提供。

K线聚合 (在每个 Worker 内部)

主循环: Worker 的主循环是一个 tokio::select!，并发处理以下事件：

从私有WebSocket收到的aggTrade数据。

从全局时钟watch通道收到的时间节拍信号。

从WorkerReadHandle收到的外部数据拉取请求。

处理交易数据:

根据symbol从全局只读的G_SymbolToIndex获取global_index。

通过 local_index = global_index - partition_start_index 公式计算出本地索引。

遍历所有K线周期，使用 get_aligned_time 确定交易所属的 kline_open_time。

如果 kline_open_time 大于当前 KlineState 的开盘时间，则意味着新K线开始，用当前交易数据重置 KlineState。

否则，将交易数据聚合到现有 KlineState。

处理时钟信号:

收到时钟信号后，遍历所有负责的品种和周期。

检查当前 KlineState 是否已经到期 (e.g., clock_time >= kline.open_time + interval_ms)。

如果到期，则将 KlineState 的 is_final 标志设为 true，并更新至写缓冲区。同时，准备好下一个周期的空 KlineState。

写入缓冲: 任何对 KlineState 的更新（无论是聚合还是终结），都会将其转换为 KlineData 写入当前活动的本地写缓冲区，并将该数据的 is_updated 标志设为 true。

持久化流程 (由 PersistenceTask 驱动)

PersistenceTask的定时器触发 (每30秒)。

PersistenceTask遍历G_WorkerReadHandles列表，并发地向所有Worker发送“请求已更新数据”的指令。

(Worker侧) 每个Worker收到请求后，执行以下原子性操作:
a. 切换其双缓冲索引，使原活动缓冲区变为只读，新缓冲区变为活动状态。
b. 遍历这个只读缓冲区，找出所有 is_updated = true 的 KlineData。
c. 收集这些数据的克隆，形成一个列表。
d. 立刻将这个只读缓冲区中所有项的 is_updated 标志设为 false，完成“已读取”的状态流转。
e. 通过oneshot通道将收集到的数据列表返回给PersistenceTask。

(PersistenceTask侧) 等待所有Worker的响应，将所有返回的列表汇总成一个大Vec。

调用db.upsert_klines_batch执行一次性的批量数据库写入。

API服务拉取数据

此流程与持久化流程高度相似。API服务层持有G_WorkerReadHandles的Arc引用。当收到API请求时，它并发地向所有Worker请求已更新的K线快照，收集数据后聚合返回给客户端。

新品种发现与添加

发现: SymbolManager发现新品种，例如 "TIAUSDT"。

锁定与分配: SymbolManager获取G_SymbolToIndex的写锁，并将 "TIAUSDT" 添加到映射中，其global_index将是当前最大的索引值。

固定路由: SymbolManager 总是通过 G_W3_Cmd_Tx 发送 { cmd: "ADD_SYMBOL", symbol: "TIAUSDT", global_index: ... } 指令。

(Worker 3侧) 本地初始化: Worker 3 从其指令通道收到消息后，执行以下操作：
a. 计算新品种的 local_index。
b. 在其预先分配好的 (10000品种容量) kline_states 和 buffers 数组的 local_index 位置上，初始化一个新的 KlineState 和 KlineData。此过程无任何内存扩容或锁操作。
c. 向自己的WebSocket连接发送订阅 "TIAUSDT@aggTrade" 的指令。订阅失败等网络问题由Worker内部的连接逻辑处理，不影响主流程。

5. 关键决策与权衡

分区策略: 采用基于global_index的连续分区。

优点: 实现简单，索引计算为O(1)。

缺点: 理论上可能导致负载不均（热门币种集中在某个Worker）。

结论: 接受此方案。简单性带来的好处远大于潜在的负载不均问题。

数据拉取与状态同步:

模型: 采用"Worker主动过滤并管理状态"的模型。外部请求者（如PersistenceTask）只发出简单的数据请求，而Worker负责筛选出标记为is_updated的数据，返回快照，并立即重置标记位。

优点: 责任边界清晰。Worker完全拥有其数据的生命周期管理，外部消费者无需实现筛选逻辑，得到的总是所需的最简数据集。

权衡: 相比于“Worker返回整个缓冲区，消费者自行筛选”的模型，此方案在Worker侧增加了少量迭代逻辑，但极大地简化了消费者和系统整体的逻辑复杂度。

内存分配策略:

模型: 在启动时为每个Worker的kline_states和buffers预分配支持10000个品种的固定内存空间。

优点: 彻底杜绝了运行时的内存分配和扩容操作，消除了潜在的性能抖动和实现复杂性。简化了新品种“热添加”的流程，使其成为一个纯计算和状态修改操作。

缺点: 启动时内存占用较高，且存在一个硬性的品种上限。

结论: 接受此方案。对于性能确定性要求极高的场景，这是经典且有效的策略。10000个品种的上限对于当前业务绰绰有余。

