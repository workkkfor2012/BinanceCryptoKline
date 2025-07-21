
K线聚合服务 - 分区模型架构
1. 核心思想

本方案旨在解决原Actor模型中因品种数量过多导致的上下文切换频繁、性能低下的问题。核心思想从“一个Actor对应一个品种”转变为 “一个计算线程（Worker）负责一个品种分区”。

静态分区：系统启动时，所有交易对根据其全局唯一索引被静态地分配到固定数量的Worker上。

计算/IO分离：

计算线程: 为每个Worker创建一个独立的、可绑核的物理线程，并在其内部运行一个current_thread的Tokio运行时。此线程专门负责K线状态的计算和更新，无任何IO操作。

IO线程池: 使用一个全局共享的多线程Tokio运行时，负责处理所有网络IO，包括WebSocket的连接、读写以及数据库持久化。

无锁计算: 在计算线程内部，对K线数据的更新是单线程的，完全避免了锁争用，最大化CPU效率和缓存局部性。

异步通信: 计算线程与IO线程之间通过无锁的MPSC通道（tokio::sync::mpsc）进行通信，实现高效解耦。

2. 关键组件与数据结构

io_runtime: 全局共享的多线程运行时，用于所有IO任务。

computation_runtime: 每个Worker独享的单线程运行时，运行在绑核的物理线程上。

Worker: K线聚合的核心计算单元，每个Worker负责一个分区。

G_Index (Global Index): HashMap<String, usize>，全局唯一的品种到索引的映射。由Arc<RwLock<...>>包裹，供所有任务读取。

L_Index (Local Index): Worker内部的HashMap<String, usize>，将品种名映射到其在Worker内部数据切片中的本地索引，实现O(1)查找。

KlineState: Worker内部用于计算的K线状态，非线程安全。

KlineData: 用于在Worker和persistence_task之间传输的K线快照数据。

3. 数据与控制流程
3.1. 交易数据流 (热路径)

WebSocket接收: run_io_loop（运行在io_runtime）从WebSocket连接接收原始交易数据。

解析与分发: run_io_loop解析数据后，通过trade_tx: mpsc::Sender<AggTradeData>将结构化的交易数据发送给对应的Worker。

聚合计算: Worker的run_computation_loop（运行在computation_runtime）从trade_rx接收数据，根据品种的L_Index定位到KlineState，并执行无锁的聚合计算。

3.2. K线快照与持久化流程

请求快照: persistence_task（运行在io_runtime）周期性地向所有Worker的WorkerReadHandle发送快照请求。该请求包含一个oneshot::Sender用于接收响应。

准备快照: Worker在其计算线程中收到请求，从其双缓冲快照snapshot_buffers中提取自上次快照以来有更新的数据（is_updated: true），并将结果通过oneshot::Sender发回。

持久化: persistence_task收集所有Worker的快照数据，聚合成一个批次，并调用db.upsert_klines_batch进行数据库写入。

3.3. 动态新增品种流程 (关键流程)

系统能够优雅地处理运行时发现的新交易对，其流程涉及多个组件的精确协作，并由一个指定的Worker（如Worker 3） 负责处理指令：

发现: run_symbol_manager 任务通过 !miniTicker@arr 流发现了一个不在全局索引G_Index中的新品种 S。

预留索引: SymbolManager 原子地增加全局品种计数器，为 S 预留一个新的全局索引 G_idx。

发送指令: SymbolManager 构造一个 WorkerCmd::AddSymbol 指令（包含 symbol, global_index, 以及一个用于确认的oneshot::Sender），并将其发送给指定的Worker 3。

Worker本地处理:
a. Worker 3的计算线程接收到指令。
b. 它更新自己的本地状态，包括 local_symbol_cache 和 managed_symbols_count，为新品种分配内存和状态。
c. 它向自己的run_io_loop任务发送一条内部WsCmd::Subscribe指令，要求订阅新品种S的aggTrade流。

处理确认 (ACK): Worker 3在完成本地状态更新后，通过oneshot::Sender向SymbolManager发送一个空的确认信号。

全局状态更新: SymbolManager 必须等待并接收到这个确认信号后，才将 {S: G_idx} 的映射关系写入全局的symbol_to_global_index和global_index_to_symbol中。

订阅生效: Worker 3的run_io_loop任务收到订阅指令后，向现有WebSocket连接发送SUBSCRIBE请求。如果失败，将在下次重连时自动订阅。

这个“指令-确认-全局更新”的流程保证了在任何其他任务（如persistence_task）看到新品种之前，负责它的Worker已经准备就绪，从而避免了竞态条件。

4. 健壮性与设计权衡

健康检查 (Watchdog):

机制: Watchdog监控每个Worker计算线程的心跳。

权衡: 监控粒度为 Worker级别 而非 品种级别。系统可以检测到整个计算线程的卡顿，但无法发现单个品种数据流中断的情况。这是为了追求极致性能所做的合理权衡。

单点故障风险:

问题: 动态新增品种的WorkerCmd指令目前由一个**特定的Worker 3**处理。如果该Worker因故宕机，系统将失去新增品种的能力。

现状: 当前设计简化了实现，但在极限情况下存在此风险。未来可考虑引入独立的管理线程或指令广播机制来提高容错性。

