好的，这是我们经过探讨、对比和提炼后形成的最终版K线聚合业务函数日志埋点分析文档。

K-line Aggregation Service - 日志埋点分析报告 (v2.0)

本文档为 kline-server 的分区K线聚合服务 (klagg_sub_threads 模型) 提供了一套全面的、遵循“信息完备性优先”原则的日志埋点规范。其核心目标是为自动化日志埋点AI提供清晰、结构化的指导蓝图。

1. 核心原则

信息完备性: 宁可产生少量冗余日志，也绝不能遗漏任何有潜在诊断价值的事件。

正交性: 业务流程类型 (结构视角) 和 建议日志级别 (语义视角) 的评级完全独立。高频业务绝不直接等同于Debug级别。

日志责任归属: 当调用者A的日志已完整概括被调用者B的执行结果时，B可评为None。此原则不适用于“决策型函数”。

决策型函数豁免: 如果一个函数的核心价值在于执行一次业务决策（如筛选、分类、添加），那么这个函数必须拥有自己的Info或Warn级别日志来记录其决策的输入和输出。

2. 模块分析: main (二进制入口)

文件路径: src/bin/klagg_sub_threads.rs

此模块负责服务的启动、顶级资源初始化和优雅停机。

fn main() -> Result<()>

业务流程类型: 低频业务

建议日志级别: Info

理由与分析:

流程类型理由: 作为程序的唯一入口点，main 函数在整个服务生命周期中仅执行一次，是典型的低频业务。

日志级别理由: 此函数标志着整个服务进程的开始和结束。记录这两个关键的生命周期事件（“Initializing...”，“Service shut down gracefully.”）对于判断服务是否成功启动或按预期关闭至关重要。Info 级别能确保这些顶层事件在标准日志中可见。

业务目标与逻辑:

创建核心的 I/O 运行时环境。

在运行时中调用 run_app 来执行核心应用逻辑。

run_app 结束后，执行运行时的优雅关闭和日志系统的关闭。

async fn run_app(io_runtime: &Runtime) -> Result<()>

业务流程类型: 低频业务

建议日志级别: Info

理由与分析:

流程类型理由: 此函数是应用的主初始化和编排函数，在服务启动时执行一次，负责组装所有组件。

日志级别理由: run_app 的执行过程是服务能否正常运行的关键。它的每一步都是一个重要的、预期的里程碑。应使用 Info 级别记录每个主要步骤的开始和成功（例如：“Initializing communication channels...”, “Starting core background services...”, "All workers started."），这可以清晰地追溯启动流程，在服务启动失败时快速定位问题环节。处理 Ctrl-C 或内部故障关机时，也应使用 Warn 或 Info 记录触发原因。

业务目标与逻辑:

初始化所有全局共享资源（配置、API客户端、数据库、时间同步器）。

初始化任务间通信设施（watch、mpsc 通道）。

启动核心后台任务（时钟、品种管理器、看门狗、持久化）。

编排计算Workers的创建和启动。

进入主循环，等待关闭信号（Ctrl-C 或内部故障通知）并协调所有组件的优雅停机。

async fn run_clock_task(...)

业务流程类型: 高频业务

建议日志级别: Trace / Error

理由与分析:

流程类型理由: 此函数在 loop 中持续运行，以固定的节拍（由最短K线周期决定）驱动整个系统的时间同步，是典型的高频事件源。

日志级别理由:

常规路径 (Trace): 成功的时钟“滴答”是一个高频、重复的“心跳”事件。将其记录为 Trace 可以在需要深度调试时，确认时钟任务本身是否在正常运行，而不会在生产环境中造成日志泛滥。

异常路径 (Error): 当时间同步失效时 (time_sync_invalid)，这是一个会导致系统关闭的严重错误，必须使用 Error 级别记录，以确保其在任何日志级别下都可见。

业务目标与逻辑:

循环检查时间同步有效性，若失效则记录 Error 并触发系统关闭。

计算到下一个K线时间周期的唤醒点，sleep 到达。

通过 watch channel 广播最新的服务器时间戳。此广播动作可记录为 Trace。

async fn initialize_symbol_indexing(...) -> Result<...>

业务流程类型: 低频业务

建议日志级别: Info / Error

理由与分析:

流程类型理由: 在服务启动时调用一次，用于获取和建立初始的品种列表和索引。

日志级别理由: 这是一个关键的“决策型函数”。它决定了系统启动时处理哪些品种。其成功结果（找到多少品种）应记录为 Info。如果获取失败或在非测试模式下未找到任何品种，这是一个阻止服务启动的失败，必须记录为 Error。

业务目标与逻辑:

从API或测试配置获取所有USDT本位永续合约符号。

从数据库查询这些品种的最早上线时间戳并排序。

构建 symbol -> index 和 index -> symbol 的映射关系并返回。

async fn run_symbol_manager(...) -> Result<()>

业务流程类型: 高频业务

建议日志级别: Info / Error

理由与分析:

流程类型理由: 函数在一个 while let Some(...) 循环中持续运行，处理来自 MiniTicker 的实时数据流，属于事件驱动的高频检查。

日志级别理由:

决策型函数: 此函数的核心价值在于“发现”并“决定”添加一个新品种。根据【决策型函数豁免】原则，这个改变系统状态的重要业务事件必须用 Info 级别清晰地记录其完整流程。

关键事件记录: Info 级别日志应覆盖：发现新品种 -> 发送 AddSymbol 命令 -> 等待并收到确认 -> 成功更新全局索引。Error 级别日志应覆盖：发送命令失败、等待确认超时/失败、回滚索引等异常情况。

业务目标与逻辑:

监听全市场行情，过滤出“新品种”。

对于每个新品种，执行“发送命令-等待确认-更新全局状态”的原子化操作流程。

async fn run_watchdog(...)

业务流程类型: 高频业务

建议日志级别: Debug / Error

理由与分析:

流程类型理由: 此任务在一个 loop 中以固定间隔周期性地检查所有计算线程的健康状况。

日志级别理由:

常规路径 (Debug): 看门狗的每一次成功巡检是一个常规的内部健康监测动作。将其记录为 Debug 可以在调试时确认看门狗任务本身是存活的，而不会干扰生产日志。

异常路径 (Error): 检测到任何一个Worker心跳超时都是一个致命错误，表明系统核心功能已失效，必须使用 Error 级别记录，并清晰地列出死掉的Worker ID。

业务目标与逻辑:

周期性地遍历健康状态数组，检查每个Worker的心跳时间戳。

如果发现任何死亡的Worker，记录 Error 日志并触发全局关闭。

3. 模块分析: klagg_sub_threads (Worker实现)

文件路径: src/klagg_sub_threads/mod.rs

此模块包含了Worker的计算逻辑、I/O循环以及持久化任务。

impl WorkerReadHandle
async fn request_snapshot(&self) -> Result<...>

业务流程类型: 高频业务

建议日志级别: Error

理由与分析:

流程类型理由: 此方法由 persistence_task 在一个高频定时循环中调用。

日志级别理由: 这是一个封装了通道通信的辅助方法。它的成功执行是常规路径，无需记录。我们只关心通信失败的情况（发送请求失败或接收响应失败），这些是 Error 级别的事件，应在错误发生的第一现场（即此函数内）被记录。

impl Worker
async fn new(...) -> Result<...>

业务流程类型: 低频业务

建议日志级别: Info

理由与分析: Worker的创建是系统启动的关键步骤。应使用 Info 记录其成功创建，并附上 worker_id 和分配的初始容量，为诊断资源分配问题提供依据。

fn get_read_handle(&self) / fn get_trade_sender(&self)

业务流程类型: 低频业务

建议日志级别: None

理由与分析: 简单的、无副作用的 getter，不执行I/O，不含复杂逻辑，符合 None 级别的所有标准。

async fn run_computation_loop(...)

业务流程类型: 高频业务

建议日志级别: Info

理由与分析: 代表了计算线程的完整生命周期。应使用 Info 记录其启动和关闭事件。循环本身不记录，日志责任下放给具体的 process_* 方法。

fn process_trade(&mut self, trade: AggTradeData)

业务流程类型: 高频业务

建议日志级别: Trace / Warn / Error

理由与分析: 系统的“热路径”。常规处理应为 Trace。收到未缓存的交易对 (uncached symbol) 是 Warn。计算出的索引越界 (out of bounds) 是严重的内部逻辑错误，应为 Error。

fn process_clock_tick(&mut self, current_time: i64)

业务流程类型: 高频业务

建议日志级别: Trace

理由与分析: 常规的、批量的内部状态更新。单个K线的完结事件价值体现在最终持久化的数据中。直接记录每一次检查会产生大量噪音，因此将日志级别设为 Trace 最为合适，仅用于最深度的调试。

async fn process_command(&mut self, cmd: WorkerCmd)

业务流程类型: 低频业务

建议日志级别: Info / Warn / Error

理由与分析: 响应 run_symbol_manager 决策的关键执行函数。应使用 Info 记录命令的接收和成功处理。索引越界等内部错误为 Error。发送ack失败等通信问题为 Warn。

fn process_snapshot_request(...)

业务流程类型: 高频业务

建议日志级别: Trace / Warn

理由与分析: 成功的快照生成是常规路径，可使用 Trace。response_tx.send() 失败意味着请求方已放弃等待，这是一个异常通信状态，应使用 Warn 记录。

async fn run_io_loop(...)

业务流程类型: 高频业务

建议日志级别: Info / Error

理由与分析:

流程类型理由: 管理WebSocket连接的持续运行的I/O任务。

日志级别理由: 应使用 Info 记录关键的状态转换（启动、收到命令、断开、尝试重连）。特别地，在重连成功后，应明确记录一条 Info 日志，说明“已成功连接并订阅了N个品种”，以确认动态添加的流程在I/O层面也已生效。连接失败 (client.start() 返回 Err) 是一个需要立即关注的问题，应为 Error。

async fn persistence_task(...)

业务流程类型: 高频业务

建议日志级别: Info

理由与分析: 作为持久化流程的“编排者”，应使用 Info 记录其生命周期事件（启动、收到关闭信号、执行最后一次写入）。

async fn perform_persistence_cycle(...)

业务流程类型: 高频业务

建议日志级别: Info / Debug / Error

理由与分析: 这是数据落盘的最终环节。应使用 Info 记录成功保存了多少条K线。保存失败是 Error。获取快照失败是 Error。周期内无数据可写是常规情况，应使用 Debug 记录。