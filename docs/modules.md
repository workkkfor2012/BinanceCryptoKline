

函数式模块定义列表.md
1. 模块：原始交易数据接收器 (Raw Trade Data Receiver)

核心职责简述： 从外部WebSocket连接接收原始的、未解析的交易数据流。

a. 输入 (Inputs)：

WebSocketConnection: 一个已建立的WebSocket连接实例，持续推送原始市场交易数据。

b. 输出 (Outputs)：

RawTradeMessageStream: 一个原始交易消息流 (例如：字节流或文本流，如JSON字符串)。

c. 核心处理逻辑摘要：

监听指定的WebSocket端点。

接收到达的原始消息数据。

将接收到的数据作为流式输出，不做任何业务逻辑解析。

d. 关键信息/约束：

连接管理： 需要处理WebSocket连接的建立、维护（心跳、重连机制）。这部分副作用应被良好封装。

数据格式： 输出数据的具体格式（原始字节、UTF-8文本等）取决于WebSocket源。

背压处理： 需要考虑下游处理速度不及上游推送速度时的背压处理机制。

测试重点： 连接稳定性，数据完整接收（不丢包、不乱序）。

2. 模块：交易数据解析器 (Trade Data Parser)

核心职责简述： 将原始交易消息转换为包含明确结构和语义的领域内交易事件。

a. 输入 (Inputs)：

RawTradeMessage: 单条原始交易消息 (例如：JSON字符串，二进制消息)。

b. 输出 (Outputs)：

ParsedTradeEvent: 结构化的交易事件对象。

概念属性： symbol (交易品种代码), price (价格), quantity (数量), timestamp (成交时间戳), trade_id (唯一交易ID，可选)等。

ParsingError: 若解析失败，则输出错误信息。

c. 核心处理逻辑摘要：

根据预定义的原始消息格式（如JSON、FIX协议片段等）解析输入。

提取关键交易信息（品种、价格、量、时间等）。

数据类型转换和基本校验（例如，价格和数量应为正数）。

生成结构化的ParsedTradeEvent对象。

d. 关键信息/约束：

数据格式定义： 强依赖于上游RawTradeMessage的具体格式规范。

错误处理： 必须能够健壮地处理格式错误、数据缺失或无效的原始消息。

性能： 解析逻辑需要高效，因其处理的是高频数据流。

测试重点： 对各种正常及异常格式输入的解析准确性，字段提取正确性，边界条件处理。

追问1: 关于ParsedTradeEvent，除了品种、价格、量、时间戳，是否还有其他对K线合成至关重要的字段需要从原始数据中解析出来（例如：买卖方向side，如果K线合成需要区分）？ （目前文档未提及，但实践中可能需要）

3. 模块：交易事件分发器 (Trade Event Distributor)

核心职责简述： 根据ParsedTradeEvent中的品种信息，将其路由到负责处理该品种的特定KlineActor（或其等效的处理单元/队列）。

a. 输入 (Inputs)：

ParsedTradeEvent: 已解析的结构化交易事件。

b. 输出 (Outputs)：

RoutedTradeEvent: 包含目标处理单元标识（如symbol或actor_id）和原始ParsedTradeEvent的事件。

c. 核心处理逻辑摘要：

从ParsedTradeEvent中提取symbol属性。

基于symbol，确定该事件应由哪个下游处理单元（如KlineActor for symbol）处理。

将事件导向该目标。在Actor模型中，这通常是向目标Actor发送消息。

d. 关键信息/约束：

路由机制： 依赖于一个品种到处理单元的映射关系（此关系在系统初始化时建立，由GlobalSymbolPeriodRegistry间接支持）。

不变性： 该模块不修改ParsedTradeEvent的内容，仅附加路由信息或执行路由动作。

测试重点： 路由逻辑的准确性，确保每个品种的事件都能正确分发到对应的处理逻辑。

4. 模块：品种周期K线合成器 (Symbol-Period K-Line Synthesizer)

核心职责简述： 针对特定品种的特定K线周期，根据流入的单个ParsedTradeEvent和当前周期的K线状态，更新或生成新的KlineData。这是一个纯函数核心。

a. 输入 (Inputs)：

ParsedTradeEvent: 当前到达的单个交易事件。

CurrentPeriodKlineState: 该交易品种、该K线周期当前的K线数据状态（可能为空表示新K线的开始）。包含 open_time, open, high, low, close, volume。

PeriodDefinition: 该K线周期的定义（例如：1分钟、5分钟；包含周期时长和如何计算K线时间窗口的逻辑）。

b. 输出 (Outputs)：

UpdatedPeriodKlineState: 更新后的该品种、该周期的K线数据。

c. 核心处理逻辑摘要：

根据ParsedTradeEvent.timestamp和PeriodDefinition，判断交易属于当前CurrentPeriodKlineState的时间窗口，还是应开启一个新的K线。

如果属于当前K线：

更新 high = max(CurrentPeriodKlineState.high, ParsedTradeEvent.price)

更新 low = min(CurrentPeriodKlineState.low, ParsedTradeEvent.price)

更新 close = ParsedTradeEvent.price

更新 volume += ParsedTradeEvent.quantity

如果开启新K线：

open_time = 根据ParsedTradeEvent.timestamp和PeriodDefinition计算的新K线起始时间。

open = high = low = close = ParsedTradeEvent.price

volume = ParsedTradeEvent.quantity

d. 关键信息/约束：

纯函数性： 此模块本身不应有副作用，仅基于输入计算输出。状态管理由其调用者（如KlineActor）负责。

时间处理： K线时间窗口的对齐和计算逻辑至关重要（例如，1分钟K线总是从00秒开始）。

数据精度： 价格和量的计算精度。

测试重点： 不同交易时间戳下K线聚合的准确性（新K线创建、现有K线更新），OHLCV值计算的正确性，边界条件（如周期切换时刻的交易）。

5. 模块：单品种K线聚合与状态管理器 (Single Symbol K-Line Aggregator & State Manager)

核心职责简述： (此模块代表KlineActor的核心数据处理逻辑) 管理单个交易品种所有预定义周期的K线状态，接收该品种的ParsedTradeEvent，利用Symbol-Period K-Line Synthesizer为每个周期更新K线，并准备待写入存储的KlineData。

a. 输入 (Inputs)：

ParsedTradeEvent: 特定于此聚合器所负责品种的交易事件。

InternalKlinesState: (隐式输入，模块内部状态) 当前该品种所有周期K线的状态集合。

SymbolIndex: (初始化时获取并缓存) 该品种在FlatKlineStore中的symbol_index。

PeriodDefinitions: (初始化时获取) 该品种需要计算的所有K线周期的定义列表，每个定义包含其period_index。

b. 输出 (Outputs)：

ListOfKlineUpdates: 一个列表，包含一个或多个待写入缓冲区的 (KlineData, target_offset) 元组。每个元组代表一个周期的K线更新。

(副作用) 更新InternalKlinesState。

(可选副作用) CompletedKlineForPersistence: 当某个周期的K线完成时，输出该K线数据用于异步持久化。

c. 核心处理逻辑摘要：

对于接收到的ParsedTradeEvent：

遍历该品种所有需要计算的PeriodDefinitions。

对每个周期，从InternalKlinesState获取该周期的当前K线状态。

调用Symbol-Period K-Line Synthesizer模块，传入交易事件、当前周期K线状态和周期定义，得到更新后的周期K线状态。

更新InternalKlinesState中对应周期的K线状态。

根据SymbolIndex和当前周期的period_index，计算出在FlatKlineStore中的target_offset。

准备包含更新后KlineData和target_offset的输出。

如果某个周期的K线因为时间窗口结束而“完成”，则触发可选的持久化输出。

d. 关键信息/约束：

状态管理： 核心是管理多个周期K线的内部状态。

并发写入协调： 虽然此模块准备数据，但实际写入由K-Line Buffer Writer执行，需考虑并发写入同一FlatKlineStore的协调（文档中是通过不同Actor写入不同区域，这是OK的）。

配置依赖： 依赖于品种的symbol_index和各周期的period_index（来自GlobalSymbolPeriodRegistry）。

测试重点： 多周期K线更新的正确性，target_offset计算的准确性，状态转换的正确性（如K线从进行中到完成）。

6. 模块：K线数据到缓冲区写入器 (K-Line Buffer Writer)

核心职责简述： 将单个KlineData对象根据计算好的偏移量写入到DoubleBufferedKlineStore的当前写缓冲区(FlatKlineStore)中。

a. 输入 (Inputs)：

KlineData: 要写入的K线数据 (OHLCV等)。

TargetOffset: 该KlineData在FlatKlineStore (Vec<KlineData>)中的精确索引位置。

WriteBufferReference: 对当前DoubleBufferedKlineStore写缓冲区的引用/指针 (即一个FlatKlineStore实例)。

b. 输出 (Outputs)：

(副作用) WriteBufferReference中klines[TargetOffset]被更新为新的KlineData。

WriteConfirmation (可选): 写入成功/失败的状态。

c. 核心处理逻辑摘要：

直接访问WriteBufferReference.klines这个Vec<KlineData>。

将输入的KlineData写入到klines[TargetOffset]。

d. 关键信息/约束：

直接内存操作： 此模块执行底层的内存写入。

原子性/线程安全： 文档提到KlineActor并发写入FlatKlineStore的不同区域，这意味着只要TargetOffset计算正确且不冲突，此模块的写入操作本身不需要额外的锁（Vec的单个元素写入在Rust中若&mut访问是安全的）。DoubleBufferedKlineStore的缓冲区切换是原子操作，保证了读写的整体一致性。

前置条件： TargetOffset必须是有效且在FlatKlineStore的界限内。WriteBufferReference必须指向当前的、正确的写缓冲区。

测试重点： 写入数据的准确性，指定offset写入的正确性。

7. 模块：全局品种周期注册表查询服务 (Global Symbol-Period Registry Query Service)

核心职责简述： 提供查询服务，根据交易品种代码（symbol）和周期标识（period_label）获取其在FlatKlineStore中的symbol_index和period_index。

a. 输入 (Inputs)：

SymbolName: 交易品种的字符串名称 (例如："BTCUSDT")。

PeriodLabel: K线周期的字符串/枚举标识 (例如："1m", "5m")。

b. 输出 (Outputs)：

SymbolPeriodIndices: 一个包含symbol_index (整数) 和 period_index (整数) 的结构。

LookupError: 如果品种或周期未找到。

c. 核心处理逻辑摘要：

(初始化时) 加载所有交易品种及其预定义的K线周期。为每个品种分配一个唯一的symbol_index（例如，按名称排序或配置文件）。为每个品种内的周期分配period_index。存储这些映射。

(运行时查询) 根据输入的SymbolName查找其symbol_index。根据输入的PeriodLabel查找其period_index。

返回查找到的索引。

d. 关键信息/约束：

数据源： 品种列表和周期定义在系统启动时固定。

不可变性： 注册表在运行时是只读的。

查询效率： 需要高效的查询机制 (例如，内部使用HashMap SymbolName -> symbol_index，以及 PeriodLabel -> period_index 的查找表或固定逻辑)。

测试重点： symbol_index 和 period_index 查询的准确性，对已知和未知品种/周期的处理。

8. 模块：K线数据从缓冲区读取器 (K-Line Buffer Reader)

核心职责简述： 根据symbol_index和period_index，从DoubleBufferedKlineStore的当前读缓冲区(FlatKlineStore)中读取并复制出指定的KlineData。

a. 输入 (Inputs)：

SymbolIndex: 品种在FlatKlineStore中的索引。

PeriodIndex: 周期在该品种内的索引。

TotalPeriodsPerSymbol: 每个品种的K线周期总数 (用于计算偏移量，通常从GlobalSymbolPeriodRegistry或配置中获取)。

ReadBufferReference: 对当前DoubleBufferedKlineStore读缓冲区的引用/指针 (即一个FlatKlineStore实例)。

b. 输出 (Outputs)：

KlineDataCopy: 从读缓冲区复制出来的KlineData对象。

ReadError: 如果索引无效或数据不可读。

c. 核心处理逻辑摘要：

计算最终的读取偏移量: offset = SymbolIndex * TotalPeriodsPerSymbol + PeriodIndex。

访问ReadBufferReference.klines这个Vec<KlineData>。

从klines[offset]复制（深拷贝）KlineData。

返回复制的数据。

d. 关键信息/约束：

无锁读取： 读取操作直接在当前读缓冲区上进行，由于双缓冲机制，此缓冲区在读取期间不会被修改。

数据复制： 返回的是数据的副本，以防止外部修改影响缓冲区。

前置条件： SymbolIndex和PeriodIndex必须有效，ReadBufferReference必须指向当前的读缓冲区。

测试重点： offset计算的正确性，读取数据的准确性，确保返回的是副本。

9. 模块：双缓冲区切换器 (Dual Buffer Switcher)

核心职责简述： (此为DoubleBufferedKlineStore的核心切换逻辑) 原子地交换读缓冲区和写缓冲区的角色。

a. 输入 (Inputs)：

SwapTriggerSignal: 一个表明需要执行切换操作的信号 (例如，由Periodic Buffer Swap Trigger模块发出)。

CurrentBufferPointers: 当前指向读缓冲区和写缓冲区的原子指针或等效机制。

b. 输出 (Outputs)：

(副作用) CurrentBufferPointers被原子更新，原写缓冲区变为新的读缓冲区，原读缓冲区变为新的写缓冲区。

SwitchConfirmation: 切换成功/失败的状态。

c. 核心处理逻辑摘要：

接收到SwapTriggerSignal。

使用原子操作（如 std::sync::atomic::AtomicPtr::swap 或等效的并发原语）交换内部持有的两个FlatKlineStore实例的指针/引用角色。

d. 关键信息/约束：

原子性： 切换操作必须是原子的，以保证数据一致性和无锁读取。

调度： 由外部调度器（如Periodic Buffer Swap Trigger）定期触发。

状态依赖： 依赖于DoubleBufferedKlineStore内部持有的两个FlatKlineStore实例。

测试重点： 缓冲区指针交换的原子性和正确性，确保切换后读写角色正确互换。

10. 模块：周期性缓冲区交换触发器 (Periodic Buffer Swap Trigger)

核心职责简述： (此为中心化调度器) 按预设的时间间隔，生成并发送SwapTriggerSignal给Dual Buffer Switcher模块。

a. 输入 (Inputs)：

SchedulingInterval: 预设的调度时间间隔 (例如：100ms)。

SystemTime: (隐式) 对系统时钟的访问。

b. 输出 (Outputs)：

SwapTriggerSignal: 周期性产生的用于触发缓冲区交换的信号。

c. 核心处理逻辑摘要：

启动一个定时器或调度任务。

每当达到SchedulingInterval时，发出一个SwapTriggerSignal。

d. 关键信息/约束：

定时准确性： 对触发间隔的准确性有一定要求，尽管系统设计上对微小偏差不敏感。

独立性： 通常作为一个独立的线程或任务运行。

测试重点： 定时信号产生的规律性和大致准确性。

11. 模块：(可选) K线数据持久化器 (Optional K-Line Data Persistor)

核心职责简述： 异步地将已完成的KlineData对象写入到外部持久化存储（如数据库）。

a. 输入 (Inputs)：

CompletedKlineForPersistence: 一个已完成的KlineData对象，包含其品种、周期和OHLCV数据。

PersistenceTargetConfig: 数据库连接信息和目标表/集合的配置。

b. 输出 (Outputs)：

(副作用) CompletedKlineForPersistence被写入数据库。

PersistenceAcknowledgement: 异步的持久化成功/失败确认。

c. 核心处理逻辑摘要：

接收到CompletedKlineForPersistence数据。

将其转换为适合数据库存储的格式。

异步地将数据写入配置的数据库中（例如，通过一个专用的I/O线程池）。

d. 关键信息/约束：

异步处理： 持久化操作应异步执行，不阻塞核心K线合成路径。

错误处理与重试： 需要考虑数据库写入失败的错误处理和可能的重试机制。

数据一致性： 虽然是异步，但需考虑与内存中K线数据的最终一致性问题（如果需要强一致性，则设计会更复杂）。

测试重点： 数据到数据库的正确映射和写入，异步处理的可靠性，错误处理逻辑。

追问2: 关于K线持久化，当一个K线周期完成时（例如，1分钟K线的第59秒结束），是由KlineActor（即Single Symbol K-Line Aggregator）直接触发向此持久化器发送数据，还是有其他机制收集已完成的K线？文档提到“K线完成时，Actor异步发送到数据库”。

