AI驱动的系统诊断标准操作流程 (SOP) v3.0 - 源码感知版
1. 文档目的

本文档旨在为能够访问源代码的AI助手或系统维护人员提供一个标准化的、可重复的流程，以诊断和分析由“AI原生日志系统”记录的应用程序问题。该流程的核心是**“信号驱动的现场重现 (Signal-Driven Scene Recreation)”**，确保以最高效的方式，从海量原始日志中定位问题根源。

2. 核心组件与信息源

诊断过程依赖于以下三个核心信息源：

组件	角色	交互方式	关键信息
问题摘要日志<br>(problem_summary.log)	分析的起点，问题信号的集合	完整文件读取	WARN/ERROR事件、失败的断言、关键检查点。提供trace_id和span_id指针。
日志MCP守护进程<br>(log_mcp_daemon)	系统的“数字孪生”，真相之源	通过HTTP API按需查询	本次运行的全部、完整的原始追踪数据 (SpanModel)。
源代码库	最终的、最底层的“真相”	通过文件读取工具按需访问	函数实现、逻辑分支、常量定义、错误处理方式。
3. 查询工具集

与log_mcp_daemon和源代码库的交互，通过一组预定义的命令完成。AI在执行分析时，应明确生成并说明将要使用的命令。

3.1 日志查询工具

Get-SpanDetails -Id <string>

用途: 这是主要调查工具。根据一个span_id，获取该操作步骤的完整详细信息 (SpanModel)。

何时使用: 在problem_summary.log中找到可疑的span_id后，这是执行的第一个命令。

Get-ChildrenOf -ParentId <string>

用途: 获取指定操作的所有直接子操作。用于向下钻取，查看一个宏观操作具体是哪个子步骤失败了。

Get-TransactionTrace -TransactionId <any>

用途: 根据业务ID（如transaction_id）获取与之相关的所有操作。用于从业务视角追踪一个完整的事务流。

Aggregate-Failures -GroupBy <attribute_name>

用途: 用于验证模式猜想。当你怀疑某个属性是导致失败的普遍原因时，用它进行统计。

<attribute_name>示例: "symbol", "interval", "error.kind"。

3.2 源码查询工具

Get-Code -FilePath <path> [-Line <int>] [-ContextLines <int>]

用途: 获取指定文件的源代码。可以附带行号和上下文行数，以精确定位到问题代码。

何时使用: 在分析SpanModel或ProblemSummary条目，并定位到具体的target或代码位置信息时调用。

4. 诊断流程五步法

请严格按照以下五个步骤进行思考和行动。每一步的产出都是下一步的输入。

第一步：识别信号 (Signal Discovery)

目标: 从problem_summary.log中找到最值得调查的切入点。

行动:

完整扫描: 完整读取 problem_summary.log 文件。

优先级排序:

最高优先级: level: "ERROR" 的日志。这些是已确认的故障。

次高优先级: level: "WARN" 且 log_type: "assertion" 的日志。这些是失败的软断言，揭示了业务逻辑或性能上的不一致。

一般优先级: 其他 level: "WARN" 的日志。

构建宏观理解: 留意log_type: "checkpoint"和log_type: "snapshot"的INFO日志，它们能帮助构建对整个流程的认知。例如，如果程序在Symbol acquisition complete之后，Table preparation complete之前崩溃，问题范围就被大大缩小了。

选定切入点: 选择一个优先级最高、信息最具体的日志条目作为调查的起点。必须从这条日志中提取并记录下span_id和trace_id。

产出: 一个明确的“问题坐标”和初步假设。

示例产出: “我将从span_id: '104'（一个ERROR级别的下载任务失败事件）开始调查，其trace_id为'1'。初步怀疑是API请求层面的问题。”

第二步：获取现场 (Scene Recreation)

目标: 根据“问题坐标”，获取问题发生瞬间的、最直接的、最完整的技术和代码上下文。

行动:

数据现场获取: 使用Get-SpanDetails -Id <span_id>命令，参数为上一步中锁定的span_id。

执行: Get-SpanDetails -Id "104"

代码现场获取: 分析返回的SpanModel中的target（如kline_server::kldata::backfill）和name（如process_single_task）来定位源文件和函数。如果events中包含更精确的文件和行号信息，优先使用。

执行: Get-Code -FilePath "src/kldata/backfill.rs" -Line 284 -ContextLines 10

产出: 一个包含了问题Span所有运行时数据的SpanModel对象，以及对应的源代码片段。这是进行推理的核心证据。

第三步：纵向分析 (Vertical Analysis: Why & Who)

目标: 结合运行时数据 (SpanModel) 和 静态代码 (Source Code)，理解问题的直接原因（向下钻取）和触发背景（向上追溯）。

行动:

向下钻取 (Why?):

交叉验证: 对比SpanModel的events列表和源代码。查看是哪个逻辑分支（如if, match）导致了ERROR或WARN事件的触发。

深入子操作: 如果SpanModel本身没有明显的错误事件，但其状态为FAILURE，说明问题出在它的子操作中。执行Get-ChildrenOf -ParentId <current_span_id>获取所有子Span。在返回的列表中寻找失败的子Span，并对其重复第二步和第三步，层层深入。

向上追溯 (Who?):

分析当前SpanModel的parent_id。

执行Get-SpanDetails -Id <parent_id>来获取其父Span的上下文。

分析父Span的attributes和代码，理解它是在什么业务背景下（例如，是“初始下载”还是“重试下载”）调用了这个失败的子操作。

产出: 一条清晰的、从高层业务到底层技术错误的因果链，并有代码作为佐证。

示例产出: “process_single_task (span 104) 失败，是因为它调用的download_continuous_klines (span 105) 返回了错误。通过查看api.rs的代码，确认了该错误是reqwest::send()调用本身失败导致的。”

第四步：横向分析 (Horizontal Analysis: How Widespread?)

目标: 判断当前问题是孤立事件还是普遍现象，以验证或修正初步结论。

行动:

提出假设: 基于第三步的发现，形成一个关于问题普遍性的假设。

示例: “我发现CGPTUSDT的下载失败原因是API限流。我假设所有418错误都是由高并发引起的，并且可能集中在某些特定的交易对上。”

数据验证: 使用Aggregate-Failures -GroupBy <attribute_name>命令来验证假设。

示例:

Aggregate-Failures -GroupBy "error.summary": 查看失败原因的分布。

Aggregate-Failures -GroupBy "symbol": 查看失败交易对的分布。

产出: 对问题范围和影响面的量化认知，将单个问题点上升为系统性的模式。

示例产出: “聚合查询显示，在47个失败任务中，100%的错误summary都是api_request_failed，并且失败的symbol分布广泛，没有集中性。这证实了问题是普遍的，很可能与系统级的配置（如并发数）有关，而不是特定数据的问题。”

第五步：结论与建议 (Conclusion & Recommendation)

目标: 综合所有信息，给出一个包含代码级证据的、清晰、准确、可操作的最终报告。

行动:

总结因果链: 将第三步和第四步的发现串联起来，清晰地描述问题是如何发生的。

提供证据: 在报告中引用关键的日志条目、SpanModel数据和代码片段作为结论的支撑。

提出代码级解决方案: 给出具体的、可操作的修复建议，最好能以代码diff的形式呈现。

示例产出:

诊断结论: 系统在backfill.rs中以硬编码的50个并发数执行下载任务，这超过了币安API的速率限制，导致普遍的HTTP 418错误和连接超时。

修复建议:

主要: 降低并发数。

文件: src/kldata/backfill.rs

修改:

Generated diff
- const CONCURRENCY: usize = 50;
+ const CONCURRENCY: usize = 15; // 建议值，需进一步测试


次要: 增加智能退避策略。

文件: src/klcommon/api.rs

建议: 在download_continuous_klines函数的错误处理中，增加对418状态码的特定判断，并根据响应体中的解封时间戳进行等待。

通过严格遵循这五步法，AI可以将调试从一种“艺术”或“直觉”变成一门“科学”，每一步都有数据和代码支撑，每一个结论都有迹可循。