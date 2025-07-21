# docs\AI驱动的系统诊断标准操作流程 (SOP).md
# AI驱动的系统诊断标准操作流程 (SOP) v3.1 - 源码分析强化版

## 1. 文档目的

本文档旨在为能够访问源代码的AI助手或系统维护人员提供一个标准化的、可重复的流程，以诊断和分析由“AI原生日志系统”记录的应用程序问题。该流程的核心是**“信号驱动的现场重现 (Signal-Driven Scene Recreation)”**，确保以最高效的方式，从海量原始日志中定位问题根源。

## 2. 核心组件与信息源

诊断过程依赖于以下三个核心信息源：

| 组件 | 角色 | 交互方式 | 关键信息 |
| --- | --- | --- | --- |
| 问题摘要日志<br>(problem_summary.log) | 分析的起点，问题信号的集合 | 完整文件读取 | WARN/ERROR事件、失败的断言、关键检查点。提供`trace_id`、`span_id`及**精确的代码位置指针** (`file`, `line`)。 |
| 日志MCP守护进程<br>(log_mcp_daemon) | 系统的“数字孪生”，真相之源 | 通过HTTP API按需查询 | 本次运行的全部、完整的原始追踪数据 (SpanModel)。 |
| 源代码库 | **程序逻辑的最终权威**，用于解释“为什么”会产生某个日志事件。 | 通过文件读取工具按需访问 | 函数实现、逻辑分支、常量定义、错误处理方式。 |

## 3. 查询工具集

与log_mcp_daemon和源代码库的交互，通过一组预定义的命令完成。AI在执行分析时，应明确生成并说明将要使用的命令。

### 3.1 日志查询工具

*   `Get-SpanDetails -Id <string>`
*   `Get-ChildrenOf -ParentId <string>`
*   `Get-TransactionTrace -TransactionId <any>`
*   `Aggregate-Failures -GroupBy <attribute_name>`

### 3.2 源码查询工具

*   `Get-Code -FilePath <path> [-Line <int>] [-ContextLines <int>]`

## 4. 诊断流程五步法

请严格按照以下五个步骤进行思考和行动。每一步的产出都是下一步的输入。

### 第一步：识别信号 (Signal Discovery)

**目标**: 从`problem_summary.log`中找到最值得调查的切入点。
**行动**:
1.  **完整扫描**: 完整读取 `problem_summary.log` 文件。
2.  **优先级排序**:
    *   最高优先级: `level: "ERROR"` 的日志。
    *   次高优先级: `level: "WARN"` 且 `log_type: "assertion"` 的日志。
    *   一般优先级: 其他 `level: "WARN"` 的日志。
3.  **构建宏观理解**: 留意`log_type: "checkpoint"`和`log_type: "snapshot"`的INFO日志。
4.  **选定切入点**: 选择一个优先级最高的日志条目。必须从这条日志中提取并记录下`span_id`, `trace_id`以及**关键的运行时变量和代码位置信息 (`file`, `line`)**。

**产出**: 一个明确的“问题坐标”（包含运行时快照和代码位置）和初步假设。
*示例产出*: “我将从`span_id: '104'`（一个`ERROR`级别的下载任务失败事件）开始调查。日志显示该事件发生在 `src/kldata/backfill.rs` 的第 `284` 行，相关的运行时数据为 `symbol: 'CGPTUSDT'`。初步怀疑是该交易对的下载请求有问题。”

### 第二步：获取现场 (Scene Recreation)

**目标**: 根据“问题坐标”，获取问题发生瞬间的、最直接的、最完整的技术和代码上下文。
**行动**:
1.  **数据现场获取**: 使用`Get-SpanDetails -Id <span_id>`命令，获取完整的运行时上下文。
    *   *执行*: `Get-SpanDetails -Id "104"`
2.  **代码现场获取**: **优先使用**第一步中从日志事件里直接获取的`file`和`line`信息。
    *   *执行*: `Get-Code -FilePath "src/kldata/backfill.rs" -Line 284 -ContextLines 10`

**产出**: 一个包含了问题Span所有运行时数据的`SpanModel`对象，以及对应的源代码片段。这是进行推理的核心证据。

### 第三步：纵向分析 (Vertical Analysis: Why & Who)

**目标**: **通过“虚拟执行”代码来解释运行时数据，从而找到问题的直接原因（向下钻取），并结合父Span理解触发背景（向上追溯）。**

**行动**:

**向下钻取 (Why?): 核心推理步骤**

1.  **交叉引用**: 将`SpanModel`中的事件（特别是触发信号的`ERROR`或`WARN`事件）及其附带的运行时变量，与第二步获取的源代码片段进行精确匹配。
2.  **虚拟执行与逻辑推演 (Virtual Execution & Logical Deduction)**:
    *   **明确声明**: “现在，我将结合运行时数据在源码中进行推演。”
    *   **执行推演**: 查看代码中的逻辑分支（`if`, `match`, `?`操作符等）。根据日志提供的运行时值（如`payment_status: "declined"`），判断程序**必然**会进入哪个分支。
    *   **得出结论**: 明确说明是哪个代码分支导致了`ERROR`或`WARN`事件的发生。

    *示例推演过程*:
    1.  *日志快照*: “`problem_summary.log`中的`WARN`事件显示`log_type: "assertion"`, `duration_ms: 350`, `threshold_ms: 200`，发生在`api.rs:115`。”
    2.  *代码获取*: `Get-Code -FilePath "src/klcommon/api.rs" -Line 115 -ContextLines 5`
    3.  *代码内容*:
        ```rust
        // line 114
        if !soft_assert!(duration < SLA_THRESHOLD, "API call is too slow") {
            tracing::warn!(
                log_type = "assertion",
                message = "Performance SLA violation",
                duration_ms = duration.as_millis(),
                threshold_ms = SLA_THRESHOLD.as_millis()
            );
        }
        ```
    4.  *推演结论*: “根据运行时数据`duration_ms: 350`和代码中的常量`SLA_THRESHOLD`（假设为200ms），`duration < SLA_THRESHOLD`这个条件为`false`。因此，程序必然进入`if`代码块，在第115行触发了`tracing::warn!`，这与日志完全吻合。**根因是API调用耗时(350ms)超过了性能阈值(200ms)。**”

3.  **深入子操作**: 如果当前Span本身没有错误，但状态为FAILURE，执行`Get-ChildrenOf`寻找失败的子Span，对其重复第二步和第三步。

**向上追溯 (Who?)**:
*   分析当前`SpanModel`的`parent_id`，并使用`Get-SpanDetails`获取父Span的上下文，理解其业务背景。

**产出**: 一条清晰的、**有代码逻辑和运行时数据双重佐证的**因果链。

### 第四步：横向分析 (Horizontal Analysis: How Widespread?)

(此步骤无重大变更，其核心是数据聚合，验证模式)
**目标**: 判断当前问题是孤立事件还是普遍现象。
**行动**:
1.  **提出假设**: 基于第三步的发现，形成一个关于问题普遍性的假设。
2.  **数据验证**: 使用`Aggregate-Failures -GroupBy <attribute_name>`来验证假设。

**产出**: 对问题范围和影响面的量化认知。

### 第五步：结论与建议 (Conclusion & Recommendation)

(此步骤无重大变更，但其结论的可靠性因第三步的强化而大大增强)
**目标**: 综合所有信息，给出一个包含**代码级证据**的、清晰、准确、可操作的最终报告。
**行动**:
1.  **总结因果链**: 将第三步（**包含虚拟执行的推演**）和第四步的发现串联起来。
2.  **提供证据**: 引用关键的日志条目、`SpanModel`数据和**导致问题的核心代码片段**作为结论的支撑。
3.  **提出代码级解决方案**: 给出具体的、可操作的修复建议，最好能以代码diff的形式呈现。该建议必须直接解决第三步中发现的逻辑问题。

---

### 修改逻辑说明

1.  **哲学升级**: SOP的标题、目标和核心组件描述都进行了微调，将“源代码”的地位从一个普通信息源提升为“**最终权威**”。
2.  **强化信号**: 第一步（Signal Discovery）现在强调从日志中捕获**代码位置信息**，为后续的源码分析提供直接入口。
3.  **核心改造第三步**: 这是本次更新的灵魂。
    *   **引入“虚拟执行”**: 我引入了一个新的、明确的子步骤：“虚拟执行与逻辑推演”。这要求AI不能再模糊地“对比”代码和日志，而是必须像一个调试器一样，用日志中的运行时数据去“执行”代码逻辑，并得出必然的结论。
    *   **提供具体范例**: 我重写了第三步的示例，使其成为一个完整的、可操作的“思想钢印”，展示了如何从日志数据、到代码、再到逻辑推演、最后形成一个不可辩驳的结论。
4.  **因果链更强健**: 经过改造后，第五步产出的结论和建议不再仅仅是基于日志的“相关性”猜测，而是基于代码逻辑的“**因果性**”推断，其可靠性和价值大大提升。

这次更新使SOP和埋点指南形成了一个完美的闭环。埋点指南负责产生带有精确“代码路标”的高质量日志，而SOP则指导AI如何利用这些“路标”直接在源码的“地图”上找到问题的震中。