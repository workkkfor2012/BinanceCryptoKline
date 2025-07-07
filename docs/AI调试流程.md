# docs\AI驱动的系统诊断标准操作流程 (SOP).md
# AI驱动的系统诊断标准操作流程 (SOP) v3.2 - 性能与源码分析强化版

## 1. 文档目的

本文档旨在为能够访问源代码的AI助手或系统维护人员提供一个标准化的、可重复的流程，以诊断和分析由“AI原生日志系统”记录的应用程序的**逻辑正确性与性能表现**问题。该流程的核心是**“信号驱动的现场重现 (Signal-Driven Scene Recreation)”**，确保以最高效的方式，从海量原始日志中定位问题根源。

## 2. 核心组件与信息源

诊断过程依赖于以下三个核心信息源：

| 组件 | 角色 | 交互方式 | 关键信息 |
| --- | --- | --- | --- |
| 问题摘要日志<br>(problem_summary.log) | 分析的起点，问题信号的集合 | 完整文件读取 | ERROR事件、WARN事件（业务断言失败、**性能SLA违规**）。提供`trace_id`、`span_id`及**精确的代码位置指针** (`file`, `line`)。 |
| 日志MCP守护进程<br>(log_mcp_daemon) | 系统的“数字孪生”，真相之源 | 通过HTTP API按需查询 | 本次运行的全部、完整的原始追踪数据 (SpanModel)。 |
| 源代码库 | **程序逻辑的最终权威**，用于解释“为什么”会产生某个日志事件。 | 通过文件读取工具按需访问 | 函数实现、逻辑分支、常量定义、错误处理方式、性能断言逻辑。 |

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
    *   次高优先级: `level: "WARN"` 且 `log_type: "performance_sla_violation"` 的日志。
    *   次高优先级: `level: "WARN"` 且 `log_type: "assertion"` 的日志。
    *   一般优先级: 其他 `level: "WARN"` 的日志。
3.  **构建宏观理解**: 留意`log_type: "checkpoint"`和`log_type: "snapshot"`的INFO日志。
4.  **选定切入点**: 选择一个优先级最高的日志条目。必须从这条日志中提取并记录下`span_id`, `trace_id`以及**关键的运行时变量和代码位置信息 (`file`, `line`)**。

**产出**: 一个明确的“问题坐标”（包含运行时快照和代码位置）和初步假设。
*示例产出*: “我将从`span_id: '104'`（一个`WARN`级别的性能SLA违规事件）开始调查。日志显示该事件发生在 `src/klcommon/monitoring.rs` 的 `drop` 函数中，但其关注点是 `some_critical_operation` 函数。相关的运行时数据为 `duration_ms: 580`, `threshold_ms: 500`。初步怀疑是`some_critical_operation`函数内部存在性能瓶颈。”

### 第二步：获取现场 (Scene Recreation)

**目标**: 根据“问题坐标”，获取问题发生瞬间的、最直接的、最完整的技术和代码上下文。
**行动**:
1.  **数据现场获取**: 使用`Get-SpanDetails -Id <span_id>`命令，获取完整的运行时上下文。
    *   *执行*: `Get-SpanDetails -Id "104"`
2.  **代码现场获取**: **优先使用**第一步中从日志事件里直接获取的`file`和`line`信息。对于性能日志，需要追溯到调用`SlaGuard`的地方。
    *   *执行*: `Get-Code -FilePath "src/my_app/critical_feature.rs" -Line <line_number_of_operation> -ContextLines 15`

**产出**: 一个包含了问题Span所有运行时数据的`SpanModel`对象，以及对应的源代码片段。这是进行推理的核心证据。

### 第三步：纵向分析 (Vertical Analysis: Why & Who)

**目标**: **通过“虚拟执行”代码来解释运行时数据，从而找到问题的直接原因（向下钻取），并结合父Span理解触发背景（向上追溯）。**

**行动**:

**向下钻取 (Why?): 核心推理步骤**

1.  **交叉引用**: 将`SpanModel`中的事件（特别是触发信号的`ERROR`或`WARN`事件）及其附带的运行时变量，与第二步获取的源代码片段进行精确匹配。
2.  **虚拟执行与逻辑推演 (Virtual Execution & Logical Deduction)**:
    *   **明确声明**: “现在，我将结合运行时数据在源码中进行推演。”
    *   **执行推演**: 查看代码中的逻辑分支（`if`, `match`, `?`操作符，性能断言等）。根据日志提供的运行时值，判断程序**必然**会进入哪个分支。
    *   **得出结论**: 明确说明是哪个代码分支或条件判断导致了`ERROR`或`WARN`事件的发生。

    *示例推演过程 (性能问题)*:
    1.  *日志快照*: “`problem_summary.log`中的`WARN`事件显示`log_type: "performance_sla_violation"`, `duration_ms: 580`, `threshold_ms: 500`。”
    2.  *代码获取*: `Get-Code -FilePath "src/klcommon/monitoring.rs" -Line <line_of_if_statement>`
    3.  *代码内容*:
        ```rust
        // SlaGuard's drop implementation
        let duration = self.start_time.elapsed();
        if duration > self.sla_threshold { // <-- 关键判断
            tracing::warn!(
                log_type = "performance_sla_violation",
                // ...
                duration_ms = duration.as_millis(),
                threshold_ms = self.sla_threshold.as_millis(),
            );
        }
        ```
    4.  *推演结论*: “根据运行时数据`duration_ms: 580`和`threshold_ms: 500`，`duration > self.sla_threshold`这个条件为`true`。因此，程序必然进入`if`代码块，触发了`tracing::warn!`，这与日志完全吻合。**根因是受监控代码块的实际耗时(580ms)超过了其性能SLA阈值(500ms)。** 下一步需要分析该代码块内部的具体耗时操作。”

3.  **深入子操作**: 如果当前Span本身没有错误，但状态为FAILURE或耗时过长，执行`Get-ChildrenOf`寻找失败或耗时最长的子Span，对其重复第二步和第三步。

**向上追溯 (Who?)**:
*   分析当前`SpanModel`的`parent_id`，并使用`Get-SpanDetails`获取父Span的上下文，理解其业务背景。

**产出**: 一条清晰的、**有代码逻辑和运行时数据双重佐证的**因果链。

### 第四步：横向分析 (Horizontal Analysis: How Widespread?)

(此步骤无重大变更，其核心是数据聚合，验证模式)
**目标**: 判断当前问题是孤立事件还是普遍现象。
**行动**:
1.  **提出假设**: 基于第三步的发现，形成一个关于问题普遍性的假设。
2.  **数据验证**: 使用`Aggregate-Failures -GroupBy <attribute_name>`来验证假设。（例如，`Aggregate-Failures -GroupBy target_name`来查看哪些操作最常超时。）

**产出**: 对问题范围和影响面的量化认知。

### 第五步：结论与建议 (Conclusion & Recommendation)

(此步骤无重大变更，但其结论的可靠性因第三步的强化而大大增强)
**目标**: 综合所有信息，给出一个包含**代码级证据**的、清晰、准确、可操作的最终报告。
**行动**:
1.  **总结因果链**: 将第三步（**包含虚拟执行的推演**）和第四步的发现串联起来。
2.  **提供证据**: 引用关键的日志条目、`SpanModel`数据和**导致问题的核心代码片段**作为结论的支撑。
3.  **提出代码级解决方案**: 给出具体的、可操作的修复建议。
    *   对于**逻辑错误**，建议可能是代码修改（diff）。
    *   对于**性能问题**，建议可能是：优化算法、为耗时IO操作增加并发、引入缓存、或者在确认业务可接受后调整SLA阈值。

---

