# Tracing日志规范文档

## 概述

本文档定义了WebLog系统和所有相关组件（如K线聚合系统）的统一日志格式规范。所有组件必须遵循此规范以确保日志的正确解析和显示。

## 支持的日志格式

### 1. JSON格式（推荐）

JSON格式提供最完整的结构化信息，推荐用于生产环境。

#### 基本结构
```json
{
  "timestamp": "2025-06-05T16:21:26.092541Z",
  "level": "INFO",
  "target": "kline_server::klaggregate::buffered_kline_store",
  "message": "缓冲区交换完成",
  "fields": {
    "key1": "value1",
    "key2": 123
  },
  "span": {
    "id": "span_id_here",
    "trace_id": "trace_id_here"
  }
}
```

#### 必需字段
- `timestamp`: ISO 8601格式的UTC时间戳，精确到微秒
- `level`: 日志级别（TRACE, DEBUG, INFO, WARN, ERROR）
- `target`: 日志来源模块，使用Rust模块路径格式
- `message`: 日志消息内容

#### 可选字段
- `fields`: 结构化字段对象
- `span.id`: Span标识符
- `span.trace_id`: Trace标识符

### 2. 文本格式

文本格式用于简单场景和调试。

#### 标准格式
```
2025-06-05T16:21:26.092541Z  INFO kline_server::klaggregate::buffered_kline_store: 缓冲区交换完成
```

#### 格式规则
- 时间戳：ISO 8601格式，UTC时区，精确到微秒，以Z结尾
- 级别：大写字母（TRACE, DEBUG, INFO, WARN, ERROR）
- 目标：Rust模块路径，使用双冒号分隔
- 消息：冒号后跟一个空格，然后是消息内容

#### 正则表达式
```regex
^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(\w+)\s+([^:]+):\s*(.+)$
```

## 日志级别定义

| 级别  | 用途 | 示例场景 |
|-------|------|----------|
| TRACE | 最详细的调试信息 | 函数进入/退出，变量值变化 |
| DEBUG | 调试信息 | 算法步骤，中间结果 |
| INFO  | 一般信息 | 系统状态，重要操作完成 |
| WARN  | 警告信息 | 可恢复的错误，性能问题 |
| ERROR | 错误信息 | 不可恢复的错误，异常情况 |

## Target命名规范

### 模块命名原则
**强制要求：使用英文模块名作为target字段值**

原因：
- 确保跨平台兼容性和编码稳定性
- 符合行业标准和最佳实践
- 避免命名管道、JSON序列化等环节的编码问题
- 便于第三方工具集成和日志分析

### 标准格式
使用Rust模块路径格式，例如：
- `kline_server::klaggregate::buffered_kline_store`
- `kline_server::klcommon::websocket`
- `weblog::log_parser`

### 功能分类
建议使用以下前缀分类：
- `klaggregate_*`: K线聚合相关
- `kldata_*`: K线数据相关
- `klserver_*`: K线服务器相关
- `weblog_*`: WebLog系统相关
- `observability_*`: 可观察性相关

### 中文显示支持
虽然target字段必须使用英文，但可以通过以下方式提供中文显示：
- 在WebLog前端界面中提供target到中文名称的映射
- 在message字段中使用中文描述
- 通过结构化字段添加中文标签：`display_name = "K线聚合器"`

## 结构化字段规范

### 字段命名
- 使用snake_case命名
- 避免使用保留字段名：`timestamp`, `level`, `target`, `message`
- 使用描述性名称

### 字段类型
- 字符串：`"value"`
- 数字：`123` 或 `123.45`
- 布尔值：`true` 或 `false`
- 数组：`[1, 2, 3]`
- 对象：`{"key": "value"}`

### 常用字段
- `symbol`: 交易品种
- `interval`: 时间周期
- `count`: 数量
- `duration_ms`: 持续时间（毫秒）
- `error_code`: 错误代码
- `connection_id`: 连接标识

## Span和Trace规范

### Span标识符
- 使用UUID或类似的唯一标识符
- 格式：`span_12345678-1234-1234-1234-123456789abc`

### Trace标识符
- 使用UUID格式
- 格式：`trace_12345678-1234-1234-1234-123456789abc`

### Span事件类型
- `new`: Span开始
- `close`: Span结束
- `event`: Span内事件

## 实现指南

### Rust tracing配置

#### 基本配置
```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

// JSON格式输出
Registry::default()
    .with(tracing_subscriber::fmt::layer().json())
    .with(tracing_subscriber::EnvFilter::new("info"))
    .init();

// 文本格式输出
Registry::default()
    .with(tracing_subscriber::fmt::layer())
    .with(tracing_subscriber::EnvFilter::new("info"))
    .init();
```

#### 自定义格式化器
```rust
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::UtcTime;

Registry::default()
    .with(
        tracing_subscriber::fmt::layer()
            .with_timer(UtcTime::rfc_3339())
            .with_target(true)
            .with_level(true)
    )
    .init();
```

### 日志记录最佳实践

#### 使用结构化字段
```rust
// 推荐
info!(
    target = "klaggregate_websocket",
    symbol = "BTCUSDT",
    connection_id = %conn_id,
    "WebSocket连接建立成功"
);

// 不推荐
info!("WebSocket连接建立成功: {} {}", symbol, conn_id);
```

#### 使用instrument宏
```rust
#[instrument(target = "klaggregate_aggregator", fields(symbol, interval), skip(self), err)]
async fn aggregate_kline(&self, symbol: &str, interval: &str) -> Result<()> {
    // 函数实现
}
```

## 验证和测试

### 日志格式验证
WebLog系统提供日志格式验证功能：
- 自动检测日志格式
- 验证必需字段
- 报告解析错误

### 测试用例
```rust
#[test]
fn test_json_format() {
    let log = r#"{"timestamp":"2025-06-05T16:21:26.092541Z","level":"INFO","target":"test","message":"test"}"#;
    assert!(parse_tracing_log_line(log).is_some());
}

#[test]
fn test_text_format() {
    let log = "2025-06-05T16:21:26.092541Z  INFO test::module: test message";
    assert!(parse_tracing_log_line(log).is_some());
}
```

## 错误处理

### 解析失败处理
- 记录原始日志行
- 提供详细的错误信息
- 继续处理后续日志

### 常见问题
1. **时间戳格式错误**: 确保使用UTC时区和正确的ISO 8601格式
2. **Target格式错误**: 使用双冒号分隔的模块路径
3. **JSON格式错误**: 确保JSON语法正确，字段名使用双引号

## 版本历史

- v1.0.0: 初始版本，定义基本格式规范
- v1.1.0: 添加Span和Trace支持
- v1.2.0: 完善结构化字段规范

## 相关文档

- [WebLog系统文档](./weblog_system.md)
- [K线聚合系统文档](../../docs/K线聚合系统使用指南.md)
- [Rust tracing官方文档](https://docs.rs/tracing/)
