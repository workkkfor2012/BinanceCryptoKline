# Log MCP Server

为AI模型提供日志查询的高性能MCP协议服务器，采用Windows命名管道实现最佳本地通信性能。

## 🚀 核心特性

- **高性能IPC**: Windows命名管道，比TCP回环更快
- **自动会话管理**: 每次新连接自动清空旧数据，开始全新调试会话
- **常驻服务**: 一次启动，支持无限次调试会话
- **配置驱动**: 其他程序可读取config.toml获取连接信息
- **跨平台兼容**: Windows命名管道 + 非Windows TCP回退

## 📋 快速开始

### 1. 启动服务

```bash
cd src/Log-MCP-Server
cargo run --bin log_mcp_daemon
```

启动后显示：
```
[Daemon] Log MCP Daemon is running (Named Pipe Auto-Clear Session Mode).
[Daemon] -> Listening for logs on named pipe: \\.\pipe\kline_mcp_log_pipe
[Daemon] -> Listening for MCP queries on http://127.0.0.1:9001
```

### 2. 配置文件

`config.toml`:
```toml
[server]
mcp_port = 9001

[logging]
pipe_name = "kline_mcp_log_pipe"  # 其他程序读取此配置
enable_debug_output = true
auto_clear_on_new_session = true
```

### 3. 发送日志

**Windows (推荐):**
```powershell
# 读取配置获取管道名称
$config = Get-Content "config.toml" -Raw
$pipeName = ($config | Select-String 'pipe_name\s*=\s*"([^"]+)"').Matches[0].Groups[1].Value

# 连接命名管道
$pipe = New-Object System.IO.Pipes.NamedPipeClientStream(".", $pipeName, [System.IO.Pipes.PipeDirection]::Out)
$pipe.Connect()
$writer = New-Object System.IO.StreamWriter($pipe)

# 发送JSON日志
$log = '{"type":"span","id":"test_001","trace_id":"trace_001","name":"test_function","target":"test_module","level":"INFO","timestamp":"2025-06-29T12:00:00.000Z","attributes":{},"events":[]}'
$writer.WriteLine($log)
$writer.Close()
$pipe.Close()
```

**非Windows:**
```bash
echo '{"type":"span","id":"test_001","trace_id":"trace_001","name":"test_function","target":"test_module","level":"INFO","timestamp":"2025-06-29T12:00:00.000Z","attributes":{},"events":[]}' | nc localhost 9000
```

### 4. 查询日志

```bash
# 查询所有日志
curl -X POST http://localhost:9001/query -H "Content-Type: application/json" -d "{}"

# 按条件查询
curl -X POST http://localhost:9001/query -H "Content-Type: application/json" -d '{"trace_id":"trace_001","level":"INFO","limit":10}'

# 清空数据库
curl -X POST http://localhost:9001/clear
```

## 📊 日志格式

```json
{
  "type": "span",
  "id": "唯一标识符",
  "trace_id": "追踪ID",
  "parent_id": "父级ID或null",
  "name": "函数/操作名称",
  "target": "模块名称",
  "level": "INFO|WARN|ERROR|DEBUG",
  "timestamp": "2025-06-29T12:00:00.000Z",
  "duration_ms": 123.45,
  "attributes": {
    "自定义属性": "值"
  },
  "events": []
}
```

## 🔍 查询参数

- `trace_id`: 按追踪ID过滤
- `target`: 按模块名过滤（支持部分匹配）
- `level`: 按日志级别过滤
- `start_time`: 开始时间过滤
- `end_time`: 结束时间过滤
- `limit`: 限制返回数量

## 🔄 理想工作流程

1. **启动一次**: `cargo run --bin log_mcp_daemon`
2. **无限循环**:
   - 修改代码
   - 运行程序 → 自动清空旧日志 → 写入新日志
   - 程序结束
   - AI查询分析日志
   - 重复...

## ⚡ 性能优势

- **命名管道 vs TCP**: 延迟降低30-50%
- **内存数据库**: 查询响应 < 1ms
- **迭代器风格**: 高效函数式查询
- **自动会话**: 零干预调试体验

## 🧪 测试验证

```bash
# 测试命名管道功能
powershell -ExecutionPolicy Bypass -File "test_named_pipe.ps1"

# 测试多会话功能
powershell -ExecutionPolicy Bypass -File "test_multi_session.ps1"
```

成功输出：`✅ 命名管道功能正常工作！`

## 🏗️ 架构设计

- **双端口架构**: 命名管道接收日志，HTTP提供查询
- **自动会话隔离**: 每次新连接开始全新会话
- **配置化连接**: 其他程序读取config.toml获取管道名称
- **跨平台兼容**: Windows高性能 + 非Windows兼容性

## ❓ 常见问题

**Q: 如何在其他程序中连接？**
A: 读取 `config.toml` 获取 `pipe_name`，使用 `\\.\pipe\{pipe_name}` 连接

**Q: 支持并发连接吗？**
A: 支持，但每次新连接会清空旧数据，适合单次调试会话

**Q: 如何停止服务？**
A: Ctrl+C 优雅关闭
