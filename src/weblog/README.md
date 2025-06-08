# WebLog - 通用Web日志显示系统

WebLog是一个完全独立的、基于Rust tracing规范的通用日志可视化工具，提供现代化的Web界面来监控和分析任何遵循tracing规范的应用程序日志。

## 🌟 主要功能

### 实时日志流显示
- 支持tracing格式的结构化日志解析（JSON和文本格式）
- 实时WebSocket数据推送
- 多种日志输入源（标准输入、文件、TCP）
- 日志级别过滤和统计

### Trace/Span可视化
- 分布式追踪可视化
- 甘特图显示Span时间线
- Trace层次结构展示
- 性能瓶颈分析

### 通用日志监控
- 按目标(target)分类统计
- 按日志级别统计
- 实时日志流监控
- 结构化字段展示

### 性能分析
- Trace持续时间统计
- Span性能指标
- 系统运行时间监控
- 日志吞吐量统计

## 🚀 快速开始

### 基本使用
```bash
# 从标准输入读取日志
echo '{"timestamp":"2024-01-01T12:00:00Z","level":"INFO","target":"app","message":"Hello"}' | cargo run --bin weblog

# 从文件读取日志
cargo run --bin weblog file --path app.log

# 监控文件变化
cargo run --bin weblog file --path app.log --follow

# 监听TCP端口
cargo run --bin weblog tcp --addr 0.0.0.0:9999
```

### 配置选项
```bash
# 指定Web端口
cargo run --bin weblog --port 3000

# 设置日志级别
cargo run --bin weblog --log-level debug

# 设置最大Trace数量
cargo run --bin weblog --max-traces 2000
```

### 与应用程序集成
```bash
# 将应用程序日志通过管道传递给weblog
your_rust_app 2>&1 | cargo run --bin weblog

# 或者让应用程序发送日志到TCP端口
cargo run --bin weblog tcp --addr 0.0.0.0:9999 &
your_app_with_tcp_logging
```

## 📁 项目结构

```
src/weblog/
├── Cargo.toml              # 独立的Cargo配置
├── README.md               # 本文档
├── bin/
│   └── weblog.rs           # WebLog主程序（通用日志可视化工具）
├── src/
│   ├── lib.rs              # 库入口
│   ├── types.rs            # 数据类型定义
│   ├── log_parser.rs       # 日志解析器
│   ├── trace_manager.rs    # Trace管理器
│   └── web_server.rs       # Web服务器
├── static/                 # 前端静态文件
│   ├── dashboard.html      # 主仪表板
│   ├── trace_viewer.html   # Trace可视化
│   ├── module_monitor.html # 模块监控
│   ├── index.html          # 首页
│   └── test.html           # 测试页面
├── start_simple.ps1        # WebLog简单启动脚本
└── start_weblog_with_window.ps1  # WebLog独立窗口启动脚本
```

## 🔧 配置选项

### 环境变量
- `LOG_TRANSPORT`: 日志传输方式 (`named_pipe` | `websocket` | 无)
- `PIPE_NAME`: 命名管道名称 (默认: `\\.\pipe\kline_log_pipe`)
- `WS_URL`: WebSocket URL (默认: `ws://localhost:8080/ws`)
- `WEB_PORT`: Web服务端口 (默认: 8080)
- `RUST_LOG`: 日志级别 (默认: info)

### 配置示例
```powershell
# 命名管道模式
$env:LOG_TRANSPORT = "named_pipe"
$env:PIPE_NAME = "\\.\pipe\kline_log_pipe"
$env:WEB_PORT = "3000"

# WebSocket模式
$env:LOG_TRANSPORT = "websocket"
$env:WS_URL = "ws://localhost:8080/ws"
```

## 🌐 Web界面

### 主仪表板 (/)
- 系统健康分数
- 验证事件统计
- 性能指标概览
- 6大模块状态监控
- 实时告警面板

### Trace可视化 (/trace)
- Trace列表
- 甘特图时间线
- Span详情面板
- 性能分析

### 模块监控 (/modules)
- 模块日志流
- 过滤和搜索
- 实时状态更新

## 🔌 API接口

### WebSocket
- `ws://localhost:8080/ws` - 实时数据推送

### REST API
- `GET /api/status` - 系统状态
- `GET /api/traces` - Trace列表
- `GET /api/trace/:id` - Trace详情
- `GET /api/modules` - 模块列表
- `POST /api/log` - 日志提交

## 🛠️ 开发指南

### 添加新的日志解析器
```rust
// 在 log_parser.rs 中添加
pub fn parse_custom_log(line: &str) -> Option<LogEntry> {
    // 实现自定义解析逻辑
}
```

### 扩展WebSocket消息类型
```rust
// 在 types.rs 中添加
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum WebSocketMessage {
    // 现有消息类型...
    CustomMessage { data: CustomData },
}
```

### 添加新的API端点
```rust
// 在 web_server.rs 中添加
async fn custom_api_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // 实现API逻辑
}
```

## 🔍 故障排除

### 常见问题

1. **命名管道连接失败**
   - 确保日志服务器已启动
   - 检查管道名称是否正确
   - 验证权限设置

2. **WebSocket连接断开**
   - 检查网络连接
   - 确认端口未被占用
   - 查看浏览器控制台错误

3. **日志解析失败**
   - 检查日志格式
   - 验证JSON结构
   - 查看解析器错误日志

### 调试模式
```powershell
$env:RUST_LOG = "debug"
cargo run --bin weblog -- --pipe-name "\\.\pipe\kline_log_pipe"
```

## 📊 性能优化

- 内存中保留最多1000个Trace
- 日志条目限制10000条
- WebSocket消息批量发送
- 静态文件缓存
- 异步日志处理

## 🤝 贡献指南

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 创建Pull Request

## 📄 许可证

本项目采用MIT许可证。
