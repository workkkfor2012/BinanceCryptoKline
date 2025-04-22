# 程序启动流程

本文档描述了程序的启动流程和各种运行模式。

## 程序组件

程序分为三个独立的可执行文件：

1. **K线下载器 (kline_downloader)**: 用于下载历史K线数据并保存到SQLite数据库
2. **K线服务器 (kline_server)**: 用于启动WebSocket客户端，实时更新K线数据
3. **网页服务器 (kline_web_server)**: 用于启动Web服务器，从数据库读取K线数据并提供Web界面

## K线下载器启动流程

1. 初始化日志系统
2. 解析命令行参数（周期、并发数等）
3. 创建下载器配置
4. 下载所有币安U本位永续合约的历史K线数据
5. 将数据存储到SQLite数据库中
6. 下载完成后自动退出

## K线服务器启动流程

1. 初始化日志系统
2. 解析命令行参数（测试模式、跳过检查等）
3. 创建数据库连接
4. 检查数据库文件是否存在
   - 如果数据库不存在，提示错误并退出
5. 如果没有跳过检查，检查BTC 1分钟K线数量
   - 如果K线数量不足，提示错误并退出
6. 启动K线合成器
7. 启动WebSocket客户端，订阅实时K线数据

## 网页服务器启动流程

1. 初始化日志系统
2. 创建数据库连接
3. 检查数据库文件是否存在
   - 如果数据库不存在，提示错误并退出
4. 检查BTC 1分钟K线数量
   - 如果K线数量不足，提示错误并退出
5. 启动Web服务器，从数据库读取K线数据并提供Web界面

## 运行模式

### K线下载器运行模式

```bash
# 下载历史K线数据
cargo run --release --bin kline_downloader
```

或者使用批处理文件：

```batch
download_klines.bat
```

### K线服务器运行模式

1. **正常模式**: 启动K线服务器，实时更新K线数据。

```bash
cargo run --release --bin kline_server
```

或者使用批处理文件：

```batch
start_server.bat
```

2. **跳过检查模式**: 启动K线服务器，但跳过数据库检查。

```bash
cargo run --release --bin kline_server -- --skip-check
```

3. **测试模式**: 用于测试连续合约K线客户端的特定功能。

```bash
cargo run --release --bin kline_server -- test
```

### 网页服务器运行模式

1. **正常模式**: 启动网页服务器，从数据库读取K线数据并提供Web界面。

```bash
cargo run --release --bin kline_web_server
```

或者使用批处理文件：

```batch
start_web_server.bat
```

## 连续合约K线模式说明

连续合约K线模式使用 `<symbol>_perpetual@continuousKline_<interval>` 格式的WebSocket流，订阅币安U本位永续合约的K线数据。

主要特点：
1. 支持所有交易对和周期（1m, 5m, 30m, 4h, 1d, 1w）
2. 自动将交易对和周期分批，每个批次最多订阅1000个流
3. 每个批次使用一个单独的WebSocket连接
4. 通过SOCKS5代理连接到币安WebSocket服务
5. 实现了自动重连和心跳机制
6. 只处理已关闭的K线，确保数据完整性

## 运行流程图

以下是程序的运行流程图：

```mermaid
graph TD
    A[程序启动] --> B(初始化日志);
    B --> C{是否测试模式?};

    C -- 是 --> D(创建数据库连接);
    D --> E(创建 TestContinuousKlineClient 配置);
    E --> F(创建并启动 TestContinuousKlineClient);
    F --> G(测试模式客户端运行);

    C -- 否 --> H(创建数据库连接);
    H --> I(从数据库获取交易对列表);
    I --> J(创建 ContinuousKlineConfig);
    J --> K(创建并启动 ContinuousKlineClient);

    K --> L(启动日志记录任务);
    K --> M(准备订阅 Streams);
    M --> N(计算连接数);
    N --> O{分批创建连接并订阅};

    O --> P(通过 SOCKS5 代理建立 TCP 连接);
    P --> Q(建立 TLS 连接);
    Q --> R(WebSocket 握手);
    R --> S(发送 SUBSCRIBE 消息);
    S --> T(WebSocket 连接建立并订阅);

    O --> T;

    K --> U(处理所有连接消息);
    U --> V(启动统计信息输出任务);
    U --> W{为每个连接创建消息处理任务};

    W --> X(接收 WebSocket 消息);
    X --> Y{消息类型?};

    Y -- 文本消息 --> Z(发送到日志任务);
    Z --> AA{尝试解析为 KlineResponse};

    AA -- 成功且为 continuous_kline --> AB(更新统计计数);
    AB --> AC(处理 K 线数据);
    AC --> AD(保存到数据库);

    AA -- 订阅响应 --> AE(处理订阅响应);
    AA -- 其他 --> AF(记录其他消息);

    Y -- Ping/Pong --> AG(处理心跳);
    Y -- 连接断开/错误 --> AH(尝试重连);
    AH --> O;

    F --> AI(程序结束);
    AD --> AI;
    AE --> AI;
    AF --> AI;
    AG --> AI;

    classDef default fill:#eee,stroke:#333,stroke-width:2px,color:#000,font-size:40px,font-weight: bold;
    classDef decision fill:#ddd,stroke:#333,stroke-width:2px,color:#000,font-size:40px,font-weight: bold;
    class C,AA,Y,O decision;
