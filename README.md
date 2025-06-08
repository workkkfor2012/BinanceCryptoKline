db相关文档在docs/db.md中
api相关文档在docs/api.md中

# 币安U本位永续合约K线服务器

这是一个使用Rust实现的高性能K线数据服务器，专门用于管理和提供币安U本位永续合约的K线数据。该工具仅使用`https://fapi.binance.com`作为唯一的API端点，采用SQLite的WAL模式提供接近内存数据库的性能。

## 特点

- 使用Rust语言实现，性能高效
- 支持并发下载多个交易对的K线数据
- 仅使用币安的fapi端点，简化访问逻辑
- 支持多种K线周期（1分钟、5分钟、30分钟、4小时、1天、1周）
- 自动处理API限制和错误重试
- 集中式代理配置，便于统一管理和修改
- 采用SQLite的WAL模式，提供接近内存数据库的性能
  - 高性能：读写速度接近内存数据库
  - 数据持久化：所有数据安全存储在磁盘上
  - 内存占用低：不会因数据增长而导致OOM
- 详细的日志记录和进度报告
- 启动时自动检查数据库和历史K线数据，必要时自动下载
- 下载完成后自动统计主要币种K线数量并验证是否正确
- 为Windows系统设计的高性能应用

## 安装

确保您已安装Rust和Cargo。然后，您可以通过以下方式构建项目：

```bash
cargo build --release
```

编译后的可执行文件将位于`target/release/kline_server`。

## 使用方法

程序提供了多种启动方式：

```batch
# 一键启动所有服务（如果数据库不存在，会先下载历史数据）
.\start_all.bat

# 启动K线数据服务（下载历史数据并实时更新）
.\start_kldata_service.bat

# 启动Web服务器（提供Web界面）
.\start_klserver.bat
```

或者使用命令行参数：

```batch
# 启动K线数据服务（下载历史数据并实时更新）
cargo run --release --bin kline_data_service

# 仅下载历史数据，不启动最新K线更新器
cargo run --release --bin kline_data_service -- --no-latest-kline-updater

# 仅启动实时更新，不下载历史数据
cargo run --release --bin kline_data_service -- --stream-only

# 启动Web服务器
cargo run --release --bin kline_server

# 跳过数据库检查启动Web服务器
cargo run --release --bin kline_server -- --skip-check
```

程序会自动使用SQLite的WAL模式，提供高性能的数据库操作。

### 程序组件

程序分为两个独立的可执行文件：

1. **K线数据服务 (kline_data_service)**: 用于下载历史K线数据、实时更新和合成其他周期的K线。
   ```bash
   cargo run --release --bin kline_data_service
   ```
   或者使用批处理文件：
   ```batch
   .\start_kldata_service.bat
   ```
   数据服务会下载历史K线数据，并通过WebSocket实时更新K线数据。它还会在本地合成其他周期的K线。

2. **Web服务器 (kline_server)**: 启动Web服务器，从数据库读取K线数据并提供Web界面。
   ```bash
   cargo run --release --bin kline_server
   ```
   或者使用批处理文件：
   ```batch
   .\start_klserver.bat
   ```
   Web服务器会启动Web服务，从数据库读取K线数据并提供Web界面。

### 运行模式

#### K线数据服务运行模式

K线数据服务支持以下运行模式：

1. **完整模式**: 下载历史数据并启动实时更新。
   ```bash
   cargo run --release --bin kline_data_service
   ```

2. **仅下载模式**: 仅下载历史数据，不启动最新K线更新器。
   ```bash
   cargo run --release --bin kline_data_service -- --no-latest-kline-updater
   ```

3. **仅实时模式**: 仅启动实时更新，不下载历史数据。
   ```bash
   cargo run --release --bin kline_data_service -- --stream-only
   ```

#### Web服务器运行模式

Web服务器支持以下运行模式：

1. **正常模式**: 启动Web服务器，从数据库读取K线数据并提供Web界面。
   ```bash
   cargo run --release --bin kline_server
   ```

2. **跳过检查模式**: 启动Web服务器，但跳过数据库检查。
   ```bash
   cargo run --release --bin kline_server -- --skip-check
   ```
## 启动流程

### K线数据服务启动流程

1. 初始化日志系统
2. 解析命令行参数（周期、并发数、运行模式等）
3. 创建数据库连接
4. 如果不是仅实时模式，则下载历史K线数据
   - 创建下载器配置
   - 下载指定交易对的历史K线数据
   - 将数据存储到SQLite数据库中
5. 创建并启动K线聚合器，用于合成其他周期的K线
6. 如果启用了最新K线更新器，则创建并启动最新K线更新任务
7. 如果启用了高频数据(eggtrade)，则创建并启动WebSocket客户端，订阅实时交易数据

### Web服务器启动流程

1. 初始化日志系统
2. 解析命令行参数（跳过检查等）
3. 创建数据库连接
4. 检查数据库文件是否存在
   - 如果数据库不存在，提示错误并退出
5. 如果没有跳过检查，检查BTC 1分钟K线数量
   - 如果K线数量不足，提示错误并退出
6. 启动Web服务器，从数据库读取K线数据并提供Web界面

程序使用SQLite的WAL模式，提供高性能的数据库操作，同时确保数据的持久性和可靠性。

## 下载参数

历史K线下载时使用的参数：

- **K线周期**: 1分钟、5分钟、30分钟、1小时，4小时、1天、1周
- **并发数量**: 15
- **存储方式**: SQLite数据库
- **交易对**: 所有U本位永续合约
- **API端点**: 自动切换机制
- **K线数量**:
  - 1分钟K线：5000根
  - 5分钟K线：5000根
  - 30分钟K线：3000根
  - 4小时K线：3000根
  - 1天K线：2000根
  - 1周K线：1000根

## 数据格式

下载的数据将保存到SQLite数据库中，每个交易对和周期组合有自己的表（例如：btc_1m、eth_5m）。每个表包含以下字段：

- `open_time`: K线开盘时间（毫秒时间戳）
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `close_time`: K线收盘时间（毫秒时间戳）
- `quote_asset_volume`: 报价资产成交量
- `number_of_trades`: 成交笔数
- `taker_buy_base_asset_volume`: 主动买入基础资产成交量
- `taker_buy_quote_asset_volume`: 主动买入报价资产成交量

## API访问说明

该工具仅使用币安的`fapi.binance.com`端点：

1. 直接访问`https://fapi.binance.com`端点
2. 使用标准HTTP请求获取交易所信息和K线数据
3. 通过配置的代理服务器访问API（默认为127.0.0.1:1080）
4. 简化了代码逻辑，提高了程序的可维护性

所有网络请求（包括HTTP API和WebSocket连接）都通过集中配置的代理服务器进行，确保在网络环境受限的情况下也能正常访问币安API。

## SQLite WAL模式

本项目采用SQLite的WAL（Write-Ahead Logging）模式，这是一种高性能的数据库运行模式。WAL模式具有以下主要特点：

- **接近内存数据库的性能**：WAL模式下的SQLite性能可以达到内存数据库的70%-100%
- **消除了写入阻塞读取**：写入和读取可以并发进行，显著提高并发性能
- **顺序写入代替随机写入**：顺序写入比随机写入快10-100倍
- **数据持久化**：所有数据变更都安全地存储在磁盘上
- **内存占用可控**：不会因为数据增长而导致内存溢出（OOM）

在本项目中，WAL模式特别适用于以下场景：

1. **高性能服务器应用**：当项目作为服务器应用运行时，WAL模式提供接近内存数据库的性能
2. **内存受限的环境**：在内存有限（4GB）但数据量大的环境中，WAL模式是理想的选择
3. **高频数据查询**：对于需要频繁查询历史K线数据的场景，WAL模式提供高效的读取性能

要了解更多关于SQLite WAL模式的详细信息，请参阅[SQLite WAL模式详解](docs/sqlite_wal_mode.md)文档。


## 代理设置

本项目使用集中式代理配置，所有需要访问网络的组件都从同一个配置源获取代理设置。代理配置位于`src/klcommon/proxy.rs`文件中：

```rust
/// 代理服务器地址
pub const PROXY_HOST: &str = "127.0.0.1";

/// 代理服务器端口
pub const PROXY_PORT: u16 = 1080;
```

如需修改代理设置，只需编辑上述文件中的常量值即可。这种集中式配置方式确保了：

1. **统一管理**：所有组件使用相同的代理设置
2. **易于维护**：只需在一处修改即可更新所有组件的代理设置
3. **配置清晰**：明确指定代理地址和端口，避免硬编码

代理设置应用于以下组件：

- API请求（历史K线下载、交易对信息获取等）
- WebSocket连接（实时K线数据、归集交易数据等）

## 注意事项

- 该工具使用币安的公共API，请注意API使用限制
- 下载大量数据可能需要较长时间
- 数据存储在SQLite数据库中，位于`./data/klines.db`
- 采用WAL模式提供高性能的数据库操作，同时确保数据安全
- 确保代理服务器正常运行，否则将无法连接到币安API

## 流程图

详细的启动流程可以在`docs\kline_data_download_flow.md`文件中查看。
