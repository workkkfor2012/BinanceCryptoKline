# 币安U本位永续合约K线下载器

这是一个使用Rust实现的高性能K线数据下载工具，专门用于下载币安U本位永续合约的K线数据。该工具实现了自动切换API端点的功能，默认使用`data-api.binance.vision`，如果失败则自动切换到使用代理的`fapi.binance.com`。

## 特点

- 使用Rust语言实现，性能高效
- 支持并发下载多个交易对的K线数据
- 自动切换API端点，提高可用性
- 固定下载起始时间为2025年4月8日
- 支持多种K线周期（1分钟和5分钟）
- 自动处理API限制和错误重试
- 将数据保存到SQLite数据库，便于后续分析
- 详细的日志记录和进度报告

## 安装

确保您已安装Rust和Cargo。然后，您可以通过以下方式构建项目：

```bash
cd src/kline_downloader_rust
cargo build --release
```

编译后的可执行文件将位于`target/release/kline_downloader_rust`。

## 使用方法

运行提供的批处理文件即可启动下载过程：

```bash
# 运行下载程序（从2025年4月8日开始下载所有U本位永续合约的1分钟和5分钟K线数据）
./download_from_april8.bat
```

所有参数已经硬编码到代码中，不需要手动指定。主要参数设置如下：

- **起始时间**: 2025年4月8日
- **K线周期**: 1分钟和5分钟
- **并发数量**: 15
- **存储方式**: SQLite数据库
- **交易对**: 所有U本位永续合约
- **API端点**: 自动切换机制

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

## 自动切换机制

该工具实现了自动切换API端点的功能：

1. 默认情况下使用`data-api.binance.vision`端点
2. 如果请求失败或数据解析错误，自动切换到使用代理的`fapi.binance.com`端点
3. 使用原子布尔值记录当前是否使用代理，确保一旦切换就对所有请求生效

这种机制确保了下载过程的可靠性，即使其中一个端点不可用，也能通过另一个端点获取数据。

## 注意事项

- 该工具使用币安的公共API，请注意API使用限制
- 下载大量数据可能需要较长时间
- 数据存储在SQLite数据库中，位于`./data/klines.db`

## 流程图

详细的流程图可以在`docs/kline_downloader_flowcharts.md`文件中查看。
