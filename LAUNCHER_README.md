# 币安U本位永续合约K线下载器启动说明

本项目提供了Windows和Linux两个版本的启动脚本，用于运行币安U本位永续合约K线数据下载工具。

## 功能特点

- 自动检查并创建数据目录
- 检查可执行文件是否存在，不存在则尝试编译
- 运行下载程序，获取币安U本位永续合约的K线数据
- 数据保存到SQLite数据库中，便于后续分析

## Windows版本使用方法

1. 确保已安装Rust和Cargo（如果需要编译）
2. 双击运行 `start_downloader_windows.bat` 文件
3. 程序将自动创建data目录并开始下载数据
4. 下载完成后，数据将保存在 `data/klines.db` 文件中

## Linux版本使用方法

1. 确保已安装Rust和Cargo（如果需要编译）
2. 首先给脚本添加执行权限：
   ```bash
   chmod +x start_downloader_linux.sh
   ```
3. 运行脚本：
   ```bash
   ./start_downloader_linux.sh
   ```
4. 程序将自动创建data目录并开始下载数据
5. 下载完成后，数据将保存在 `data/klines.db` 文件中

## 注意事项

- 下载过程可能需要较长时间，取决于网络状况和要下载的数据量
- 程序会自动处理API限制和错误重试
- 如果下载过程中断，再次运行脚本将从上次中断的地方继续下载
- 数据存储在SQLite数据库中，可以使用任何支持SQLite的工具查看和分析数据

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
