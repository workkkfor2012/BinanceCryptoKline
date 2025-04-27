# K线合成算法改动文档

## 改动概述

本文档描述了对K线合成算法的改动计划，以实现更高效、更准确的K线合成。

## 第一步：删除现有K线合成代码

首先，我们需要删除现有的所有K线合成代码，为新的实现做准备。

### 需要删除的文件/代码：

1. 所有与K线合成相关的模块和函数
2. 任何处理高级别周期K线（5m, 30m, 4h, 1d, 1w）合成的代码
3. 相关的测试代码

### 删除步骤：

1. 识别所有包含K线合成逻辑的文件
2. 备份这些文件（可选，但建议执行）
3. 删除相关代码或整个文件
4. 确保项目仍然可以编译，可能需要临时注释掉依赖于已删除代码的部分

## 第二步：实现新的K线合成算法

在删除旧代码后，我们将根据新的K线合成算法文档实现更高效的合成逻辑。新的实现将包括：

1. 每次接收WebSocket推送的1分钟K线数据时触发合成
2. 使用正确的周期开始时间计算和主键查询
3. 准确计算成交量增量
4. 高效处理K线更新和插入

详细的实现步骤和代码将在后续文档中提供。

## WebSocket连接格式

合约K线WebSocket连接使用以下格式：

```
wss://fstream.binance.com/ws/<pair>_<contractType>@continuousKline_<interval>
```

例如，订阅BTCUSDT永续合约的1分钟K线：

```
wss://fstream.binance.com/ws/btcusdt_perpetual@continuousKline_1m
```

对于组合流订阅，使用以下格式：

```
wss://fstream.binance.com/stream?streams=<pair1>_<contractType>@continuousKline_<interval1>/<pair2>_<contractType>@continuousKline_<interval2>
```

例如，同时订阅BTCUSDT和ETHUSDT永续合约的1分钟K线：

```
wss://fstream.binance.com/stream?streams=btcusdt_perpetual@continuousKline_1m/ethusdt_perpetual@continuousKline_1m
wss://fstream.binance.com/stream?streams=btcusdt_perpetual@continuousKline_1m/ethusdt_perpetual@continuousKline_1m
```

在新的K线合成算法中，我们将使用这种格式的WebSocket连接来接收1分钟K线数据，并在本地合成其他周期的K线。

## 注意事项

- 确保在删除代码前进行充分的备份
- 可能需要临时禁用依赖于K线合成的功能，直到新实现完成
- 删除过程中保持谨慎，确保不会误删其他功能的代码
