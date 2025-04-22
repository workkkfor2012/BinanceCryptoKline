# K线服务重构计划

本文档描述了对币安U本位永续合约K线服务器项目的重构计划，旨在改进代码架构，使各模块职责更加清晰，并通过SQLite数据库实现完全解耦。

## 当前架构问题

当前的架构存在以下问题：

1. **模块职责不清晰**：
   - `kldownload` 模块不仅包含下载历史 K 线的功能，还包含 WebSocket 连接和 K 线合成的功能
   - `klserver` 模块直接依赖 `kldownload` 模块中的多个组件

2. **命名不准确**：
   - `kldownload` 的名称无法准确反映其实际功能（下载、实时更新和合成）

3. **代码重复**：
   - `klserver` 和 `kldownload` 模块都有自己的 `db.rs`、`models.rs` 等文件，内容几乎相同

4. **耦合度高**：
   - `kl_server` 程序直接使用 `kldownload` 模块中的 `KlineAggregator` 和 `ContinuousKlineClient`
   - 这使得两个组件无法独立开发和测试

## 重构目标

1. 创建清晰的模块结构，每个模块有明确的职责
2. 通过SQLite数据库实现模块间的完全解耦
3. 提高代码的可维护性和可测试性
4. 使模块名称准确反映其功能

## 新架构设计

重构后的项目将包含三个主要模块：

1. **`klcommon`**：共享的数据模型、工具和数据库操作
2. **`kldata`**：负责数据获取、处理和存储
3. **`klserver`**：负责Web服务和API

### 目录结构

```
src/
├── klcommon/              # 共享模块
│   ├── models.rs          # 共享数据模型
│   ├── db.rs              # 共享数据库操作
│   ├── error.rs           # 共享错误类型
│   └── mod.rs             # 模块导出
├── kldata/                # 数据服务模块
│   ├── downloader/        # 历史数据下载
│   │   ├── config.rs      # 下载配置
│   │   ├── api.rs         # API客户端
│   │   └── mod.rs         # 下载器实现
│   ├── streamer/          # WebSocket实时数据
│   │   ├── config.rs      # WebSocket配置
│   │   ├── connection.rs  # 连接管理
│   │   ├── message.rs     # 消息处理
│   │   └── mod.rs         # 客户端实现
│   ├── aggregator/        # K线合成
│   │   ├── processor.rs   # 合成处理器
│   │   └── mod.rs         # 合成器实现
│   └── mod.rs             # 模块导出
├── klserver/              # Web服务器模块
│   ├── web/               # Web服务
│   │   ├── server.rs      # 服务器实现
│   │   ├── handlers.rs    # 请求处理器
│   │   └── mod.rs         # 模块导出
│   ├── api.rs             # API客户端
│   ├── config.rs          # 服务器配置
│   └── mod.rs             # 模块导出
├── bin/                   # 可执行文件
│   ├── kline_data_service.rs  # 数据服务程序
│   └── kline_server.rs        # Web服务器程序
└── lib.rs                 # 导出所有模块
```

## 重构步骤

### 第一步：创建 klcommon 模块

1. 创建 `src/klcommon` 目录
2. 实现共享的数据模型 (`models.rs`)
3. 实现共享的数据库操作 (`db.rs`)
4. 实现共享的错误类型 (`error.rs`)
5. 创建模块导出文件 (`mod.rs`)

### 第二步：重构 kldownload 为 kldata

1. 将 `src/kldownload` 重命名为 `src/kldata`
2. 重组内部结构，按功能划分子模块：
   - `downloader`：历史数据下载
   - `streamer`：WebSocket实时数据
   - `aggregator`：K线合成
3. 更新所有引用路径
4. 确保所有子模块使用 `klcommon` 中的共享组件

### 第三步：重构 klserver 模块

1. 移除对 `kldownload` 的依赖，改为依赖 `klcommon`
2. 更新数据库操作，确保只从数据库读取数据
3. 简化Web服务器实现，专注于提供API和静态文件服务

### 第四步：更新可执行文件

1. 创建 `src/bin/kline_data_service.rs`，替代原有的 `kline_downloader.rs`
2. 更新 `src/bin/kline_server.rs`，移除对 `kldata` 的直接依赖
3. 确保两个程序可以独立运行，只通过数据库交互

### 第五步：更新批处理文件

1. 创建 `start_kldata_service.bat`，替代原有的 `start_kldownload.bat`
2. 更新 `start_klserver.bat`，确保正确启动新的服务
3. 更新 `start_all.bat`，按正确顺序启动所有服务

## 模块职责详解

### klcommon

- 提供共享的数据模型（如 `Kline`）
- 提供共享的数据库操作（如表创建、数据读写）
- 提供共享的错误类型和工具函数

### kldata

- **downloader**：负责下载历史K线数据
  - 获取交易对列表
  - 下载各个周期的历史K线
  - 将数据保存到数据库

- **streamer**：负责WebSocket实时数据
  - 建立与币安的WebSocket连接
  - 接收实时K线更新
  - 将数据传递给合成器和数据库

- **aggregator**：负责K线合成
  - 接收1分钟K线数据
  - 合成其他5个周期的K线
  - 将合成结果保存到数据库

### klserver

- **web**：负责Web服务
  - 提供静态文件服务
  - 提供K线数据API
  - 处理用户请求

## 数据流

1. `kldata/downloader` 下载历史K线数据并保存到数据库
2. `kldata/streamer` 接收实时1分钟K线更新并保存到数据库
3. `kldata/aggregator` 读取1分钟K线，合成其他周期K线，并保存到数据库
4. `klserver/web` 从数据库读取K线数据，提供给前端

## 预期收益

1. **职责清晰**：每个模块都有明确的职责
2. **解耦合**：模块间只通过数据库交互，不直接依赖
3. **可维护性**：每个模块可以独立开发和测试
4. **命名准确**：模块名称准确反映其功能
5. **扩展性**：新功能可以更容易地集成到现有架构中

## 注意事项

1. 重构过程中需要保持功能完整性，确保所有现有功能正常工作
2. 数据库表结构应保持一致，确保模块间可以正确交互
3. 应编写充分的测试，验证重构后的代码行为与原代码一致
4. 文档应及时更新，反映新的架构和使用方法
