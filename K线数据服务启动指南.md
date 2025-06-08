# K线数据服务启动指南

## 概述

K线数据服务是币安U本位永续合约K线数据的下载和处理系统，支持多种K线周期的历史数据补齐和实时数据更新。现已集成WebLog日志系统，支持实时日志可视化。

## 🚀 快速启动

### 方案1：一键启动（推荐）
```powershell
# 同时启动K线数据服务和WebLog系统
.\start_kline_data_simple.ps1
```

### 方案2：单独启动
```powershell
# 只启动K线数据服务（可选择日志模式）
.\start_kline_data_service.ps1
```

### 方案3：分别启动
```powershell
# 先启动WebLog系统
.\src\weblog\start_simple.ps1

# 再启动K线数据服务
.\start_kline_data_service.ps1
```

## 📋 脚本说明

### start_kline_data_simple.ps1 ⭐ **推荐**
- **用途**: 快速启动K线数据服务和WebLog系统
- **特点**: 简化操作，自动配置，一键启动
- **窗口**: 两个独立PowerShell窗口
- **访问**: http://localhost:8080/modules

### start_kline_data_service.ps1
- **用途**: 单独启动K线数据服务
- **选项**: 支持文件日志或命名管道传输
- **灵活性**: 可以选择是否连接WebLog系统
- **适用**: 需要单独测试K线服务时

## 🏗️ 系统架构

```
┌─────────────────┐    命名管道    ┌─────────────────┐
│  K线数据服务    │ ──────────────> │   WebLog系统    │
│                 │                 │                 │
│ • 历史数据补齐  │                 │ • 实时日志显示  │
│ • 实时数据更新  │                 │ • Web界面监控   │
│ • 归集交易处理  │                 │ • 模块分类显示  │
│ • 服务器时间同步│                 │ • 高频日志聚合  │
└─────────────────┘                 └─────────────────┘
```

## 📊 服务功能

### K线数据服务特性
- **多周期支持**: 1m, 5m, 30m, 1h, 4h, 1d, 1w
- **历史数据补齐**: 自动下载缺失的历史K线数据
- **服务器时间同步**: 每分钟同步一次币安服务器时间
- **实时数据更新**: 使用归集交易数据实时合成K线
- **测试配置**: 当前仅使用BTCUSDT交易对进行测试

### WebLog系统特性
- **实时日志流**: 通过命名管道接收日志
- **模块分类**: 按target自动分类显示
- **高频聚合**: 相同日志5秒内聚合显示
- **Web界面**: 现代化的日志监控界面
- **JSON解析**: 支持tracing格式的结构化日志

## 🔧 环境变量

### 命名管道模式
```powershell
$env:LOG_TRANSPORT = "named_pipe"
$env:PIPE_NAME = "\\.\pipe\kline_log_pipe"
$env:RUST_LOG = "info"
```

### 文件日志模式
```powershell
$env:LOG_TRANSPORT = "file"
$env:RUST_LOG = "info"
```

## 📁 目录结构

```
项目根目录/
├── start_kline_data_simple.ps1      # 快速启动脚本（推荐）
├── start_kline_data_service.ps1     # 单独启动脚本
├── src/
│   ├── bin/
│   │   └── kline_data_service.rs     # K线数据服务主程序
│   └── weblog/
│       └── start_simple.ps1          # WebLog单独启动脚本
├── data/                             # 数据库文件目录
└── logs/                             # 日志文件目录
```

## 🌐 Web界面访问

### 主要页面
- **模块监控**: http://localhost:8080/modules
- **主仪表板**: http://localhost:8080
- **API接口**: http://localhost:8080/api/log

### 功能说明
- **实时日志**: 显示K线数据服务的实时日志
- **模块分类**: 按功能模块自动分类
- **日志聚合**: 高频日志自动聚合显示
- **详情查看**: 点击日志查看完整JSON信息

## ⚠️ 注意事项

### 启动顺序
1. **自动启动脚本**: 脚本会自动处理启动顺序
2. **手动启动**: 先启动WebLog系统，再启动K线数据服务

### 网络要求
- **代理设置**: 系统使用内置代理配置访问币安API
- **防火墙**: 确保8080端口可访问（WebLog界面）

### 资源要求
- **磁盘空间**: 历史数据下载需要足够的磁盘空间
- **内存**: 建议至少4GB可用内存
- **网络**: 稳定的网络连接用于数据下载

## 🛠️ 故障排除

### 常见问题

1. **编译失败**
   ```powershell
   cargo clean
   cargo check --bin kline_data_service
   ```

2. **WebLog无法访问**
   - 检查8080端口是否被占用
   - 确认WebLog系统是否正常启动

3. **命名管道连接失败**
   - 确保WebLog系统先启动
   - 检查管道名称是否正确

4. **数据下载缓慢**
   - 检查网络连接
   - 确认代理设置是否正确

### 日志查看
- **控制台日志**: 在各自的PowerShell窗口中查看
- **文件日志**: 查看 `logs/kldata.log` 文件
- **Web日志**: 访问 http://localhost:8080/modules

## 📞 技术支持

如遇到问题，请检查：
1. 项目是否在正确的目录下运行
2. 所有依赖是否正确安装
3. 网络连接是否正常
4. 端口是否被占用

建议使用 `start_kline_data_simple.ps1` 进行快速测试和日常使用。
