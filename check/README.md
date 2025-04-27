# BTC K线监控脚本使用说明

这个目录包含了用于监控BTC K线数据的PowerShell脚本，可以帮助您检验各个周期的数据库和WebSocket数据是否一致。

## 脚本文件

- `simple_menu.ps1`: 主菜单脚本，用于选择要监控的周期
- `run_monitor.ps1`: 单周期监控脚本
- `run_all_monitors.ps1`: 所有周期监控脚本

## 使用方法

### 1. 使用菜单选择周期

在PowerShell中运行：

```powershell
cd F:\work\github\BinanceCryptoKline
.\check\simple_menu.ps1
```

然后从菜单中选择要监控的周期。

### 2. 直接监控指定周期

在PowerShell中运行：

```powershell
cd F:\work\github\BinanceCryptoKline
.\check\run_monitor.ps1 -Period 4h
```

可选参数：
- `-Period`: 周期，支持 1m, 5m, 15m, 30m, 4h, 1d
- `-Duration`: 监控持续时间（秒），默认为600秒（10分钟）

### 3. 监控所有周期

在PowerShell中运行：

```powershell
cd F:\work\github\BinanceCryptoKline
.\check\run_all_monitors.ps1
```

可选参数：
- `-Duration`: 监控持续时间（秒），默认为600秒（10分钟）

## 注意事项

- 这些脚本需要在PowerShell中运行
- 如果遇到执行策略限制，可以使用以下命令临时允许脚本执行：
  ```powershell
  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
  ```
- 监控结果会保存在当前目录下的 `btc_[周期]_monitor.log` 文件中
