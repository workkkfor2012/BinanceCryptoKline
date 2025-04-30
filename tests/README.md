# WebSocket性能测试说明

本目录包含用于测试两种WebSocket实现性能的脚本：
1. FastWebSockets (`fastwebsockets` 库)
2. Async-Tungstenite (`async-tungstenite` 库)

## 测试方法

为了方便区分两个测试，我们提供了两个独立的脚本，每个脚本只运行一个测试。这样您可以同时运行它们，并在任务管理器中清楚地知道哪个是哪个。

### 步骤1: 启动任务管理器

1. 打开任务管理器 (按 Ctrl+Shift+Esc)
2. 切换到"详细信息"选项卡
3. 按"CPU"列排序，以便更容易找到高CPU使用率的进程

### 步骤2: 运行FastWebSockets测试

在一个PowerShell窗口中运行:

```powershell
.\tests\run_fastws_test.ps1 -Duration 60
```

### 步骤3: 运行Async-Tungstenite测试

在另一个PowerShell窗口中运行:

```powershell
.\tests\run_async_tungstenite_test.ps1 -Duration 60
```

### 步骤4: 观察性能

在测试运行期间，观察任务管理器中两个`cargo.exe`进程的性能指标:
- CPU使用率
- 内存使用
- 线程数

### 步骤5: 比较结果

测试完成后，运行比较脚本输入您观察到的性能数据:

```powershell
.\tests\compare_results.ps1 -Duration 60
```

## 注意事项

- 两个测试应该同时运行，以确保网络条件相同
- 测试持续时间应该足够长，以获得有代表性的平均值
- 如果您的系统资源有限，可以减少测试持续时间
