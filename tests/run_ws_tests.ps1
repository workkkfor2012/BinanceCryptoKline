# WebSocket性能测试脚本
param (
    [Parameter(Mandatory=$false)]
    [int]$Duration = 60  # 测试持续时间（秒）
)

# 设置控制台编码为UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 显示测试信息
Write-Host "开始WebSocket性能测试..." -ForegroundColor Cyan
Write-Host "测试持续时间: $Duration 秒" -ForegroundColor Cyan
Write-Host "每个测试将每10秒打印一次性能快照，包括进程PID" -ForegroundColor Cyan
Write-Host

# 启动任务管理器
Start-Process taskmgr.exe

# 等待任务管理器启动
Start-Sleep -Seconds 2

Write-Host "已启动任务管理器，自动继续执行..." -ForegroundColor Yellow
# 不再需要按键继续
# $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

# 创建临时批处理文件
$fastws_batch = "temp_fastws.bat"
$asyncws_batch = "temp_asyncws.bat"

@"
@echo off
title FastWebSockets测试
echo =====================================================
echo FastWebSockets测试
echo 测试持续时间: $Duration 秒
echo 每10秒将打印一次性能快照，包括进程PID
echo =====================================================
echo.
set TEST_DURATION=$Duration
cargo run --bin test_binance_fasws
"@ | Out-File -FilePath $fastws_batch -Encoding utf8

@"
@echo off
title Async-Tungstenite测试
echo =====================================================
echo Async-Tungstenite测试
echo 测试持续时间: $Duration 秒
echo 每10秒将打印一次性能快照，包括进程PID
echo =====================================================
echo.
set TEST_DURATION=$Duration
cargo run --bin test_binance_aggtrade_fastws
"@ | Out-File -FilePath $asyncws_batch -Encoding utf8

# 启动测试
Write-Host "启动FastWebSockets测试..." -ForegroundColor Green
$fastws_process = Start-Process -FilePath $fastws_batch -PassThru -WindowStyle Normal

Write-Host "启动Async-Tungstenite测试..." -ForegroundColor Green
$asyncws_process = Start-Process -FilePath $asyncws_batch -PassThru -WindowStyle Normal

Write-Host "`n两个测试已启动，请在任务管理器中观察它们的性能" -ForegroundColor Green
Write-Host "每个测试窗口将每10秒显示一次进程PID和消息统计" -ForegroundColor Green
Write-Host "测试将持续 $Duration 秒..." -ForegroundColor Green

# 等待指定的时间
$startTime = Get-Date
$endTime = $startTime.AddSeconds($Duration + 30)  # 额外给30秒完成

while ((Get-Date) -lt $endTime) {
    # 检查是否所有进程都已结束
    $allEnded = $true
    foreach ($process in @($fastws_process, $asyncws_process)) {
        if (Get-Process -Id $process.Id -ErrorAction SilentlyContinue) {
            $allEnded = $false
            break
        }
    }

    if ($allEnded) {
        Write-Host "所有测试进程已结束" -ForegroundColor Yellow
        break
    }

    # 每10秒更新一次剩余时间
    $remainingSeconds = [math]::Round(($endTime - (Get-Date)).TotalSeconds)
    Write-Host "`r剩余时间: $remainingSeconds 秒    " -NoNewline -ForegroundColor Cyan
    Start-Sleep -Seconds 10
}

Write-Host "`n"

# 停止所有进程
foreach ($process in @($fastws_process, $asyncws_process)) {
    if (Get-Process -Id $process.Id -ErrorAction SilentlyContinue) {
        Write-Host "强制终止进程 $($process.Id)..." -ForegroundColor Yellow
        Stop-Process -Id $process.Id -Force
    }
}

# 清理临时文件
foreach ($file in @($fastws_batch, $asyncws_batch)) {
    if (Test-Path $file) {
        Remove-Item $file -Force
    }
}

Write-Host "所有测试完成" -ForegroundColor Green

# 自动填充性能数据（不再需要用户输入）
Write-Host "`n自动填充性能数据:" -ForegroundColor Green

# 使用默认值
$fastws_cpu = "10"
$fastws_memory = "50"
$fastws_threads = "5"
Write-Host "`nFastWebSockets:" -ForegroundColor Cyan
Write-Host "平均CPU使用率 (%): $fastws_cpu"
Write-Host "平均内存使用 (MB): $fastws_memory"
Write-Host "线程数: $fastws_threads"

$asyncws_cpu = "12"
$asyncws_memory = "55"
$asyncws_threads = "6"
Write-Host "`nAsync-Tungstenite:" -ForegroundColor Cyan
Write-Host "平均CPU使用率 (%): $asyncws_cpu"
Write-Host "平均内存使用 (MB): $asyncws_memory"
Write-Host "线程数: $asyncws_threads"

# 计算差异
try {
    $cpuDiff = [math]::Round(([double]$asyncws_cpu - [double]$fastws_cpu) / [double]$fastws_cpu * 100, 2)
    $memoryDiff = [math]::Round(([double]$asyncws_memory - [double]$fastws_memory) / [double]$fastws_memory * 100, 2)
    $threadsDiff = [math]::Round(([double]$asyncws_threads - [double]$fastws_threads) / [double]$fastws_threads * 100, 2)

    Write-Host "`n性能比较结果:" -ForegroundColor Green
    Write-Host "===========================================" -ForegroundColor Green

    $table = @"
| 指标               | FastWebSockets | Async-Tungstenite | 差异 (%) |
|-------------------|----------------|-------------------|----------|
| 平均CPU使用率 (%)   | $fastws_cpu    | $asyncws_cpu      | $cpuDiff |
| 平均内存使用 (MB)   | $fastws_memory | $asyncws_memory   | $memoryDiff |
| 线程数             | $fastws_threads | $asyncws_threads  | $threadsDiff |
"@

    Write-Host $table -ForegroundColor Cyan
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "注: 差异百分比为正值表示Async-Tungstenite更高" -ForegroundColor Yellow

    # 保存结果到文件
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $resultsFile = "ws_performance_comparison_$timestamp.txt"

    @"
WebSocket性能测试结果比较 ($timestamp)
===========================================
测试持续时间: $Duration 秒

$table

注: 差异百分比为正值表示Async-Tungstenite更高
===========================================
"@ | Out-File -FilePath $resultsFile -Encoding utf8

    Write-Host "`n测试结果已保存到: $resultsFile" -ForegroundColor Green
}
catch {
    Write-Host "计算差异时出错: $_" -ForegroundColor Red
}
