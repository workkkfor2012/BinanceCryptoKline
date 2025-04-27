# BTC K线所有周期监控脚本
param (
    [Parameter(Mandatory=$false)]
    [int]$Duration = 600
)

# 支持的周期列表
$periods = @("1m", "5m", "15m", "30m", "4h", "1d")

# 显示信息
Write-Host "开始监控所有周期的BTC K线数据..." -ForegroundColor Cyan
Write-Host "将监控以下周期: $($periods -join ', ')" -ForegroundColor Cyan
Write-Host "持续时间: $Duration 秒" -ForegroundColor Cyan
Write-Host

# 删除之前的日志文件
foreach ($period in $periods) {
    $logFile = "btc_${period}_monitor.log"
    if (Test-Path $logFile) {
        Remove-Item $logFile -Force
    }
}

# 创建临时目录
$tempDir = "temp"
if (-not (Test-Path $tempDir)) {
    New-Item -ItemType Directory -Path $tempDir | Out-Null
}

# 启动所有周期的监控脚本
foreach ($period in $periods) {
    Write-Host "启动 $period 周期监控..." -ForegroundColor Green

    # 创建临时批处理文件
    $dbBatContent = @"
@echo off
set BTC_PERIOD=$period
set BTC_DURATION=$Duration
python check\db_monitor.py
"@

    $wsBatContent = @"
@echo off
set BTC_PERIOD=$period
set BTC_DURATION=$Duration
python check\ws_monitor.py
"@

    $dbBatPath = "$tempDir\db_$period.bat"
    $wsBatPath = "$tempDir\ws_$period.bat"

    # 写入临时批处理文件
    $dbBatContent | Out-File -FilePath $dbBatPath -Encoding ascii
    $wsBatContent | Out-File -FilePath $wsBatPath -Encoding ascii

    # 启动脚本
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $dbBatPath -WindowStyle Normal
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $wsBatPath -WindowStyle Normal
}

Write-Host
Write-Host "所有监控脚本已启动，日志文件保存在各自的 btc_[周期]_monitor.log 文件中" -ForegroundColor Green
Write-Host

# 等待用户按键后清理临时文件
Read-Host "按Enter键继续..."

# 清理临时文件
if (Test-Path $tempDir) {
    Remove-Item -Path $tempDir -Recurse -Force
}
