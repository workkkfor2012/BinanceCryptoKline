# BTC K线监控脚本
param (
    [Parameter(Mandatory=$false)]
    [string]$Period = "1m",

    [Parameter(Mandatory=$false)]
    [int]$Duration = 600
)

# 设置环境变量
$env:BTC_PERIOD = $Period
$env:BTC_DURATION = $Duration

# 显示信息
Write-Host "开始监控BTC $Period 周期K线数据..." -ForegroundColor Cyan
Write-Host "持续时间: $Duration 秒" -ForegroundColor Cyan
Write-Host

# 删除之前的日志文件
$logFile = "btc_${Period}_monitor.log"
if (Test-Path $logFile) {
    Remove-Item $logFile -Force
}

# 创建临时批处理文件
$dbBatContent = @"
@echo off
set BTC_PERIOD=$Period
set BTC_DURATION=$Duration
python check\db_monitor.py
"@

$wsBatContent = @"
@echo off
set BTC_PERIOD=$Period
set BTC_DURATION=$Duration
python check\ws_monitor.py
"@

$dbBatPath = "temp_db.bat"
$wsBatPath = "temp_ws.bat"

# 写入临时批处理文件
$dbBatContent | Out-File -FilePath $dbBatPath -Encoding ascii
$wsBatContent | Out-File -FilePath $wsBatPath -Encoding ascii

# 启动数据库和WebSocket监控脚本
Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $dbBatPath -WindowStyle Normal
Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $wsBatPath -WindowStyle Normal

# 等待1秒，确保日志文件已经创建
Start-Sleep -Seconds 1

# 显示合并的日志
Write-Host "显示 $Period 周期日志，按Ctrl+C可以随时终止..." -ForegroundColor Yellow
Write-Host

# 显示日志
try {
    Get-Content -Path $logFile -Wait -Tail 100
}
catch {
    Write-Host "读取日志文件时出错: $_" -ForegroundColor Red
}
finally {
    # 清理临时文件
    if (Test-Path $dbBatPath) { Remove-Item $dbBatPath -Force }
    if (Test-Path $wsBatPath) { Remove-Item $wsBatPath -Force }

    Write-Host
    Write-Host "监控完成，完整日志保存在 $logFile 文件中" -ForegroundColor Green
    Write-Host

    Read-Host "按Enter键继续..."
}
