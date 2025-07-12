# Watchdog系统测试脚本
# 用于验证Watchdog功能是否正常工作

Write-Host "开始测试Watchdog系统..." -ForegroundColor Green

# 设置环境变量启用测试模式
$env:KLINE_TEST_MODE = "true"

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "启动K线聚合服务（测试模式）..." -ForegroundColor Yellow

# 启动服务并运行30秒，然后停止
$job = Start-Job -ScriptBlock {
    Set-Location "f:\work\github\BinanceCryptoKline"
    $env:KLINE_TEST_MODE = "true"
    cargo run --bin klagg_simple
}

Write-Host "服务已启动，等待30秒观察Watchdog日志..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host "停止服务..." -ForegroundColor Yellow
Stop-Job $job
Remove-Job $job

Write-Host "测试完成。请检查日志文件中的Watchdog相关信息：" -ForegroundColor Green
Write-Host "- 查找 'Watchdog服务已启动' 消息" -ForegroundColor Cyan
Write-Host "- 查找 '运行健康检查周期' 消息" -ForegroundColor Cyan
Write-Host "- 确认没有 '检测到死亡或停滞的Actor' 错误" -ForegroundColor Cyan

Write-Host "`n如果需要查看实时日志，请运行：" -ForegroundColor Yellow
Write-Host "cargo run --bin klagg_simple" -ForegroundColor White
