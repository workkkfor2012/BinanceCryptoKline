# 数据稽核服务启动脚本
# 用于启动持续的K线数据正确性稽核服务

param(
    [string]$Mode = "service",  # service: 持续服务模式, once: 单次运行模式
    [string]$Symbols = "ALL",   # 要稽核的交易对，用逗号分隔，或使用"ALL"
    [string]$Intervals = "1m,5m,30m",  # 要稽核的K线周期（固定）
    [int]$AuditDurationSeconds = 1800,  # 每次稽核的时长（秒），默认30分钟
    [string]$DbPath = "data/klines.db"  # 数据库路径
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 获取脚本所在目录的父目录（项目根目录）
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

Write-Host "🚀 启动数据稽核服务..." -ForegroundColor Green
Write-Host "📊 配置参数:" -ForegroundColor Cyan
Write-Host "   - 运行模式: $Mode" -ForegroundColor White
Write-Host "   - 交易对: $Symbols" -ForegroundColor White
Write-Host "   - K线周期: $Intervals" -ForegroundColor White
Write-Host "   - 数据库路径: $DbPath" -ForegroundColor White

if ($Mode -eq "service") {
    Write-Host "   - 执行时机: 每分钟第40秒" -ForegroundColor White
    Write-Host "   - 每次稽核时长: $AuditDurationSeconds 秒" -ForegroundColor White
    Write-Host "" -ForegroundColor White
    Write-Host "💡 提示: 按 Ctrl+C 可优雅关闭服务" -ForegroundColor Yellow
    Write-Host "" -ForegroundColor White

    # 持续服务模式
    cargo run --release --bin data_audit -- `
        --symbols $Symbols `
        --db-path $DbPath `
        --intervals $Intervals `
        --audit-duration-seconds $AuditDurationSeconds
} else {
    Write-Host "   - 稽核时长: $AuditDurationSeconds 秒" -ForegroundColor White
    Write-Host "" -ForegroundColor White
    
    # 单次运行模式
    cargo run --release --bin data_audit -- `
        --symbols $Symbols `
        --db-path $DbPath `
        --intervals $Intervals `
        --audit-duration-seconds $AuditDurationSeconds `
        --run-once
}

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ 数据稽核服务已完成" -ForegroundColor Green
} else {
    Write-Host "❌ 数据稽核服务异常退出，退出码: $LASTEXITCODE" -ForegroundColor Red
}
