# 数据稽核服务快速启动脚本
# 使用默认配置快速启动数据稽核服务

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 获取脚本所在目录的父目录（项目根目录）
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

Write-Host "🚀 启动数据稽核服务（生产模式）..." -ForegroundColor Green
Write-Host "📊 服务配置:" -ForegroundColor Cyan
Write-Host "   - 交易对: ALL（所有交易对）" -ForegroundColor White
Write-Host "   - 检查周期: 1m,5m,30m（固定）" -ForegroundColor White
Write-Host "   - 执行时机: 每分钟第40秒" -ForegroundColor White
Write-Host "   - 每次稽核时长: 30分钟" -ForegroundColor White
Write-Host "   - 数据库路径: data/klines.db" -ForegroundColor White
Write-Host "" -ForegroundColor White
Write-Host "💡 提示: 按 Ctrl+C 可优雅关闭服务" -ForegroundColor Yellow
Write-Host "" -ForegroundColor White

cargo run --release --bin data_audit -- `
    --symbols ALL `
    --db-path data/klines.db `
    --intervals 1m,5m,30m `
    --audit-duration-seconds 1800

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ 数据稽核服务已完成" -ForegroundColor Green
} else {
    Write-Host "❌ 数据稽核服务异常退出，退出码: $LASTEXITCODE" -ForegroundColor Red
}
