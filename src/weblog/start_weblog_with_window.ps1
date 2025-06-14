# WebLog日志系统启动脚本（带独立命令行窗口）
# 
# 功能：启动WebLog日志可视化系统
# 端口：8080
# 传输：命名管道模式
# 窗口：独立PowerShell窗口显示日志
#
# 使用方法：
# .\start_weblog_with_window.ps1

Write-Host "🌐 启动WebLog日志系统（独立窗口模式）" -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Green

# 检查当前目录是否正确
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在weblog目录下运行此脚本" -ForegroundColor Red
    Write-Host "当前目录：$(Get-Location)" -ForegroundColor Yellow
    Write-Host "应该在：src\weblog 目录下" -ForegroundColor Yellow
    pause
    exit 1
}

# 显示配置信息
Write-Host "📋 启动配置：" -ForegroundColor Cyan
Write-Host "  - 日志传输：命名管道" -ForegroundColor White
Write-Host "  - 管道名称：\\.\pipe\kline_log_pipe" -ForegroundColor White
Write-Host "  - Web端口：8080" -ForegroundColor White
Write-Host "  - 日志级别：info" -ForegroundColor White
Write-Host "  - 访问地址：http://localhost:8080" -ForegroundColor White
Write-Host ""

# 启动WebLog系统（在独立窗口中）
Write-Host "🚀 正在启动WebLog系统..." -ForegroundColor Green

Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
Write-Host '🌐 WebLog日志系统启动中...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
Write-Host ''
Write-Host '📡 配置信息：' -ForegroundColor Cyan
Write-Host '  传输方式：命名管道' -ForegroundColor White
Write-Host '  管道名称：\\.\pipe\kline_log_pipe' -ForegroundColor White
Write-Host '  Web端口：8080' -ForegroundColor White
Write-Host '  访问地址：http://localhost:8080/modules' -ForegroundColor Yellow
Write-Host ''
Write-Host '🔧 设置环境变量...' -ForegroundColor Cyan
`$env:LOG_TRANSPORT='named_pipe'
`$env:PIPE_NAME='\\.\pipe\kline_log_pipe'
`$env:RUST_LOG='trace'
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host ''
Write-Host '🚀 启动WebLog服务器...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
cargo run --bin weblog -- --pipe-name '\\.\pipe\kline_log_pipe'
"@

Write-Host "✅ WebLog系统启动命令已发送到独立窗口" -ForegroundColor Green
Write-Host ""
Write-Host "📖 使用说明：" -ForegroundColor Cyan
Write-Host "  1. 独立窗口将显示WebLog系统的实时日志" -ForegroundColor White
Write-Host "  2. 等待系统完全启动后，访问：http://localhost:8080/modules" -ForegroundColor White
Write-Host "  3. 在独立窗口中按 Ctrl+C 可停止服务" -ForegroundColor White
Write-Host ""
Write-Host "⚠️  注意：请先启动WebLog系统，再启动K线合成服务" -ForegroundColor Yellow
Write-Host ""

pause
