# 一键启动两个系统脚本（带独立命令行窗口）
# 
# 功能：按正确顺序启动WebLog和K线合成系统
# 窗口：每个系统都在独立的PowerShell窗口中运行
#
# 使用方法：
# .\start_both_systems_with_windows.ps1

Write-Host "🚀 一键启动K线合成和日志系统（独立窗口模式）" -ForegroundColor Magenta
Write-Host "=" * 60 -ForegroundColor Magenta

# 检查当前目录是否正确
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录下运行此脚本" -ForegroundColor Red
    Write-Host "当前目录：$(Get-Location)" -ForegroundColor Yellow
    Write-Host "应该在：BinanceCryptoKline 根目录下" -ForegroundColor Yellow
    pause
    exit 1
}

Write-Host ""
Write-Host "📋 启动计划：" -ForegroundColor Cyan
Write-Host "  1. 🌐 启动WebLog日志系统（端口8080）" -ForegroundColor White
Write-Host "  2. ⏳ 等待5秒让WebLog系统完全启动" -ForegroundColor White
Write-Host "  3. 📊 启动K线合成系统（连接到WebLog）" -ForegroundColor White
Write-Host ""

# 第一步：启动WebLog系统
Write-Host "🌐 第一步：启动WebLog日志系统..." -ForegroundColor Green

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
cd src\weblog
`$env:LOG_TRANSPORT='named_pipe'
`$env:PIPE_NAME='\\.\pipe\kline_log_pipe'
`$env:RUST_LOG='info'
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host ''
Write-Host '🚀 启动WebLog服务器...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
cargo run --bin weblog -- --pipe-name '\\.\pipe\kline_log_pipe'
"@

Write-Host "✅ WebLog系统启动命令已发送" -ForegroundColor Green

# 等待WebLog系统启动
Write-Host ""
Write-Host "⏳ 等待WebLog系统启动..." -ForegroundColor Cyan
for ($i = 5; $i -gt 0; $i--) {
    Write-Host "   倒计时：$i 秒" -ForegroundColor Yellow
    Start-Sleep -Seconds 1
}

# 第二步：启动K线合成系统
Write-Host ""
Write-Host "📊 第二步：启动K线合成系统..." -ForegroundColor Yellow

Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
Write-Host '📊 K线合成系统启动中...' -ForegroundColor Yellow
Write-Host '=' * 60 -ForegroundColor Yellow
Write-Host ''
Write-Host '📡 配置信息：' -ForegroundColor Cyan
Write-Host '  传输方式：命名管道' -ForegroundColor White
Write-Host '  管道名称：\\.\pipe\kline_log_pipe' -ForegroundColor White
Write-Host '  连接目标：WebLog系统' -ForegroundColor White
Write-Host '  监控地址：http://localhost:8080/modules' -ForegroundColor Yellow
Write-Host ''
Write-Host '🔧 设置环境变量...' -ForegroundColor Cyan
`$env:PIPE_NAME='\\.\pipe\kline_log_pipe'
`$env:LOG_TRANSPORT='named_pipe'
`$env:RUST_LOG='info'
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host ''
Write-Host '🚀 启动K线聚合服务...' -ForegroundColor Yellow
Write-Host '=' * 60 -ForegroundColor Yellow
cargo run --bin kline_aggregate_service
"@

Write-Host "✅ K线合成系统启动命令已发送" -ForegroundColor Green

Write-Host ""
Write-Host "🎉 两个系统都已启动！" -ForegroundColor Magenta
Write-Host "=" * 60 -ForegroundColor Magenta
Write-Host ""
Write-Host "📖 使用说明：" -ForegroundColor Cyan
Write-Host "  1. 现在有两个独立的PowerShell窗口正在运行" -ForegroundColor White
Write-Host "  2. 绿色窗口：WebLog日志系统" -ForegroundColor White
Write-Host "  3. 黄色窗口：K线合成系统" -ForegroundColor White
Write-Host "  4. 等待系统完全启动后访问监控页面" -ForegroundColor White
Write-Host ""
Write-Host "🔗 访问链接：" -ForegroundColor Cyan
Write-Host "  - 模块监控：http://localhost:8080/modules" -ForegroundColor White
Write-Host "  - 主仪表板：http://localhost:8080" -ForegroundColor White
Write-Host ""
Write-Host "⚠️  停止服务：在各自的窗口中按 Ctrl+C" -ForegroundColor Yellow
Write-Host ""

pause
