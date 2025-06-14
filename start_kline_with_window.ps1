# K线合成系统启动脚本（带独立命令行窗口）
# 
# 功能：启动K线聚合服务
# 传输：命名管道模式（连接到WebLog系统）
# 窗口：独立PowerShell窗口显示日志
#
# 使用方法：
# .\start_kline_with_window.ps1

Write-Host "📊 启动K线合成系统（独立窗口模式）" -ForegroundColor Yellow
Write-Host "=" * 50 -ForegroundColor Yellow

# 检查当前目录是否正确
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录下运行此脚本" -ForegroundColor Red
    Write-Host "当前目录：$(Get-Location)" -ForegroundColor Yellow
    Write-Host "应该在：BinanceCryptoKline 根目录下" -ForegroundColor Yellow
    pause
    exit 1
}

# 显示配置信息
Write-Host "📋 启动配置：" -ForegroundColor Cyan
Write-Host "  - 日志传输：命名管道" -ForegroundColor White
Write-Host "  - 管道名称：\\.\pipe\kline_log_pipe" -ForegroundColor White
Write-Host "  - 日志级别：trace" -ForegroundColor White
Write-Host "  - 连接目标：WebLog系统" -ForegroundColor White
Write-Host ""

# 检查WebLog系统是否已启动
Write-Host "🔍 检查WebLog系统状态..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 3 -ErrorAction Stop
    Write-Host "✅ WebLog系统已启动，可以连接" -ForegroundColor Green
} catch {
    Write-Host "⚠️  警告：WebLog系统可能未启动" -ForegroundColor Yellow
    Write-Host "   请确保先启动WebLog系统：src\weblog\start_weblog_with_window.ps1" -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "是否继续启动K线系统？(y/N)"
    if ($continue -ne "y" -and $continue -ne "Y") {
        Write-Host "❌ 用户取消启动" -ForegroundColor Red
        pause
        exit 1
    }
}

Write-Host ""

# 启动K线合成系统（在独立窗口中）
Write-Host "🚀 正在启动K线合成系统..." -ForegroundColor Yellow

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
`$env:RUST_LOG='trace'
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host ''
Write-Host '🚀 启动K线聚合服务...' -ForegroundColor Yellow
Write-Host '=' * 60 -ForegroundColor Yellow
cargo run --bin kline_aggregate_service
"@

Write-Host "✅ K线合成系统启动命令已发送到独立窗口" -ForegroundColor Green
Write-Host ""
Write-Host "📖 使用说明：" -ForegroundColor Cyan
Write-Host "  1. 独立窗口将显示K线系统的实时日志" -ForegroundColor White
Write-Host "  2. 日志将自动发送到WebLog系统进行可视化" -ForegroundColor White
Write-Host "  3. 访问 http://localhost:8080/modules 查看模块监控" -ForegroundColor White
Write-Host "  4. 在独立窗口中按 Ctrl+C 可停止服务" -ForegroundColor White
Write-Host ""
Write-Host "🔗 相关链接：" -ForegroundColor Cyan
Write-Host "  - 模块监控：http://localhost:8080/modules" -ForegroundColor White
Write-Host "  - 主仪表板：http://localhost:8080" -ForegroundColor White
Write-Host ""

pause
