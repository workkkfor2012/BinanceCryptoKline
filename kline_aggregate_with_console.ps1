# K线聚合服务启动脚本（带控制台输出）
# 解决命名管道模式下看不到日志的问题

Write-Host "🚀 启动K线聚合服务（带控制台输出）" -ForegroundColor Magenta
Write-Host "=" * 60 -ForegroundColor Magenta

# 检查当前目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录下运行此脚本" -ForegroundColor Red
    exit 1
}

# 读取配置
. "scripts\read_config.ps1"
$config = Read-LoggingConfig

Write-Host ""
Write-Host "📡 配置信息：" -ForegroundColor Cyan
Write-Host "  日志级别：$($config.LogLevel)" -ForegroundColor White
Write-Host "  传输方式：$($config.LogTransport)" -ForegroundColor White
Write-Host "  管道名称：$($config.PipeName)" -ForegroundColor White
Write-Host ""

# 设置环境变量 - 使用特殊模式
Write-Host "🔧 设置环境变量（混合输出模式）..." -ForegroundColor Cyan

# 关键修改：使用 "file" 模式而不是 "named_pipe"，这样会有控制台输出
# 但同时手动设置PIPE_NAME，让程序仍然尝试连接命名管道
$env:RUST_LOG = $config.LogLevel
$env:LOG_TRANSPORT = "file"  # 使用file模式确保有控制台输出
$env:PIPE_NAME = $config.PipeName  # 仍然设置管道名称
$env:FORCE_NAMED_PIPE = "true"  # 自定义环境变量，提示程序使用命名管道

Write-Host "✅ 环境变量设置完成：" -ForegroundColor Green
Write-Host "  RUST_LOG = $env:RUST_LOG" -ForegroundColor Gray
Write-Host "  LOG_TRANSPORT = $env:LOG_TRANSPORT (file模式确保控制台输出)" -ForegroundColor Gray
Write-Host "  PIPE_NAME = $env:PIPE_NAME" -ForegroundColor Gray
Write-Host "  FORCE_NAMED_PIPE = $env:FORCE_NAMED_PIPE" -ForegroundColor Gray
Write-Host ""

Write-Host "💡 说明：" -ForegroundColor Yellow
Write-Host "  - 使用file模式确保你能看到控制台日志" -ForegroundColor White
Write-Host "  - 同时程序仍会尝试连接到命名管道" -ForegroundColor White
Write-Host "  - 这样可以诊断命名管道连接问题" -ForegroundColor White
Write-Host ""

Write-Host "🚀 启动K线聚合服务..." -ForegroundColor Green
Write-Host "=" * 60 -ForegroundColor Green
Write-Host ""

# 启动服务
cargo run --bin kline_aggregate_service
