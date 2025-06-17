# K线聚合服务调试脚本
# 用于诊断命名管道连接问题

Write-Host "🔍 K线聚合服务调试模式" -ForegroundColor Cyan
Write-Host "=" * 50 -ForegroundColor Cyan

# 检查当前目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录下运行此脚本" -ForegroundColor Red
    exit 1
}

# 读取配置
. "scripts\read_config.ps1"
$config = Read-LoggingConfig

Write-Host "📡 当前配置：" -ForegroundColor Yellow
Write-Host "  日志级别：$($config.LogLevel)" -ForegroundColor White
Write-Host "  传输方式：$($config.LogTransport)" -ForegroundColor White
Write-Host "  管道名称：$($config.PipeName)" -ForegroundColor White
Write-Host ""

# 设置环境变量，但强制使用混合模式（既有控制台输出又有命名管道）
Write-Host "🔧 设置调试环境变量..." -ForegroundColor Cyan
$env:RUST_LOG = $config.LogLevel
$env:LOG_TRANSPORT = "websocket"  # 使用websocket模式，这样会有控制台输出
$env:PIPE_NAME = $config.PipeName
$env:WEB_PORT = "3000"

Write-Host "✅ 调试环境变量设置完成：" -ForegroundColor Green
Write-Host "  RUST_LOG = $env:RUST_LOG" -ForegroundColor Gray
Write-Host "  LOG_TRANSPORT = $env:LOG_TRANSPORT" -ForegroundColor Gray
Write-Host "  PIPE_NAME = $env:PIPE_NAME" -ForegroundColor Gray
Write-Host "  WEB_PORT = $env:WEB_PORT" -ForegroundColor Gray
Write-Host ""

Write-Host "🚀 启动K线聚合服务（调试模式）..." -ForegroundColor Green
Write-Host "💡 在这个模式下，你应该能看到控制台输出" -ForegroundColor Yellow
Write-Host "💡 如果看到连接错误，说明命名管道有问题" -ForegroundColor Yellow
Write-Host "=" * 50 -ForegroundColor Green
Write-Host ""

# 启动服务
cargo run --bin kline_aggregate_service
