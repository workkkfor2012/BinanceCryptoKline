# K线聚合服务调试脚本（带超时检测）
# 用于诊断程序卡在哪个步骤

Write-Host "🔍 K线聚合服务超时调试模式" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan

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

# 设置环境变量，强制使用文件模式确保有输出
Write-Host "🔧 设置调试环境变量..." -ForegroundColor Cyan
$env:RUST_LOG = "trace"
$env:LOG_TRANSPORT = "file"  # 强制使用文件模式
$env:PIPE_NAME = $config.PipeName
$env:WEB_PORT = "3000"

# 添加网络调试环境变量
$env:RUST_BACKTRACE = "1"
$env:TOKIO_CONSOLE = "1"

Write-Host "✅ 调试环境变量设置完成：" -ForegroundColor Green
Write-Host "  RUST_LOG = $env:RUST_LOG" -ForegroundColor Gray
Write-Host "  LOG_TRANSPORT = $env:LOG_TRANSPORT" -ForegroundColor Gray
Write-Host "  RUST_BACKTRACE = $env:RUST_BACKTRACE" -ForegroundColor Gray
Write-Host ""

Write-Host "🚀 启动K线聚合服务（超时调试模式）..." -ForegroundColor Green
Write-Host "💡 如果30秒内没有输出，程序可能卡在网络请求上" -ForegroundColor Yellow
Write-Host "💡 常见卡住的地方：时间同步、网络连接、数据库初始化" -ForegroundColor Yellow
Write-Host "=" * 60 -ForegroundColor Green
Write-Host ""

# 启动服务，但设置超时
$job = Start-Job -ScriptBlock {
    param($env_vars)
    
    # 设置环境变量
    foreach ($var in $env_vars.GetEnumerator()) {
        Set-Item -Path "env:$($var.Key)" -Value $var.Value
    }
    
    # 切换到正确目录
    Set-Location $using:PWD
    
    # 运行程序
    cargo run --bin kline_aggregate_service
} -ArgumentList @{
    RUST_LOG = $env:RUST_LOG
    LOG_TRANSPORT = $env:LOG_TRANSPORT
    PIPE_NAME = $env:PIPE_NAME
    WEB_PORT = $env:WEB_PORT
    RUST_BACKTRACE = $env:RUST_BACKTRACE
    TOKIO_CONSOLE = $env:TOKIO_CONSOLE
}

# 等待30秒，如果没有完成就显示诊断信息
$timeout = 30
$elapsed = 0

while ($job.State -eq "Running" -and $elapsed -lt $timeout) {
    Start-Sleep -Seconds 1
    $elapsed++
    
    if ($elapsed % 5 -eq 0) {
        Write-Host "⏱️  等待中... ($elapsed/$timeout 秒)" -ForegroundColor Yellow
    }
}

if ($job.State -eq "Running") {
    Write-Host ""
    Write-Host "⚠️  程序运行超过 $timeout 秒，可能卡住了！" -ForegroundColor Red
    Write-Host ""
    Write-Host "🔍 可能的原因：" -ForegroundColor Cyan
    Write-Host "  1. 网络连接问题 - 时间同步需要访问外部服务器" -ForegroundColor White
    Write-Host "  2. 代理设置问题 - 检查网络代理配置" -ForegroundColor White
    Write-Host "  3. 数据库初始化问题 - 检查数据库文件权限" -ForegroundColor White
    Write-Host "  4. 配置文件问题 - 检查 config/aggregate_config.toml" -ForegroundColor White
    Write-Host ""
    Write-Host "🛠️  建议的解决方案：" -ForegroundColor Yellow
    Write-Host "  1. 检查网络连接：ping www.baidu.com" -ForegroundColor White
    Write-Host "  2. 检查代理设置：查看系统代理配置" -ForegroundColor White
    Write-Host "  3. 检查配置文件：确保 config/aggregate_config.toml 存在且格式正确" -ForegroundColor White
    Write-Host "  4. 尝试离线模式：修改配置跳过网络依赖" -ForegroundColor White
    Write-Host ""
    
    $continue = Read-Host "是否继续等待？(y/N)"
    if ($continue -ne "y" -and $continue -ne "Y") {
        Write-Host "🛑 停止程序..." -ForegroundColor Red
        Stop-Job $job
        Remove-Job $job
        exit 1
    }
}

# 获取输出
$output = Receive-Job $job
if ($output) {
    Write-Host "📋 程序输出：" -ForegroundColor Cyan
    $output | ForEach-Object { Write-Host "  $_" -ForegroundColor White }
}

# 清理
Remove-Job $job

Write-Host ""
Write-Host "✅ 调试完成" -ForegroundColor Green
