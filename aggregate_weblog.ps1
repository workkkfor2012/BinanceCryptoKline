# WebLog + K线聚合服务启动脚本（双窗口版本）
#
# 功能：按正确顺序启动WebLog日志系统和K线聚合服务
# 窗口：每个系统都在独立的PowerShell窗口中运行
#
# 使用方法：
# .\weblog_aggregate.ps1

Write-Host "🚀 启动WebLog日志系统和K线聚合服务" -ForegroundColor Magenta
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
Write-Host "  1. 🌐 启动WebLog日志系统（前端聚合版，端口8080）" -ForegroundColor White
Write-Host "  2. ⏳ 等待5秒让WebLog系统完全启动" -ForegroundColor White
Write-Host "  3. 📊 启动K线聚合服务（连接到WebLog）" -ForegroundColor White
Write-Host ""
Write-Host "🔄 重构说明：" -ForegroundColor Yellow
Write-Host "  - 后端：只负责缓存和转发原始日志" -ForegroundColor White
Write-Host "  - 前端：负责模块分类和高频折叠" -ForegroundColor White
Write-Host "  - 功能：与旧版本完全一致，性能更好" -ForegroundColor White
Write-Host ""

# 直接从配置文件读取日志设置
$configContent = Get-Content "config\aggregate_config.toml" -Raw
$logLevel = if ($configContent -match 'log_level\s*=\s*"(.+?)"') { $matches[1] } else { "info" }
$logTransport = if ($configContent -match 'log_transport\s*=\s*"(.+?)"') { $matches[1] } else { "named_pipe" }
$pipeName = if ($configContent -match 'pipe_name\s*=\s*"(.+?)"') {
    # 确保管道名称格式正确
    $rawPipeName = $matches[1]
    if ($rawPipeName -notmatch '^\\\\\.\\pipe\\') {
        "\\.\pipe\$rawPipeName"
    } else {
        $rawPipeName
    }
} else {
    "\\.\pipe\kline_log_pipe"
}

# 创建配置对象
$loggingConfig = @{
    LogLevel = $logLevel
    LogTransport = $logTransport
    PipeName = $pipeName
}

# 第一步：启动WebLog系统
Write-Host "🌐 第一步：启动WebLog日志系统（前端聚合版）..." -ForegroundColor Green

Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
Write-Host '🌐 WebLog日志系统启动中（前端聚合版）...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
Write-Host ''
Write-Host '📡 配置信息：' -ForegroundColor Cyan
Write-Host '  版本：前端聚合架构' -ForegroundColor White
Write-Host '  传输方式：$($loggingConfig.LogTransport)' -ForegroundColor White
Write-Host '  管道名称：$($loggingConfig.PipeName)' -ForegroundColor White
Write-Host '  日志级别：$($loggingConfig.LogLevel)' -ForegroundColor White
Write-Host '  Web端口：8080' -ForegroundColor White
Write-Host '  访问地址：http://localhost:8080' -ForegroundColor Yellow
Write-Host ''
Write-Host '🔄 架构说明：' -ForegroundColor Cyan
Write-Host '  后端：只负责缓存历史日志和实时转发' -ForegroundColor White
Write-Host '  前端：负责模块分类和高频日志折叠' -ForegroundColor White
Write-Host '  优势：性能更好，刷新后状态完整恢复' -ForegroundColor White
Write-Host ''
Write-Host '🔧 设置环境变量...' -ForegroundColor Cyan
cd src\weblog
`$env:LOG_TRANSPORT='$($loggingConfig.LogTransport)'
`$env:PIPE_NAME='$($loggingConfig.PipeName)'
# 注意：不设置RUST_LOG，让weblog.rs中的设置生效
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host '  日志级别：$($loggingConfig.LogLevel)' -ForegroundColor Gray
Write-Host '  传输方式：$($loggingConfig.LogTransport)' -ForegroundColor Gray
Write-Host '  管道名称：$($loggingConfig.PipeName)' -ForegroundColor Gray
Write-Host ''
Write-Host '🚀 启动WebLog服务器（前端聚合版）...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
cargo run --bin weblog -- --pipe-name '$($loggingConfig.PipeName)'
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
Write-Host '  传输方式：$($loggingConfig.LogTransport)' -ForegroundColor White
Write-Host '  管道名称：$($loggingConfig.PipeName)' -ForegroundColor White
Write-Host '  日志级别：$($loggingConfig.LogLevel)' -ForegroundColor White
Write-Host '  连接目标：WebLog系统' -ForegroundColor White
Write-Host '  监控地址：http://localhost:8080/modules' -ForegroundColor Yellow
Write-Host ''
Write-Host '🔧 设置环境变量...' -ForegroundColor Cyan
`$env:PIPE_NAME='$($loggingConfig.PipeName)'
`$env:LOG_TRANSPORT='named_pipe'
`$env:RUST_LOG='$($loggingConfig.LogLevel)'
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host '  日志级别：$($loggingConfig.LogLevel)' -ForegroundColor Gray
Write-Host '  传输方式：$($loggingConfig.LogTransport)' -ForegroundColor Gray
Write-Host '  管道名称：$($loggingConfig.PipeName)' -ForegroundColor Gray
Write-Host ''
Write-Host '🚀 启动K线聚合服务...' -ForegroundColor Yellow
Write-Host '=' * 60 -ForegroundColor Yellow
cargo run --bin kline_aggregate_service
"@

Write-Host "✅ K线合成系统启动命令已发送" -ForegroundColor Green

Write-Host ""
Write-Host "🎉 两个系统都已启动（前端聚合版）！" -ForegroundColor Magenta
Write-Host "=" * 60 -ForegroundColor Magenta
Write-Host ""
Write-Host "📖 使用说明：" -ForegroundColor Cyan
Write-Host "  1. 现在有两个独立的PowerShell窗口正在运行" -ForegroundColor White
Write-Host "  2. 绿色窗口：WebLog日志系统（前端聚合版）" -ForegroundColor White
Write-Host "  3. 黄色窗口：K线合成系统" -ForegroundColor White
Write-Host "  4. 等待系统完全启动后访问监控页面" -ForegroundColor White
Write-Host ""
Write-Host "🔗 访问链接：" -ForegroundColor Cyan
Write-Host "  - 主监控页面：http://localhost:8080" -ForegroundColor White
Write-Host "  - 旧版本对比：http://localhost:8080/static/indexold.html" -ForegroundColor Gray
Write-Host ""
Write-Host "🔄 新版本特性：" -ForegroundColor Yellow
Write-Host "  ✅ 功能与旧版本完全一致" -ForegroundColor White
Write-Host "  ✅ 后端性能大幅提升" -ForegroundColor White
Write-Host "  ✅ 前端响应更快" -ForegroundColor White
Write-Host "  ✅ 刷新后状态完整恢复" -ForegroundColor White
Write-Host "  ✅ 架构更清晰，维护更简单" -ForegroundColor White
Write-Host ""
Write-Host "⚠️  停止服务：在各自的窗口中按 Ctrl+C" -ForegroundColor Yellow
Write-Host ""

pause
