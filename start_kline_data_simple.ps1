# K线数据服务简化启动脚本
# 快速启动K线数据服务和WebLog系统

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚀 启动K线数据服务 + WebLog系统" -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Green

# 检查目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录运行此脚本" -ForegroundColor Red
    exit 1
}

# 创建必要目录
@("data", "logs") | ForEach-Object {
    if (-not (Test-Path $_)) { New-Item -ItemType Directory -Path $_ -Force | Out-Null }
}

Write-Host "📡 架构：K线数据服务 → 命名管道 → WebLog系统" -ForegroundColor Cyan
Write-Host "🌐 访问：http://localhost:8080/modules" -ForegroundColor Yellow
Write-Host ""

# 全局进程变量
$global:weblogProcess = $null
$global:klineProcess = $null

# 清理函数
function Cleanup {
    Write-Host "🛑 停止服务..." -ForegroundColor Yellow
    if ($global:klineProcess -and !$global:klineProcess.HasExited) {
        $global:klineProcess.Kill(); $global:klineProcess.WaitForExit(3000)
    }
    if ($global:weblogProcess -and !$global:weblogProcess.HasExited) {
        $global:weblogProcess.Kill(); $global:weblogProcess.WaitForExit(3000)
    }
    Write-Host "✅ 服务已停止" -ForegroundColor Green
}

# 注册退出处理
Register-EngineEvent PowerShell.Exiting -Action { Cleanup }

try {
    # 启动WebLog系统
    Write-Host "🌐 启动WebLog系统..." -ForegroundColor Green
    $global:weblogProcess = Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
`$Host.UI.RawUI.WindowTitle = 'WebLog日志系统'
Write-Host '🌐 WebLog系统启动中...' -ForegroundColor Green
cd src\weblog
`$env:LOG_TRANSPORT='named_pipe'
`$env:PIPE_NAME='\\.\pipe\kline_log_pipe'
`$env:RUST_LOG='trace'
Write-Host '📡 命名管道模式，端口8080' -ForegroundColor Cyan
cargo run --bin weblog -- --pipe-name '\\.\pipe\kline_log_pipe'
"@ -PassThru

    # 等待WebLog启动
    Start-Sleep -Seconds 5

    # 启动K线数据服务
    Write-Host "📊 启动K线数据服务..." -ForegroundColor Green
    $global:klineProcess = Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
`$Host.UI.RawUI.WindowTitle = 'K线数据服务'
Write-Host '📊 K线数据服务启动中...' -ForegroundColor Yellow
`$env:PIPE_NAME='\\.\pipe\kline_log_pipe'
`$env:LOG_TRANSPORT='named_pipe'
`$env:RUST_LOG='trace'
Write-Host '📡 连接到WebLog系统' -ForegroundColor Cyan
cargo run --bin kline_data_service
"@ -PassThru

    Write-Host ""
    Write-Host "✅ 系统启动完成" -ForegroundColor Green
    Write-Host "📋 WebLog: PID $($global:weblogProcess.Id)" -ForegroundColor White
    Write-Host "📋 K线服务: PID $($global:klineProcess.Id)" -ForegroundColor White
    Write-Host "🌐 访问: http://localhost:8080/modules" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "💡 按 Ctrl+C 停止所有服务" -ForegroundColor Gray

    # 监控进程
    while ($true) {
        Start-Sleep -Seconds 5
        if ($global:weblogProcess.HasExited -or $global:klineProcess.HasExited) {
            Write-Host "⚠️ 有服务退出，停止所有服务" -ForegroundColor Yellow
            break
        }
    }
}
catch {
    Write-Host "❌ 启动失败: $_" -ForegroundColor Red
}
finally {
    Cleanup
    Read-Host "按任意键退出"
}
