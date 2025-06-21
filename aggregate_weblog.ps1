# WebLog + K线聚合服务启动脚本
. "scripts\read_unified_config.ps1"

if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    exit 1
}

$config = Read-UnifiedConfig

if (-not $config) {
    Write-Host "❌ 配置读取失败，使用默认配置" -ForegroundColor Red
    $aggregateLogLevel = "trace"
    $weblogLogLevel = "trace"
    $pipeName = "\\.\pipe\kline_log_pipe"
} else {
    $aggregateLogLevel = $config.Logging.log_level
    $weblogLogLevel = $config.Logging.Services.weblog
    $pipeName = $config.Logging.pipe_name

    # 确保管道名称格式正确
    if ($pipeName -and -not $pipeName.StartsWith("\\.\pipe\")) {
        $pipeName = "\\.\pipe\$pipeName"
    } elseif (-not $pipeName) {
        $pipeName = "\\.\pipe\kline_log_pipe"
    }
}

$buildMode = Get-BuildMode

Write-Host "🌐 启动WebLog+K线聚合系统 ($buildMode)" -ForegroundColor Green
Write-Host "配置信息: 聚合日志级别=$aggregateLogLevel, WebLog日志级别=$weblogLogLevel, 管道名称=$pipeName" -ForegroundColor Cyan

# 启动WebLog系统
$weblogCargoCmd = Get-CargoCommand -BinaryName 'weblog'
Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
Write-Host '🌐 WebLog系统启动中...' -ForegroundColor Green
cd src\weblog
`$env:LOG_TRANSPORT='named_pipe'
`$env:PIPE_NAME='$pipeName'
`$env:RUST_LOG='$weblogLogLevel'
$weblogCargoCmd -- --pipe-name '$pipeName'
"@

Start-Sleep -Seconds 3

# 启动K线合成系统
$aggregateCargoCmd = Get-CargoCommand -BinaryName 'kline_aggregate_service'
Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
Write-Host '📊 K线合成系统启动中...' -ForegroundColor Yellow
`$env:PIPE_NAME='$pipeName'
`$env:LOG_TRANSPORT='named_pipe'
`$env:RUST_LOG='$aggregateLogLevel'
$aggregateCargoCmd
"@

Write-Host "✅ 系统启动完成 - http://localhost:8080" -ForegroundColor Green
pause
