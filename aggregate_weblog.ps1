# WebLog + K线聚合服务启动脚本
. "scripts\read_unified_config.ps1"

if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    exit 1
}

$config = Read-UnifiedConfig
$aggregateLogLevel = $config.Logging.default_log_level
$weblogLogLevel = $config.Logging.Services.weblog
$pipeName = $config.Logging.pipe_name
if (-not $pipeName.StartsWith("\\.\pipe\")) {
    $pipeName = "\\.\pipe\$pipeName"
}
$buildMode = Get-BuildMode

Write-Host "🌐 启动WebLog+K线聚合系统 ($buildMode)" -ForegroundColor Green

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
