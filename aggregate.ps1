# K线聚合服务启动脚本
# 导入统一配置读取脚本
. "scripts\read_unified_config.ps1"

# 检查项目目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    exit 1
}

$buildMode = Get-BuildMode
Write-Host "🚀 启动K线聚合服务 ($buildMode)" -ForegroundColor Yellow

Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
. 'scripts\read_unified_config.ps1'
Set-LoggingEnvironment
`$cargoCmd = Get-CargoCommand -BinaryName 'kline_aggregate_service'
Write-Host '🚀 K线聚合服务启动中...' -ForegroundColor Yellow
Invoke-Expression `$cargoCmd
"@
