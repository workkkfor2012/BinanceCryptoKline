# K线数据服务 + WebLog系统启动脚本
. "scripts\read_unified_config.ps1"

$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    exit 1
}

$config = Read-UnifiedConfig
$klineLogLevel = $config.Logging.default_log_level
$weblogLogLevel = $config.Logging.Services.weblog
$pipeName = $config.Logging.pipe_name
if (-not $pipeName.StartsWith("\\.\pipe\")) {
    $pipeName = "\\.\pipe\$pipeName"
}

$buildMode = Get-BuildMode
Write-Host "🚀 启动K线+WebLog系统 ($buildMode)" -ForegroundColor Green

# 创建必要目录
@("data", "logs") | ForEach-Object {
    if (-not (Test-Path $_)) { New-Item -ItemType Directory -Path $_ -Force | Out-Null }
}

$global:weblogProcess = $null
$global:klineProcess = $null

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

Register-EngineEvent PowerShell.Exiting -Action { Cleanup }

try {
    # 启动WebLog系统
    $weblogCargoCmd = Get-CargoCommand -BinaryName 'weblog'
    $global:weblogProcess = Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
`$Host.UI.RawUI.WindowTitle = 'WebLog系统'
cd src\weblog
`$env:LOG_TRANSPORT='named_pipe'
`$env:PIPE_NAME='$pipeName'
`$env:RUST_LOG='$weblogLogLevel'
$weblogCargoCmd -- --pipe-name '$pipeName'
"@ -PassThru

    Start-Sleep -Seconds 3

    # 启动K线数据服务
    $klineCargoCmd = Get-CargoCommand -BinaryName 'kline_data_service'
    $global:klineProcess = Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
`$Host.UI.RawUI.WindowTitle = 'K线数据服务'
`$env:PIPE_NAME='$pipeName'
`$env:LOG_TRANSPORT='named_pipe'
`$env:RUST_LOG='$klineLogLevel'
$klineCargoCmd
"@ -PassThru

    Write-Host "✅ 系统启动完成 - http://localhost:8080" -ForegroundColor Green

    # 监控进程
    while ($true) {
        Start-Sleep -Seconds 5
        if ($global:weblogProcess.HasExited -or $global:klineProcess.HasExited) {
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
