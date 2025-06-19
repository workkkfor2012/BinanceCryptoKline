# K线数据服务启动脚本
# 导入统一配置读取脚本
. "scripts\read_unified_config.ps1"

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 检查项目目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    exit 1
}

# 创建必要目录
if (-not (Test-Path "data")) { New-Item -ItemType Directory -Path "data" -Force | Out-Null }
if (-not (Test-Path "logs")) { New-Item -ItemType Directory -Path "logs" -Force | Out-Null }

# 选择日志传输方式
Write-Host "选择日志模式: 1=文件 2=命名管道" -ForegroundColor Cyan
$choice = Read-Host "请选择 (1-2)"
$useNamedPipe = ($choice -eq "2")

# 设置环境变量
Set-LoggingEnvironment
if ($useNamedPipe) {
    $env:LOG_TRANSPORT = "named_pipe"
} else {
    $env:LOG_TRANSPORT = "file"
}

$buildMode = Get-BuildMode
Write-Host "🚀 启动K线数据服务 ($buildMode)" -ForegroundColor Yellow

try {
    $cargoCmd = Get-CargoCommand -BinaryName 'kline_data_service'
    Invoke-Expression $cargoCmd
}
catch {
    Write-Host "服务启动失败: $_" -ForegroundColor Red
}
finally {
    Write-Host ""
    Write-Host "K线数据服务已停止" -ForegroundColor Yellow
    Read-Host "按任意键退出"
}
