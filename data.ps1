# K线数据服务启动脚本
# 用于启动币安U本位永续合约K线数据服务

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 设置窗口标题
$Host.UI.RawUI.WindowTitle = "币安K线数据服务 - Binance Kline Data Service"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "      币安K线数据服务启动脚本" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 检查是否在正确的目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "错误: 未找到Cargo.toml文件，请确保在项目根目录运行此脚本" -ForegroundColor Red
    Write-Host "当前目录: $(Get-Location)" -ForegroundColor Yellow
    Read-Host "按任意键退出"
    exit 1
}

# 检查源文件是否存在
if (-not (Test-Path "src\bin\kline_data_service.rs")) {
    Write-Host "错误: 未找到源文件 src\bin\kline_data_service.rs" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

# 创建必要的目录
Write-Host "创建必要的目录..." -ForegroundColor Green
if (-not (Test-Path "data")) {
    New-Item -ItemType Directory -Path "data" -Force | Out-Null
    Write-Host "已创建 data 目录" -ForegroundColor Gray
}

if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" -Force | Out-Null
    Write-Host "已创建 logs 目录" -ForegroundColor Gray
}

Write-Host ""
Write-Host "服务功能说明:" -ForegroundColor Yellow
Write-Host "- 支持多个K线周期: 1m, 5m, 30m, 1h, 4h, 1d, 1w" -ForegroundColor Gray
Write-Host "- 自动补齐历史K线数据" -ForegroundColor Gray
Write-Host "- 服务器时间同步" -ForegroundColor Gray
Write-Host "- 使用归集交易数据实时合成K线" -ForegroundColor Gray
Write-Host "- 当前配置: 仅使用BTCUSDT交易对进行测试" -ForegroundColor Gray
Write-Host "- 支持命名管道日志传输到WebLog系统" -ForegroundColor Gray
Write-Host ""

# 询问是否启用命名管道日志传输
Write-Host "日志传输选项:" -ForegroundColor Yellow
Write-Host "1. 仅文件日志 (默认)" -ForegroundColor Gray
Write-Host "2. 命名管道传输到WebLog系统" -ForegroundColor Gray
Write-Host ""
$choice = Read-Host "请选择日志传输方式 (1-2，默认为1)"

$useNamedPipe = $false
if ($choice -eq "2") {
    $useNamedPipe = $true
    Write-Host "✅ 已选择命名管道传输模式" -ForegroundColor Green
    Write-Host "💡 请确保WebLog系统已启动: .\src\weblog\start_simple.ps1" -ForegroundColor Yellow
} else {
    Write-Host "✅ 已选择文件日志模式" -ForegroundColor Green
}
Write-Host ""

# 编译检查
Write-Host "正在进行编译检查..." -ForegroundColor Green
$checkResult = cargo check --bin kline_data_service 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "编译检查失败:" -ForegroundColor Red
    Write-Host $checkResult -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}
Write-Host "编译检查通过" -ForegroundColor Green
Write-Host ""

# 导入配置读取函数
. "scripts\read_config.ps1"

# 从配置文件读取日志设置
$loggingConfig = Read-LoggingConfig

# 设置环境变量
if ($useNamedPipe) {
    Write-Host "🔧 设置命名管道环境变量..." -ForegroundColor Cyan
    # 使用配置文件中的设置，但强制使用命名管道传输
    $loggingConfig.LogTransport = "named_pipe"
    Set-LoggingEnvironment -LoggingConfig $loggingConfig
} else {
    Write-Host "🔧 设置文件日志环境变量..." -ForegroundColor Cyan
    # 使用配置文件中的设置，但强制使用文件传输
    $loggingConfig.LogTransport = "file"
    Set-LoggingEnvironment -LoggingConfig $loggingConfig
}
Write-Host ""

# 启动服务
Write-Host "正在启动K线数据服务..." -ForegroundColor Green
Write-Host "注意: 首次运行可能需要较长时间来下载历史数据" -ForegroundColor Yellow
if ($useNamedPipe) {
    Write-Host "📡 日志将传输到WebLog系统 (http://localhost:8080/modules)" -ForegroundColor Cyan
}
Write-Host "按 Ctrl+C 可以停止服务" -ForegroundColor Yellow
Write-Host ""

try {
    # 使用cargo run启动服务
    cargo run --bin kline_data_service
}
catch {
    Write-Host "服务启动失败: $_" -ForegroundColor Red
}
finally {
    Write-Host ""
    Write-Host "K线数据服务已停止" -ForegroundColor Yellow
    Read-Host "按任意键退出"
}
