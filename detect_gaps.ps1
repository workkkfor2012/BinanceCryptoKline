# K线数据空洞检测脚本
# 用于检测数据库中K线数据的缺失和不连续问题

param(
    [string]$DatabasePath = "data\klines.db",
    [string]$Intervals = "1m,5m,30m,1h,4h,1d",
    [switch]$Verbose,
    [switch]$Help
)

# 显示帮助信息
if ($Help) {
    Write-Host "K线数据空洞检测脚本" -ForegroundColor Green
    Write-Host ""
    Write-Host "用法:" -ForegroundColor Yellow
    Write-Host "  .\detect_gaps.ps1 [参数]"
    Write-Host ""
    Write-Host "参数:" -ForegroundColor Yellow
    Write-Host "  -DatabasePath <路径>    数据库文件路径 (默认: data\klines.db)"
    Write-Host "  -Intervals <间隔>       要检测的时间间隔，用逗号分隔 (默认: 1m,5m,30m,1h,4h,1d)"
    Write-Host "  -Verbose               显示详细输出"
    Write-Host "  -Help                  显示此帮助信息"
    Write-Host ""
    Write-Host "示例:" -ForegroundColor Yellow
    Write-Host "  .\detect_gaps.ps1"
    Write-Host "  .\detect_gaps.ps1 -DatabasePath 'custom\path\klines.db'"
    Write-Host "  .\detect_gaps.ps1 -Intervals '1m,5m' -Verbose"
    exit 0
}

# 设置控制台编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🔍 K线数据空洞检测器" -ForegroundColor Green
Write-Host "===================" -ForegroundColor Green

# 检查数据库文件是否存在
if (-not (Test-Path $DatabasePath)) {
    Write-Host "❌ 错误: 数据库文件不存在: $DatabasePath" -ForegroundColor Red
    Write-Host "请确保数据库文件路径正确，或者先运行K线数据服务生成数据库。" -ForegroundColor Yellow
    exit 1
}

Write-Host "📁 数据库路径: $DatabasePath" -ForegroundColor Cyan
Write-Host "⏰ 检测间隔: $Intervals" -ForegroundColor Cyan

# 检查Rust项目是否存在
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误: 当前目录不是Rust项目根目录" -ForegroundColor Red
    Write-Host "请在项目根目录运行此脚本。" -ForegroundColor Yellow
    exit 1
}

# 构建参数
$args = @(
    "run", "--bin", "gap_detector", "--"
    "--database", $DatabasePath
    "--intervals", $Intervals
)

if ($Verbose) {
    $env:RUST_LOG = "debug"
    Write-Host "🔧 启用详细日志输出" -ForegroundColor Yellow
} else {
    $env:RUST_LOG = "info"
}

Write-Host ""
Write-Host "🚀 开始检测数据空洞..." -ForegroundColor Green
Write-Host ""

try {
    # 运行空洞检测程序
    $startTime = Get-Date
    
    & cargo $args
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "✅ 空洞检测完成" -ForegroundColor Green
        Write-Host "⏱️  耗时: $($duration.TotalSeconds.ToString('F2')) 秒" -ForegroundColor Cyan
    } else {
        Write-Host ""
        Write-Host "❌ 空洞检测失败，退出代码: $LASTEXITCODE" -ForegroundColor Red
    }
}
catch {
    Write-Host ""
    Write-Host "❌ 运行时发生错误: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "📊 检测说明:" -ForegroundColor Yellow
Write-Host "  - 空洞检测会分析每个品种每个时间间隔的K线数据连续性"
Write-Host "  - 报告会显示缺失的时间段和缺失的周期数量"
Write-Host "  - 可以根据报告结果使用历史数据回填服务补充缺失数据"
Write-Host ""
Write-Host "💡 提示:" -ForegroundColor Yellow
Write-Host "  - 如果发现大量空洞，可能需要运行历史数据回填程序"
Write-Host "  - 使用 .\start_kldata_service.ps1 启动历史数据回填服务"
Write-Host "  - 空洞通常出现在服务停机期间或网络中断时"
