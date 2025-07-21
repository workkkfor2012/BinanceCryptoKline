# K线数据快速完整性检查脚本
# 用于快速检查数据库中K线数据的基本完整性

param(
    [string]$DatabasePath = "data\klines.db",
    [switch]$Verbose,
    [switch]$Help
)

# 显示帮助信息
if ($Help) {
    Write-Host "K线数据快速完整性检查器" -ForegroundColor Green
    Write-Host ""
    Write-Host "用法:" -ForegroundColor Yellow
    Write-Host "  .\quick_gap_check.ps1 [参数]"
    Write-Host ""
    Write-Host "参数:" -ForegroundColor Yellow
    Write-Host "  -DatabasePath <路径>    数据库文件路径 (默认: data\klines.db)"
    Write-Host "  -Verbose               显示详细输出"
    Write-Host "  -Help                  显示此帮助信息"
    Write-Host ""
    Write-Host "说明:" -ForegroundColor Yellow
    Write-Host "  - 快速检查所有时间间隔(1m,5m,30m,1h,4h,1d)的数据完整性"
    Write-Host "  - 通过统计方法快速识别明显的数据空洞"
    Write-Host "  - 比完整检测更快，适合日常监控"
    Write-Host ""
    Write-Host "示例:" -ForegroundColor Yellow
    Write-Host "  .\quick_gap_check.ps1                                       # 快速检查"
    Write-Host "  .\quick_gap_check.ps1 -Verbose                             # 详细输出"
    Write-Host "  .\quick_gap_check.ps1 -DatabasePath 'custom\path\db.db'    # 自定义数据库路径"
    exit 0
}

# 设置控制台编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "⚡ K线数据快速完整性检查器" -ForegroundColor Green
Write-Host "=========================" -ForegroundColor Green

# 检查数据库文件是否存在
if (-not (Test-Path $DatabasePath)) {
    Write-Host "❌ 错误: 数据库文件不存在: $DatabasePath" -ForegroundColor Red
    Write-Host "请确保数据库文件路径正确，或者先运行K线数据服务生成数据库。" -ForegroundColor Yellow
    exit 1
}

Write-Host "📁 数据库路径: $DatabasePath" -ForegroundColor Cyan
Write-Host "⚡ 检查模式: 快速检查 (所有时间间隔)" -ForegroundColor Cyan

# 检查Rust项目是否存在
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误: 当前目录不是Rust项目根目录" -ForegroundColor Red
    Write-Host "请在项目根目录运行此脚本。" -ForegroundColor Yellow
    exit 1
}

# 构建参数
$args = @(
    "run", "--bin", "quick_gap_check"
)

if ($Verbose) {
    $env:RUST_LOG = "debug"
    Write-Host "🔧 启用详细日志输出" -ForegroundColor Yellow
} else {
    $env:RUST_LOG = "info"
}

Write-Host ""
Write-Host "⚡ 开始快速数据完整性检查..." -ForegroundColor Green
Write-Host ""

try {
    # 运行快速检查程序
    $startTime = Get-Date
    
    & cargo $args
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "✅ 快速检查完成" -ForegroundColor Green
        Write-Host "⏱️  耗时: $($duration.TotalSeconds.ToString('F2')) 秒" -ForegroundColor Cyan
    } else {
        Write-Host ""
        Write-Host "❌ 快速检查失败，退出代码: $LASTEXITCODE" -ForegroundColor Red
    }
}
catch {
    Write-Host ""
    Write-Host "❌ 运行时发生错误: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "📊 检查说明:" -ForegroundColor Yellow
Write-Host "  - 快速检查通过统计方法识别明显的数据空洞"
Write-Host "  - 只检查主要时间间隔，速度更快"
Write-Host "  - 适合日常监控和快速诊断"

Write-Host ""
Write-Host "🔗 进一步操作:" -ForegroundColor Yellow
Write-Host "  - .\start_gap_detector.ps1        # 完整的空洞检测和修复"
Write-Host "  - .\start_kldata_service.ps1      # 历史数据回填服务"
Write-Host "  - .\data.ps1                      # K线数据服务"

Write-Host ""
Write-Host "💡 提示:" -ForegroundColor Yellow
Write-Host "  - 如果发现数据空洞，建议运行完整的空洞检测器进行详细分析"
Write-Host "  - 快速检查主要用于监控，不能替代完整检测"
