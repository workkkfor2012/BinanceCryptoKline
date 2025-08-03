# K线聚合服务高保真自定义品种测试启动脚本
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

# 清理日志文件，确保干净的启动环境
Write-Host "🧹 清理日志文件..." -ForegroundColor Cyan
$logFiles = @(
    "logs\ai_detailed.log",
    "logs\low_freq.log",
    "logs\problem_summary.log",
    "logs\performance.folded"
)

foreach ($logFile in $logFiles) {
    if (Test-Path $logFile) {
        try {
            Remove-Item $logFile -Force
            Write-Host "  ✅ 已删除: $logFile" -ForegroundColor Green
        }
        catch {
            Write-Host "  ⚠️ 删除失败: $logFile - $_" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "  ℹ️ 文件不存在: $logFile" -ForegroundColor Gray
    }
}

# 设置环境变量
Set-LoggingEnvironment
$env:LOG_TRANSPORT = "named_pipe"
$env:ENABLE_PERF_LOG = "1"             # 启用性能日志分析

Write-Host "🔧 高保真自定义品种测试模式配置完成:" -ForegroundColor Cyan
Write-Host "  ✅ 使用专用的高保真测试入口 (klagg_custom_sub_test)" -ForegroundColor Green
Write-Host "  ✅ 自定义品种列表，易于修改测试范围" -ForegroundColor Green
Write-Host "  ✅ 完整保留数据库持久化任务" -ForegroundColor Green
Write-Host "  ✅ 与生产模式高度一致，确保测试可靠性" -ForegroundColor Green

Write-Host "🔥 性能日志分析已启用，将生成 logs\performance.folded" -ForegroundColor Magenta
Write-Host "🧪 高保真自定义品种测试模式已启用，当前测试品种:" -ForegroundColor Cyan
Write-Host "   BTCUSDT, ETHUSDT, SOLUSDT (可在代码中轻松修改)" -ForegroundColor White
Write-Host "🌐 Web服务器将在启动后提供数据可视化界面" -ForegroundColor Magenta
Write-Host "💾 数据库持久化任务已启用，模拟真实I/O负载" -ForegroundColor Green

$buildMode = Get-BuildMode
$auditEnabled = Get-AuditEnabled

Write-Host "🚀 启动K线聚合服务 - 高保真自定义品种测试模式 ($buildMode)" -ForegroundColor Yellow

# 显示审计功能状态
if ($auditEnabled) {
    Write-Host "🔍 审计功能已启用 - 包含生命周期事件校验和数据完整性审计" -ForegroundColor Magenta
    Write-Host "  ✅ 生命周期校验器：已启用" -ForegroundColor Green
    Write-Host "  ✅ 数据完整性审计器：已启用" -ForegroundColor Green
} else {
    Write-Host "⚠️ 审计功能已禁用 - 使用零成本抽象模式，生产性能最优" -ForegroundColor Yellow
}

try {
    $cargoCmd = Get-CargoCommand -BinaryName 'klagg_custom_sub_test'
    Invoke-Expression $cargoCmd
}
catch {
    Write-Host "高保真自定义品种测试服务启动失败: $_" -ForegroundColor Red
}
finally {
    Write-Host ""
    Write-Host "K线聚合高保真自定义品种测试服务已停止" -ForegroundColor Yellow
    Read-Host "按任意键退出"
}
