# K线聚合服务生产模式启动脚本
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

Write-Host "🔧 生产模式配置完成:" -ForegroundColor Cyan
Write-Host "  ✅ 使用生产入口 (klagg_sub_threads)" -ForegroundColor Green
Write-Host "  ✅ 从币安API获取所有U本位永续合约品种" -ForegroundColor Green
Write-Host "  ✅ 启用数据库持久化" -ForegroundColor Green
Write-Host "  ✅ 基于MiniTicker实时发现新品种" -ForegroundColor Green
Write-Host "  ✅ 禁用Web服务器" -ForegroundColor Green

Write-Host "🔥 性能日志分析已启用，将生成 logs\performance.folded" -ForegroundColor Magenta
Write-Host "🏭 生产模式已启用，将处理所有币安U本位永续合约品种" -ForegroundColor Cyan

$buildMode = Get-BuildMode
Write-Host "🚀 启动K线聚合服务 - 生产模式 ($buildMode)" -ForegroundColor Yellow

try {
    $cargoCmd = Get-CargoCommand -BinaryName 'klagg_sub_threads'
    Invoke-Expression $cargoCmd
}
catch {
    Write-Host "生产服务启动失败: $_" -ForegroundColor Red
}
finally {
    Write-Host ""
    Write-Host "K线聚合生产服务已停止" -ForegroundColor Yellow
    Read-Host "按任意键退出"
}
