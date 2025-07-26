# K线聚合服务启动脚本 (单线程重构版架构 - 测试模式)
# 注意：此脚本已更新为使用新的单线程重构版架构 (klagg)
# 旧的 klineaggnew 二进制文件已被弃用
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
    "logs\low_freq.log",
    "logs\problem_summary.log",
    "logs\beacon.log",
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
$env:KLINE_TEST_MODE = "true"  # 启用测试模式，订阅btcusdt
$env:ENABLE_PERF_LOG = "1"     # 启用性能日志分析

Write-Host "🔥 性能日志分析已启用，将生成 logs\performance.folded" -ForegroundColor Magenta
Write-Host "🧪 测试模式已启用，将订阅少量测试品种" -ForegroundColor Cyan
Write-Host "⚡ 使用单线程异步事件驱动架构，提供更好的性能和可维护性" -ForegroundColor Green

$buildMode = Get-BuildMode
Write-Host "🚀 启动K线聚合服务 - 单线程重构版架构 ($buildMode) [测试模式]" -ForegroundColor Yellow

try {
    $cargoCmd = Get-CargoCommand -BinaryName 'klagg'
    Invoke-Expression $cargoCmd
}
catch {
    Write-Host "服务启动失败: $_" -ForegroundColor Red
}
finally {
    Write-Host ""
    Write-Host "K线聚合服务已停止" -ForegroundColor Yellow
    Read-Host "按任意键退出"
}
