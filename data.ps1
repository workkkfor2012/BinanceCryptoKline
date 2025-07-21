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

# 设置环境变量 - 使用命名管道传输到Log MCP
Set-LoggingEnvironment
$env:LOG_TRANSPORT = "named_pipe"

# 启用性能日志分析
$env:ENABLE_PERF_LOG = "1"
Write-Host "🔥 性能日志分析已启用，将生成 logs\performance.folded" -ForegroundColor Magenta

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
