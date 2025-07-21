# 启动标准版本的K线聚合服务
# 使用标准线性扫描实现

param(
    [string]$LogLevel = "info",
    [string]$ConfigPath = "config\BinanceKlineConfig.toml"
)

Write-Host "🚀 启动K线聚合服务 - 标准版本" -ForegroundColor Green
Write-Host "实现方式: 标准线性扫描" -ForegroundColor Yellow
Write-Host "日志级别: $LogLevel" -ForegroundColor Cyan
Write-Host "配置文件: $ConfigPath" -ForegroundColor Cyan
Write-Host "编译时间: $(Get-Date)" -ForegroundColor Gray
Write-Host ""

# 设置环境变量
$env:RUST_LOG = $LogLevel
$env:HTTPS_PROXY = "http://127.0.0.1:1080"
$env:HTTP_PROXY = "http://127.0.0.1:1080"

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 编译并运行标准版本（不使用simd feature）
Write-Host "📦 编译标准版本..." -ForegroundColor Blue
cargo build --release --bin klagg_sub_threads

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ 编译失败！" -ForegroundColor Red
    exit 1
}

Write-Host "✅ 编译完成，启动服务..." -ForegroundColor Green
Write-Host "💡 提示：查看性能日志请关注 target='性能分析' 的日志条目" -ForegroundColor Yellow
Write-Host "📊 性能统计：每60秒输出一次汇总数据" -ForegroundColor Yellow
Write-Host ""

# 启动服务
try {
    cargo run --release --bin klagg_sub_threads
}
catch {
    Write-Host "❌ 服务启动失败: $_" -ForegroundColor Red
    exit 1
}
finally {
    Write-Host "🛑 服务已停止" -ForegroundColor Yellow
}
