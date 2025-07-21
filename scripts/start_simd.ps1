# 启动SIMD优化版本的K线聚合服务
# 使用SIMD批量时间戳比较和掩码跳过优化

param(
    [string]$LogLevel = "info",
    [string]$ConfigPath = "config\BinanceKlineConfig.toml"
)

Write-Host "🚀 启动K线聚合服务 - SIMD优化版本" -ForegroundColor Green
Write-Host "实现方式: SIMD批量比较 + 掩码跳过" -ForegroundColor Yellow
Write-Host "SIMD宽度: 4个i64 (wide库)" -ForegroundColor Yellow
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

# 编译并运行SIMD版本（使用simd feature）
Write-Host "📦 编译SIMD优化版本..." -ForegroundColor Blue
cargo build --release --bin klagg_sub_threads --features simd

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ 编译失败！" -ForegroundColor Red
    Write-Host "💡 提示：SIMD版本需要wide库支持，请检查依赖" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ 编译完成，启动服务..." -ForegroundColor Green
Write-Host "💡 提示：查看性能日志请关注 target='性能分析' 的日志条目" -ForegroundColor Yellow
Write-Host "📊 性能统计：每60秒输出一次汇总数据" -ForegroundColor Yellow
Write-Host "🔬 SIMD优化：批量处理4个时间戳，掩码跳过无效块" -ForegroundColor Yellow
Write-Host ""

# 启动服务
try {
    cargo run --release --bin klagg_sub_threads --features simd
}
catch {
    Write-Host "❌ 服务启动失败: $_" -ForegroundColor Red
    exit 1
}
finally {
    Write-Host "🛑 服务已停止" -ForegroundColor Yellow
}
