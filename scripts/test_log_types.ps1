# 测试新的日志类型功能
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🧪 测试新的日志类型功能..." -ForegroundColor Cyan

# 编译测试程序
Write-Host "📦 编译测试程序..." -ForegroundColor Yellow
cargo build --bin test_new_log_types 2>&1 | Out-Host

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ 编译失败" -ForegroundColor Red
    exit 1
}

# 运行测试
Write-Host "🚀 运行测试..." -ForegroundColor Yellow
cargo run --bin test_new_log_types 2>&1 | Out-Host

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ 测试完成" -ForegroundColor Green
} else {
    Write-Host "❌ 测试失败" -ForegroundColor Red
    exit 1
}
