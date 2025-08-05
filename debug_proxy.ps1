# 调试代理服务脚本
# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "=== 内置代理服务调试 ===" -ForegroundColor Cyan
Write-Host ""

# 检查可执行文件是否存在
if (-not (Test-Path ".\target\release\builtin_proxy_service.exe")) {
    Write-Host "✗ 可执行文件不存在，请先构建项目:" -ForegroundColor Red
    Write-Host "  cargo build --bin builtin_proxy_service --release" -ForegroundColor Yellow
    exit 1
}

Write-Host "✓ 可执行文件存在" -ForegroundColor Green

# 设置详细日志
$env:RUST_LOG = "info"

Write-Host ""
Write-Host "启动代理服务（详细日志模式）..." -ForegroundColor Yellow
Write-Host "按 Ctrl+C 停止服务" -ForegroundColor Red
Write-Host ""

# 运行代理服务
try {
    & ".\target\release\builtin_proxy_service.exe"
} catch {
    Write-Host "启动失败: $($_.Exception.Message)" -ForegroundColor Red
}