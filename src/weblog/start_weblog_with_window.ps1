# WebLog日志系统启动脚本
#
# 功能：启动WebLog日志可视化系统
# 端口：8080
# 传输：命名管道模式
# 窗口：在当前PowerShell窗口中运行
#
# 使用方法：
# .\start_weblog_with_window.ps1

Write-Host "🌐 启动WebLog日志系统" -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Green

# 检查当前目录是否正确
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录下运行此脚本" -ForegroundColor Red
    Write-Host "当前目录：$(Get-Location)" -ForegroundColor Yellow
    Write-Host "应该在项目根目录下（包含主Cargo.toml文件）" -ForegroundColor Yellow
    pause
    exit 1
}

# 检查weblog目录是否存在
if (-not (Test-Path "src\weblog")) {
    Write-Host "❌ 错误：未找到src\weblog目录" -ForegroundColor Red
    Write-Host "当前目录：$(Get-Location)" -ForegroundColor Yellow
    pause
    exit 1
}

# 显示配置信息
Write-Host "📋 启动配置：" -ForegroundColor Cyan
Write-Host "  - 日志传输：命名管道" -ForegroundColor White
Write-Host "  - 管道名称：\\.\pipe\weblog_pipe" -ForegroundColor White
Write-Host "  - Web端口：8080" -ForegroundColor White
Write-Host "  - 日志级别：info" -ForegroundColor White
Write-Host "  - 访问地址：http://localhost:8080" -ForegroundColor White
Write-Host ""

# 启动WebLog系统（在当前窗口中）
Write-Host "🚀 正在启动WebLog系统..." -ForegroundColor Green

Write-Host '🌐 WebLog日志系统启动中...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
Write-Host ''
Write-Host '📡 配置信息：' -ForegroundColor Cyan
Write-Host '  传输方式：命名管道' -ForegroundColor White
Write-Host '  管道名称：\\.\pipe\weblog_pipe' -ForegroundColor White
Write-Host '  Web端口：8080' -ForegroundColor White
Write-Host '  访问地址：http://localhost:8080/modules' -ForegroundColor Yellow
Write-Host ''
Write-Host '🔧 设置环境变量...' -ForegroundColor Cyan
$env:LOG_TRANSPORT='named_pipe'
$env:PIPE_NAME='\\.\pipe\weblog_pipe'
# 注意：不设置RUST_LOG，让weblog.rs中的设置生效
Write-Host '✅ 环境变量设置完成' -ForegroundColor Green
Write-Host ''
Write-Host '🚀 启动WebLog服务器...' -ForegroundColor Green
Write-Host '=' * 60 -ForegroundColor Green
Write-Host "当前工作目录: $(Get-Location)" -ForegroundColor Cyan
Write-Host "执行命令: cargo run --manifest-path src\weblog\Cargo.toml --bin weblog -- --pipe-name '\\.\pipe\weblog_pipe'" -ForegroundColor Cyan
Write-Host ""

try {
    cargo run --manifest-path src\weblog\Cargo.toml --bin weblog -- --pipe-name '\\.\pipe\weblog_pipe'
}
catch {
    Write-Host "❌ WebLog启动失败: $_" -ForegroundColor Red
    Write-Host "错误详情: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    Write-Host ""
    Write-Host "WebLog服务已停止" -ForegroundColor Yellow
    Write-Host "按任意键退出..." -ForegroundColor Gray
    Read-Host
}
