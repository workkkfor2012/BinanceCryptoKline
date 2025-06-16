# K线系统启动器UI启动脚本
# 启动Python GUI界面来管理所有PowerShell脚本

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚀 启动K线系统启动器UI" -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Green

# 检查是否在项目根目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 错误：请在项目根目录运行此脚本" -ForegroundColor Red
    Write-Host "当前目录：$(Get-Location)" -ForegroundColor Yellow
    Read-Host "按任意键退出"
    exit 1
}

# 检查Python是否安装
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✅ Python已安装: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ 错误：未找到Python" -ForegroundColor Red
    Write-Host "请安装Python 3.6或更高版本" -ForegroundColor Yellow
    Read-Host "按任意键退出"
    exit 1
}

# 检查启动器文件是否存在
if (-not (Test-Path "launcher_ui.py")) {
    Write-Host "❌ 错误：未找到启动器文件 launcher_ui.py" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

Write-Host ""
Write-Host "📋 功能说明：" -ForegroundColor Cyan
Write-Host "  - 图形化管理所有PowerShell脚本" -ForegroundColor White
Write-Host "  - 支持Debug/Release模式切换" -ForegroundColor White
Write-Host "  - 自动更新所有脚本的编译模式" -ForegroundColor White
Write-Host "  - 实时查看运行日志" -ForegroundColor White
Write-Host "  - 进程管理和状态监控" -ForegroundColor White
Write-Host ""

Write-Host "🚀 启动图形界面..." -ForegroundColor Yellow

try {
    # 启动Python GUI
    python launcher_ui.py
} catch {
    Write-Host "❌ 启动失败: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "💡 可能的解决方案：" -ForegroundColor Yellow
    Write-Host "  1. 确保Python已正确安装" -ForegroundColor White
    Write-Host "  2. 确保tkinter模块可用（通常随Python一起安装）" -ForegroundColor White
    Write-Host "  3. 尝试使用: python -m tkinter 测试tkinter是否可用" -ForegroundColor White
}

Write-Host ""
Write-Host "✅ 启动器已退出" -ForegroundColor Green
Read-Host "按任意键退出"
