# K线系统启动器UI启动脚本 (修复版)
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚀 启动K线系统启动器UI" -ForegroundColor Green

if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

# 使用指定的Python路径
$pythonPath = "F:\work\tool\python312\\python.exe"

try {
    $pythonVersion = & $pythonPath --version 2>&1
    Write-Host "✅ Python: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ 未找到Python" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

if (-not (Test-Path "launcher_ui.py")) {
    Write-Host "❌ 未找到启动器文件" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

Write-Host "🚀 启动图形界面..." -ForegroundColor Yellow

try {
    & $pythonPath launcher_ui.py
} catch {
    Write-Host "❌ 启动失败: $_" -ForegroundColor Red
    Write-Host "错误详情: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "✅ 启动器已退出" -ForegroundColor Green
Read-Host "按任意键退出"
