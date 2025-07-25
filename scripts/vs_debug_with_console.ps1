# Visual Studio调试 - 带控制台输出
# 这个脚本帮助你在Visual Studio中调试时看到控制台输出

param(
    [string]$Target = "klagg_sub_threads",
    [switch]$KeepOpen = $false
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🔧 Visual Studio调试 - 控制台模式" -ForegroundColor Green
Write-Host "📋 目标程序: $Target" -ForegroundColor Yellow

# 设置环境变量
$env:RUST_BACKTRACE = "1"
$env:RUST_LOG = "debug"

Write-Host "🌍 环境变量已设置:" -ForegroundColor Cyan
Write-Host "   RUST_BACKTRACE = $env:RUST_BACKTRACE" -ForegroundColor Gray
Write-Host "   RUST_LOG = $env:RUST_LOG" -ForegroundColor Gray

# 检查可执行文件
$ExePath = "target\debug\$Target.exe"
if (-not (Test-Path $ExePath)) {
    Write-Host "❌ 找不到可执行文件: $ExePath" -ForegroundColor Red
    Write-Host "💡 请先编译程序: cargo build --bin $Target" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ 找到可执行文件: $ExePath" -ForegroundColor Green

Write-Host ""
Write-Host "🚀 启动程序..." -ForegroundColor Cyan
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

try {
    # 启动程序并捕获输出
    & $ExePath
    $ExitCode = $LASTEXITCODE
    
    Write-Host ""
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    
    if ($ExitCode -eq 0) {
        Write-Host "✅ 程序正常退出 (退出码: $ExitCode)" -ForegroundColor Green
    } else {
        Write-Host "❌ 程序异常退出 (退出码: $ExitCode)" -ForegroundColor Red
    }
} catch {
    Write-Host ""
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    Write-Host "❌ 程序执行出错: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "💡 调试提示:" -ForegroundColor Cyan
Write-Host "1. 如果程序立即退出，检查配置文件是否存在" -ForegroundColor White
Write-Host "2. 查看上面的输出信息了解具体错误" -ForegroundColor White
Write-Host "3. 在Visual Studio中设置断点来调试具体问题" -ForegroundColor White
Write-Host "4. 使用 'Debug $Target (External Console)' 配置" -ForegroundColor White

if ($KeepOpen) {
    Write-Host ""
    Read-Host "按Enter键退出"
}
