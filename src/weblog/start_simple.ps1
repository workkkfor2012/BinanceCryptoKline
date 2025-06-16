# WebLog简单启动脚本

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# 获取脚本所在目录并切换到该目录
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host "⚡ WebLog简单启动" -ForegroundColor Green
Write-Host "📍 工作目录: $scriptDir" -ForegroundColor Cyan

# 智能编译检查
$needsCompile = $false
$exePath = "target\debug\weblog.exe"

if (-not (Test-Path $exePath)) {
    Write-Host "🔨 首次运行，需要编译..." -ForegroundColor Yellow
    $needsCompile = $true
} else {
    # 检查源代码是否比可执行文件新
    $exeTime = (Get-Item $exePath).LastWriteTime
    $sourceFiles = Get-ChildItem -Path "src" -Recurse -Include "*.rs"
    $cargoToml = Get-Item "Cargo.toml"

    $latestSourceTime = ($sourceFiles + $cargoToml | Sort-Object LastWriteTime -Descending | Select-Object -First 1).LastWriteTime

    if ($latestSourceTime -gt $exeTime) {
        Write-Host "🔨 检测到源代码更新，重新编译..." -ForegroundColor Yellow
        $needsCompile = $true
    } else {
        Write-Host "✅ 可执行文件是最新的，跳过编译" -ForegroundColor Green
    }
}

if ($needsCompile) {
    cargo build --bin weblog
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ 编译失败" -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ 编译完成" -ForegroundColor Green
}

# 设置环境变量
$env:LOG_TRANSPORT = "named_pipe"
$env:PIPE_NAME = "\\.\pipe\kline_log_pipe"
# 注意：不设置RUST_LOG，让weblog.rs中的设置生效

Write-Host "🚀 启动WebLog服务器 (端口: 8080)" -ForegroundColor Cyan
Write-Host "🌐 访问: http://localhost:8080" -ForegroundColor Green
Write-Host "💡 按 Ctrl+C 停止服务" -ForegroundColor Yellow
Write-Host "📡 命名管道: $env:PIPE_NAME" -ForegroundColor Cyan
Write-Host ""

# 启动服务器 - 使用命名管道模式
cargo run --bin weblog -- --pipe-name "\\.\pipe\kline_log_pipe"
