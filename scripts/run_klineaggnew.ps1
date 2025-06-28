# K线聚合系统测试脚本
# 运行高内聚的K线合成系统

Write-Host "🚀 启动高内聚K线聚合系统..." -ForegroundColor Green

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 检查数据库文件
$dbFile = "kline_data_integrated.db"
if (Test-Path $dbFile) {
    Write-Host "📁 发现现有数据库文件: $dbFile" -ForegroundColor Yellow
    $response = Read-Host "是否删除现有数据库重新开始? (y/N)"
    if ($response -eq 'y' -or $response -eq 'Y') {
        Remove-Item $dbFile -Force
        Write-Host "🗑️ 已删除现有数据库文件" -ForegroundColor Red
    }
} else {
    Write-Host "📁 将创建新的数据库文件: $dbFile" -ForegroundColor Cyan
}

# 编译程序
Write-Host "🔨 编译程序..." -ForegroundColor Blue
cargo build --bin klineaggnew --release

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ 编译失败!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ 编译成功!" -ForegroundColor Green

# 运行程序
Write-Host "🎯 启动K线聚合系统..." -ForegroundColor Magenta
Write-Host "按 Ctrl+C 停止程序" -ForegroundColor Yellow
Write-Host "=" * 50

# 设置环境变量以启用详细日志
$env:RUST_LOG = "info,klineaggnew=debug"

# 运行程序
cargo run --bin klineaggnew --release

Write-Host "=" * 50
Write-Host "🏁 程序已退出" -ForegroundColor Green

# 检查数据库文件
if (Test-Path $dbFile) {
    $fileSize = (Get-Item $dbFile).Length
    Write-Host "📊 数据库文件大小: $([math]::Round($fileSize/1KB, 2)) KB" -ForegroundColor Cyan
    
    # 显示数据库表信息
    Write-Host "📋 数据库表信息:" -ForegroundColor Cyan
    sqlite3 $dbFile ".tables"
}
