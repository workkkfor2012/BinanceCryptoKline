# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "========================================================"
Write-Host "开始运行K线比对工具 - 测试BTC一个品种（改进版）"
Write-Host "========================================================"

# 切换到项目根目录
Set-Location "F:\work\github\BinanceCryptoKline"

# 运行比对工具
cargo run --bin compare_klines 1m 1

Write-Host ""
Write-Host "测试完成。"
Write-Host "========================================================"

# 等待用户按任意键继续
Write-Host "按任意键继续..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
