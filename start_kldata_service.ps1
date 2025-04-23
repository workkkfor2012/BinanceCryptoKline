Write-Host "Starting Binance USDT-M Futures Kline Data Service..."
Write-Host "This program will download historical kline data, maintain real-time updates, and aggregate klines"

# 代理和日志设置已硬编码到代码中
Write-Host "Proxy and log settings are hardcoded in the source code"
Write-Host "Proxy: http://127.0.0.1:1080"
Write-Host "Log level: INFO"

# 启动K线数据服务，参数已硬编码
# 时间周期: 1m,5m,30m,4h,1d,1w
# 并发数: 5
cargo run --bin kline_data_service

Write-Host "按任意键继续..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
