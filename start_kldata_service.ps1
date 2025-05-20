# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 创建必要的目录
if (-not (Test-Path -Path "logs")) {
    New-Item -Path "logs" -ItemType Directory | Out-Null
}
if (-not (Test-Path -Path "data")) {
    New-Item -Path "data" -ItemType Directory | Out-Null
}

# 设置日志文件
$logfile = "logs\kldata.log"

Write-Host "启动币安U本位合约K线数据服务..."
Write-Host "该程序将下载历史K线数据，维护实时更新，并聚合K线"
Write-Host "日志输出到 $logfile"

# 设置环境变量
$env:RUST_LOG = "info"

# 运行程序
Write-Host "正在运行kline_data_service..."
cargo run --bin kline_data_service

Write-Host "程序执行完成。请查看 $logfile 获取详细信息。"
