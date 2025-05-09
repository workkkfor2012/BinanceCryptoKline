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
$actor_logfile = "logs\kline_actor_service.log"
$compare_logfile = "logs\compare_klines.log"

Write-Host "启动币安U本位合约K线测试..."
Write-Host "该测试将启动K线Actor服务和K线比对工具"
Write-Host "K线Actor服务日志输出到 $actor_logfile"
Write-Host "K线比对工具日志输出到 $compare_logfile"

# 设置环境变量
$env:RUST_LOG = "info"

# 在新窗口中启动K线Actor服务
Write-Host "正在启动K线Actor服务..."
Start-Process powershell -ArgumentList "-Command `"cd '$PWD'; `$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8; cargo run --bin kline_actor_service | Tee-Object -FilePath '$actor_logfile'`""

# 等待一段时间，确保K线Actor服务已经启动
Write-Host "等待K线Actor服务启动 (10秒)..."
Start-Sleep -Seconds 10

# 启动K线比对工具
Write-Host "正在启动K线比对工具..."
cargo run --bin compare_klines | Tee-Object -FilePath $compare_logfile

Write-Host "测试完成。"
