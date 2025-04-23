@echo off
chcp 65001 > nul
echo Starting Binance USDT-M Futures Kline Web Server...

echo Web server will be available at: http://localhost:3000

REM 启动K线 Web服务器，无需额外参数
cargo run --bin kline_server

pause

