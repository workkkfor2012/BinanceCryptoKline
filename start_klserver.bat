@echo off
chcp 65001 > nul
echo Starting Binance USDT-M Futures Kline Server...
echo This program will start WebSocket client, kline aggregator, and web server

REM Set proxy environment variables (required for Binance API)
set https_proxy=http://127.0.0.1:1080
set http_proxy=http://127.0.0.1:1080
set HTTPS_PROXY=http://127.0.0.1:1080
set HTTP_PROXY=http://127.0.0.1:1080


set LOG_LEVEL=info

REM Set RUST_BACKTRACE for better error reporting
set RUST_BACKTRACE=1

echo Proxy setting: http://127.0.0.1:1080
echo Log level: INFO
echo Web server will be available at: http://localhost:3000

REM Start kline server
cargo run --release --bin kline_server

