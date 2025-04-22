@echo off
echo Starting Binance USDT-M Futures Kline Downloader...
echo This program will download historical kline data and save to SQLite database

REM Set proxy environment variables (required for Binance API)
set https_proxy=http://127.0.0.1:1080
set http_proxy=http://127.0.0.1:1080
set HTTPS_PROXY=http://127.0.0.1:1080
set HTTP_PROXY=http://127.0.0.1:1080

REM Set log level to INFO
set RUST_LOG=info
set LOG_LEVEL=info

REM Set RUST_BACKTRACE for better error reporting
set RUST_BACKTRACE=1

echo Proxy setting: http://127.0.0.1:1080
echo Log level: INFO

REM Start kline downloader with specific parameters
REM Use --intervals to specify which timeframes to download
REM Use --concurrency to control parallel download tasks
cargo run --bin kline_downloader -- --intervals "1m" --concurrency 5


