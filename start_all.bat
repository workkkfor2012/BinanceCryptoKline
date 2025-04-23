@echo off
chcp 65001 > nul
echo Starting all Binance USDT-M Futures Kline services...

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

REM Start kline data service in a new window
echo Starting Kline Data Service in a new window...
start "Kline Data Service" cmd /c "start_kldata_service.bat"

REM Wait for 5 seconds to ensure data service has started
echo Waiting for 5 seconds to ensure data service has started...
timeout /t 5 /nobreak > nul

REM Start kline web server in a new window
echo Starting Kline Web Server in a new window...
start "Kline Web Server" cmd /c "start_klserver.bat"

echo All services started. Please check the individual windows for details.
echo Web server will be available at: http://localhost:3000

pause
