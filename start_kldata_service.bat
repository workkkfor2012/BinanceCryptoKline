@echo off

:: Set console code page to UTF-8
chcp 65001 > nul

:: Create logs directory if it doesn't exist
if not exist logs mkdir logs

:: Use a simple fixed log file name
set logfile=logs\kldata.log

echo Starting Binance USDT-M Futures Kline Data Service...
echo This program will download historical kline data, maintain real-time updates, and aggregate klines
echo Logging to %logfile%



:: Run the program and redirect output to log file
cargo run --bin kline_data_service > nul 2>&1

echo Program execution completed. Check %logfile% for details.
