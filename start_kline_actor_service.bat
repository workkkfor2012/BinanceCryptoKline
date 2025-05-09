@echo off

:: Set console code page to UTF-8
chcp 65001 > nul

:: Create logs directory if it doesn't exist
if not exist logs mkdir logs
:: Create data directory if it doesn't exist
if not exist data mkdir data

:: Use a simple fixed log file name
set logfile=logs\kline_actor_service.log

echo Starting Binance USDT-M Futures Kline Actor Service...
echo This program will connect to Binance WebSocket, process aggregate trades, and generate klines
echo Logging to %logfile%

:: Set RUST_LOG environment variable for logging
set RUST_LOG=info

:: Run the program with output to console and log file
echo Running kline_actor_service...
cargo run --bin kline_actor_service

echo Program execution completed. Check %logfile% for details.
