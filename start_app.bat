@echo off
echo Starting application...
echo Initializing environment...

REM Create output directory
mkdir data 2>nul

REM Check if executable exists
if not exist "target\release\kline_server.exe" (
    echo Error: Executable not found, trying to compile...
    cargo build --release
    if not exist "target\release\kline_server.exe" (
        echo Compilation failed, please make sure Rust and Cargo are installed
        pause
        exit /b 1
    )
)

echo Starting application...
echo Using SQLite database with WAL mode for high performance
echo The program will automatically check the database and historical K-line data, and download if needed

REM Run the program
target\release\kline_server.exe

echo Program execution completed!
pause
