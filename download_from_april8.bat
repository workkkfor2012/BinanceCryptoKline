@echo off
echo 开始下载币安U本位永续合约K线数据（从2025年4月8日开始）...

REM 创建输出目录
mkdir data 2>nul

REM 运行下载程序
cargo run --release

echo 下载完成!
pause
