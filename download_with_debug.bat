@echo off
echo 开始下载币安U本位永续合约K线数据...

REM 设置日志级别为debug
set RUST_LOG=debug

REM 创建输出目录
mkdir data 2>nul

REM 运行下载程序
target\release\kline_downloader_rust.exe

echo 下载完成!
pause
