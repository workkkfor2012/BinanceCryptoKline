@echo off
echo 开始下载币安U本位永续合约K线数据...
echo 正在初始化下载环境...

REM 创建输出目录
mkdir data 2>nul

REM 检查是否存在可执行文件
if not exist "target\release\kline_downloader_rust.exe" (
    echo 错误: 未找到可执行文件，正在尝试编译...
    cargo build --release
    if not exist "target\release\kline_downloader_rust.exe" (
        echo 编译失败，请确保已安装Rust和Cargo
        pause
        exit /b 1
    )
)

echo 开始下载数据，这可能需要一些时间...
echo 数据将保存在 data 目录下的 klines.db 文件中

REM 运行下载程序
target\release\kline_downloader_rust.exe

echo 下载完成!
echo 数据已保存到 data\klines.db
pause
