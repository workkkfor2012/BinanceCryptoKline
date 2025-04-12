#!/bin/bash

echo "开始下载币安U本位永续合约K线数据..."

# 创建输出目录
mkdir -p data

# 直接运行可执行文件
./target/release/kline_downloader_rust

echo "下载完成!"
echo "数据已保存到 data/klines.db"
