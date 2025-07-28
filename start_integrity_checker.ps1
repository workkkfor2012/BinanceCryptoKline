#!/usr/bin/env pwsh
# -*- coding: utf-8 -*-
<#
.SYNOPSIS
    启动K线数据完整性检查器
    
.DESCRIPTION
    启动简化版的K线数据完整性检查器
    只检查1分钟、5分钟、30分钟这3个固定周期
    每分钟的40秒开始检查，持续运行直到手动关闭
    
.NOTES
    Author: K线系统
    Version: 1.0
#>

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 脚本路径
$SCRIPT_PATH = "scripts\kline_integrity_checker.ps1"

Write-Host "=== K线数据完整性检查器启动器 ===" -ForegroundColor Green
Write-Host ""
Write-Host "检查周期: 1分钟, 5分钟, 30分钟" -ForegroundColor Yellow
Write-Host "检查时间: 每分钟第40秒" -ForegroundColor Yellow
Write-Host "运行模式: 持续运行，手动关闭" -ForegroundColor Yellow
Write-Host ""

# 检查脚本文件是否存在
if (-not (Test-Path $SCRIPT_PATH)) {
    Write-Host "错误: 脚本文件不存在 - $SCRIPT_PATH" -ForegroundColor Red
    Write-Host "请确保在项目根目录运行此脚本" -ForegroundColor Red
    Read-Host "按回车键退出"
    exit 1
}

# 检查数据库文件是否存在
$DATABASE_PATH = "data/binance_klines.db"
if (-not (Test-Path $DATABASE_PATH)) {
    Write-Host "警告: 数据库文件不存在 - $DATABASE_PATH" -ForegroundColor Yellow
    Write-Host "请确保K线数据服务已运行并生成了数据" -ForegroundColor Yellow
    Write-Host ""
}

# 检查sqlite3命令是否可用
if (-not (Get-Command sqlite3.exe -ErrorAction SilentlyContinue)) {
    Write-Host "错误: sqlite3.exe 命令不可用" -ForegroundColor Red
    Write-Host "请安装SQLite或确保sqlite3.exe在PATH中" -ForegroundColor Red
    Read-Host "按回车键退出"
    exit 1
}

Write-Host "正在启动K线数据完整性检查器..." -ForegroundColor Green
Write-Host "按 Ctrl+C 可以停止程序" -ForegroundColor Cyan
Write-Host ""

try {
    # 执行检查脚本
    & $SCRIPT_PATH
} catch {
    Write-Host "启动失败: $($_.Exception.Message)" -ForegroundColor Red
    Read-Host "按回车键退出"
    exit 1
}

Write-Host ""
Write-Host "K线数据完整性检查器已停止" -ForegroundColor Yellow
