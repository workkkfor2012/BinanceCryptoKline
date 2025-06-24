#!/usr/bin/env pwsh
# -*- coding: utf-8 -*-
<#
.SYNOPSIS
    自动分析最新的 Trace 日志文件

.DESCRIPTION
    这个脚本会自动找到 logs\debug_snapshots\ 目录下最新的日志文件，
    然后使用 Python 脚本进行分析并生成详细报告。

.EXAMPLE
    .\scripts\run_trace_analysis.ps1
#>

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 定义颜色输出函数
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )

    try {
        switch ($Color) {
            "Red" { Write-Host $Message -ForegroundColor Red }
            "Green" { Write-Host $Message -ForegroundColor Green }
            "Yellow" { Write-Host $Message -ForegroundColor Yellow }
            "Blue" { Write-Host $Message -ForegroundColor Blue }
            "Cyan" { Write-Host $Message -ForegroundColor Cyan }
            "Magenta" { Write-Host $Message -ForegroundColor Magenta }
            default { Write-Host $Message }
        }
    }
    catch {
        Write-Host $Message
    }
}

# 主函数
function Main {
    Write-ColorOutput "🔍 Trace 日志分析工具" "Cyan"
    Write-ColorOutput "=" * 50 "Cyan"
    
    # 获取脚本所在目录的父目录（项目根目录）
    $scriptPath = $MyInvocation.MyCommand.Path
    if (-not $scriptPath) {
        $scriptPath = $PSCommandPath
    }
    $scriptDir = Split-Path -Parent $scriptPath
    $projectRoot = Split-Path -Parent $scriptDir
    
    # 检查日志目录是否存在（相对于项目根目录）
    $logDir = Join-Path $projectRoot "logs\debug_snapshots"
    if (-not (Test-Path $logDir)) {
        Write-ColorOutput "❌ 日志目录不存在: $logDir" "Red"
        Write-ColorOutput "请先运行程序生成日志文件" "Yellow"
        exit 1
    }
    
    # 查找所有日志文件
    $logFiles = Get-ChildItem -Path $logDir -Filter "final_snapshot_*.log" | Sort-Object LastWriteTime -Descending
    
    if ($logFiles.Count -eq 0) {
        Write-ColorOutput "❌ 未找到任何日志文件" "Red"
        Write-ColorOutput "请先运行程序生成日志文件" "Yellow"
        exit 1
    }
    
    # 获取最新的日志文件
    $latestLog = $logFiles[0]
    $latestLogPath = $latestLog.FullName
    
    Write-ColorOutput "📁 找到 $($logFiles.Count) 个日志文件" "Green"
    Write-ColorOutput "📄 最新日志文件: $($latestLog.Name)" "Green"
    Write-ColorOutput "📅 创建时间: $($latestLog.LastWriteTime)" "Green"
    Write-ColorOutput "📏 文件大小: $([math]::Round($latestLog.Length / 1KB, 2)) KB" "Green"
    
    # 检查 Python 是否可用
    try {
        $pythonVersion = python --version 2>&1
        Write-ColorOutput "🐍 Python 版本: $pythonVersion" "Green"
    }
    catch {
        Write-ColorOutput "❌ Python 未安装或不在 PATH 中" "Red"
        Write-ColorOutput "请安装 Python 3.6+ 并确保在 PATH 中" "Yellow"
        exit 1
    }
    
    # 检查分析脚本是否存在（在同一目录下）
    $analyzeScript = Join-Path $scriptDir "analyze_trace.py"
    if (-not (Test-Path $analyzeScript)) {
        Write-ColorOutput "❌ 分析脚本不存在: $analyzeScript" "Red"
        Write-ColorOutput "请确保 analyze_trace.py 文件在 scripts 目录中" "Yellow"
        exit 1
    }
    
    Write-ColorOutput "`n🚀 开始分析日志文件..." "Yellow"
    Write-ColorOutput "-" * 50 "Yellow"
    
    # 执行 Python 分析脚本
    try {
        $analysisCommand = "python `"$analyzeScript`" `"$latestLogPath`""
        Write-ColorOutput "执行命令: $analysisCommand" "Cyan"
        Write-ColorOutput ""
        
        # 使用 Invoke-Expression 执行命令并实时显示输出
        Invoke-Expression $analysisCommand
        
        if ($LASTEXITCODE -eq 0) {
            Write-ColorOutput "`n✅ 分析完成！" "Green"
        } else {
            Write-ColorOutput "`n❌ 分析过程中出现错误 (退出码: $LASTEXITCODE)" "Red"
        }
    }
    catch {
        Write-ColorOutput "❌ 执行分析脚本时出错: $($_.Exception.Message)" "Red"
        exit 1
    }
    
    # 显示其他可用的日志文件
    if ($logFiles.Count -gt 1) {
        Write-ColorOutput "`n📋 其他可用的日志文件:" "Cyan"
        for ($i = 1; $i -lt [Math]::Min($logFiles.Count, 6); $i++) {
            $file = $logFiles[$i]
            Write-ColorOutput "  $($i + 1). $($file.Name) ($($file.LastWriteTime))" "White"
        }
        
        if ($logFiles.Count -gt 5) {
            Write-ColorOutput "  ... 还有 $($logFiles.Count - 5) 个文件" "White"
        }
        
        Write-ColorOutput "`n💡 要分析其他文件，请使用:" "Yellow"
        Write-ColorOutput "   python scripts\analyze_trace.py `"logs\debug_snapshots\<文件名>`"" "Yellow"
    }
    
    Write-ColorOutput "`n🎯 分析完成！" "Green"
}

# 错误处理
trap {
    Write-ColorOutput "`n❌ 脚本执行出错: $($_.Exception.Message)" "Red"
    Write-ColorOutput "请检查错误信息并重试" "Yellow"
    exit 1
}

# 执行主函数
Main
