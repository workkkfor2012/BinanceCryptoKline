#!/usr/bin/env pwsh
# -*- coding: utf-8 -*-
<#
.SYNOPSIS
    K线数据完整性检查器 - 简化版
    
.DESCRIPTION
    定时检查1分钟、5分钟、30分钟K线数据的完整性
    每分钟的40秒开始检查，持续运行直到手动关闭
    
.NOTES
    Author: K线系统
    Version: 1.0
    检查周期: 1m, 5m, 30m
    检查时间: 每分钟40秒
#>

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 配置参数
$CHECK_INTERVALS = @("1m", "5m", "30m")  # 固定检查这3个周期
$CHECK_SECOND = 40  # 每分钟的40秒开始检查
$DATABASE_PATH = "data/klines.db"
$CONCURRENT_DOWNLOADS = 50  # 并发下载数量
$BINANCE_API_BASE = "https://fapi.binance.com"

# 日志函数
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage
}

# 检查数据库是否存在
function Test-Database {
    if (-not (Test-Path $DATABASE_PATH)) {
        Write-Log "数据库文件不存在: $DATABASE_PATH" "ERROR"
        return $false
    }
    return $true
}

# 获取活跃交易对
function Get-ActiveSymbols {
    try {
        # 从表名中提取交易对（获取所有k_*_1m表）
        $query = @"
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'k_%_1m'
ORDER BY name
"@

        $result = sqlite3.exe $DATABASE_PATH $query
        if ($LASTEXITCODE -eq 0 -and $result) {
            $symbols = @()
            foreach ($tableName in $result) {
                if ($tableName -match '^k_(.+)_1m$') {
                    $symbol = $matches[1].ToUpper()
                    $symbols += $symbol
                }
            }
            Write-Log "获取到 $($symbols.Count) 个活跃交易对"
            return $symbols
        } else {
            Write-Log "获取活跃交易对失败" "ERROR"
            return @()
        }
    } catch {
        Write-Log "获取活跃交易对异常: $($_.Exception.Message)" "ERROR"
        return @()
    }
}

# 并发下载K线数据
function Get-KlineDataConcurrently {
    param(
        [array]$Symbols,
        [string]$Interval,
        [int]$StartTime,
        [int]$EndTime
    )

    Write-Log "开始并发下载 $Interval 周期K线数据，交易对数量: $($Symbols.Count)"

    # 创建任务列表
    $jobs = @()
    $results = @{}

    # 分批处理，避免过多并发
    $batchSize = $CONCURRENT_DOWNLOADS
    for ($i = 0; $i -lt $Symbols.Count; $i += $batchSize) {
        $batch = $Symbols[$i..([Math]::Min($i + $batchSize - 1, $Symbols.Count - 1))]

        foreach ($symbol in $batch) {
            $job = Start-Job -ScriptBlock {
                param($Symbol, $Interval, $StartTime, $EndTime, $ApiBase)

                try {
                    $url = "$ApiBase/fapi/v1/klines"
                    $params = @{
                        symbol = $Symbol
                        interval = $Interval
                        startTime = $StartTime
                        endTime = $EndTime
                        limit = 1000
                    }

                    $queryString = ($params.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join "&"
                    $fullUrl = "$url?$queryString"

                    $response = Invoke-RestMethod -Uri $fullUrl -Method Get -TimeoutSec 10
                    return @{
                        Symbol = $Symbol
                        Success = $true
                        Data = $response
                        Error = $null
                    }
                } catch {
                    return @{
                        Symbol = $Symbol
                        Success = $false
                        Data = $null
                        Error = $_.Exception.Message
                    }
                }
            } -ArgumentList $symbol, $Interval, $StartTime, $EndTime, $BINANCE_API_BASE

            $jobs += $job
        }

        # 等待当前批次完成
        $jobs | Wait-Job | Out-Null

        # 收集结果
        foreach ($job in $jobs) {
            $result = Receive-Job -Job $job
            $results[$result.Symbol] = $result
            Remove-Job -Job $job
        }

        $jobs = @()
        Write-Log "完成批次下载，已处理 $([Math]::Min($i + $batchSize, $Symbols.Count))/$($Symbols.Count) 个交易对"
    }

    # 统计结果
    $successCount = ($results.Values | Where-Object { $_.Success }).Count
    $failedCount = ($results.Values | Where-Object { -not $_.Success }).Count

    Write-Log "并发下载完成 - 成功: $successCount, 失败: $failedCount"

    if ($failedCount -gt 0) {
        $failedSymbols = ($results.Values | Where-Object { -not $_.Success } | Select-Object -First 5).Symbol -join ", "
        Write-Log "部分下载失败的交易对: $failedSymbols" "WARN"
    }

    return $results
}

# 检查单个交易对的数据完整性（使用下载的数据进行比对）
function Test-SymbolIntegrityWithDownloadedData {
    param(
        [string]$Symbol,
        [string]$Interval,
        [hashtable]$DownloadedData
    )

    try {
        # 构建表名（小写symbol）
        $tableName = "k_$($Symbol.ToLower())_$Interval"

        # 先检查表是否存在
        $checkTableQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name='$tableName';"
        $tableExists = sqlite3.exe $DATABASE_PATH $checkTableQuery

        if (-not $tableExists) {
            Write-Log "$Symbol [$Interval] 表不存在: $tableName" "WARN"
            return $false
        }

        # 检查是否有下载的数据
        if (-not $DownloadedData.ContainsKey($Symbol) -or -not $DownloadedData[$Symbol].Success) {
            $errorMsg = if ($DownloadedData.ContainsKey($Symbol)) { $DownloadedData[$Symbol].Error } else { "未找到下载数据" }
            Write-Log "$Symbol [$Interval] 无法获取API数据: $errorMsg" "WARN"
            return $false
        }

        $apiData = $DownloadedData[$Symbol].Data
        if (-not $apiData -or $apiData.Count -eq 0) {
            Write-Log "$Symbol [$Interval] API返回空数据" "WARN"
            return $false
        }

        # 获取最新的K线时间戳（API数据中的最后一根K线）
        $latestApiKline = $apiData[-1]  # 最后一根K线
        $latestApiTimestamp = [long]$latestApiKline[0]  # open_time

        # 查询数据库中的最新K线时间戳
        $query = @"
SELECT MAX(open_time) as latest_time
FROM $tableName
"@

        $result = sqlite3.exe $DATABASE_PATH $query
        if ($LASTEXITCODE -eq 0 -and $result -and $result -ne "") {
            $dbLatestTimestamp = [long]$result

            # 比较时间戳
            $timeDiffMs = $latestApiTimestamp - $dbLatestTimestamp
            $timeDiffMinutes = [math]::Abs($timeDiffMs) / (1000 * 60)

            # 根据周期设置容忍度
            $toleranceMinutes = switch ($Interval) {
                "1m" { 2 }     # 1分钟周期容忍2分钟差异
                "5m" { 10 }    # 5分钟周期容忍10分钟差异
                "30m" { 60 }   # 30分钟周期容忍60分钟差异
                default { 5 }
            }

            if ($timeDiffMinutes -le $toleranceMinutes) {
                Write-Log "$Symbol [$Interval] 数据同步正常 (时差: $([math]::Round($timeDiffMinutes, 1))分钟)" "INFO"
                return $true
            } else {
                $apiTime = [DateTimeOffset]::FromUnixTimeMilliseconds($latestApiTimestamp).ToString("yyyy-MM-dd HH:mm:ss")
                $dbTime = [DateTimeOffset]::FromUnixTimeMilliseconds($dbLatestTimestamp).ToString("yyyy-MM-dd HH:mm:ss")
                Write-Log "$Symbol [$Interval] 数据不同步 - API最新: $apiTime, DB最新: $dbTime (差异: $([math]::Round($timeDiffMinutes, 1))分钟)" "WARN"
                return $false
            }
        } else {
            Write-Log "$Symbol [$Interval] 数据库中无数据" "WARN"
            return $false
        }
    } catch {
        Write-Log "$Symbol [$Interval] 检查异常: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# 执行完整性检查（新版本：先并发下载，再比对）
function Start-IntegrityCheck {
    Write-Log "开始数据完整性检查..." "INFO"

    if (-not (Test-Database)) {
        return
    }

    # 获取活跃交易对
    $symbols = Get-ActiveSymbols
    if ($symbols.Count -eq 0) {
        Write-Log "没有找到活跃交易对，跳过检查" "WARN"
        return
    }

    # 计算时间范围（检查最近2小时的数据以确保覆盖所有周期）
    $endTime = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $startTime = [DateTimeOffset]::UtcNow.AddHours(-2).ToUnixTimeMilliseconds()

    $totalChecks = 0
    $passedChecks = 0

    # 检查每个周期
    foreach ($interval in $CHECK_INTERVALS) {
        Write-Log "检查 $interval 周期数据..." "INFO"

        # 第一步：并发下载所有交易对的K线数据
        $downloadedData = Get-KlineDataConcurrently -Symbols $symbols -Interval $interval -StartTime $startTime -EndTime $endTime

        # 第二步：逐个比对数据库数据
        Write-Log "开始比对数据库数据..." "INFO"
        foreach ($symbol in $symbols) {
            $totalChecks++
            if (Test-SymbolIntegrityWithDownloadedData -Symbol $symbol -Interval $interval -DownloadedData $downloadedData) {
                $passedChecks++
            }
        }
    }

    $successRate = [math]::Round(($passedChecks / $totalChecks) * 100, 2)
    Write-Log "检查完成: $passedChecks/$totalChecks 通过 ($successRate%)" "INFO"

    if ($successRate -lt 95) {
        Write-Log "数据完整性较低，建议检查数据采集服务" "WARN"
    }
}

# 等待到指定秒数
function Wait-ForTargetSecond {
    param([int]$TargetSecond)
    
    do {
        $currentSecond = (Get-Date).Second
        if ($currentSecond -eq $TargetSecond) {
            return
        }
        Start-Sleep -Milliseconds 100
    } while ($true)
}

# 主循环
function Start-MainLoop {
    Write-Log "K线数据完整性检查器启动" "INFO"
    Write-Log "检查周期: $($CHECK_INTERVALS -join ', ')" "INFO"
    Write-Log "检查时间: 每分钟第 $CHECK_SECOND 秒" "INFO"
    Write-Log "按 Ctrl+C 停止程序" "INFO"
    
    # 注册Ctrl+C处理
    [Console]::TreatControlCAsInput = $false
    $null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
        Write-Log "正在关闭..." "INFO"
    }
    
    try {
        while ($true) {
            # 等待到目标秒数
            Wait-ForTargetSecond -TargetSecond $CHECK_SECOND
            
            # 执行检查
            Start-IntegrityCheck
            
            # 等待到下一分钟（避免重复检查）
            Start-Sleep -Seconds 20
        }
    } catch [System.Management.Automation.PipelineStoppedException] {
        Write-Log "程序被用户中断" "INFO"
    } catch {
        Write-Log "程序异常退出: $($_.Exception.Message)" "ERROR"
    } finally {
        Write-Log "K线数据完整性检查器已停止" "INFO"
    }
}

# 检查sqlite3命令是否可用
if (-not (Get-Command sqlite3.exe -ErrorAction SilentlyContinue)) {
    Write-Log "sqlite3.exe 命令不可用，请确保已安装SQLite" "ERROR"
    exit 1
}

# 启动主循环
Start-MainLoop
