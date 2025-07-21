# 性能对比脚本
# 用于对比标准版本和SIMD版本的性能差异

param(
    [int]$TestDurationMinutes = 10,
    [string]$LogLevel = "info"
)

Write-Host "📊 K线聚合服务性能对比测试" -ForegroundColor Green
Write-Host "测试时长: $TestDurationMinutes 分钟" -ForegroundColor Cyan
Write-Host "日志级别: $LogLevel" -ForegroundColor Cyan
Write-Host ""

# 创建结果目录
$ResultDir = "performance_results\$(Get-Date -Format 'yyyy-MM-dd_HH-mm-ss')"
New-Item -ItemType Directory -Path $ResultDir -Force | Out-Null

Write-Host "📁 结果保存目录: $ResultDir" -ForegroundColor Gray
Write-Host ""

# 测试标准版本
Write-Host "🔄 第一阶段：测试标准版本" -ForegroundColor Blue
Write-Host "⏱️  运行时长: $TestDurationMinutes 分钟" -ForegroundColor Yellow

$StandardLogFile = "$ResultDir\standard_performance.log"
$StandardJob = Start-Job -ScriptBlock {
    param($Duration, $LogLevel, $LogFile)
    
    $env:RUST_LOG = $LogLevel
    $env:HTTPS_PROXY = "http://127.0.0.1:1080"
    $env:HTTP_PROXY = "http://127.0.0.1:1080"
    
    # 编译标准版本
    cargo build --release --bin klagg_sub_threads
    
    # 运行并记录日志
    $Process = Start-Process -FilePath "cargo" -ArgumentList "run", "--release", "--bin", "klagg_sub_threads" -PassThru -RedirectStandardOutput $LogFile -RedirectStandardError "$LogFile.err"
    
    # 等待指定时间
    Start-Sleep -Seconds ($Duration * 60)
    
    # 停止进程
    if (!$Process.HasExited) {
        $Process.Kill()
        $Process.WaitForExit()
    }
    
    return "Standard version test completed"
} -ArgumentList $TestDurationMinutes, $LogLevel, $StandardLogFile

Write-Host "⏳ 标准版本测试进行中..." -ForegroundColor Yellow
$StandardResult = Receive-Job -Job $StandardJob -Wait
Remove-Job -Job $StandardJob

Write-Host "✅ 标准版本测试完成" -ForegroundColor Green
Write-Host ""

# 等待一段时间让系统稳定
Write-Host "⏸️  等待系统稳定..." -ForegroundColor Gray
Start-Sleep -Seconds 30

# 测试SIMD版本
Write-Host "🔄 第二阶段：测试SIMD版本" -ForegroundColor Blue
Write-Host "⏱️  运行时长: $TestDurationMinutes 分钟" -ForegroundColor Yellow

$SimdLogFile = "$ResultDir\simd_performance.log"
$SimdJob = Start-Job -ScriptBlock {
    param($Duration, $LogLevel, $LogFile)
    
    $env:RUST_LOG = $LogLevel
    $env:HTTPS_PROXY = "http://127.0.0.1:1080"
    $env:HTTP_PROXY = "http://127.0.0.1:1080"
    
    # 编译SIMD版本
    cargo build --release --bin klagg_sub_threads --features simd
    
    # 运行并记录日志
    $Process = Start-Process -FilePath "cargo" -ArgumentList "run", "--release", "--bin", "klagg_sub_threads", "--features", "simd" -PassThru -RedirectStandardOutput $LogFile -RedirectStandardError "$LogFile.err"
    
    # 等待指定时间
    Start-Sleep -Seconds ($Duration * 60)
    
    # 停止进程
    if (!$Process.HasExited) {
        $Process.Kill()
        $Process.WaitForExit()
    }
    
    return "SIMD version test completed"
} -ArgumentList $TestDurationMinutes, $LogLevel, $SimdLogFile

Write-Host "⏳ SIMD版本测试进行中..." -ForegroundColor Yellow
$SimdResult = Receive-Job -Job $SimdJob -Wait
Remove-Job -Job $SimdJob

Write-Host "✅ SIMD版本测试完成" -ForegroundColor Green
Write-Host ""

# 生成对比报告
Write-Host "📋 生成性能对比报告..." -ForegroundColor Blue

$ReportFile = "$ResultDir\performance_comparison_report.md"
$Report = @"
# K线聚合服务性能对比报告

## 测试信息
- 测试时间: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
- 测试时长: $TestDurationMinutes 分钟
- 日志级别: $LogLevel

## 测试结果

### 标准版本
- 日志文件: standard_performance.log
- 实现方式: 标准线性扫描

### SIMD版本  
- 日志文件: simd_performance.log
- 实现方式: SIMD批量比较 + 掩码跳过

## 性能分析说明

请查看日志文件中 target='性能分析' 且 log_type='performance_summary' 的条目。

关键指标：
- avg_duration_micros: 平均处理时间（微秒）
- calls_per_second: 每秒调用次数
- klines_scanned_per_second: 每秒扫描K线数
- processing_efficiency: 处理效率百分比

## 使用方法

1. 使用日志分析工具（如jq）提取性能数据：
   ```bash
   # 提取标准版本性能数据
   grep "performance_summary" standard_performance.log | jq .
   
   # 提取SIMD版本性能数据  
   grep "performance_summary" simd_performance.log | jq .
   ```

2. 对比关键指标，选择最优实现版本。

"@

$Report | Out-File -FilePath $ReportFile -Encoding UTF8

Write-Host "📄 对比报告已生成: $ReportFile" -ForegroundColor Green
Write-Host "📊 日志文件位置:" -ForegroundColor Cyan
Write-Host "   标准版本: $StandardLogFile" -ForegroundColor Gray
Write-Host "   SIMD版本: $SimdLogFile" -ForegroundColor Gray
Write-Host ""
Write-Host "🎯 下一步：分析日志中的 performance_summary 条目进行性能对比" -ForegroundColor Yellow
