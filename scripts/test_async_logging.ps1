# 测试异步批量日志系统
# 验证新的架构是否正常工作

Write-Host "=== 测试异步批量日志系统 ===" -ForegroundColor Green

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 清理旧的日志文件
Write-Host "清理旧日志文件..." -ForegroundColor Yellow
if (Test-Path "logs\ai_detailed.log") { Remove-Item "logs\ai_detailed.log" -Force }
if (Test-Path "logs\problem_summary.log") { Remove-Item "logs\problem_summary.log" -Force }
if (Test-Path "logs\module.log") { Remove-Item "logs\module.log" -Force }

# 启动Log MCP Server（如果没有运行）
Write-Host "检查Log MCP Server状态..." -ForegroundColor Yellow
$mcpProcess = Get-Process -Name "log_mcp_daemon" -ErrorAction SilentlyContinue
if (-not $mcpProcess) {
    Write-Host "启动Log MCP Server..." -ForegroundColor Yellow
    Start-Process -FilePath "cargo" -ArgumentList "run", "--bin", "log_mcp_daemon" -WorkingDirectory "src\Log-MCP-Server" -WindowStyle Minimized
    Start-Sleep -Seconds 3
}

# 运行K线数据服务（短时间测试）
Write-Host "启动K线数据服务进行日志测试..." -ForegroundColor Yellow
$env:RUST_LOG = "info"
$env:ENABLE_FULL_TRACING = "true"

# 启动服务并在15秒后停止
Write-Host "启动服务进程..." -ForegroundColor Yellow
$process = Start-Process -FilePath "cargo" -ArgumentList "run", "--bin", "kline_data_service" -PassThru -WindowStyle Hidden

Write-Host "等待15秒收集日志数据..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# 停止服务
Write-Host "停止服务..." -ForegroundColor Yellow
if (-not $process.HasExited) {
    $process.Kill()
    $process.WaitForExit(5000)
}

# 检查日志文件
Write-Host "`n=== 日志文件检查结果 ===" -ForegroundColor Green

if (Test-Path "logs\ai_detailed.log") {
    $detailedSize = (Get-Item "logs\ai_detailed.log").Length
    $detailedLines = (Get-Content "logs\ai_detailed.log" | Measure-Object -Line).Lines
    Write-Host "✅ ai_detailed.log: $detailedLines 行, $detailedSize 字节" -ForegroundColor Green
    
    # 显示前几行日志样本
    Write-Host "`n--- ai_detailed.log 样本 (前3行) ---" -ForegroundColor Cyan
    Get-Content "logs\ai_detailed.log" -Head 3 | ForEach-Object { Write-Host $_ -ForegroundColor White }
} else {
    Write-Host "❌ ai_detailed.log 文件不存在" -ForegroundColor Red
}

if (Test-Path "logs\problem_summary.log") {
    $problemSize = (Get-Item "logs\problem_summary.log").Length
    $problemLines = (Get-Content "logs\problem_summary.log" | Measure-Object -Line).Lines
    Write-Host "✅ problem_summary.log: $problemLines 行, $problemSize 字节" -ForegroundColor Green
} else {
    Write-Host "⚠️ problem_summary.log 文件不存在（可能没有错误日志）" -ForegroundColor Yellow
}

if (Test-Path "logs\module.log") {
    $moduleSize = (Get-Item "logs\module.log").Length
    $moduleLines = (Get-Content "logs\module.log" | Measure-Object -Line).Lines
    Write-Host "✅ module.log: $moduleLines 行, $moduleSize 字节" -ForegroundColor Green
} else {
    Write-Host "⚠️ module.log 文件不存在（可能没有模块日志）" -ForegroundColor Yellow
}

# 检查批量处理效果
if (Test-Path "logs\ai_detailed.log") {
    Write-Host "`n=== 批量处理效果分析 ===" -ForegroundColor Green
    
    # 统计JSON日志条数
    $jsonCount = (Get-Content "logs\ai_detailed.log" | Where-Object { $_.Trim() -ne "" } | Measure-Object -Line).Lines
    Write-Host "总JSON日志条数: $jsonCount" -ForegroundColor White
    
    # 检查是否包含批量处理的标志
    $content = Get-Content "logs\ai_detailed.log" -Raw
    if ($content -match '"type":"span"') {
        Write-Host "✅ 检测到结构化Span日志" -ForegroundColor Green
    }
    
    if ($content -match '"trace_id"') {
        Write-Host "✅ 检测到Trace ID字段" -ForegroundColor Green
    }
    
    if ($content -match '"duration_ms"') {
        Write-Host "✅ 检测到持续时间字段" -ForegroundColor Green
    }
}

Write-Host "`n=== 测试完成 ===" -ForegroundColor Green
Write-Host "新的异步批量日志系统测试完成！" -ForegroundColor Yellow
Write-Host "主要改进：" -ForegroundColor White
Write-Host "  - 主线程非阻塞发送日志到通道" -ForegroundColor White
Write-Host "  - 后台工作线程负责批量处理和I/O" -ForegroundColor White
Write-Host "  - 使用crossbeam_channel实现高性能通信" -ForegroundColor White
Write-Host "  - 批量大小: 1000条，超时: 5秒" -ForegroundColor White
