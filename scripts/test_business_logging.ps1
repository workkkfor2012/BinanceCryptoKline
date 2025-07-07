# 测试业务日志埋点脚本
# 用于验证K线数据下载服务的业务日志是否正确记录

Write-Host "=== 测试业务日志埋点 ===" -ForegroundColor Green

# 1. 清理旧的日志文件
Write-Host "清理旧的日志文件..." -ForegroundColor Yellow
if (Test-Path "logs\low_freq.log") {
    Remove-Item "logs\low_freq.log" -Force
    Write-Host "已删除 logs\low_freq.log" -ForegroundColor Gray
}
if (Test-Path "logs\problem_summary.log") {
    Remove-Item "logs\problem_summary.log" -Force
    Write-Host "已删除 logs\problem_summary.log" -ForegroundColor Gray
}
if (Test-Path "logs\ai_detailed.log") {
    Remove-Item "logs\ai_detailed.log" -Force
    Write-Host "已删除 logs\ai_detailed.log" -ForegroundColor Gray
}

# 2. 启动Log MCP Server（后台运行）
Write-Host "启动Log MCP Server..." -ForegroundColor Yellow
$logMcpProcess = Start-Process -FilePath "cargo" -ArgumentList "run --bin log-mcp-server" -WorkingDirectory "src\Log-MCP-Server" -WindowStyle Hidden -PassThru
Start-Sleep -Seconds 3

# 3. 运行K线数据服务（测试模式，短时间运行）
Write-Host "运行K线数据服务（测试模式）..." -ForegroundColor Yellow
$env:RUST_LOG = "info"
$klineProcess = Start-Process -FilePath "cargo" -ArgumentList "run --bin kline_data_service" -NoNewWindow -PassThru

# 等待服务运行一段时间
Write-Host "等待服务运行30秒..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# 4. 停止服务
Write-Host "停止服务..." -ForegroundColor Yellow
if (!$klineProcess.HasExited) {
    $klineProcess.Kill()
    $klineProcess.WaitForExit()
}

# 停止Log MCP Server
if (!$logMcpProcess.HasExited) {
    $logMcpProcess.Kill()
    $logMcpProcess.WaitForExit()
}

# 5. 检查日志文件
Write-Host "检查生成的日志文件..." -ForegroundColor Yellow

if (Test-Path "logs\low_freq.log") {
    Write-Host "✓ low_freq.log 文件已生成" -ForegroundColor Green
    $lowFreqContent = Get-Content "logs\low_freq.log" -Raw
    
    # 检查是否包含预期的业务日志事件
    $hasStartEvent = $lowFreqContent -match '"message":"K线数据补齐任务启动"'
    $hasProgressEvent = $lowFreqContent -match '"message":"K线数据补齐进度摘要"'
    $hasEndEvent = $lowFreqContent -match '"message":"K线数据补齐任务完成"'
    
    Write-Host "业务日志事件检查:" -ForegroundColor Cyan
    Write-Host "  Start事件: $(if($hasStartEvent){'✓'}else{'✗'})" -ForegroundColor $(if($hasStartEvent){'Green'}else{'Red'})
    Write-Host "  Progress事件: $(if($hasProgressEvent){'✓'}else{'✗'})" -ForegroundColor $(if($hasProgressEvent){'Green'}else{'Red'})
    Write-Host "  End事件: $(if($hasEndEvent){'✓'}else{'✗'})" -ForegroundColor $(if($hasEndEvent){'Green'}else{'Red'})
    
    # 显示日志内容摘要
    Write-Host "`n=== low_freq.log 内容摘要 ===" -ForegroundColor Cyan
    $lines = Get-Content "logs\low_freq.log"
    foreach ($line in $lines) {
        $json = $line | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($json -and $json.message) {
            Write-Host "[$($json.timestamp)] $($json.message)" -ForegroundColor White
        }
    }
} else {
    Write-Host "✗ low_freq.log 文件未生成" -ForegroundColor Red
}

Write-Host "`n=== 测试完成 ===" -ForegroundColor Green
