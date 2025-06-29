# 测试向Log MCP Server发送日志的脚本

Write-Host "=== 测试日志发送 ===" -ForegroundColor Green

# 创建测试日志数据
$testSpan = @{
    type = "span"
    id = "span_001"
    trace_id = "trace_001"
    parent_id = $null
    name = "test_function"
    target = "test_module"
    level = "INFO"
    timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    duration_ms = 123.45
    attributes = @{
        user_id = "test_user"
        request_id = "req_123"
    }
    events = @()
} | ConvertTo-Json -Depth 10 -Compress

Write-Host "准备发送的日志数据:" -ForegroundColor Yellow
Write-Host $testSpan -ForegroundColor Cyan

# 发送日志到TCP端口9000
try {
    Write-Host "连接到 127.0.0.1:9000..." -ForegroundColor Yellow
    
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    $tcpClient.Connect("127.0.0.1", 9000)
    
    $stream = $tcpClient.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    
    # 发送JSON数据（每行一个JSON对象）
    $writer.WriteLine($testSpan)
    $writer.Flush()
    
    Write-Host "日志发送成功!" -ForegroundColor Green
    
    # 关闭连接
    $writer.Close()
    $stream.Close()
    $tcpClient.Close()
    
} catch {
    Write-Host "发送日志时出错: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""

# 等待一下让服务器处理
Start-Sleep -Seconds 1

# 查询刚才发送的日志
Write-Host "查询刚才发送的日志..." -ForegroundColor Yellow
try {
    $queryBody = @{
        trace_id = "trace_001"
    } | ConvertTo-Json
    
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/query" -Method POST -ContentType "application/json" -Body $queryBody
    Write-Host "查询结果:" -ForegroundColor Green
    Write-Host $response.Content -ForegroundColor Cyan
} catch {
    Write-Host "查询时出错: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== 测试完成 ===" -ForegroundColor Green
