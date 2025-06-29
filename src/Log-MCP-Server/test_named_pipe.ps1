# 测试命名管道功能

Write-Host "=== 测试命名管道日志发送 ===" -ForegroundColor Green

# 读取配置文件获取管道名称
$configContent = Get-Content "config.toml" -Raw
$pipeName = ""
if ($configContent -match 'pipe_name\s*=\s*"([^"]+)"') {
    $pipeName = $matches[1]
    Write-Host "从配置文件读取到管道名称: $pipeName" -ForegroundColor Cyan
} else {
    Write-Host "无法从配置文件读取管道名称，使用默认值" -ForegroundColor Yellow
    $pipeName = "kline_mcp_log_pipe"
}

$fullPipeName = "\\.\pipe\$pipeName"
Write-Host "完整管道路径: $fullPipeName" -ForegroundColor Cyan

# 创建测试日志数据
$testSpan = @{
    type = "span"
    id = "span_pipe_test"
    trace_id = "trace_pipe_test"
    parent_id = $null
    name = "named_pipe_test_function"
    target = "named_pipe_module"
    level = "INFO"
    timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    duration_ms = 456.78
    attributes = @{
        transport = "named_pipe"
        test_id = "pipe_test_001"
    }
    events = @()
} | ConvertTo-Json -Depth 10 -Compress

Write-Host "准备发送的日志数据:" -ForegroundColor Yellow
Write-Host $testSpan -ForegroundColor Cyan

# 发送日志到命名管道
try {
    Write-Host "连接到命名管道: $fullPipeName" -ForegroundColor Yellow
    
    # 使用.NET的NamedPipeClientStream
    $pipeClient = New-Object System.IO.Pipes.NamedPipeClientStream(".", $pipeName, [System.IO.Pipes.PipeDirection]::Out)
    $pipeClient.Connect(5000)  # 5秒超时
    
    $writer = New-Object System.IO.StreamWriter($pipeClient)
    $writer.WriteLine($testSpan)
    $writer.Flush()
    
    Write-Host "日志通过命名管道发送成功!" -ForegroundColor Green
    
    $writer.Close()
    $pipeClient.Close()
    
} catch {
    Write-Host "发送日志时出错: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""

# 等待一下让服务器处理
Start-Sleep -Seconds 1

# 查询刚才发送的日志
Write-Host "查询刚才通过命名管道发送的日志..." -ForegroundColor Yellow
try {
    $queryBody = @{
        trace_id = "trace_pipe_test"
    } | ConvertTo-Json
    
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/query" -Method POST -ContentType "application/json" -Body $queryBody
    $results = $response.Content | ConvertFrom-Json
    
    Write-Host "查询结果:" -ForegroundColor Green
    if ($results.Count -gt 0) {
        Write-Host "  - ID: $($results[0].id)" -ForegroundColor Cyan
        Write-Host "  - Target: $($results[0].target)" -ForegroundColor Cyan
        Write-Host "  - Transport: $($results[0].attributes.transport)" -ForegroundColor Cyan
        Write-Host "✅ 命名管道功能正常工作！" -ForegroundColor Green
    } else {
        Write-Host "❌ 未找到日志数据" -ForegroundColor Red
    }
} catch {
    Write-Host "查询时出错: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== 命名管道测试完成 ===" -ForegroundColor Green
