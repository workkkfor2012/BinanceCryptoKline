# 测试多会话自动清理功能

Write-Host "=== 测试多会话自动清理功能 ===" -ForegroundColor Green

# 会话1：发送第一个span
Write-Host "1. 会话1：发送span_session1" -ForegroundColor Yellow
$session1Span = @{
    type = "span"
    id = "span_session1"
    trace_id = "trace_session1"
    parent_id = $null
    name = "session1_function"
    target = "session1_module"
    level = "INFO"
    timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    duration_ms = 100.0
    attributes = @{
        session = "session1"
    }
    events = @()
} | ConvertTo-Json -Depth 10 -Compress

try {
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    $tcpClient.Connect("127.0.0.1", 9000)
    $stream = $tcpClient.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $writer.WriteLine($session1Span)
    $writer.Flush()
    $writer.Close()
    $stream.Close()
    $tcpClient.Close()
    Write-Host "会话1数据发送成功" -ForegroundColor Green
} catch {
    Write-Host "会话1发送失败: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 1

# 查询会话1的数据
Write-Host "2. 查询会话1数据" -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/query" -Method POST -ContentType "application/json" -Body "{}"
    $session1Data = $response.Content | ConvertFrom-Json
    Write-Host "会话1查询结果: $($session1Data.Count) 条记录" -ForegroundColor Cyan
    if ($session1Data.Count -gt 0) {
        Write-Host "  - ID: $($session1Data[0].id)" -ForegroundColor Cyan
    }
} catch {
    Write-Host "查询会话1失败: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""

# 会话2：发送第二个span（应该自动清空会话1的数据）
Write-Host "3. 会话2：发送span_session2（应该自动清空会话1数据）" -ForegroundColor Yellow
$session2Span = @{
    type = "span"
    id = "span_session2"
    trace_id = "trace_session2"
    parent_id = $null
    name = "session2_function"
    target = "session2_module"
    level = "ERROR"
    timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    duration_ms = 200.0
    attributes = @{
        session = "session2"
    }
    events = @()
} | ConvertTo-Json -Depth 10 -Compress

try {
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    $tcpClient.Connect("127.0.0.1", 9000)
    $stream = $tcpClient.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $writer.WriteLine($session2Span)
    $writer.Flush()
    $writer.Close()
    $stream.Close()
    $tcpClient.Close()
    Write-Host "会话2数据发送成功" -ForegroundColor Green
} catch {
    Write-Host "会话2发送失败: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 1

# 查询会话2的数据（应该只有会话2的数据，会话1的数据应该被清空）
Write-Host "4. 查询当前数据（应该只有会话2的数据）" -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/query" -Method POST -ContentType "application/json" -Body "{}"
    $currentData = $response.Content | ConvertFrom-Json
    Write-Host "当前查询结果: $($currentData.Count) 条记录" -ForegroundColor Cyan
    if ($currentData.Count -gt 0) {
        Write-Host "  - ID: $($currentData[0].id)" -ForegroundColor Cyan
        Write-Host "  - Level: $($currentData[0].level)" -ForegroundColor Cyan
        if ($currentData[0].id -eq "span_session2") {
            Write-Host "✅ 自动清理功能正常工作！" -ForegroundColor Green
        } else {
            Write-Host "❌ 自动清理功能可能有问题" -ForegroundColor Red
        }
    }
} catch {
    Write-Host "查询当前数据失败: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== 多会话测试完成 ===" -ForegroundColor Green
