# 测试日志发送脚本
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$baseUrl = "http://localhost:8081/api/log"

# 发送一些测试日志
$testLogs = @(
    @{
        timestamp = "2024-01-01T12:00:00Z"
        level = "INFO"
        target = "test_module_1"
        message = "测试消息1 - 这是第一条日志"
    },
    @{
        timestamp = "2024-01-01T12:01:00Z"
        level = "WARN"
        target = "test_module_1"
        message = "测试消息2 - 这是一个警告"
    },
    @{
        timestamp = "2024-01-01T12:02:00Z"
        level = "ERROR"
        target = "test_module_2"
        message = "测试消息3 - 这是一个错误"
    },
    @{
        timestamp = "2024-01-01T12:03:00Z"
        level = "INFO"
        target = "test_module_2"
        message = "测试消息4 - 另一个模块的信息"
    },
    @{
        timestamp = "2024-01-01T12:04:00Z"
        level = "DEBUG"
        target = "test_module_3"
        message = "测试消息5 - 调试信息"
    }
)

Write-Host "开始发送测试日志到 $baseUrl" -ForegroundColor Green

foreach ($log in $testLogs) {
    $json = $log | ConvertTo-Json -Compress
    Write-Host "发送: $json" -ForegroundColor Yellow
    
    try {
        $response = Invoke-RestMethod -Uri $baseUrl -Method POST -Body $json -ContentType "application/json"
        Write-Host "✅ 成功发送" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ 发送失败: $($_.Exception.Message)" -ForegroundColor Red
    }
    
    Start-Sleep -Milliseconds 500
}

Write-Host "测试日志发送完成！" -ForegroundColor Green
Write-Host "请打开浏览器查看: http://localhost:8081" -ForegroundColor Cyan
