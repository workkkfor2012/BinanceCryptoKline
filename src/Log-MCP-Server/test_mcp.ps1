# 测试Log MCP Server的脚本

Write-Host "=== 测试Log MCP Server ===" -ForegroundColor Green

# 测试查询接口
Write-Host "1. 测试查询接口 (空查询)" -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/query" -Method POST -ContentType "application/json" -Body "{}"
    Write-Host "状态码: $($response.StatusCode)" -ForegroundColor Green
    Write-Host "响应内容: $($response.Content)" -ForegroundColor Cyan
} catch {
    Write-Host "错误: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""

# 测试清空接口
Write-Host "2. 测试清空接口" -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/clear" -Method POST
    Write-Host "状态码: $($response.StatusCode)" -ForegroundColor Green
    Write-Host "响应内容: $($response.Content)" -ForegroundColor Cyan
} catch {
    Write-Host "错误: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""

# 测试带参数的查询
Write-Host "3. 测试带参数的查询" -ForegroundColor Yellow
$queryBody = @{
    target = "test"
    level = "INFO"
    limit = 10
} | ConvertTo-Json

try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:9001/query" -Method POST -ContentType "application/json" -Body $queryBody
    Write-Host "状态码: $($response.StatusCode)" -ForegroundColor Green
    Write-Host "响应内容: $($response.Content)" -ForegroundColor Cyan
} catch {
    Write-Host "错误: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== 测试完成 ===" -ForegroundColor Green
