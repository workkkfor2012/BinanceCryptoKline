# 使用内置代理的示例脚本
# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "使用内置代理进行网络请求示例..." -ForegroundColor Green
Write-Host ""

# 代理配置
$proxyUri = "socks5://127.0.0.1:78900"

# 测试URL
$testUrl = "https://httpbin.org/ip"

Write-Host "测试URL: $testUrl" -ForegroundColor Yellow
Write-Host "代理: $proxyUri" -ForegroundColor Yellow
Write-Host ""

try {
    # 使用curl测试代理
    Write-Host "使用curl测试代理连接..." -ForegroundColor Cyan
    $result = curl.exe --proxy $proxyUri --connect-timeout 10 --max-time 30 $testUrl 2>$null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ 代理连接成功!" -ForegroundColor Green
        Write-Host "响应内容:" -ForegroundColor White
        Write-Host $result -ForegroundColor Gray
    } else {
        Write-Host "✗ 代理连接失败" -ForegroundColor Red
        Write-Host "请检查代理服务是否正常运行" -ForegroundColor Yellow
    }
} catch {
    Write-Host "✗ 测试失败: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "提示: 你可以在任何支持SOCKS5代理的应用中使用 127.0.0.1:78900" -ForegroundColor Cyan