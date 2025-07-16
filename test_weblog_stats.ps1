# 测试WebLog统计输出功能
#
# 功能：启动WebLog并发送测试日志，验证每10秒统计输出功能
# 使用方法：.\test_weblog_stats.ps1

Write-Host "🧪 测试WebLog统计输出功能" -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Green

# 检查当前目录是否正确
if (-not (Test-Path "src\weblog")) {
    Write-Host "❌ 错误：请在项目根目录下运行此脚本" -ForegroundColor Red
    exit 1
}

# 启动WebLog（后台运行）
Write-Host "🚀 启动WebLog系统..." -ForegroundColor Green
$weblogJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    cargo run --manifest-path src\weblog\Cargo.toml --bin weblog -- --pipe-name '\\.\pipe\weblog_test_pipe'
}

# 等待WebLog启动
Start-Sleep -Seconds 3
Write-Host "✅ WebLog已启动" -ForegroundColor Green

# 发送测试日志
Write-Host "📡 开始发送测试日志..." -ForegroundColor Cyan
Write-Host "将发送100条日志，观察统计输出..." -ForegroundColor Yellow

try {
    # 连接到命名管道并发送测试日志
    $pipeClient = New-Object System.IO.Pipes.NamedPipeClientStream(".", "weblog_test_pipe", [System.IO.Pipes.PipeDirection]::Out)
    $pipeClient.Connect(5000)  # 5秒超时
    
    $writer = New-Object System.IO.StreamWriter($pipeClient)
    
    # 发送100条测试日志
    for ($i = 1; $i -le 100; $i++) {
        $timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        $logEntry = @{
            timestamp = $timestamp
            level = "INFO"
            target = "test"
            message = "测试日志消息 #$i"
            log_type = "module"
        } | ConvertTo-Json -Compress
        
        $writer.WriteLine($logEntry)
        $writer.Flush()
        
        if ($i % 10 -eq 0) {
            Write-Host "已发送 $i 条日志..." -ForegroundColor Gray
        }
        
        Start-Sleep -Milliseconds 100  # 每100ms发送一条
    }
    
    Write-Host "✅ 已发送100条测试日志" -ForegroundColor Green
    Write-Host "📊 观察WebLog输出，应该看到每10秒的统计信息" -ForegroundColor Yellow
    Write-Host "等待20秒以观察统计输出..." -ForegroundColor Cyan
    
    Start-Sleep -Seconds 20
    
} catch {
    Write-Host "❌ 发送日志失败: $_" -ForegroundColor Red
} finally {
    if ($writer) { $writer.Close() }
    if ($pipeClient) { $pipeClient.Close() }
}

# 停止WebLog
Write-Host "🛑 停止WebLog系统..." -ForegroundColor Yellow
Stop-Job $weblogJob -Force
Remove-Job $weblogJob -Force

Write-Host "✅ 测试完成" -ForegroundColor Green
