# 命名管道日志服务器
# 创建命名管道服务器，接收K线聚合服务的日志并转发到Web Observer

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$pipeName = "\\.\pipe\kline_log_pipe"

Write-Host "🚀 启动命名管道日志服务器" -ForegroundColor Green
Write-Host "📡 管道名称: $pipeName" -ForegroundColor Cyan

# 检查是否要启动Web仪表板
$startWeb = $true
if ($args.Length -gt 0 -and $args[0] -eq "--no-web") {
    $startWeb = $false
    Write-Host "💡 仅启动日志服务器，不启动Web仪表板" -ForegroundColor Yellow
} else {
    Write-Host "💡 将同时启动Web仪表板" -ForegroundColor Cyan
}

# 启动Web Observer（如果需要）
if ($startWeb) {
    Write-Host "🌐 启动WebLog系统..." -ForegroundColor Yellow
    $env:LOG_TRANSPORT = "named_pipe"
    $env:PIPE_NAME = $pipeName
    $env:RUST_LOG = "info"

    $webProcess = Start-Process powershell -ArgumentList "-Command", "`$env:LOG_TRANSPORT='named_pipe'; `$env:PIPE_NAME='$pipeName'; `$env:RUST_LOG='info'; cd src\weblog; cargo run --bin weblog -- --pipe-name '$pipeName'" -WindowStyle Hidden -PassThru

    # 等待WebLog系统启动
    Start-Sleep -Seconds 3
    Write-Host "✅ WebLog系统已启动 (PID: $($webProcess.Id))" -ForegroundColor Green
}

# 创建命名管道服务器
Write-Host "📡 创建命名管道服务器..." -ForegroundColor Yellow

try {
    # 使用.NET创建命名管道服务器
    Add-Type -AssemblyName System.Core
    $pipeServer = New-Object System.IO.Pipes.NamedPipeServerStream("kline_log_pipe", [System.IO.Pipes.PipeDirection]::In)
    
    Write-Host "✅ 命名管道服务器已创建，等待客户端连接..." -ForegroundColor Green
    Write-Host "💡 按 Ctrl+C 停止服务器" -ForegroundColor Yellow
    
    # 等待客户端连接
    $pipeServer.WaitForConnection()
    Write-Host "🔗 客户端已连接" -ForegroundColor Green
    
    # 读取数据
    $reader = New-Object System.IO.StreamReader($pipeServer)
    $lineCount = 0
    
    while ($true) {
        try {
            $line = $reader.ReadLine()
            if ($line -eq $null) {
                Write-Host "📡 客户端断开连接" -ForegroundColor Yellow
                break
            }
            
            $lineCount++
            Write-Host "📝 [$lineCount] $line" -ForegroundColor White

            # 注意：HTTP API已禁用，WebLog现在只支持命名管道输入
            # 转发日志到Web Observer（已禁用）
            if ($false) {  # 禁用HTTP API转发
                try {
                    $body = @{
                        log_line = $line
                    } | ConvertTo-Json

                    Invoke-RestMethod -Uri "http://localhost:8080/api/log" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 1 | Out-Null
                }
                catch {
                    # 忽略转发错误，避免影响主要功能
                }
            }

            # 每100行显示统计
            if ($lineCount % 100 -eq 0) {
                Write-Host "📊 已接收 $lineCount 行日志" -ForegroundColor Cyan
            }
        }
        catch {
            Write-Host "❌ 读取错误: $($_.Exception.Message)" -ForegroundColor Red
            break
        }
    }
}
catch {
    Write-Host "❌ 管道服务器错误: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    if ($pipeServer) {
        $pipeServer.Close()
        $pipeServer.Dispose()
        Write-Host "🔒 管道服务器已关闭" -ForegroundColor Yellow
    }
    
    # 清理Web Observer进程
    if ($startWeb -and $webProcess -and !$webProcess.HasExited) {
        Write-Host "🛑 停止Web Observer..." -ForegroundColor Yellow
        $webProcess.Kill()
        $webProcess.WaitForExit(3000)
        Write-Host "✅ Web Observer已停止" -ForegroundColor Green
    }
}

Write-Host "✅ 日志服务器已停止" -ForegroundColor Green
