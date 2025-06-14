# 简单的命名管道启动方案（无需修改Rust代码）
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001 | Out-Null

Write-Host "🚀 启动K线系统（一键启动方案）" -ForegroundColor Green
Write-Host "💡 自动启动WebLog服务和K线聚合系统" -ForegroundColor Cyan

# 清理现有资源
function Clear-AllResources {
    Write-Host "🔍 检查并清理现有的进程和管道..." -ForegroundColor Cyan

    # 查找并终止所有K线系统相关进程（包括weblog和cargo）
    $allExecutables = @(
        "kline_aggregate_service",
        "kline_data_service",
        "kline_server",
        "weblog",
        "cargo"
    )

    $foundProcesses = @()
    foreach ($execName in $allExecutables) {
        $processes = Get-Process -Name $execName -ErrorAction SilentlyContinue
        if ($processes) {
            $foundProcesses += $processes
        }
    }

    if ($foundProcesses.Count -gt 0) {
        Write-Host "🔄 发现 $($foundProcesses.Count) 个相关进程，正在终止..." -ForegroundColor Yellow
        $foundProcesses | ForEach-Object {
            try {
                $_.Kill()
                Write-Host "✅ 已终止进程: $($_.ProcessName) PID: $($_.Id)" -ForegroundColor Green
            }
            catch {
                Write-Host "⚠️ 无法终止进程: $($_.ProcessName) PID: $($_.Id)" -ForegroundColor Red
            }
        }
        Start-Sleep -Seconds 3
    } else {
        Write-Host "✅ 未发现相关进程" -ForegroundColor Green
    }

    Write-Host "✅ 资源清理完成" -ForegroundColor Green
}

Clear-AllResources

# 简化的一键启动脚本 - 只启动WebLog和K线系统

# 检查WebLog是否在运行
function Test-WebLog {
    # 检查端口
    try {
        $portCheck = netstat -ano | findstr ":8080" | findstr "LISTENING"
        if ($portCheck -ne $null -and $portCheck.Length -gt 0) {
            # 如果端口在监听，进行API检查
            try {
                $response = Invoke-WebRequest -Uri "http://localhost:8080/api/status" -Method GET -TimeoutSec 1
                return $response.StatusCode -eq 200
            }
            catch {
                # API未响应，但端口在监听，认为正在启动中，返回true
                return $true
            }
        }
    }
    catch {
        return $false
    }
    return $false
}

# 启动WebLog服务（如果需要）
if (-not (Test-WebLog)) {
    Write-Host "🌐 启动WebLog服务..." -ForegroundColor Yellow

    # 启动WebLog系统（直接连接架构）
    $webLogProcess = Start-Process powershell -ArgumentList "-Command", "`$env:LOG_TRANSPORT='named_pipe'; `$env:PIPE_NAME='\\.\pipe\kline_log_pipe'; `$env:RUST_LOG='trace'; cd src\weblog; cargo run --bin weblog -- --pipe-name '\\.\pipe\kline_log_pipe'" -WindowStyle Hidden -PassThru

    # 等待WebLog启动
    $maxWait = 15
    $waited = 0
    $webReady = $false

    while ($waited -lt $maxWait -and -not $webReady) {
        Start-Sleep -Seconds 1
        $waited++

        # 检查服务状态
        $webReady = Test-WebObserver

        Write-Host "⏳ 等待WebLog启动... ($waited/$maxWait)" -ForegroundColor Yellow

        # 每5秒显示详细状态
        if ($waited % 5 -eq 0) {
            $webStatus = if ($webReady) { "✅" } else { "❌" }
            Write-Host "   状态检查: WebLog $webStatus" -ForegroundColor Cyan
        }

        # 如果服务准备好了，提前退出
        if ($webReady) {
            Write-Host "✅ WebLog服务已准备就绪" -ForegroundColor Green
            break
        }
    }

    if ($webReady) {
        Write-Host "✅ WebLog服务已启动" -ForegroundColor Green
    } else {
        Write-Host "⚠️ WebLog启动可能失败，但继续启动K线系统" -ForegroundColor Yellow
        Write-Host "💡 Web页面可能无法访问，请检查 http://localhost:8080" -ForegroundColor Cyan
    }
} else {
    Write-Host "✅ WebLog服务已在运行" -ForegroundColor Green
}

# 启动K线系统
Write-Host "📊 启动K线系统..." -ForegroundColor Yellow

# 启动K线聚合程序 - 直接使用cargo run启动
Write-Host "🔧 设置环境变量..." -ForegroundColor Cyan
$env:PIPE_NAME = "\\.\pipe\kline_log_pipe"
$env:LOG_TRANSPORT = "named_pipe"
$env:RUST_LOG = "trace"

Write-Host "🚀 启动K线聚合服务..." -ForegroundColor Yellow
$global:klineProcess = Start-Process powershell -ArgumentList "-Command", "`$env:PIPE_NAME='\\.\pipe\kline_log_pipe'; `$env:LOG_TRANSPORT='named_pipe'; `$env:RUST_LOG='trace'; cargo run --bin kline_aggregate_service" -WindowStyle Hidden -PassThru

Write-Host "✅ 启动完成" -ForegroundColor Green
Write-Host ""
Write-Host "🎯 系统运行状态:" -ForegroundColor Cyan
Write-Host "  🌐 WebLog服务: 运行中 (Web仪表板)" -ForegroundColor Green
Write-Host "  📊 K线聚合服务: 运行中" -ForegroundColor Green
Write-Host "  🌐 Web仪表板: http://localhost:8080" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 按 Ctrl+C 停止所有服务并退出" -ForegroundColor Yellow
Write-Host "🔗 访问 http://localhost:8080 查看模块监控" -ForegroundColor Cyan
Write-Host ""

# 定义清理函数
function Stop-KlineSystem {
    Write-Host ""
    Write-Host "🛑 正在停止K线系统..." -ForegroundColor Yellow

    # 停止K线聚合进程
    if ($global:klineProcess -and !$global:klineProcess.HasExited) {
        Write-Host "🔄 停止K线聚合服务..." -ForegroundColor Yellow
        try {
            $global:klineProcess.Kill()
            $global:klineProcess.WaitForExit(3000)
        }
        catch {
            Write-Host "⚠️ 强制停止K线聚合进程失败" -ForegroundColor Yellow
        }
    }

    # 停止所有K线系统相关进程
    Write-Host "🔄 清理K线系统进程..." -ForegroundColor Yellow
    $klineExecutables = @(
        "kline_aggregate_service",
        "kline_data_service",
        "kline_server",
        "web_observer",
        "weblog"
    )

    $stoppedCount = 0
    foreach ($execName in $klineExecutables) {
        $processes = Get-Process -Name $execName -ErrorAction SilentlyContinue
        if ($processes) {
            $processes | ForEach-Object {
                try {
                    $_.Kill()
                    $_.WaitForExit(2000)
                    Write-Host "✅ 已停止: $($_.ProcessName) (PID: $($_.Id))" -ForegroundColor Green
                    $stoppedCount++
                }
                catch {
                    Write-Host "⚠️ 无法停止: $($_.ProcessName) (PID: $($_.Id))" -ForegroundColor Yellow
                }
            }
        }
    }

    if ($stoppedCount -gt 0) {
        Write-Host "✅ 已停止 $stoppedCount 个进程" -ForegroundColor Green
    } else {
        Write-Host "ℹ️ 未发现需要停止的进程" -ForegroundColor Cyan
    }

    Write-Host "✅ K线系统已停止" -ForegroundColor Green
}

# 注册Ctrl+C处理程序
$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
    Stop-KlineSystem
}

try {
    # 等待用户按Ctrl+C或进程退出
    while ($true) {
        Start-Sleep -Seconds 1

        # 检查K线聚合进程是否还在运行
        if ($global:klineProcess -and $global:klineProcess.HasExited) {
            Write-Host "⚠️ K线聚合服务意外退出" -ForegroundColor Red
            Stop-KlineSystem
            break
        }
    }
}
catch [System.Management.Automation.PipelineStoppedException] {
    # Ctrl+C被按下
    Stop-KlineSystem
}
finally {
    # 确保清理
    Stop-KlineSystem
}
