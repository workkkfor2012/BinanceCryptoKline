# Log MCP Server 查询命令集合
# 为AI助手提供预定义的日志查询命令，避免临时拼接字符串

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 基础配置
$LogServerUrl = "http://127.0.0.1:9002"

Write-Host "=== Log MCP Server 查询命令集合 ===" -ForegroundColor Green
Write-Host "服务器地址: $LogServerUrl" -ForegroundColor Cyan
Write-Host ""

# ==================== 基础查询命令 ====================

function Query-AllLogs {
    param([int]$Limit = 10)
    Write-Host "🔍 查询所有日志 (限制 $Limit 条)" -ForegroundColor Yellow
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body "{`"limit`":$Limit}"
}

function Query-ByTraceId {
    param(
        [Parameter(Mandatory=$true)][string]$TraceId,
        [int]$Limit = 50
    )
    Write-Host "🔍 按TraceId查询: $TraceId (限制 $Limit 条)" -ForegroundColor Yellow
    $body = "{`"trace_id`":`"$TraceId`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-ByTarget {
    param(
        [Parameter(Mandatory=$true)][string]$Target,
        [int]$Limit = 20
    )
    Write-Host "🔍 按模块查询: $Target (限制 $Limit 条)" -ForegroundColor Yellow
    $body = "{`"target`":`"$Target`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-BySpanId {
    param(
        [Parameter(Mandatory=$true)][string]$SpanId,
        [int]$Limit = 10
    )
    Write-Host "🔍 按SpanId查询: $SpanId (限制 $Limit 条)" -ForegroundColor Yellow
    $body = "{`"span_id`":`"$SpanId`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-ByLevel {
    param(
        [Parameter(Mandatory=$true)][ValidateSet("TRACE","DEBUG","INFO","WARN","ERROR")][string]$Level,
        [int]$Limit = 20
    )
    Write-Host "🔍 按日志级别查询: $Level (限制 $Limit 条)" -ForegroundColor Yellow
    $body = "{`"level`":`"$Level`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-ErrorsOnly {
    param([int]$Limit = 10)
    Write-Host "❌ 查询错误日志 (限制 $Limit 条)" -ForegroundColor Red
    $body = "{`"level`":`"ERROR`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-WarningsOnly {
    param([int]$Limit = 10)
    Write-Host "⚠️ 查询警告日志 (限制 $Limit 条)" -ForegroundColor Yellow
    $body = "{`"level`":`"WARN`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

# ==================== 业务相关查询 ====================

function Query-DatabaseLogs {
    param([int]$Limit = 15)
    Write-Host "🗄️ 查询数据库相关日志 (限制 $Limit 条)" -ForegroundColor Cyan
    $body = "{`"target`":`"db`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-WebSocketLogs {
    param([int]$Limit = 15)
    Write-Host "🌐 查询WebSocket相关日志 (限制 $Limit 条)" -ForegroundColor Cyan
    $body = "{`"target`":`"websocket`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-KlineLogs {
    param([int]$Limit = 15)
    Write-Host "📈 查询K线相关日志 (限制 $Limit 条)" -ForegroundColor Cyan
    $body = "{`"target`":`"kline`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-AggregatorLogs {
    param([int]$Limit = 15)
    Write-Host "🔄 查询聚合器相关日志 (限制 $Limit 条)" -ForegroundColor Cyan
    $body = "{`"target`":`"aggregate`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

# ==================== 组合查询 ====================

function Query-DatabaseErrors {
    param([int]$Limit = 10)
    Write-Host "🗄️❌ 查询数据库错误日志 (限制 $Limit 条)" -ForegroundColor Red
    $body = "{`"target`":`"db`",`"level`":`"ERROR`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-WebSocketErrors {
    param([int]$Limit = 10)
    Write-Host "🌐❌ 查询WebSocket错误日志 (限制 $Limit 条)" -ForegroundColor Red
    $body = "{`"target`":`"websocket`",`"level`":`"ERROR`",`"limit`":$Limit}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

function Query-TraceWithErrors {
    param(
        [Parameter(Mandatory=$true)][string]$TraceId
    )
    Write-Host "🔍❌ 查询TraceId $TraceId 的所有错误" -ForegroundColor Red
    $body = "{`"trace_id`":`"$TraceId`",`"level`":`"ERROR`"}"
    Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
}

# ==================== 管理命令 ====================

function Clear-LogDatabase {
    Write-Host "🗑️ 清空日志数据库" -ForegroundColor Magenta
    Write-Host "⚠️ 这将删除所有日志数据，确认请按回车，取消请按Ctrl+C" -ForegroundColor Yellow
    Read-Host
    Invoke-WebRequest -Uri "$LogServerUrl/clear" -Method POST
    Write-Host "✅ 日志数据库已清空" -ForegroundColor Green
}

function Test-LogServer {
    Write-Host "🔧 测试日志服务器连接" -ForegroundColor Cyan
    try {
        $response = Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body "{`"limit`":1}" -TimeoutSec 5
        if ($response.StatusCode -eq 200) {
            Write-Host "✅ 日志服务器连接正常" -ForegroundColor Green
            $logCount = ($response.Content | ConvertFrom-Json).Count
            Write-Host "📊 当前日志总数: $logCount" -ForegroundColor Cyan
        }
    } catch {
        Write-Host "❌ 日志服务器连接失败: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# ==================== 快捷命令别名 ====================

# 设置别名让命令更简短
Set-Alias -Name qall -Value Query-AllLogs
Set-Alias -Name qspan -Value Query-BySpanId
Set-Alias -Name qtrace -Value Query-ByTraceId
Set-Alias -Name qtarget -Value Query-ByTarget
Set-Alias -Name qlevel -Value Query-ByLevel
Set-Alias -Name qerr -Value Query-ErrorsOnly
Set-Alias -Name qwarn -Value Query-WarningsOnly
Set-Alias -Name qdb -Value Query-DatabaseLogs
Set-Alias -Name qws -Value Query-WebSocketLogs
Set-Alias -Name qkline -Value Query-KlineLogs
Set-Alias -Name qagg -Value Query-AggregatorLogs
Set-Alias -Name qdberr -Value Query-DatabaseErrors
Set-Alias -Name qwserr -Value Query-WebSocketErrors
Set-Alias -Name clear-logs -Value Clear-LogDatabase
Set-Alias -Name test-logs -Value Test-LogServer

# ==================== 帮助信息 ====================

function Show-QueryHelp {
    Write-Host ""
    Write-Host "=== 可用查询命令 ===" -ForegroundColor Green
    Write-Host ""
    Write-Host "📋 基础查询:" -ForegroundColor Cyan
    Write-Host "  Query-AllLogs [-Limit 10]           # qall - 查询所有日志"
    Write-Host "  Query-BySpanId -SpanId 'xxx'        # qspan - 按SpanId查询"
    Write-Host "  Query-ByTraceId -TraceId 'xxx'      # qtrace - 按TraceId查询"
    Write-Host "  Query-ByTarget -Target 'xxx'        # qtarget - 按模块查询"
    Write-Host "  Query-ByLevel -Level ERROR           # qlevel - 按级别查询"
    Write-Host "  Query-ErrorsOnly [-Limit 10]        # qerr - 只查询错误"
    Write-Host "  Query-WarningsOnly [-Limit 10]      # qwarn - 只查询警告"
    Write-Host ""
    Write-Host "🏢 业务查询:" -ForegroundColor Cyan
    Write-Host "  Query-DatabaseLogs [-Limit 15]      # qdb - 数据库日志"
    Write-Host "  Query-WebSocketLogs [-Limit 15]     # qws - WebSocket日志"
    Write-Host "  Query-KlineLogs [-Limit 15]         # qkline - K线日志"
    Write-Host "  Query-AggregatorLogs [-Limit 15]    # qagg - 聚合器日志"
    Write-Host ""
    Write-Host "🔍 组合查询:" -ForegroundColor Cyan
    Write-Host "  Query-DatabaseErrors [-Limit 10]    # qdberr - 数据库错误"
    Write-Host "  Query-WebSocketErrors [-Limit 10]   # qwserr - WebSocket错误"
    Write-Host "  Query-TraceWithErrors -TraceId 'xxx' # 特定trace的错误"
    Write-Host ""
    Write-Host "🔧 管理命令:" -ForegroundColor Cyan
    Write-Host "  Clear-LogDatabase                    # clear-logs - 清空数据库"
    Write-Host "  Test-LogServer                       # test-logs - 测试连接"
    Write-Host "  Show-QueryHelp                       # 显示此帮助"
    Write-Host ""
    Write-Host "💡 使用示例:" -ForegroundColor Yellow
    Write-Host "  qall 5                              # 查询最新5条日志"
    Write-Host "  qspan '209417932428541954'          # 查询指定SpanId的日志"
    Write-Host "  qtrace '1'                          # 查询trace_id为1的日志"
    Write-Host "  qtarget 'db' 20                     # 查询数据库相关的20条日志"
    Write-Host "  qerr                                # 查询所有错误日志"
    Write-Host ""
}

# 启动时显示帮助
Show-QueryHelp
