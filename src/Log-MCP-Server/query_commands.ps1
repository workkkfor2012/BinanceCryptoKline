# Log MCP Server 查询命令集合
# 为AI助手提供预定义的日志查询命令，专注于AI调试提示词中定义的核心查询功能

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 基础配置
$LogServerUrl = "http://127.0.0.1:9002"

Write-Host "=== Log MCP Server 查询命令集合 ===" -ForegroundColor Green
Write-Host "服务器地址: $LogServerUrl" -ForegroundColor Cyan
Write-Host "专为AI调试提示词6.0设计" -ForegroundColor Yellow
Write-Host ""

# ==================== 核心查询命令 ====================

function Query-Trace-By-Id {
    param(
        [Parameter(Mandatory=$true)][string]$TraceId
    )
    Write-Host "🔍 按TraceId查询完整调用链: $TraceId" -ForegroundColor Yellow
    try {
        $response = Invoke-WebRequest -Uri "$LogServerUrl/api/v1/trace/$TraceId" -Method GET
        if ($response.StatusCode -eq 200) {
            $jsonData = $response.Content | ConvertFrom-Json
            Write-Host "✅ 成功获取 $($jsonData.Count) 个Span" -ForegroundColor Green
            return $response.Content | ConvertTo-Json -Depth 10
        }
    } catch {
        Write-Host "❌ 查询失败: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "💡 请检查log_mcp_daemon是否运行，端口是否正确" -ForegroundColor Yellow
    }
}

function Test-LogServer {
    Write-Host "🔧 测试日志服务器连接" -ForegroundColor Cyan
    try {
        # 测试基本连接，使用一个简单的trace查询
        $response = Invoke-WebRequest -Uri "$LogServerUrl/api/v1/trace/test" -Method GET -TimeoutSec 5
        Write-Host "✅ 日志服务器连接正常" -ForegroundColor Green
        Write-Host "� API端点可访问" -ForegroundColor Cyan
    } catch {
        Write-Host "❌ 日志服务器连接失败: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "💡 请检查log_mcp_daemon是否运行在端口9002" -ForegroundColor Yellow
    }
}

# ==================== 快捷命令别名 ====================

# 为AI调试提示词6.0提供的核心别名
Set-Alias -Name qtrace -Value Query-Trace-By-Id
Set-Alias -Name test-logs -Value Test-LogServer

# ==================== 帮助信息 ====================

function Show-QueryHelp {
    Write-Host ""
    Write-Host "=== AI调试提示词6.0 - 核心查询命令 ===" -ForegroundColor Green
    Write-Host ""
    Write-Host "� 核心查询:" -ForegroundColor Cyan
    Write-Host "  Query-Trace-By-Id -TraceId 'xxx'    # qtrace - 按TraceId查询完整调用链"
    Write-Host ""
    Write-Host "🔧 测试命令:" -ForegroundColor Cyan
    Write-Host "  Test-LogServer                       # test-logs - 测试连接"
    Write-Host "  Show-QueryHelp                       # 显示此帮助"
    Write-Host ""
    Write-Host "� 使用示例:" -ForegroundColor Yellow
    Write-Host "  qtrace \"1\"                          # 查询trace_id为1的完整调用链"
    Write-Host "  Query-Trace-By-Id -TraceId \"23\"     # 查询trace_id为23的完整调用链"
    Write-Host "  test-logs                           # 测试log_mcp_daemon连接"
    Write-Host ""
    Write-Host "� API映射:" -ForegroundColor Magenta
    Write-Host "  Query-Trace-By-Id -> GET /api/v1/trace/{trace_id}"
    Write-Host ""
}

# 启动时显示帮助
Show-QueryHelp
