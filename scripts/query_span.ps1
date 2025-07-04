# 查询指定SpanId的脚本
# 使用方法: .\scripts\query_span.ps1 "209417932428541954"

param(
    [Parameter(Mandatory=$true)]
    [string]$SpanId,
    [int]$Limit = 10
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$LogServerUrl = "http://127.0.0.1:9002"

Write-Host "=== 查询 SpanId: $SpanId ===" -ForegroundColor Green
Write-Host "🔍 正在查询..." -ForegroundColor Yellow

try {
    $body = @{
        span_id = $SpanId
        limit = $Limit
    } | ConvertTo-Json

    $response = Invoke-WebRequest -Uri "$LogServerUrl/query" -Method POST -ContentType "application/json" -Body $body
    
    Write-Host "✅ 查询成功，状态码: $($response.StatusCode)" -ForegroundColor Green
    
    $jsonResponse = $response.Content | ConvertFrom-Json
    Write-Host "📊 返回结果数量: $($jsonResponse.Count)" -ForegroundColor Cyan
    
    if ($jsonResponse.Count -gt 0) {
        Write-Host "📋 查询结果:" -ForegroundColor Cyan
        Write-Host ""
        
        foreach ($span in $jsonResponse) {
            Write-Host "🔸 Span详情:" -ForegroundColor White
            Write-Host "  ID: $($span.id)" -ForegroundColor Yellow
            Write-Host "  TraceId: $($span.trace_id)" -ForegroundColor Cyan
            Write-Host "  ParentId: $($span.parent_id)" -ForegroundColor Gray
            Write-Host "  名称: $($span.name)" -ForegroundColor Green
            Write-Host "  模块: $($span.target)" -ForegroundColor Magenta
            Write-Host "  级别: $($span.level)" -ForegroundColor Blue
            Write-Host "  时间: $($span.timestamp)" -ForegroundColor DarkGray
            Write-Host "  耗时: $($span.duration_ms) ms" -ForegroundColor Red
            
            if ($span.attributes -and $span.attributes.PSObject.Properties.Count -gt 0) {
                Write-Host "  属性:" -ForegroundColor DarkYellow
                $span.attributes.PSObject.Properties | ForEach-Object {
                    Write-Host "    $($_.Name): $($_.Value)" -ForegroundColor DarkCyan
                }
            }
            
            if ($span.events -and $span.events.Count -gt 0) {
                Write-Host "  事件 ($($span.events.Count)个):" -ForegroundColor DarkGreen
                foreach ($event in $span.events) {
                    Write-Host "    ⏰ $($event.timestamp) - $($event.name)" -ForegroundColor DarkGreen
                    if ($event.attributes -and $event.attributes.PSObject.Properties.Count -gt 0) {
                        $event.attributes.PSObject.Properties | ForEach-Object {
                            Write-Host "       $($_.Name): $($_.Value)" -ForegroundColor DarkCyan
                        }
                    }
                }
            }
            Write-Host ""
        }
        
        # 如果只有一个结果，显示完整JSON
        if ($jsonResponse.Count -eq 1) {
            Write-Host "📄 完整JSON数据:" -ForegroundColor Magenta
            $jsonResponse | ConvertTo-Json -Depth 10 | Write-Host
        }
    } else {
        Write-Host "⚠️ 未找到匹配的 SpanId: $SpanId" -ForegroundColor Yellow
        Write-Host "💡 提示: 请检查SpanId是否正确，或者该Span可能已被清理" -ForegroundColor Gray
    }
} catch {
    Write-Host "❌ 查询失败: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "💡 请确保Log MCP Server正在运行 (http://127.0.0.1:9002)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "🔧 其他查询选项:" -ForegroundColor Gray
Write-Host "  加载完整查询工具: . .\src\Log-MCP-Server\query_commands.ps1" -ForegroundColor Gray
Write-Host "  按TraceId查询: qtrace 'trace_id'" -ForegroundColor Gray
Write-Host "  查看所有日志: qall 20" -ForegroundColor Gray
