# 测试实时原始日志显示修复
# 这个脚本会启动WebLog系统并发送测试日志来验证实时原始日志模块是否正常工作

param(
    [switch]$Quick = $false  # 快速测试模式
)

$ErrorActionPreference = "Stop"

Write-Host "🧪 测试实时原始日志显示修复" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 检查是否有WebLog进程在运行
$existingProcess = Get-Process -Name "weblog" -ErrorAction SilentlyContinue
if ($existingProcess) {
    Write-Host "⚠️  发现已运行的WebLog进程，正在停止..." -ForegroundColor Yellow
    Stop-Process -Name "weblog" -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

# 启动WebLog系统
Write-Host "🚀 启动WebLog系统..." -ForegroundColor Green
$weblogProcess = Start-Process powershell -ArgumentList "-Command", "cd src\weblog; cargo run --bin weblog -- --pipe-name 'test_realtime_pipe'" -WindowStyle Normal -PassThru

# 等待WebLog启动
Write-Host "⏳ 等待WebLog系统启动..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# 检查WebLog是否成功启动
if ($weblogProcess.HasExited) {
    Write-Host "❌ WebLog启动失败" -ForegroundColor Red
    exit 1
}

Write-Host "✅ WebLog系统已启动 (PID: $($weblogProcess.Id))" -ForegroundColor Green
Write-Host "🌐 Web界面: http://localhost:8080" -ForegroundColor Cyan

# 创建测试日志发送器
Write-Host "📡 创建测试日志发送器..." -ForegroundColor Yellow

$testLogScript = @"
`$pipeName = "test_realtime_pipe"
`$pipe = New-Object System.IO.Pipes.NamedPipeClientStream(".", `$pipeName, [System.IO.Pipes.PipeDirection]::Out)

try {
    Write-Host "🔗 连接到命名管道: `$pipeName"
    `$pipe.Connect(5000)
    
    `$writer = New-Object System.IO.StreamWriter(`$pipe)
    `$writer.AutoFlush = `$true
    
    Write-Host "📝 开始发送测试日志..."
    
    # 发送不同类型的测试日志
    `$testLogs = @(
        '{"timestamp":"2024-01-01T12:00:01Z","level":"INFO","target":"test_module","message":"测试信息日志 - 实时原始日志应该显示这条消息"}',
        '{"timestamp":"2024-01-01T12:00:02Z","level":"WARN","target":"test_module","message":"测试警告日志 - 检查颜色显示"}',
        '{"timestamp":"2024-01-01T12:00:03Z","level":"ERROR","target":"test_module","message":"测试错误日志 - 应该显示为红色"}',
        '{"timestamp":"2024-01-01T12:00:04Z","level":"INFO","target":"another_module","message":"另一个模块的日志"}',
        '这是一条非JSON格式的原始日志行',
        '{"timestamp":"2024-01-01T12:00:05Z","level":"DEBUG","target":"debug_module","message":"调试日志测试"}',
        '{"timestamp":"2024-01-01T12:00:06Z","level":"INFO","target":"test_module","message":"重复消息测试 - 第1条"}',
        '{"timestamp":"2024-01-01T12:00:07Z","level":"INFO","target":"test_module","message":"重复消息测试 - 第2条"}',
        '{"timestamp":"2024-01-01T12:00:08Z","level":"INFO","target":"test_module","message":"重复消息测试 - 第3条"}'
    )
    
    foreach (`$log in `$testLogs) {
        `$writer.WriteLine(`$log)
        Write-Host "📤 发送: `$log"
        Start-Sleep -Milliseconds 500
    }
    
    Write-Host "✅ 测试日志发送完成"
    
} catch {
    Write-Host "❌ 发送日志失败: `$_" -ForegroundColor Red
} finally {
    if (`$writer) { `$writer.Close() }
    if (`$pipe) { `$pipe.Close() }
}
"@

# 启动日志发送器
Write-Host "📤 启动日志发送器..." -ForegroundColor Green
$logSenderProcess = Start-Process powershell -ArgumentList "-Command", $testLogScript -WindowStyle Normal -PassThru

# 等待日志发送完成
Write-Host "⏳ 等待日志发送完成..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

Write-Host "" -ForegroundColor White
Write-Host "🎯 测试完成！请检查以下内容：" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green
Write-Host "1. 打开浏览器访问: http://localhost:8080" -ForegroundColor Cyan
Write-Host "2. 点击 '📊 显示实时原始日志' 按钮" -ForegroundColor Cyan
Write-Host "3. 检查实时原始日志模块是否显示:" -ForegroundColor Cyan
Write-Host "   - 状态应该从 '等待日志' 变为 '实时接收'" -ForegroundColor Yellow
Write-Host "   - 实时日志数应该显示正确的数量" -ForegroundColor Yellow
Write-Host "   - 最后更新时间应该显示当前时间" -ForegroundColor Yellow
Write-Host "   - 日志频率应该显示 > 0 条/秒" -ForegroundColor Yellow
Write-Host "   - 日志内容应该正确显示并有颜色区分" -ForegroundColor Yellow
Write-Host "4. 检查原始日志高频折叠模块是否正常工作" -ForegroundColor Cyan
Write-Host "5. 检查动态模块是否正确创建 (test_module, another_module, debug_module)" -ForegroundColor Cyan

if (-not $Quick) {
    Write-Host "" -ForegroundColor White
    Write-Host "按任意键停止测试..." -ForegroundColor Yellow
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

# 清理进程
Write-Host "🧹 清理测试环境..." -ForegroundColor Yellow
if (-not $weblogProcess.HasExited) {
    Stop-Process -Id $weblogProcess.Id -Force -ErrorAction SilentlyContinue
}
if (-not $logSenderProcess.HasExited) {
    Stop-Process -Id $logSenderProcess.Id -Force -ErrorAction SilentlyContinue
}

Write-Host "✅ 测试完成！" -ForegroundColor Green
