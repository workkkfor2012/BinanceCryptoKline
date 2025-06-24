# 测试循环追踪修复的脚本
# 运行 kline_data_service 并分析生成的 trace 日志

Write-Host "🔧 测试循环追踪修复..." -ForegroundColor Green

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 确保日志目录存在
$logDir = "logs\debug_snapshots"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force
    Write-Host "✅ 创建日志目录: $logDir" -ForegroundColor Green
}

# 清理旧的日志文件
Write-Host "🧹 清理旧的日志文件..." -ForegroundColor Yellow
Get-ChildItem $logDir -Filter "*.log" | Remove-Item -Force
Write-Host "✅ 清理完成" -ForegroundColor Green

# 运行 kline_data_service
Write-Host "🚀 启动 kline_data_service..." -ForegroundColor Green
Write-Host "⏱️  程序将运行30秒，每5秒保存一次trace快照..." -ForegroundColor Yellow

try {
    # 启动程序并等待完成
    $process = Start-Process -FilePath "cargo" -ArgumentList "run", "--bin", "kline_data_service" -NoNewWindow -PassThru -Wait
    
    if ($process.ExitCode -eq 0) {
        Write-Host "✅ 程序执行完成" -ForegroundColor Green
    } else {
        Write-Host "❌ 程序执行失败，退出码: $($process.ExitCode)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ 启动程序时发生错误: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 等待一下确保文件写入完成
Start-Sleep -Seconds 2

# 查找最新的日志文件
Write-Host "🔍 查找生成的日志文件..." -ForegroundColor Green
$logFiles = Get-ChildItem $logDir -Filter "*.log" | Sort-Object LastWriteTime -Descending

if ($logFiles.Count -eq 0) {
    Write-Host "❌ 没有找到日志文件" -ForegroundColor Red
    exit 1
}

$latestLog = $logFiles[0]
Write-Host "📄 找到最新日志文件: $($latestLog.Name)" -ForegroundColor Green
Write-Host "📊 文件大小: $([math]::Round($latestLog.Length / 1KB, 2)) KB" -ForegroundColor Cyan

# 分析日志内容
Write-Host "`n🔍 分析日志内容..." -ForegroundColor Green
$content = Get-Content $latestLog.FullName -Encoding UTF8

# 统计Trace数量
$traceCount = ($content | Select-String "Trace ID:" | Measure-Object).Count
Write-Host "📈 总Trace数量: $traceCount" -ForegroundColor Cyan

# 查找循环相关的内容
$loopLines = $content | Select-String "(循环)"
if ($loopLines.Count -gt 0) {
    Write-Host "✅ 找到循环识别内容:" -ForegroundColor Green
    foreach ($line in $loopLines) {
        Write-Host "   $line" -ForegroundColor White
    }
} else {
    Write-Host "❌ 没有找到循环识别内容" -ForegroundColor Red
}

# 查找路径原型
$archetypeLines = $content | Select-String "路径原型"
if ($archetypeLines.Count -gt 0) {
    Write-Host "✅ 找到路径原型内容:" -ForegroundColor Green
    foreach ($line in $archetypeLines[0..4]) {  # 只显示前5行
        Write-Host "   $line" -ForegroundColor White
    }
    if ($archetypeLines.Count -gt 5) {
        Write-Host "   ... 还有 $($archetypeLines.Count - 5) 行路径原型内容" -ForegroundColor Gray
    }
} else {
    Write-Host "❌ 没有找到路径原型内容" -ForegroundColor Red
}

# 查找孤立的download_kline_task
$orphanTasks = $content | Select-String "根函数: download_kline_task"
if ($orphanTasks.Count -gt 0) {
    Write-Host "⚠️  发现 $($orphanTasks.Count) 个孤立的 download_kline_task Trace" -ForegroundColor Yellow
    Write-Host "   这表明 tracing 上下文传递可能仍有问题" -ForegroundColor Yellow
} else {
    Write-Host "✅ 没有发现孤立的 download_kline_task，上下文传递正常" -ForegroundColor Green
}

Write-Host "`n📋 分析完成！" -ForegroundColor Green
Write-Host "📄 详细日志文件: $($latestLog.FullName)" -ForegroundColor Cyan
