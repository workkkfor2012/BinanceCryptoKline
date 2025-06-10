# 测试 Xterm.js WebLog 实现
# 这个脚本会临时替换 index.html 文件来测试新的 Xterm.js 实现

param(
    [switch]$Restore = $false
)

$weblogPath = "src\weblog\static"
$originalFile = "$weblogPath\index.html"
$xtermFile = "$weblogPath\index_xterm.html"
$backupFile = "$weblogPath\index_original_backup.html"

if ($Restore) {
    # 恢复原始文件
    if (Test-Path $backupFile) {
        Write-Host "🔄 恢复原始 index.html 文件..." -ForegroundColor Yellow
        Copy-Item $backupFile $originalFile -Force
        Remove-Item $backupFile -Force
        Write-Host "✅ 已恢复原始文件" -ForegroundColor Green
    } else {
        Write-Host "❌ 找不到备份文件" -ForegroundColor Red
    }
    exit
}

# 检查文件是否存在
if (-not (Test-Path $xtermFile)) {
    Write-Host "❌ 找不到 Xterm.js 实现文件: $xtermFile" -ForegroundColor Red
    exit 1
}

# 备份原始文件
if (Test-Path $originalFile) {
    Write-Host "💾 备份原始 index.html 文件..." -ForegroundColor Cyan
    Copy-Item $originalFile $backupFile -Force
}

# 替换为 Xterm.js 实现
Write-Host "🔄 替换为 Xterm.js 实现..." -ForegroundColor Yellow
Copy-Item $xtermFile $originalFile -Force

Write-Host "✅ 已切换到 Xterm.js 实现" -ForegroundColor Green
Write-Host ""
Write-Host "📋 使用说明:" -ForegroundColor Cyan
Write-Host "  1. 运行 start_with_pipe_simple.ps1 启动系统"
Write-Host "  2. 访问 http://localhost:8080 查看新的 Xterm.js 界面"
Write-Host "  3. 测试完成后运行: .\test_xterm_weblog.ps1 -Restore 恢复原始文件"
Write-Host ""
Write-Host "🎯 新功能特点:" -ForegroundColor Green
Write-Host "  ✓ 使用专业的 Xterm.js 终端组件"
Write-Host "  ✓ 更好的性能和滚动体验"
Write-Host "  ✓ 支持颜色编码的日志级别"
Write-Host "  ✓ 自动链接检测"
Write-Host "  ✓ 简化的代码结构"
