# 修复Visual Studio调试问题的脚本
# 解决 "manifest path Cargo.toml does not exist" 错误

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🔧 修复Visual Studio调试问题" -ForegroundColor Green
Write-Host ""

Write-Host "📋 问题诊断:" -ForegroundColor Cyan
Write-Host "   错误: manifest path 'Cargo.toml' does not exist" -ForegroundColor Red
Write-Host "   原因: Visual Studio工作目录配置不正确" -ForegroundColor Yellow
Write-Host ""

Write-Host "🛠️ 解决步骤:" -ForegroundColor Cyan
Write-Host ""

Write-Host "步骤1: 关闭Visual Studio" -ForegroundColor White
Write-Host "   - 完全关闭Visual Studio" -ForegroundColor Gray
Write-Host "   - 确保所有VS进程都已结束" -ForegroundColor Gray
Write-Host ""

Write-Host "步骤2: 清理缓存文件" -ForegroundColor White
Write-Host "   - 删除 .vs 文件夹（如果可能）" -ForegroundColor Gray
Write-Host "   - 这会重置Visual Studio的项目缓存" -ForegroundColor Gray
Write-Host ""

Write-Host "步骤3: 重新打开项目" -ForegroundColor White
Write-Host "   - 启动Visual Studio" -ForegroundColor Gray
Write-Host "   - 文件 → 打开 → 文件夹" -ForegroundColor Gray
Write-Host "   - 选择项目根目录: F:\work\github\BinanceCryptoKline" -ForegroundColor Gray
Write-Host ""

Write-Host "步骤4: 配置调试" -ForegroundColor White
Write-Host "   - 等待rust-analyzer加载完成" -ForegroundColor Gray
Write-Host "   - 在调试下拉菜单中选择 'Debug klagg_sub_threads'" -ForegroundColor Gray
Write-Host "   - 按F5启动调试" -ForegroundColor Gray
Write-Host ""

Write-Host "🎯 关键配置信息:" -ForegroundColor Cyan
Write-Host "   已更新 .vs\launch.vs.json 配置文件" -ForegroundColor Green
Write-Host "   使用 'cargo' 类型而不是 'default'" -ForegroundColor Green
Write-Host "   设置正确的工作目录: \${workspaceRoot}" -ForegroundColor Green
Write-Host ""

Write-Host "💡 如果问题仍然存在:" -ForegroundColor Cyan
Write-Host "1. 检查项目是否通过'打开文件夹'方式打开" -ForegroundColor White
Write-Host "2. 确认rust-analyzer插件已正确安装" -ForegroundColor White
Write-Host "3. 尝试在命令行中运行程序确认代码无误" -ForegroundColor White
Write-Host "4. 重启Visual Studio并重新加载项目" -ForegroundColor White
Write-Host ""

Write-Host "🚀 测试命令:" -ForegroundColor Cyan
Write-Host "   在命令行中测试程序是否正常运行:" -ForegroundColor Gray
Write-Host "   cargo build --bin klagg_sub_threads" -ForegroundColor Yellow
Write-Host "   .\target\debug\klagg_sub_threads.exe" -ForegroundColor Yellow
Write-Host ""

Write-Host "📚 参考文档:" -ForegroundColor Cyan
Write-Host "   docs\Visual Studio内存调试指南.md" -ForegroundColor Gray
Write-Host ""

# 检查当前配置
if (Test-Path ".vs\launch.vs.json") {
    Write-Host "✅ launch.vs.json 配置文件已存在并已更新" -ForegroundColor Green
} else {
    Write-Host "⚠️ launch.vs.json 配置文件不存在，需要重新创建" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🎯 现在请按照上述步骤操作，问题应该会得到解决！" -ForegroundColor Green
