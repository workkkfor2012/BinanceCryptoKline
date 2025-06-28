<#
.SYNOPSIS
    使用 watchexec 启动一个高可靠性的文件监控任务。
    它会监控指定文件和目录的变化，并在变化时调用 copy-on-demand.ps1 脚本。
#>

# --- 配置区 ---
$targetFolderName = "tempfold"
$copyScriptPath = ".\copy-on-demand.ps1" # 指向我们的复制脚本

# --- 准备阶段 ---

# 1. 检查 watchexec 是否存在
if (-not (Get-Command watchexec -ErrorAction SilentlyContinue)) {
    Write-Host "错误: 'watchexec' 命令未找到。" -ForegroundColor Red
    Write-Host "请确保已经安装了 watchexec-cli 并将其添加到了系统的 PATH 环境变量中。" -ForegroundColor Yellow
    Write-Host "安装方法: cargo install watchexec-cli" -ForegroundColor Yellow
    exit 1
}

# 2. 清空目标文件夹
$targetFolderPath = Join-Path -Path (Get-Location) -ChildPath $targetFolderName
if (Test-Path -Path $targetFolderPath -PathType Container) {
    Write-Host "清空目标文件夹 '$targetFolderName'..." -ForegroundColor Yellow
    Get-ChildItem -Path $targetFolderPath -Force | Remove-Item -Recurse -Force
}

# 3. 执行一次初始复制
Write-Host "正在执行初始文件复制..." -ForegroundColor Cyan
try {
    # -NoProfile 提升启动速度, -ExecutionPolicy Bypass 避免执行策略问题
    powershell -NoProfile -ExecutionPolicy Bypass -File $copyScriptPath
    Write-Host "✅ 初始复制完成。" -ForegroundColor Green
} catch {
    Write-Host "❌ 初始复制失败。" -ForegroundColor Red
    exit 1
}


# --- 启动监控 ---
Write-Host "`n🔍 `watchexec` 开始监控文件变化..." -ForegroundColor Cyan
Write-Host "   将监控 'src' 目录下的所有 '.rs' 和 '.html' 文件。"
Write-Host "   将监控 'logs\debug_snapshots' 目录下的所有 '.log' 文件。"
Write-Host "   按 Ctrl+C 停止监控。" -ForegroundColor Yellow

# 定义要执行的命令
# 当文件变化时，调用 PowerShell 来运行我们的复制脚本
$commandToRun = "powershell -NoProfile -ExecutionPolicy Bypass -File $copyScriptPath"

# 启动 watchexec
# -w 'src': 监控 'src' 目录
# -w 'logs\debug_snapshots': 监控日志快照目录
# --exts 'rs,html,log': 监控这三种扩展名的文件
# -c: 每次触发时清空控制台，保持界面整洁
# -r: 如果监控的命令失败了，重启它（这里我们的脚本不会失败，但这是个好习惯）
# --: 分隔符，后面是具体要执行的命令
watchexec --watch 'src' --watch 'logs\debug_snapshots' --exts 'rs,html,log' --clear --restart -- $commandToRun