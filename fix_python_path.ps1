# 修复Python PATH问题 - 移除Windows Store Python别名的影响
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🔧 修复Python PATH配置" -ForegroundColor Green

# 获取当前用户PATH
$userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
$systemPath = [Environment]::GetEnvironmentVariable("PATH", "Machine")

Write-Host "📋 当前Python路径:" -ForegroundColor Yellow
$pythonPaths = @()
foreach ($path in ($userPath + ";" + $systemPath).Split(';')) {
    if ($path -like "*python*" -and $path.Trim() -ne "") {
        $pythonPaths += $path.Trim()
        Write-Host "  - $($path.Trim())" -ForegroundColor Cyan
    }
}

# 检查是否存在WindowsApps中的Python
$windowsAppsPython = $pythonPaths | Where-Object { $_ -like "*WindowsApps*" }

if ($windowsAppsPython) {
    Write-Host "⚠️  发现Windows Store Python别名路径:" -ForegroundColor Red
    foreach ($path in $windowsAppsPython) {
        Write-Host "  - $path" -ForegroundColor Red
    }
    
    Write-Host "`n💡 建议解决方案:" -ForegroundColor Yellow
    Write-Host "1. 通过Windows设置禁用应用别名（推荐）:" -ForegroundColor White
    Write-Host "   - 打开 Windows设置 (Win + I)" -ForegroundColor Gray
    Write-Host "   - 应用 -> 应用执行别名" -ForegroundColor Gray
    Write-Host "   - 关闭Python相关的别名" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. 或者修改启动器脚本使用完整路径" -ForegroundColor White
} else {
    Write-Host "✅ 未发现Windows Store Python别名问题" -ForegroundColor Green
}

# 检查可用的Python版本
Write-Host "`n🐍 检查可用的Python版本:" -ForegroundColor Green
$workingPythons = @()

foreach ($path in $pythonPaths) {
    if ($path -notlike "*WindowsApps*" -and (Test-Path "$path\python.exe")) {
        try {
            $version = & "$path\python.exe" --version 2>&1
            if ($version -match "Python \d+\.\d+\.\d+") {
                $workingPythons += @{
                    Path = "$path\python.exe"
                    Version = $version
                }
                Write-Host "  ✅ $path\python.exe - $version" -ForegroundColor Green
            }
        } catch {
            Write-Host "  ❌ $path\python.exe - 无法执行" -ForegroundColor Red
        }
    }
}

# 推荐使用的Python路径
if ($workingPythons.Count -gt 0) {
    $recommendedPython = $workingPythons[0]
    Write-Host "`n🎯 推荐使用的Python路径:" -ForegroundColor Yellow
    Write-Host "  $($recommendedPython.Path)" -ForegroundColor Cyan
    Write-Host "  版本: $($recommendedPython.Version)" -ForegroundColor Cyan
    
    # 创建修复后的启动器脚本
    Write-Host "`n🔨 创建修复后的启动器脚本..." -ForegroundColor Yellow
    
    $fixedLauncherContent = @"
# K线系统启动器UI启动脚本 (修复版)
`$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚀 启动K线系统启动器UI" -ForegroundColor Green

if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

# 使用指定的Python路径
`$pythonPath = "$($recommendedPython.Path)"

try {
    `$pythonVersion = & `$pythonPath --version 2>&1
    Write-Host "✅ Python: `$pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ 未找到Python" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

if (-not (Test-Path "launcher_ui.py")) {
    Write-Host "❌ 未找到启动器文件" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

Write-Host "🚀 启动图形界面..." -ForegroundColor Yellow

try {
    & `$pythonPath launcher_ui.py
} catch {
    Write-Host "❌ 启动失败: `$_" -ForegroundColor Red
    Write-Host "错误详情: `$(`$_.Exception.Message)" -ForegroundColor Red
}

Write-Host "✅ 启动器已退出" -ForegroundColor Green
Read-Host "按任意键退出"
"@

    # 保存修复后的启动器
    $fixedLauncherContent | Out-File -FilePath "启动器_修复版.ps1" -Encoding UTF8
    Write-Host "✅ 已创建修复版启动器: 启动器_修复版.ps1" -ForegroundColor Green
    
} else {
    Write-Host "`n❌ 未找到可用的Python安装" -ForegroundColor Red
}

Write-Host "`n📝 总结:" -ForegroundColor Yellow
Write-Host "1. 如果要彻底解决，请通过Windows设置禁用Python应用别名" -ForegroundColor White
Write-Host "2. 或者使用生成的 '启动器_修复版.ps1' 脚本" -ForegroundColor White
Write-Host "3. 确保tkinter模块已安装 (通常Python自带)" -ForegroundColor White

Read-Host "`n按任意键退出"
