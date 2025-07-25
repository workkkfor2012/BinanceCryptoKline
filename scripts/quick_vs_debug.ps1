# 快速Visual Studio调试启动脚本
# 使用方法: .\scripts\quick_vs_debug.ps1 [程序名]

param(
    [string]$Target = "klagg_sub_threads",
    [switch]$OpenVS = $false
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚀 快速Visual Studio调试启动" -ForegroundColor Green
Write-Host "📋 目标程序: $Target" -ForegroundColor Yellow

# 可用的目标程序
$AvailableTargets = @("klagg_sub_threads", "kline_server", "kline_data_service")
if ($Target -notin $AvailableTargets) {
    Write-Host "❌ 无效的目标程序: $Target" -ForegroundColor Red
    Write-Host "📋 可用的目标程序: $($AvailableTargets -join ', ')" -ForegroundColor Yellow
    exit 1
}

# 编译调试版本
Write-Host "🔨 编译调试版本..." -ForegroundColor Cyan
cargo build --bin $Target

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ 编译成功" -ForegroundColor Green

    # 设置环境变量
    $env:RUST_BACKTRACE = "1"
    $env:RUST_LOG = "debug"

    # 检查文件
    $ExePath = "target\debug\$Target.exe"
    $PdbPath = "target\debug\$Target.pdb"

    Write-Host "🎯 调试环境已准备就绪！" -ForegroundColor Green
    Write-Host ""
    Write-Host "📁 文件信息:" -ForegroundColor Cyan
    Write-Host "🎯 可执行文件: $ExePath" -ForegroundColor Yellow
    Write-Host "🔍 调试符号: $PdbPath" -ForegroundColor Yellow

    # 检查文件大小
    $ExeFile = Get-Item $ExePath -ErrorAction SilentlyContinue
    if ($ExeFile) {
        Write-Host "📊 可执行文件大小: $([math]::Round($ExeFile.Length/1MB, 2)) MB" -ForegroundColor Gray
    }

    $PdbFile = Get-Item $PdbPath -ErrorAction SilentlyContinue
    if ($PdbFile) {
        Write-Host "📊 调试符号大小: $([math]::Round($PdbFile.Length/1MB, 2)) MB" -ForegroundColor Gray
    } else {
        Write-Host "⚠️ 未找到调试符号文件" -ForegroundColor Yellow
    }

    # 可选：启动Visual Studio
    if ($OpenVS) {
        Write-Host ""
        Write-Host "🚀 正在启动Visual Studio..." -ForegroundColor Cyan
        try {
            # 查找Visual Studio安装路径
            $VSWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
            if (Test-Path $VSWhere) {
                $VSInstallPath = & $VSWhere -latest -property installationPath
                if ($VSInstallPath) {
                    $DevEnvPath = Join-Path $VSInstallPath "Common7\IDE\devenv.exe"
                    if (Test-Path $DevEnvPath) {
                        Start-Process -FilePath $DevEnvPath -ArgumentList "." -NoNewWindow
                        Write-Host "✅ Visual Studio已启动" -ForegroundColor Green
                    } else {
                        throw "找不到devenv.exe"
                    }
                } else {
                    throw "无法获取Visual Studio安装路径"
                }
            } else {
                # 尝试使用devenv命令
                $VSPath = Get-Command devenv -ErrorAction SilentlyContinue
                if ($VSPath) {
                    Start-Process -FilePath "devenv" -ArgumentList "." -NoNewWindow
                    Write-Host "✅ Visual Studio已启动" -ForegroundColor Green
                } else {
                    throw "找不到Visual Studio"
                }
            }
        } catch {
            Write-Host "⚠️ 无法自动启动Visual Studio: $_" -ForegroundColor Yellow
            Write-Host "💡 请手动打开Visual Studio" -ForegroundColor Cyan
        }
    }

    Write-Host ""
    Write-Host "📋 下一步操作:" -ForegroundColor Cyan
    Write-Host "1. 打开Visual Studio (或使用 -OpenVS 参数自动打开)" -ForegroundColor White
    Write-Host "2. 文件 → 打开 → 文件夹 → 选择当前目录" -ForegroundColor White
    Write-Host "3. 在解决方案资源管理器中找到 '$Target.exe'" -ForegroundColor White
    Write-Host "4. 右键点击 '$Target.exe' → 设为启动项" -ForegroundColor White
    Write-Host "5. 按 F5 开始调试，或 Ctrl+F5 开始执行(不调试)" -ForegroundColor White
    Write-Host "6. 使用 调试 → 性能探查器 (Alt+F2) 进行性能分析" -ForegroundColor White

} else {
    Write-Host "❌ 编译失败" -ForegroundColor Red
    Write-Host "请检查代码错误后重试" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🔧 调试技巧:" -ForegroundColor Cyan
Write-Host "- 在代码中设置断点 (F9)" -ForegroundColor Gray
Write-Host "- 使用诊断工具窗口监控内存使用" -ForegroundColor Gray
Write-Host "- 在关键时刻拍摄堆快照进行内存分析" -ForegroundColor Gray
Write-Host "- 查看调用堆栈了解程序执行流程" -ForegroundColor Gray
Write-Host ""
Write-Host "💡 提示:" -ForegroundColor Cyan
Write-Host "- 使用 -OpenVS 参数自动打开Visual Studio" -ForegroundColor Gray
Write-Host "- 使用 -Target <程序名> 指定要调试的程序" -ForegroundColor Gray
Write-Host "- 可用程序: $($AvailableTargets -join ', ')" -ForegroundColor Gray
Write-Host ""
Write-Host "📚 更多信息请参考: docs\Visual Studio内存调试指南.md" -ForegroundColor Cyan
