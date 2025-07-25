# Visual Studio 调试环境设置脚本

param(
    [string]$Target = "klagg_sub_threads",
    [switch]$Release = $false,
    [switch]$OpenVS = $false,
    [switch]$CreateVSConfig = $false
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🔧 Visual Studio 调试环境设置" -ForegroundColor Green
Write-Host "📋 目标程序: $Target" -ForegroundColor Yellow

# 可用的目标程序
$AvailableTargets = @("klagg_sub_threads", "kline_server", "kline_data_service")
if ($Target -notin $AvailableTargets) {
    Write-Host "❌ 无效的目标程序: $Target" -ForegroundColor Red
    Write-Host "📋 可用的目标程序: $($AvailableTargets -join ', ')" -ForegroundColor Yellow
    exit 1
}

# 确定编译配置
$BuildConfig = if ($Release) { "release" } else { "debug" }
$BuildArgs = if ($Release) { @("--release") } else { @() }

Write-Host "🏗️ 编译配置: $BuildConfig" -ForegroundColor Cyan

# 编译程序
Write-Host "🔨 编译程序..." -ForegroundColor Cyan
$BuildArgs += @("--bin", $Target)

$compileResult = cargo build @BuildArgs 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ 编译失败:" -ForegroundColor Red
    Write-Host $compileResult
    exit 1
}

Write-Host "✅ 编译成功" -ForegroundColor Green

# 检查可执行文件
$ExePath = "target\$BuildConfig\$Target.exe"
if (-not (Test-Path $ExePath)) {
    Write-Host "❌ 找不到可执行文件: $ExePath" -ForegroundColor Red
    exit 1
}

$FileInfo = Get-Item $ExePath
Write-Host "📁 可执行文件: $ExePath" -ForegroundColor Green
Write-Host "📊 文件大小: $([math]::Round($FileInfo.Length/1MB, 2)) MB" -ForegroundColor Yellow
Write-Host "🕒 编译时间: $($FileInfo.LastWriteTime)" -ForegroundColor Yellow

# 检查PDB文件
$PdbPath = "target\$BuildConfig\$Target.pdb"
if (Test-Path $PdbPath) {
    $PdbInfo = Get-Item $PdbPath
    Write-Host "🔍 调试符号: $PdbPath" -ForegroundColor Green
    Write-Host "📊 符号文件大小: $([math]::Round($PdbInfo.Length/1MB, 2)) MB" -ForegroundColor Yellow
} else {
    Write-Host "⚠️ 未找到调试符号文件: $PdbPath" -ForegroundColor Yellow
    Write-Host "💡 提示: 确保Cargo.toml中配置了debug = true" -ForegroundColor Cyan
}

# 设置环境变量
Write-Host "🌍 设置调试环境变量..." -ForegroundColor Cyan
$env:RUST_BACKTRACE = "1"
$env:RUST_LOG = "debug"

Write-Host "✅ 环境变量已设置:" -ForegroundColor Green
Write-Host "   RUST_BACKTRACE = $env:RUST_BACKTRACE" -ForegroundColor Gray
Write-Host "   RUST_LOG = $env:RUST_LOG" -ForegroundColor Gray

# 显示调试信息
Write-Host ""
Write-Host "🎯 调试准备完成！" -ForegroundColor Green
Write-Host ""
Write-Host "📋 调试选项:" -ForegroundColor Cyan
Write-Host "1. 使用Visual Studio:" -ForegroundColor White
Write-Host "   - 打开Visual Studio" -ForegroundColor Gray
Write-Host "   - 文件 → 打开 → 文件夹 → 选择项目根目录" -ForegroundColor Gray
Write-Host "   - 在解决方案资源管理器中右键点击 $Target.exe" -ForegroundColor Gray
Write-Host "   - 选择'设为启动项'" -ForegroundColor Gray
Write-Host "   - 按F5开始调试，或Ctrl+F5开始执行(不调试)" -ForegroundColor Gray
Write-Host "   - 调试 → 性能探查器 (Alt+F2) 进行性能分析" -ForegroundColor Gray
Write-Host ""
Write-Host "2. 使用VSCode:" -ForegroundColor White
Write-Host "   - 按F5启动调试" -ForegroundColor Gray
Write-Host "   - 选择'Debug $Target'配置" -ForegroundColor Gray
Write-Host ""
Write-Host "3. 直接运行:" -ForegroundColor White
Write-Host "   - .\\$ExePath" -ForegroundColor Gray
Write-Host ""
Write-Host "💡 提示:" -ForegroundColor Cyan
Write-Host "   - 使用 -CreateVSConfig 参数创建Visual Studio配置文件" -ForegroundColor Gray
Write-Host "   - 使用 -OpenVS 参数自动打开Visual Studio" -ForegroundColor Gray
Write-Host "   - 使用 -Target <程序名> 指定要调试的程序" -ForegroundColor Gray
Write-Host "   - 可用程序: $($AvailableTargets -join ', ')" -ForegroundColor Gray
Write-Host ""

# 创建Visual Studio配置文件
if ($CreateVSConfig) {
    Write-Host "📝 创建Visual Studio配置文件..." -ForegroundColor Cyan

    # 创建.vs文件夹
    $VSFolder = ".vs"
    if (-not (Test-Path $VSFolder)) {
        New-Item -ItemType Directory -Path $VSFolder -Force | Out-Null
    }

    # 创建launch.vs.json配置文件
    $LaunchConfig = @{
        version = "0.2.1"
        defaults = @{}
        configurations = @()
    }

    # 为每个目标程序创建配置
    foreach ($TargetName in $AvailableTargets) {
        $DebugConfig = @{
            type = "default"
            project = "Cargo.toml"
            projectTarget = "$TargetName.exe"
            name = "Debug $TargetName"
            currentDir = "`${workspaceRoot}"
            args = @()
            env = @{
                RUST_BACKTRACE = "1"
                RUST_LOG = "debug"
            }
        }

        $ReleaseConfig = @{
            type = "default"
            project = "Cargo.toml"
            projectTarget = "$TargetName.exe"
            name = "Release $TargetName"
            currentDir = "`${workspaceRoot}"
            args = @()
            env = @{
                RUST_BACKTRACE = "1"
                RUST_LOG = "info"
            }
        }

        $LaunchConfig.configurations += $DebugConfig, $ReleaseConfig
    }

    $LaunchConfigJson = $LaunchConfig | ConvertTo-Json -Depth 10
    $LaunchConfigPath = "$VSFolder\launch.vs.json"
    $LaunchConfigJson | Out-File -FilePath $LaunchConfigPath -Encoding UTF8

    Write-Host "✅ 已创建 $LaunchConfigPath" -ForegroundColor Green
}

# 可选：打开Visual Studio
if ($OpenVS) {
    Write-Host "🚀 正在启动Visual Studio..." -ForegroundColor Cyan
    try {
        # 尝试使用devenv命令打开
        $VSPath = Get-Command devenv -ErrorAction SilentlyContinue
        if ($VSPath) {
            Start-Process -FilePath "devenv" -ArgumentList "." -NoNewWindow
            Write-Host "✅ Visual Studio已启动" -ForegroundColor Green
        } else {
            # 尝试查找Visual Studio安装路径
            $VSWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
            if (Test-Path $VSWhere) {
                $VSInstallPath = & $VSWhere -latest -property installationPath
                if ($VSInstallPath) {
                    $DevEnvPath = Join-Path $VSInstallPath "Common7\IDE\devenv.exe"
                    if (Test-Path $DevEnvPath) {
                        Start-Process -FilePath $DevEnvPath -ArgumentList "." -NoNewWindow
                        Write-Host "✅ Visual Studio已启动" -ForegroundColor Green
                    } else {
                        Write-Host "💡 请手动打开Visual Studio并选择'打开文件夹'" -ForegroundColor Yellow
                        explorer .
                    }
                } else {
                    Write-Host "💡 请手动打开Visual Studio并选择'打开文件夹'" -ForegroundColor Yellow
                    explorer .
                }
            } else {
                Write-Host "💡 请手动打开Visual Studio并选择'打开文件夹'" -ForegroundColor Yellow
                explorer .
            }
        }
    } catch {
        Write-Host "⚠️ 无法自动启动Visual Studio: $_" -ForegroundColor Yellow
        Write-Host "💡 请手动打开Visual Studio" -ForegroundColor Cyan
    }
}

Write-Host ""
Write-Host "📚 更多信息请参考: docs\Visual Studio内存调试指南.md" -ForegroundColor Cyan
