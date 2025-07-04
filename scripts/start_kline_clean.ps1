# K线下载程序启动脚本 - 自动清理日志版本
# 功能: 启动前自动删除日志文件，确保每次运行都是全新的日志环境

param(
    [string]$Target = "kline_server",
    [switch]$SkipLogClean,
    [switch]$Verbose
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚀 K线下载程序启动脚本" -ForegroundColor Green
Write-Host "目标程序: $Target" -ForegroundColor Cyan
Write-Host ""

# 定义需要清理的日志文件
$LogFiles = @(
    "logs\problem_summary.log",
    "logs\ai_detailed.log"
)

# 定义可选清理的目录（如果需要完全清理）
$LogDirectories = @(
    "logs\debug_snapshots",
    "logs\transaction_log"
)

if (-not $SkipLogClean) {
    Write-Host "🧹 清理日志文件..." -ForegroundColor Yellow
    
    $cleanedFiles = 0
    $cleanedDirs = 0
    
    # 清理主要日志文件
    foreach ($logFile in $LogFiles) {
        if (Test-Path $logFile) {
            try {
                Remove-Item $logFile -Force
                Write-Host "  ✅ 已删除: $logFile" -ForegroundColor Green
                $cleanedFiles++
            } catch {
                Write-Host "  ❌ 删除失败: $logFile - $($_.Exception.Message)" -ForegroundColor Red
            }
        } else {
            if ($Verbose) {
                Write-Host "  ℹ️ 文件不存在: $logFile" -ForegroundColor Gray
            }
        }
    }
    
    # 可选：清理日志目录中的文件（保留目录结构）
    foreach ($logDir in $LogDirectories) {
        if (Test-Path $logDir) {
            try {
                $files = Get-ChildItem $logDir -File
                if ($files.Count -gt 0) {
                    $files | Remove-Item -Force
                    Write-Host "  🗂️ 已清理目录: $logDir ($($files.Count) 个文件)" -ForegroundColor Green
                    $cleanedDirs++
                } else {
                    if ($Verbose) {
                        Write-Host "  ℹ️ 目录为空: $logDir" -ForegroundColor Gray
                    }
                }
            } catch {
                Write-Host "  ❌ 清理目录失败: $logDir - $($_.Exception.Message)" -ForegroundColor Red
            }
        } else {
            if ($Verbose) {
                Write-Host "  ℹ️ 目录不存在: $logDir" -ForegroundColor Gray
            }
        }
    }
    
    Write-Host "📊 清理完成: $cleanedFiles 个文件, $cleanedDirs 个目录" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host "⏭️ 跳过日志清理 (使用了 -SkipLogClean 参数)" -ForegroundColor Yellow
    Write-Host ""
}

# 确保logs目录存在
if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" -Force | Out-Null
    Write-Host "📁 创建logs目录" -ForegroundColor Green
}

Write-Host "🎯 启动K线下载程序..." -ForegroundColor Green
Write-Host "命令: cargo run --bin $Target" -ForegroundColor Gray
Write-Host ""

# 启动程序
try {
    # 使用cargo run启动程序
    cargo run --bin $Target
} catch {
    Write-Host "❌ 程序启动失败: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ 程序执行完成" -ForegroundColor Green

# 显示生成的日志文件信息
Write-Host ""
Write-Host "📋 生成的日志文件:" -ForegroundColor Cyan
foreach ($logFile in $LogFiles) {
    if (Test-Path $logFile) {
        $fileInfo = Get-Item $logFile
        $sizeKB = [math]::Round($fileInfo.Length / 1KB, 2)
        Write-Host "  📄 $logFile ($sizeKB KB)" -ForegroundColor Green
    } else {
        Write-Host "  ⚪ $logFile (未生成)" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "💡 使用提示:" -ForegroundColor Yellow
Write-Host "  查看问题摘要: Get-Content logs\problem_summary.log" -ForegroundColor Gray
Write-Host "  查看详细日志: Get-Content logs\ai_detailed.log" -ForegroundColor Gray
Write-Host "  跳过清理启动: .\scripts\start_kline_clean.ps1 -SkipLogClean" -ForegroundColor Gray
Write-Host "  详细输出模式: .\scripts\start_kline_clean.ps1 -Verbose" -ForegroundColor Gray
