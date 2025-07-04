# 日志清理脚本
# 专门用于清理K线程序的日志文件

param(
    [switch]$All,        # 清理所有日志文件和目录
    [switch]$Confirm,    # 需要确认才执行
    [switch]$Verbose     # 详细输出
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🧹 K线程序日志清理工具" -ForegroundColor Green
Write-Host ""

# 定义需要清理的文件
$MainLogFiles = @(
    "logs\problem_summary.log",
    "logs\ai_detailed.log"
)

# 定义可选清理的文件
$OptionalLogFiles = @(
    "logs\problem_summary.txt"
)

# 定义需要清理的目录
$LogDirectories = @(
    "logs\debug_snapshots",
    "logs\transaction_log"
)

# 统计信息
$totalFiles = 0
$totalDirs = 0
$cleanedFiles = 0
$cleanedDirs = 0

# 显示将要清理的内容
Write-Host "📋 将要清理的内容:" -ForegroundColor Cyan

Write-Host "  🔸 主要日志文件:" -ForegroundColor Yellow
foreach ($file in $MainLogFiles) {
    $exists = Test-Path $file
    $status = if ($exists) { "存在" } else { "不存在" }
    $color = if ($exists) { "Green" } else { "Gray" }
    Write-Host "    $file ($status)" -ForegroundColor $color
    if ($exists) { $totalFiles++ }
}

if ($All) {
    Write-Host "  🔸 可选日志文件:" -ForegroundColor Yellow
    foreach ($file in $OptionalLogFiles) {
        $exists = Test-Path $file
        $status = if ($exists) { "存在" } else { "不存在" }
        $color = if ($exists) { "Green" } else { "Gray" }
        Write-Host "    $file ($status)" -ForegroundColor $color
        if ($exists) { $totalFiles++ }
    }
    
    Write-Host "  🔸 日志目录:" -ForegroundColor Yellow
    foreach ($dir in $LogDirectories) {
        if (Test-Path $dir) {
            $files = Get-ChildItem $dir -File -Recurse
            $fileCount = $files.Count
            Write-Host "    $dir ($fileCount 个文件)" -ForegroundColor Green
            $totalDirs++
            $totalFiles += $fileCount
        } else {
            Write-Host "    $dir (不存在)" -ForegroundColor Gray
        }
    }
}

Write-Host ""
Write-Host "📊 统计: 将清理 $totalFiles 个文件" -ForegroundColor Cyan
if ($All) {
    Write-Host "       将清理 $totalDirs 个目录中的文件" -ForegroundColor Cyan
}

# 确认清理
if ($Confirm -and $totalFiles -gt 0) {
    Write-Host ""
    Write-Host "⚠️ 确认清理操作" -ForegroundColor Yellow
    $response = Read-Host "是否继续清理? (y/N)"
    if ($response -ne 'y' -and $response -ne 'Y') {
        Write-Host "❌ 操作已取消" -ForegroundColor Red
        exit 0
    }
}

if ($totalFiles -eq 0) {
    Write-Host "✅ 没有需要清理的文件" -ForegroundColor Green
    exit 0
}

Write-Host ""
Write-Host "🚀 开始清理..." -ForegroundColor Green

# 清理主要日志文件
foreach ($file in $MainLogFiles) {
    if (Test-Path $file) {
        try {
            Remove-Item $file -Force
            Write-Host "  ✅ 已删除: $file" -ForegroundColor Green
            $cleanedFiles++
        } catch {
            Write-Host "  ❌ 删除失败: $file - $($_.Exception.Message)" -ForegroundColor Red
        }
    } else {
        if ($Verbose) {
            Write-Host "  ℹ️ 文件不存在: $file" -ForegroundColor Gray
        }
    }
}

# 清理可选文件（仅在-All模式下）
if ($All) {
    foreach ($file in $OptionalLogFiles) {
        if (Test-Path $file) {
            try {
                Remove-Item $file -Force
                Write-Host "  ✅ 已删除: $file" -ForegroundColor Green
                $cleanedFiles++
            } catch {
                Write-Host "  ❌ 删除失败: $file - $($_.Exception.Message)" -ForegroundColor Red
            }
        } else {
            if ($Verbose) {
                Write-Host "  ℹ️ 文件不存在: $file" -ForegroundColor Gray
            }
        }
    }
    
    # 清理目录中的文件
    foreach ($dir in $LogDirectories) {
        if (Test-Path $dir) {
            try {
                $files = Get-ChildItem $dir -File -Recurse
                if ($files.Count -gt 0) {
                    $files | Remove-Item -Force
                    Write-Host "  🗂️ 已清理目录: $dir ($($files.Count) 个文件)" -ForegroundColor Green
                    $cleanedFiles += $files.Count
                    $cleanedDirs++
                } else {
                    if ($Verbose) {
                        Write-Host "  ℹ️ 目录为空: $dir" -ForegroundColor Gray
                    }
                }
            } catch {
                Write-Host "  ❌ 清理目录失败: $dir - $($_.Exception.Message)" -ForegroundColor Red
            }
        } else {
            if ($Verbose) {
                Write-Host "  ℹ️ 目录不存在: $dir" -ForegroundColor Gray
            }
        }
    }
}

Write-Host ""
Write-Host "✅ 清理完成!" -ForegroundColor Green
Write-Host "📊 清理统计: $cleanedFiles 个文件" -ForegroundColor Cyan
if ($All -and $cleanedDirs -gt 0) {
    Write-Host "           $cleanedDirs 个目录已清理" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "💡 使用提示:" -ForegroundColor Yellow
Write-Host "  清理所有日志: .\scripts\clean_logs.ps1 -All" -ForegroundColor Gray
Write-Host "  需要确认清理: .\scripts\clean_logs.ps1 -Confirm" -ForegroundColor Gray
Write-Host "  详细输出模式: .\scripts\clean_logs.ps1 -Verbose" -ForegroundColor Gray
Write-Host "  启动并清理: .\scripts\start_kline_clean.ps1" -ForegroundColor Gray
