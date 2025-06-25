#!/usr/bin/env pwsh
# -*- coding: utf-8 -*-
# 修复日志宏中的target语法错误

Write-Host "🔧 修复日志宏中的target语法错误" -ForegroundColor Green

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 切换到项目根目录
Set-Location -Path "F:\work\github\BinanceCryptoKline"

# 需要修复的文件列表
$files = @(
    "src/kldata/backfill.rs"
)

foreach ($file in $files) {
    Write-Host "修复文件: $file" -ForegroundColor Cyan
    
    if (Test-Path $file) {
        $content = Get-Content $file -Raw
        
        # 只替换日志宏中的target:，不影响#[instrument]中的target:
        # 匹配模式：(info!|warn!|error!|debug!)(...target: 替换为 target =
        $patterns = @(
            @{ Pattern = '(info!\([^)]*?)target:'; Replacement = '$1target =' },
            @{ Pattern = '(warn!\([^)]*?)target:'; Replacement = '$1target =' },
            @{ Pattern = '(error!\([^)]*?)target:'; Replacement = '$1target =' },
            @{ Pattern = '(debug!\([^)]*?)target:'; Replacement = '$1target =' }
        )
        
        $modified = $false
        foreach ($pattern in $patterns) {
            $newContent = $content -replace $pattern.Pattern, $pattern.Replacement
            if ($newContent -ne $content) {
                $content = $newContent
                $modified = $true
            }
        }
        
        if ($modified) {
            Set-Content $file $content -Encoding UTF8
            Write-Host "  ✅ 已修复" -ForegroundColor Green
        } else {
            Write-Host "  ⚠️  无需修复" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  ❌ 文件不存在" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "🧪 检查编译状态..." -ForegroundColor Cyan
$compileResult = cargo check 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ 编译成功" -ForegroundColor Green
} else {
    Write-Host "❌ 编译失败，需要手动修复剩余错误:" -ForegroundColor Red
    Write-Host $compileResult
}

Write-Host ""
Write-Host "🎉 target语法修复完成!" -ForegroundColor Green
