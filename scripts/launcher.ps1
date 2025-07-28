# 币安K线系统统一启动器
# 提供多种服务的启动选项

param(
    [string]$Service = "",
    [string]$LogLevel = "info",
    [string]$ConfigPath = "config\BinanceKlineConfig.toml"
)

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 获取脚本所在目录的父目录（项目根目录）
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

function Show-Menu {
    Write-Host ""
    Write-Host "🚀 币安K线系统统一启动器" -ForegroundColor Green
    Write-Host "================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 K线聚合服务:" -ForegroundColor Cyan
    Write-Host "  1. 标准版K线聚合服务" -ForegroundColor White
    Write-Host "  2. SIMD优化版K线聚合服务" -ForegroundColor White
    Write-Host ""
    Write-Host "🔍 数据稽核服务:" -ForegroundColor Cyan
    Write-Host "  3. 数据稽核服务（生产模式）" -ForegroundColor White
    Write-Host "  4. 数据稽核服务（测试模式）" -ForegroundColor White
    Write-Host "  5. 单次数据稽核" -ForegroundColor White
    Write-Host ""
    Write-Host "🛠️  开发工具:" -ForegroundColor Cyan
    Write-Host "  6. VS Code调试模式" -ForegroundColor White
    Write-Host "  7. 性能对比测试" -ForegroundColor White
    Write-Host ""
    Write-Host "  0. 退出" -ForegroundColor Red
    Write-Host ""
}

function Start-StandardKlineService {
    Write-Host "🚀 启动标准版K线聚合服务..." -ForegroundColor Green
    & "$ProjectRoot\scripts\start_standard.ps1" -LogLevel $LogLevel -ConfigPath $ConfigPath
}

function Start-SimdKlineService {
    Write-Host "🚀 启动SIMD优化版K线聚合服务..." -ForegroundColor Green
    & "$ProjectRoot\scripts\start_simd.ps1" -LogLevel $LogLevel -ConfigPath $ConfigPath
}

function Start-DataAuditService {
    Write-Host "🚀 启动数据稽核服务（生产模式）..." -ForegroundColor Green
    & "$ProjectRoot\scripts\start_data_audit_quick.ps1"
}

function Start-DataAuditTest {
    Write-Host "🚀 启动数据稽核服务（测试模式）..." -ForegroundColor Green
    & "$ProjectRoot\scripts\start_data_audit_test.ps1"
}

function Start-DataAuditOnce {
    Write-Host "🚀 执行单次数据稽核..." -ForegroundColor Green
    & "$ProjectRoot\scripts\start_data_audit.ps1" -Mode once -Symbols "BTCUSDT,ETHUSDT" -Intervals "1m,5m" -AuditDurationSeconds 3600
}

function Start-VsDebug {
    Write-Host "🚀 启动VS Code调试模式..." -ForegroundColor Green
    & "$ProjectRoot\scripts\vs_debug_with_console.ps1"
}

function Start-PerformanceComparison {
    Write-Host "🚀 启动性能对比测试..." -ForegroundColor Green
    & "$ProjectRoot\scripts\performance_comparison.ps1"
}

# 如果指定了服务参数，直接启动对应服务
if ($Service -ne "") {
    switch ($Service.ToLower()) {
        "standard" { Start-StandardKlineService; return }
        "simd" { Start-SimdKlineService; return }
        "audit" { Start-DataAuditService; return }
        "audit-test" { Start-DataAuditTest; return }
        "audit-once" { Start-DataAuditOnce; return }
        "debug" { Start-VsDebug; return }
        "perf" { Start-PerformanceComparison; return }
        default {
            Write-Host "❌ 未知的服务类型: $Service" -ForegroundColor Red
            Write-Host "💡 可用的服务类型: standard, simd, audit, audit-test, audit-once, debug, perf" -ForegroundColor Yellow
            return
        }
    }
}

# 交互式菜单
while ($true) {
    Show-Menu
    $choice = Read-Host "请选择要启动的服务 (0-7)"
    
    switch ($choice) {
        "1" { Start-StandardKlineService; break }
        "2" { Start-SimdKlineService; break }
        "3" { Start-DataAuditService; break }
        "4" { Start-DataAuditTest; break }
        "5" { Start-DataAuditOnce; break }
        "6" { Start-VsDebug; break }
        "7" { Start-PerformanceComparison; break }
        "0" { 
            Write-Host "👋 再见！" -ForegroundColor Green
            break 
        }
        default { 
            Write-Host "❌ 无效选择，请输入 0-7" -ForegroundColor Red
            Start-Sleep -Seconds 1
        }
    }
}
