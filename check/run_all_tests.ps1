# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "========================================================"
Write-Host "K线比对工具测试套件"
Write-Host "========================================================"
Write-Host ""
Write-Host "请选择要运行的测试:"
Write-Host "[1] 仅测试BTC一个品种"
Write-Host "[2] 仅测试6个主要品种"
Write-Host "[3] 仅测试所有品种"
Write-Host "[4] 运行所有测试"
Write-Host "[0] 退出"
Write-Host ""

$choice = Read-Host "请输入选项 [0-4]"

switch ($choice) {
    "0" {
        Write-Host "已取消测试。"
        break
    }
    "1" {
        Write-Host ""
        Write-Host "正在运行测试1: 测试BTC一个品种..."
        & "$PSScriptRoot\run_btc_test.ps1"
        break
    }
    "2" {
        Write-Host ""
        Write-Host "正在运行测试2: 测试6个主要品种..."
        & "$PSScriptRoot\test_main_symbols.ps1"
        break
    }
    "3" {
        Write-Host ""
        Write-Host "正在运行测试3: 测试所有品种..."
        & "$PSScriptRoot\test_all_symbols.ps1"
        break
    }
    "4" {
        Write-Host ""
        Write-Host "正在运行所有测试..."
        
        Write-Host ""
        Write-Host "--------------------------------------------------------"
        Write-Host "测试1: 测试BTC一个品种"
        Write-Host "--------------------------------------------------------"
        & "$PSScriptRoot\run_btc_test.ps1"
        
        Write-Host ""
        Write-Host "--------------------------------------------------------"
        Write-Host "测试2: 测试6个主要品种"
        Write-Host "--------------------------------------------------------"
        & "$PSScriptRoot\test_main_symbols.ps1"
        
        Write-Host ""
        Write-Host "--------------------------------------------------------"
        Write-Host "测试3: 测试所有品种"
        Write-Host "--------------------------------------------------------"
        & "$PSScriptRoot\test_all_symbols.ps1"
        
        break
    }
    default {
        Write-Host "无效的选项，请重新运行脚本并选择有效的选项。"
        break
    }
}

Write-Host ""
Write-Host "测试完成。"
Write-Host "========================================================
