# Log MCP 守护进程启动脚本
# 启动 Log MCP 守护进程服务，用于接收和处理日志数据

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 导入统一配置读取脚本
. "scripts\read_unified_config.ps1"

Write-Host "🚀 启动Log MCP守护进程" -ForegroundColor Green

# 检查项目目录
if (-not (Test-Path "Cargo.toml")) {
    Write-Host "❌ 请在项目根目录运行" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

# 检查Log-MCP-Server目录
if (-not (Test-Path "src\Log-MCP-Server")) {
    Write-Host "❌ 未找到Log-MCP-Server目录" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

# 检查配置文件
if (-not (Test-Path "src\Log-MCP-Server\config.toml")) {
    Write-Host "❌ 未找到Log MCP配置文件: src\Log-MCP-Server\config.toml" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

$buildMode = Get-BuildMode
Write-Host "📋 编译模式: $buildMode" -ForegroundColor Cyan

# 获取cargo命令
$cargoCmd = if ($buildMode -eq "release") {
    "cargo run --release --bin log_mcp_daemon"
} else {
    "cargo run --bin log_mcp_daemon"
}

Write-Host "🔧 Cargo命令: $cargoCmd" -ForegroundColor Yellow

# 启动Log MCP守护进程
Write-Host "🚀 启动Log MCP守护进程..." -ForegroundColor Green
Write-Host "📡 服务将监听命名管道和HTTP查询接口" -ForegroundColor Cyan
Write-Host "💡 按 Ctrl+C 停止服务" -ForegroundColor Yellow
Write-Host ""

try {
    # 切换到Log-MCP-Server目录
    Push-Location "src\Log-MCP-Server"
    
    # 启动守护进程
    Invoke-Expression $cargoCmd
    
} catch {
    Write-Host "❌ 启动失败: $_" -ForegroundColor Red
    Write-Host "错误详情: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    # 恢复原目录
    Pop-Location
    Write-Host "✅ Log MCP守护进程已退出" -ForegroundColor Green
    Read-Host "按任意键退出"
}
