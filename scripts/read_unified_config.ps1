# 统一配置文件读取脚本
# 从 BinanceKlineConfig.toml 读取所有配置信息

function Read-UnifiedConfig {
    <#
    .SYNOPSIS
    从统一配置文件读取所有配置信息
    
    .DESCRIPTION
    读取 config/BinanceKlineConfig.toml 文件并返回配置对象
    
    .OUTPUTS
    返回包含所有配置信息的哈希表
    #>
    
    $configPath = "config\BinanceKlineConfig.toml"
    
    if (-not (Test-Path $configPath)) {
        Write-Warning "配置文件不存在: $configPath"
        return $null
    }
    
    try {
        $content = Get-Content $configPath -Raw -Encoding UTF8
        
        # 创建配置对象
        $config = @{
            Build = @{}
            Logging = @{}
            Database = @{}
            WebSocket = @{}
            Buffer = @{}
            Persistence = @{}
            # 顶层配置项
            supported_intervals = @()
            max_symbols = 0
            buffer_swap_interval_ms = 0
            persistence_interval_ms = 0
        }
        
        # 解析配置文件
        $currentSection = ""
        $currentSubSection = ""
        
        foreach ($line in $content -split "`n") {
            $line = $line.Trim()
            
            # 跳过注释和空行
            if ($line -eq "" -or $line.StartsWith("#")) {
                continue
            }
            
            # 检查是否是节标题
            if ($line -match '^\[(.+)\]$') {
                $sectionName = $matches[1]
                
                if ($sectionName -eq "build") {
                    $currentSection = "Build"
                    $currentSubSection = ""
                } elseif ($sectionName -eq "logging") {
                    $currentSection = "Logging"
                    $currentSubSection = ""
                } elseif ($sectionName -eq "logging.services") {
                    $currentSection = "Logging"
                    $currentSubSection = "Services"
                } elseif ($sectionName -eq "database") {
                    $currentSection = "Database"
                    $currentSubSection = ""
                } elseif ($sectionName -eq "websocket") {
                    $currentSection = "WebSocket"
                    $currentSubSection = ""
                } elseif ($sectionName -eq "buffer") {
                    $currentSection = "Buffer"
                    $currentSubSection = ""
                } elseif ($sectionName -eq "persistence") {
                    $currentSection = "Persistence"
                    $currentSubSection = ""
                } elseif ($sectionName -eq "weblog") {
                    $currentSection = "WebLog"
                    $currentSubSection = ""
                }
                continue
            }
            
            # 解析键值对
            if ($line -match '^([^=]+)=(.+)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                
                # 移除引号
                if ($value.StartsWith('"') -and $value.EndsWith('"')) {
                    $value = $value.Substring(1, $value.Length - 2)
                } elseif ($value.StartsWith("'") -and $value.EndsWith("'")) {
                    $value = $value.Substring(1, $value.Length - 2)
                }
                
                # 处理数组
                if ($value.StartsWith("[") -and $value.EndsWith("]")) {
                    $arrayContent = $value.Substring(1, $value.Length - 2)
                    $value = @()
                    foreach ($item in $arrayContent -split ",") {
                        $item = $item.Trim()
                        if ($item.StartsWith('"') -and $item.EndsWith('"')) {
                            $item = $item.Substring(1, $item.Length - 2)
                        }
                        $value += $item
                    }
                }
                
                # 处理布尔值
                if ($value -eq "true") {
                    $value = $true
                } elseif ($value -eq "false") {
                    $value = $false
                }
                
                # 处理数字
                if ($value -match '^\d+$') {
                    $value = [int]$value
                }
                
                # 存储到配置对象
                if ($currentSubSection -ne "") {
                    $config[$currentSection][$currentSubSection][$key] = $value
                } elseif ($currentSection -ne "") {
                    $config[$currentSection][$key] = $value
                } else {
                    # 顶层配置项
                    $config[$key] = $value
                }
            }
        }
        
        return $config
        
    } catch {
        Write-Error "读取配置文件失败: $_"
        return $null
    }
}

function Get-BuildMode {
    <#
    .SYNOPSIS
    获取编译模式
    
    .OUTPUTS
    返回 "debug" 或 "release"
    #>
    
    $config = Read-UnifiedConfig
    if ($config -and $config.Build.mode) {
        return $config.Build.mode
    }
    return "debug"  # 默认值
}

function Get-AuditEnabled {
    <#
    .SYNOPSIS
    获取审计功能开关状态

    .OUTPUTS
    返回 $true 或 $false
    #>

    $config = Read-UnifiedConfig
    if ($config -and $config.Build.enable_audit) {
        return $config.Build.enable_audit
    }
    return $false  # 默认值
}

function Get-CargoCommand {
    <#
    .SYNOPSIS
    根据配置生成cargo命令

    .PARAMETER BinaryName
    要运行的二进制文件名

    .PARAMETER Command
    cargo命令类型 (run 或 build)，默认为 run

    .OUTPUTS
    返回完整的cargo命令字符串
    #>
    param(
        [Parameter(Mandatory=$true)]
        [string]$BinaryName,

        [Parameter(Mandatory=$false)]
        [string]$Command = "run"
    )

    $buildMode = Get-BuildMode
    $auditEnabled = Get-AuditEnabled

    $cargoCmd = "cargo $Command"

    if ($buildMode -eq "release") {
        $cargoCmd += " --release"
    }

    if ($auditEnabled) {
        $cargoCmd += " --features full-audit"
    }

    $cargoCmd += " --bin $BinaryName"

    return $cargoCmd
}

function Get-LoggingConfig {
    <#
    .SYNOPSIS
    获取日志配置
    
    .OUTPUTS
    返回日志配置对象
    #>
    
    $config = Read-UnifiedConfig
    if ($config -and $config.Logging) {
        return $config.Logging
    }
    
    # 返回默认配置
    return @{
        log_level = "info"
        log_transport = "named_pipe"
        pipe_name = "kline_log_pipe"
    }
}

function Set-LoggingEnvironment {
    <#
    .SYNOPSIS
    设置基础环境变量（程序会直接从配置文件读取所有日志配置）

    .PARAMETER ServiceName
    服务名称（保留参数以兼容现有调用）
    #>
    param(
        [Parameter(Mandatory=$false)]
        [string]$ServiceName = "kline_aggregate_service"
    )

    # 程序会直接从 config/BinanceKlineConfig.toml 读取所有日志配置
    # 这里只需要确保配置文件存在
    if (-not (Test-Path "config\BinanceKlineConfig.toml")) {
        Write-Warning "配置文件不存在: config\BinanceKlineConfig.toml"
        Write-Warning "程序可能无法正常启动"
    } else {
        Write-Host "✅ 程序将从 config\BinanceKlineConfig.toml 读取日志配置" -ForegroundColor Green
    }
}

# 脚本模式下不需要导出函数
