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
            Logging = @{
                Services = @{}
            }
            Database = @{}
            WebSocket = @{}
            Buffer = @{}
            Persistence = @{}
            WebLog = @{}
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
                } else {
                    $config[$currentSection][$key] = $value
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
    
    if ($buildMode -eq "release") {
        return "cargo $Command --release --bin $BinaryName"
    } else {
        return "cargo $Command --bin $BinaryName"
    }
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
        default_log_level = "info"
        log_transport = "named_pipe"
        pipe_name = "kline_log_pipe"
        Services = @{
            weblog = "info"
            kline_data_service = "info"
            kline_aggregate_service = "info"
        }
    }
}

function Set-LoggingEnvironment {
    <#
    .SYNOPSIS
    设置日志相关的环境变量
    
    .PARAMETER ServiceName
    服务名称，用于获取特定的日志级别
    #>
    param(
        [Parameter(Mandatory=$false)]
        [string]$ServiceName = "kline_aggregate_service"
    )
    
    $loggingConfig = Get-LoggingConfig
    
    # 设置传输方式
    $env:LOG_TRANSPORT = $loggingConfig.log_transport
    
    # 设置管道名称
    if ($loggingConfig.pipe_name) {
        $pipeName = $loggingConfig.pipe_name
        if (-not $pipeName.StartsWith("\\.\pipe\")) {
            $pipeName = "\\.\pipe\$pipeName"
        }
        $env:PIPE_NAME = $pipeName
    }
    
    # 设置日志级别
    if ($ServiceName -eq "weblog" -and $loggingConfig.Services -and $loggingConfig.Services.weblog) {
        $env:RUST_LOG = $loggingConfig.Services.weblog
    } else {
        # 其他服务使用默认日志级别
        $env:RUST_LOG = $loggingConfig.default_log_level
    }
}

# 导出函数
Export-ModuleMember -Function Read-UnifiedConfig, Get-BuildMode, Get-CargoCommand, Get-LoggingConfig, Set-LoggingEnvironment
