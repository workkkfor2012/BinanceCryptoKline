# 配置读取函数
# 用于从aggregate_config.toml文件中读取日志配置

function Read-LoggingConfig {
    <#
    .SYNOPSIS
    从配置文件读取日志配置
    
    .DESCRIPTION
    读取config/aggregate_config.toml文件中的日志配置，返回包含LogLevel、LogTransport和PipeName的对象
    
    .OUTPUTS
    PSCustomObject 包含日志配置的对象
    #>
    
    $configPath = "config\aggregate_config.toml"
    
    if (-not (Test-Path $configPath)) {
        Write-Warning "配置文件不存在: $configPath，使用默认配置"
        return @{
            LogLevel = "trace"
            LogTransport = "file"
            PipeName = "\\.\pipe\kline_log_pipe"
        }
    }
    
    try {
        $configContent = Get-Content $configPath -Raw -Encoding UTF8
        
        # 解析日志级别
        $logLevel = if ($configContent -match 'log_level\s*=\s*"(.+?)"') { 
            $matches[1] 
        } else { 
            "trace" 
        }
        
        # 解析日志传输方式
        $logTransport = if ($configContent -match 'log_transport\s*=\s*"(.+?)"') { 
            $matches[1] 
        } else { 
            "file" 
        }
        
        # 解析管道名称
        $pipeName = if ($configContent -match 'pipe_name\s*=\s*"(.+?)"') { 
            # 确保管道名称格式正确
            $rawPipeName = $matches[1]
            if ($rawPipeName -notmatch '^\\\\\.\\pipe\\') {
                "\\.\pipe\$rawPipeName"
            } else {
                $rawPipeName
            }
        } else { 
            "\\.\pipe\kline_log_pipe" 
        }
        
        return @{
            LogLevel = $logLevel
            LogTransport = $logTransport
            PipeName = $pipeName
        }
    }
    catch {
        Write-Error "读取配置文件失败: $_"
        return @{
            LogLevel = "trace"
            LogTransport = "file"
            PipeName = "\\.\pipe\kline_log_pipe"
        }
    }
}

function Read-WebLogConfig {
    <#
    .SYNOPSIS
    从WebLog配置文件读取日志配置

    .DESCRIPTION
    读取src/weblog/config/logging_config.toml文件中的WebLog日志配置，返回包含LogLevel的对象

    .OUTPUTS
    PSCustomObject 包含WebLog日志配置的对象
    #>

    $configPath = "src\weblog\config\logging_config.toml"

    if (-not (Test-Path $configPath)) {
        Write-Warning "WebLog配置文件不存在: $configPath，使用默认配置"
        return @{
            LogLevel = "info"
        }
    }

    try {
        $configContent = Get-Content $configPath -Raw -Encoding UTF8

        # 解析WebLog日志级别 - 查找[weblog]部分的log_level
        $logLevel = "info"  # 默认值
        $inWeblogSection = $false

        foreach ($line in ($configContent -split "`n")) {
            $line = $line.Trim()
            if ($line -eq "[weblog]") {
                $inWeblogSection = $true
            } elseif ($line.StartsWith("[") -and $line -ne "[weblog]") {
                $inWeblogSection = $false
            } elseif ($inWeblogSection -and $line -match 'log_level\s*=\s*"(.+?)"') {
                $logLevel = $matches[1]
                break
            }
        }

        return @{
            LogLevel = $logLevel
        }
    }
    catch {
        Write-Error "读取WebLog配置文件失败: $_"
        return @{
            LogLevel = "info"
        }
    }
}

function Set-LoggingEnvironment {
    <#
    .SYNOPSIS
    设置日志环境变量
    
    .DESCRIPTION
    根据提供的日志配置对象设置相应的环境变量
    
    .PARAMETER LoggingConfig
    包含日志配置的对象
    #>
    param(
        [Parameter(Mandatory=$true)]
        [hashtable]$LoggingConfig
    )
    
    try {
        # 设置环境变量
        $env:RUST_LOG = $LoggingConfig.LogLevel
        $env:LOG_TRANSPORT = $LoggingConfig.LogTransport
        $env:PIPE_NAME = $LoggingConfig.PipeName
        
        Write-Host "✅ 环境变量设置完成" -ForegroundColor Green
        Write-Host "  日志级别: $($LoggingConfig.LogLevel)" -ForegroundColor Gray
        Write-Host "  传输方式: $($LoggingConfig.LogTransport)" -ForegroundColor Gray
        Write-Host "  管道名称: $($LoggingConfig.PipeName)" -ForegroundColor Gray
    }
    catch {
        Write-Error "设置环境变量失败: $_"
    }
}

# 函数已定义，可以直接使用
