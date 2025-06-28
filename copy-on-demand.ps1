<#
.SYNOPSIS
    按需复制指定的源文件到目标文件夹，并转换扩展名。
    这个脚本被设计为由 watchexec 等外部工具调用。
#>

# --- 配置区 ---
# 目标文件夹名称
$targetFolderName = "tempfold"

# 定义要复制的源文件列表（按功能分组）

# 公共组件
$commonFiles = @(
    "src\klcommon\api.rs",
    "src\klcommon\context.rs",
    "src\klcommon\db.rs",
    "src\klcommon\websocket.rs"
)

# K线下载相关
$klineDownloadFiles = @(
    "src\bin\kline_data_service.rs",
    "src\kldata\backfill.rs"
)

# K线合成相关
$klineAggregateFiles = @(
    "src\bin\kline_aggregate_service.rs",
    "src\klaggregate\buffered_kline_store.rs",
    "src\klaggregate\kline_data_persistence.rs",
    "src\klaggregate\market_data_ingestor.rs",
    "src\klaggregate\symbol_kline_aggregator.rs",
    "src\klaggregate\symbol_metadata_registry.rs",
    "src\klaggregate\trade_event_router.rs"
)

# 日志和观测性组件
$observabilityFiles = @(
    "src\klcommon\log\trace_distiller.rs",
    "src\klcommon\log\trace_visualization.rs",
    "src\klcommon\log\module_logging.rs",
    "src\klcommon\log\transaction_logging.rs",
   
    "src\klcommon\log\observability.rs"
)
# 单独定义HTML文件
$htmlFilePath = "src\weblog\static\index.html"

# 定义日志快照目录
$logSnapshotDir = "logs\debug_snapshots"

# 定义事务日志目录
$transactionLogDir = "logs\transaction_log"

# --- 核心逻辑 ---

# 获取脚本所在的当前路径，确保路径解析正确
$currentPath = Get-Location
$targetFolderPath = Join-Path -Path $currentPath -ChildPath $targetFolderName

# 获取最新日志文件的函数
function Get-LatestLogFile {
    param(
        [string]$LogDirectory,
        [string]$FilePattern = "*.log"
    )
    if (Test-Path -Path $LogDirectory -PathType Container) {
        $latestLog = Get-ChildItem -Path $LogDirectory -Filter $FilePattern |
                     Sort-Object LastWriteTime -Descending |
                     Select-Object -First 1
        if ($latestLog) {
            return $latestLog.FullName
        }
    }
    return $null
}

# 获取多个最新日志文件的函数
function Get-LatestLogFiles {
    param(
        [string]$LogDirectory,
        [string]$FilePattern = "*.log",
        [int]$Count = 3
    )
    if (Test-Path -Path $LogDirectory -PathType Container) {
        $latestLogs = Get-ChildItem -Path $LogDirectory -Filter $FilePattern |
                      Sort-Object LastWriteTime -Descending |
                      Select-Object -First $Count
        if ($latestLogs) {
            return $latestLogs.FullName
        }
    }
    return @()
}

# 复制文件的函数
function Copy-FileToTxt {
    param(
        [string]$SourcePath,
        [string]$TargetFolder,
        [string]$SubFolder = ""
    )
    # 检查源文件是否存在
    if (Test-Path -Path $SourcePath -PathType Leaf) {
        $fileName = [System.IO.Path]::GetFileNameWithoutExtension($SourcePath)
        $extension = [System.IO.Path]::GetExtension($SourcePath)

        # 根据扩展名智能重命名
        if ($extension -eq ".rs") {
            $newFileName = "$fileName.txt"
        } elseif ($extension -eq ".html") {
            $newFileName = "index.html" # 保持HTML文件名
        } elseif ($extension -eq ".log") {
            $newFileName = "$fileName.txt" # 将.log文件转换为.txt
        } else {
            $newFileName = "$fileName$extension" # 其他文件保持原样
        }

        # 如果指定了子文件夹，则创建子文件夹路径
        if ($SubFolder) {
            $finalTargetFolder = Join-Path -Path $TargetFolder -ChildPath $SubFolder
            # 确保子文件夹存在
            if (-not (Test-Path -Path $finalTargetFolder -PathType Container)) {
                New-Item -Path $finalTargetFolder -ItemType Directory -Force | Out-Null
            }
        } else {
            $finalTargetFolder = $TargetFolder
        }

        $destinationPath = Join-Path -Path $finalTargetFolder -ChildPath $newFileName
        try {
            Copy-Item -Path $SourcePath -Destination $destinationPath -Force -ErrorAction Stop
            # 使用 $SourcePath 原始路径，而不是处理后的新文件名，让日志更清晰
            $timestamp = Get-Date -Format "HH:mm:ss"
            $displayPath = if ($SubFolder) { "$SubFolder\$newFileName" } else { $newFileName }
            Write-Host "  ✅ 已同步: $SourcePath -> $displayPath [$timestamp]" -ForegroundColor Green
        } catch {
            $timestamp = Get-Date -Format "HH:mm:ss"
            Write-Host "  ❌ 复制失败: $SourcePath -> $($_.Exception.Message) [$timestamp]" -ForegroundColor Red
        }
    } else {
        # 如果文件在列表中但不存在，也给出提示
        $timestamp = Get-Date -Format "HH:mm:ss"
        Write-Host "  ⚠️ 未找到源文件: $SourcePath [$timestamp]" -ForegroundColor Yellow
    }
}

# 确保目标文件夹存在
if (-not (Test-Path -Path $targetFolderPath -PathType Container)) {
    New-Item -Path $targetFolderPath -ItemType Directory | Out-Null
}

Write-Host "📁 开始按功能分组复制文件到子文件夹..." -ForegroundColor Cyan

# 复制公共组件文件到 common 子文件夹
Write-Host "📂 复制公共组件文件..." -ForegroundColor Yellow
foreach ($file in $commonFiles) {
    Copy-FileToTxt -SourcePath $file -TargetFolder $targetFolderPath -SubFolder "common"
}

# 复制K线下载文件到 kline_download 子文件夹（可选）
# Write-Host "📂 复制K线下载文件..." -ForegroundColor Yellow
foreach ($file in $klineDownloadFiles) {
    Copy-FileToTxt -SourcePath $file -TargetFolder $targetFolderPath -SubFolder "kline_download"
}

# 复制K线合成文件到 kline_aggregate 子文件夹
Write-Host "📂 复制K线合成文件..." -ForegroundColor Yellow
foreach ($file in $klineAggregateFiles) {
    Copy-FileToTxt -SourcePath $file -TargetFolder $targetFolderPath -SubFolder "kline_aggregate"
}

# 复制日志和观测性文件到 observability 子文件夹
Write-Host "📂 复制日志和观测性文件..." -ForegroundColor Yellow
foreach ($file in $observabilityFiles) {
    Copy-FileToTxt -SourcePath $file -TargetFolder $targetFolderPath -SubFolder "observability"
}

# 复制HTML文件到 web 子文件夹（可选）
# Write-Host "📂 复制Web文件..." -ForegroundColor Yellow
# Copy-FileToTxt -SourcePath $htmlFilePath -TargetFolder $targetFolderPath -SubFolder "web"

# 复制最新的调试快照日志文件到 logs 子文件夹
Write-Host "📂 复制调试快照日志..." -ForegroundColor Yellow
$latestLogFile = Get-LatestLogFile -LogDirectory $logSnapshotDir
if ($latestLogFile) {
    Copy-FileToTxt -SourcePath $latestLogFile -TargetFolder $targetFolderPath -SubFolder "logs"
}

# 复制最新的事务日志文件到 logs 子文件夹（支持多种文件格式）
Write-Host "� 复制事务日志文件..." -ForegroundColor Yellow

# 检查 .log 文件
$latestTransactionLog = Get-LatestLogFile -LogDirectory $transactionLogDir -FilePattern "*.log"
if ($latestTransactionLog) {
    $timestamp = Get-Date -Format "HH:mm:ss"
    Write-Host "  📄 发现最新事务日志: $(Split-Path $latestTransactionLog -Leaf) [$timestamp]" -ForegroundColor Cyan
    Copy-FileToTxt -SourcePath $latestTransactionLog -TargetFolder $targetFolderPath -SubFolder "logs"
}

# 检查 .json 文件
$latestTransactionJson = Get-LatestLogFile -LogDirectory $transactionLogDir -FilePattern "*.json"
if ($latestTransactionJson) {
    $timestamp = Get-Date -Format "HH:mm:ss"
    Write-Host "  📄 发现最新事务JSON: $(Split-Path $latestTransactionJson -Leaf) [$timestamp]" -ForegroundColor Cyan
    Copy-FileToTxt -SourcePath $latestTransactionJson -TargetFolder $targetFolderPath -SubFolder "logs"
}

# 检查 .txt 文件
$latestTransactionTxt = Get-LatestLogFile -LogDirectory $transactionLogDir -FilePattern "*.txt"
if ($latestTransactionTxt) {
    $timestamp = Get-Date -Format "HH:mm:ss"
    Write-Host "  📄 发现最新事务文本: $(Split-Path $latestTransactionTxt -Leaf) [$timestamp]" -ForegroundColor Cyan
    Copy-FileToTxt -SourcePath $latestTransactionTxt -TargetFolder $targetFolderPath -SubFolder "logs"
}

# 如果没有找到任何事务日志文件，给出提示
if (-not $latestTransactionLog -and -not $latestTransactionJson -and -not $latestTransactionTxt) {
    $timestamp = Get-Date -Format "HH:mm:ss"
    if (Test-Path -Path $transactionLogDir -PathType Container) {
        Write-Host "  ⚠️ 事务日志目录存在但未找到日志文件 [$timestamp]" -ForegroundColor Yellow
    } else {
        Write-Host "  ⚠️ 事务日志目录不存在: $transactionLogDir [$timestamp]" -ForegroundColor Yellow
    }
}

Write-Host "✅ 文件分组复制完成！" -ForegroundColor Green