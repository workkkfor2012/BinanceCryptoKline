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
    "src\klcommon\log\observability.rs"
)
# 单独定义HTML文件
$htmlFilePath = "src\weblog\static\index.html"

# 定义日志快照目录
$logSnapshotDir = "logs\debug_snapshots"

# --- 核心逻辑 ---

# 获取脚本所在的当前路径，确保路径解析正确
$currentPath = Get-Location
$targetFolderPath = Join-Path -Path $currentPath -ChildPath $targetFolderName

# 获取最新日志文件的函数
function Get-LatestLogFile {
    param(
        [string]$LogDirectory
    )
    if (Test-Path -Path $LogDirectory -PathType Container) {
        $latestLog = Get-ChildItem -Path $LogDirectory -Filter "*.log" |
                     Sort-Object LastWriteTime -Descending |
                     Select-Object -First 1
        if ($latestLog) {
            return $latestLog.FullName
        }
    }
    return $null
}

# 复制文件的函数
function Copy-FileToTxt {
    param(
        [string]$SourcePath,
        [string]$TargetFolder
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

        $destinationPath = Join-Path -Path $TargetFolder -ChildPath $newFileName
        try {
            Copy-Item -Path $SourcePath -Destination $destinationPath -Force -ErrorAction Stop
            # 使用 $SourcePath 原始路径，而不是处理后的新文件名，让日志更清晰
            $timestamp = Get-Date -Format "HH:mm:ss"
            Write-Host "  ✅ 已同步: $SourcePath [$timestamp]" -ForegroundColor Green
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

# 合并所有要复制的文件（可以选择性注释掉不需要的组）
$allFilesToCopy = @()

# 添加公共组件文件
$allFilesToCopy += $commonFiles

# 添加K线下载文件
$allFilesToCopy += $klineDownloadFiles

# 添加K线合成文件
#$allFilesToCopy += $klineAggregateFiles

# 添加日志和观测性文件
$allFilesToCopy += $observabilityFiles

# 添加HTML文件
#$allFilesToCopy += $htmlFilePath

# 遍历并复制所有文件
foreach ($file in $allFilesToCopy) {
    Copy-FileToTxt -SourcePath $file -TargetFolder $targetFolderPath
}

# 复制最新的日志文件
$latestLogFile = Get-LatestLogFile -LogDirectory $logSnapshotDir
if ($latestLogFile) {
    Copy-FileToTxt -SourcePath $latestLogFile -TargetFolder $targetFolderPath
}