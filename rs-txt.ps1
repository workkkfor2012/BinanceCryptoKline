# Rust文件转换为txt脚本
$targetFolderName = "tempfold"

# 定义要转换的文件列表
$sourceFiles = @(
    "src\bin\kline_data_service.rs",
    "src\bin\kline_aggregate_service.rs",
    "src\kldata\backfill.rs",
    "src\klcommon\log\trace_distiller.rs",
    "src\klcommon\log\trace_visualization.rs",
    "src\klcommon\api.rs",
    "src\klcommon\db.rs",
    "src\klcommon\log\module_logging.rs",
    "src\klcommon\log\observability.rs"

)

$htmlFilePath = "src\weblog\static\index.html"
$currentPath = Get-Location

# 准备目标文件夹
$targetFolderPath = Join-Path -Path $currentPath -ChildPath $targetFolderName

if (Test-Path -Path $targetFolderPath -PathType Container) {
    Write-Host "清空目标文件夹..." -ForegroundColor Yellow
    Get-ChildItem -Path $targetFolderPath -Force | Remove-Item -Recurse -Force
} else {
    Write-Host "创建目标文件夹..." -ForegroundColor Yellow
    New-Item -Path $targetFolderPath -ItemType Directory | Out-Null
}

# 复制指定的.rs文件
$copiedCount = 0
$notFoundFiles = @()

Write-Host "开始复制指定的.rs文件..." -ForegroundColor Green

foreach ($filePath in $sourceFiles) {
    if (Test-Path -Path $filePath -PathType Leaf) {
        $fileName = [System.IO.Path]::GetFileNameWithoutExtension($filePath)
        $newFileName = "$fileName.txt"
        $destinationPath = Join-Path -Path $targetFolderPath -ChildPath $newFileName
        Copy-Item -Path $filePath -Destination $destinationPath
        Write-Host "  ✅ 复制: $filePath -> $newFileName" -ForegroundColor Green
        $copiedCount++
    } else {
        $notFoundFiles += $filePath
        Write-Host "  ❌ 未找到: $filePath" -ForegroundColor Red
    }
}

Write-Host "✅ 成功复制 $copiedCount 个.rs文件" -ForegroundColor Green
if ($notFoundFiles.Count -gt 0) {
    Write-Host "❌ 未找到 $($notFoundFiles.Count) 个文件" -ForegroundColor Red
}

# 复制HTML文件
if (Test-Path -Path $htmlFilePath -PathType Leaf) {
    $htmlDestinationPath = Join-Path -Path $targetFolderPath -ChildPath "index.html"
    Copy-Item -Path $htmlFilePath -Destination $htmlDestinationPath
    Write-Host "✅ HTML文件复制完成" -ForegroundColor Green
} else {
    Write-Host "❌ 未找到HTML文件" -ForegroundColor Red
}

Write-Host "✅ 所有操作完成" -ForegroundColor Green
