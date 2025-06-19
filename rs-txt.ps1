# Rust文件转换为txt脚本
$targetFolderName = "tempfold"
$sourcePath = "src\klaggregate"
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

# 查找并复制.rs文件
$sourceFiles = Get-ChildItem -Path $sourcePath -Filter "*.rs" -File

if ($sourceFiles) {
    Write-Host "找到 $($sourceFiles.Count) 个.rs文件，开始复制..." -ForegroundColor Green
    
    foreach ($file in $sourceFiles) {
        $newFileName = "$($file.BaseName).txt"
        $destinationPath = Join-Path -Path $targetFolderPath -ChildPath $newFileName
        Copy-Item -Path $file.FullName -Destination $destinationPath
    }
    Write-Host "✅ .rs文件复制完成" -ForegroundColor Green
} else {
    Write-Host "❌ 未找到.rs文件" -ForegroundColor Red
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
