# ===================================================================
# 脚本名称: Convert-RsToTxt_V2.ps1
# 脚本功能:
# 1. 检查 'tempfold' 文件夹是否存在。
#    - 如果存在，清空其所有内容。
#    - 如果不存在，创建它。
# 2. 查找 src\weblog\src 目录下的所有 .rs 扩展名文件。
# 3. 将这些 .rs 文件以 .txt 扩展名复制到 'tempfold' 文件夹中。
# 4. 复制 src\weblog\static\index.html 文件到 'tempfold' 文件夹中。
# ===================================================================

# --- 步骤 1: 定义变量 ---
# 定义目标文件夹的名称
$targetFolderName = "tempfold"
# 定义源文件路径
#$sourcePath = "src\weblog\src"
$sourcePath = "src\klaggregate"
# 定义HTML文件路径
$htmlFilePath = "src\weblog\static\index.html"
# 获取当前脚本所在的路径
$currentPath = Get-Location

# --- 步骤 2: 准备目标文件夹 (更新后的核心逻辑) ---
# 拼接出目标文件夹的完整路径
$targetFolderPath = Join-Path -Path $currentPath -ChildPath $targetFolderName

# 检查目标文件夹是否存在
if (Test-Path -Path $targetFolderPath -PathType Container) {
    # 如果文件夹已存在，清空其内容
    Write-Host "目标文件夹 '$targetFolderName' 已存在，正在清空..."
    # 获取文件夹内的所有项目（包括文件和子文件夹），并强制、递归地删除它们
    Get-ChildItem -Path $targetFolderPath -Force | Remove-Item -Recurse -Force
    Write-Host "文件夹已清空。"
} else {
    # 如果文件夹不存在，则创建它
    Write-Host "目标文件夹 '$targetFolderName' 不存在，正在创建..."
    New-Item -Path $targetFolderPath -ItemType Directory | Out-Null
    Write-Host "文件夹创建成功。"
}

# --- 步骤 3: 查找所有.rs文件 ---
# -File 参数确保我们只获取文件，不包括文件夹
# -Path $sourcePath 表示在指定路径下查找
Write-Host "----------------------------------------"
Write-Host "正在 '$sourcePath' 文件夹中查找 .rs 文件..."
$sourceFiles = Get-ChildItem -Path $sourcePath -Filter "*.rs" -File

# --- 步骤 4: 检查并处理文件 ---
if ($sourceFiles) {
    # 如果找到了文件
    Write-Host "找到了 $($sourceFiles.Count) 个 .rs 文件。开始复制和重命名..."

    # 遍历每一个找到的 .rs 文件
    foreach ($file in $sourceFiles) {
        # 构建新的文件名（例如： "main.rs" -> "main.txt"）
        $newFileName = "$($file.BaseName).txt"

        # 构建完整的目标文件路径
        $destinationPath = Join-Path -Path $targetFolderPath -ChildPath $newFileName

        # 执行复制操作，直接将文件以新名称和新扩展名复制到目标文件夹
        Copy-Item -Path $file.FullName -Destination $destinationPath
        
        # 在屏幕上打印操作日志，方便追踪
        Write-Host "已将 `"$($file.Name)`" 复制到 `"$destinationPath`""
    }

    Write-Host "----------------------------------------"
    Write-Host ".rs 文件复制完成！"

} else {
    # 如果没有找到任何 .rs 文件
    Write-Host "----------------------------------------"
    Write-Host "未在 '$sourcePath' 文件夹中找到任何 .rs 文件。"
}

# --- 步骤 5: 复制 HTML 文件 ---
Write-Host "----------------------------------------"
Write-Host "正在复制 HTML 文件..."

# 检查 HTML 文件是否存在
if (Test-Path -Path $htmlFilePath -PathType Leaf) {
    # 构建目标文件路径
    $htmlDestinationPath = Join-Path -Path $targetFolderPath -ChildPath "index.html"

    # 复制 HTML 文件
    Copy-Item -Path $htmlFilePath -Destination $htmlDestinationPath

    Write-Host "已将 `"$htmlFilePath`" 复制到 `"$htmlDestinationPath`""
    Write-Host "HTML 文件复制完成！"
} else {
    Write-Host "警告：未找到 HTML 文件 '$htmlFilePath'"
}

Write-Host "----------------------------------------"
Write-Host "所有操作完成！"