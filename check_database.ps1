# 数据库查询脚本 - 检查K线数据下载情况
# 作者: Augment Agent
# 日期: 2025-04-22

# SQLite工具路径
$SQLITE_PATH = "F:\work\tool\sqlite-tools-win-x64-3490100\sqlite3.exe"
# 数据库路径
$DB_PATH = "./data/klines.db"

# 检查SQLite工具是否存在
if (-not (Test-Path $SQLITE_PATH)) {
    Write-Host "错误: SQLite工具不存在于路径: $SQLITE_PATH" -ForegroundColor Red
    exit 1
}

# 检查数据库是否存在
if (-not (Test-Path $DB_PATH)) {
    Write-Host "错误: 数据库文件不存在于路径: $DB_PATH" -ForegroundColor Red
    exit 1
}

# 创建临时文件用于存储查询结果
$TEMP_FILE = [System.IO.Path]::GetTempFileName()

# 函数: 执行SQLite查询并返回结果
function Invoke-SqliteQuery {
    param (
        [string]$query
    )
    
    & $SQLITE_PATH $DB_PATH $query | Out-File -FilePath $TEMP_FILE -Encoding utf8
    $result = Get-Content -Path $TEMP_FILE -Encoding utf8
    return $result
}

# 函数: 将Unix时间戳转换为可读时间
function Convert-UnixTimeToDateTime {
    param (
        [long]$unixTime
    )
    
    # 币安时间戳是毫秒级的，需要除以1000转换为秒
    $dateTime = (Get-Date "1970-01-01 00:00:00").AddSeconds($unixTime / 1000)
    return $dateTime.ToString("yyyy-MM-dd HH:mm:ss")
}

# 获取品种数量（从symbols表）
$symbolCount = Invoke-SqliteQuery "SELECT COUNT(*) FROM symbols;"
Write-Host "数据库中的品种数量: $symbolCount" -ForegroundColor Green

# 获取表数量（排除symbols表）
$tableCount = Invoke-SqliteQuery "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name != 'symbols';"
Write-Host "数据库中的K线表数量: $tableCount" -ForegroundColor Green

# 获取所有表名（排除symbols表）
$tables = Invoke-SqliteQuery "SELECT name FROM sqlite_master WHERE type='table' AND name != 'symbols' ORDER BY name;"

# 创建结果数组
$results = @()

# 遍历每个表，获取数据数量和时间范围
foreach ($table in $tables) {
    if ($table -match "k_(\w+)_(\w+)") {
        $symbol = $matches[1]
        $interval = $matches[2]
        
        # 获取数据数量
        $count = Invoke-SqliteQuery "SELECT COUNT(*) FROM $table;"
        
        # 获取最早和最晚的时间
        $minTime = Invoke-SqliteQuery "SELECT MIN(open_time) FROM $table;"
        $maxTime = Invoke-SqliteQuery "SELECT MAX(open_time) FROM $table;"
        
        # 转换时间戳
        $startTime = "N/A"
        $endTime = "N/A"
        
        if ($minTime -match "\d+") {
            $startTime = Convert-UnixTimeToDateTime ([long]$minTime)
        }
        
        if ($maxTime -match "\d+") {
            $endTime = Convert-UnixTimeToDateTime ([long]$maxTime)
        }
        
        # 创建结果对象
        $result = [PSCustomObject]@{
            Symbol = $symbol
            Interval = $interval
            Count = $count
            StartTime = $startTime
            EndTime = $endTime
        }
        
        $results += $result
    }
}

# 按品种和周期排序结果
$sortedResults = $results | Sort-Object Symbol, @{Expression={
    switch($_.Interval) {
        "1m" { 1 }
        "5m" { 2 }
        "30m" { 3 }
        "4h" { 4 }
        "1d" { 5 }
        "1w" { 6 }
        default { 99 }
    }
}}

# 输出结果表格
Write-Host "`n数据库中的K线数据详情:" -ForegroundColor Yellow
$sortedResults | Format-Table -Property Symbol, Interval, Count, StartTime, EndTime -AutoSize

# 按周期统计
$intervalStats = $sortedResults | Group-Object -Property Interval | 
    Select-Object @{Name="周期"; Expression={$_.Name}}, @{Name="表数量"; Expression={$_.Count}}, @{Name="总记录数"; Expression={($_.Group | Measure-Object -Property Count -Sum).Sum}}

Write-Host "`n按周期统计:" -ForegroundColor Yellow
$intervalStats | Sort-Object -Property @{Expression={
    switch($_.周期) {
        "1m" { 1 }
        "5m" { 2 }
        "30m" { 3 }
        "4h" { 4 }
        "1d" { 5 }
        "1w" { 6 }
        default { 99 }
    }
}} | Format-Table -AutoSize

# 按品种统计
$symbolStats = $sortedResults | Group-Object -Property Symbol | 
    Select-Object @{Name="品种"; Expression={$_.Name}}, @{Name="表数量"; Expression={$_.Count}}, @{Name="总记录数"; Expression={($_.Group | Measure-Object -Property Count -Sum).Sum}}

Write-Host "`n按品种统计 (前20个):" -ForegroundColor Yellow
$symbolStats | Sort-Object -Property "总记录数" -Descending | Select-Object -First 20 | Format-Table -AutoSize

# 总记录数
$totalRecords = ($sortedResults | Measure-Object -Property Count -Sum).Sum
Write-Host "`n数据库中的总记录数: $totalRecords" -ForegroundColor Green

# 清理临时文件
Remove-Item -Path $TEMP_FILE -Force
