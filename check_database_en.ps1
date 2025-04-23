# Database Query Script - Check Kline Data Download Status
# Author: Augment Agent
# Date: 2025-04-22

# SQLite tool path
$SQLITE_PATH = "F:\work\tool\sqlite-tools-win-x64-3490100\sqlite3.exe"
# Database path
$DB_PATH = "./data/klines.db"

# Check if SQLite tool exists
if (-not (Test-Path $SQLITE_PATH)) {
    Write-Host "Error: SQLite tool does not exist at path: $SQLITE_PATH" -ForegroundColor Red
    exit 1
}

# Check if database exists
if (-not (Test-Path $DB_PATH)) {
    Write-Host "Error: Database file does not exist at path: $DB_PATH" -ForegroundColor Red
    exit 1
}

# Create temporary file for storing query results
$TEMP_FILE = [System.IO.Path]::GetTempFileName()

# Function: Execute SQLite query and return results
function Invoke-SqliteQuery {
    param (
        [string]$query
    )
    
    & $SQLITE_PATH $DB_PATH $query | Out-File -FilePath $TEMP_FILE -Encoding utf8
    $result = Get-Content -Path $TEMP_FILE -Encoding utf8
    return $result
}

# Function: Convert Unix timestamp to readable time
function Convert-UnixTimeToDateTime {
    param (
        [long]$unixTime
    )
    
    # Binance timestamps are in milliseconds, need to divide by 1000 to convert to seconds
    $dateTime = (Get-Date "1970-01-01 00:00:00").AddSeconds($unixTime / 1000)
    return $dateTime.ToString("yyyy-MM-dd HH:mm:ss")
}

# Get symbol count (from symbols table)
$symbolCount = Invoke-SqliteQuery "SELECT COUNT(*) FROM symbols;"
Write-Host "Number of symbols in database: $symbolCount" -ForegroundColor Green

# Get table count (excluding symbols table)
$tableCount = Invoke-SqliteQuery "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name != 'symbols';"
Write-Host "Number of kline tables in database: $tableCount" -ForegroundColor Green

# Get all table names (excluding symbols table)
$tables = Invoke-SqliteQuery "SELECT name FROM sqlite_master WHERE type='table' AND name != 'symbols' ORDER BY name;"

# Create results array
$results = @()

# Loop through each table, get data count and time range
foreach ($table in $tables) {
    if ($table -match "k_(\w+)_(\w+)") {
        $symbol = $matches[1]
        $interval = $matches[2]
        
        # Get data count
        $count = Invoke-SqliteQuery "SELECT COUNT(*) FROM $table;"
        
        # Get earliest and latest time
        $minTime = Invoke-SqliteQuery "SELECT MIN(open_time) FROM $table;"
        $maxTime = Invoke-SqliteQuery "SELECT MAX(open_time) FROM $table;"
        
        # Convert timestamps
        $startTime = "N/A"
        $endTime = "N/A"
        
        if ($minTime -match "\d+") {
            $startTime = Convert-UnixTimeToDateTime ([long]$minTime)
        }
        
        if ($maxTime -match "\d+") {
            $endTime = Convert-UnixTimeToDateTime ([long]$maxTime)
        }
        
        # Create result object
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

# Sort results by symbol and interval
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

# Output results table
Write-Host "`nKline data details in database:" -ForegroundColor Yellow
$sortedResults | Format-Table -Property Symbol, Interval, Count, StartTime, EndTime -AutoSize

# Statistics by interval
$intervalStats = $sortedResults | Group-Object -Property Interval | 
    Select-Object @{Name="Interval"; Expression={$_.Name}}, @{Name="TableCount"; Expression={$_.Count}}, @{Name="TotalRecords"; Expression={($_.Group | Measure-Object -Property Count -Sum).Sum}}

Write-Host "`nStatistics by interval:" -ForegroundColor Yellow
$intervalStats | Sort-Object -Property @{Expression={
    switch($_.Interval) {
        "1m" { 1 }
        "5m" { 2 }
        "30m" { 3 }
        "4h" { 4 }
        "1d" { 5 }
        "1w" { 6 }
        default { 99 }
    }
}} | Format-Table -AutoSize

# Statistics by symbol
$symbolStats = $sortedResults | Group-Object -Property Symbol | 
    Select-Object @{Name="Symbol"; Expression={$_.Name}}, @{Name="TableCount"; Expression={$_.Count}}, @{Name="TotalRecords"; Expression={($_.Group | Measure-Object -Property Count -Sum).Sum}}

Write-Host "`nStatistics by symbol (top 20):" -ForegroundColor Yellow
$symbolStats | Sort-Object -Property "TotalRecords" -Descending | Select-Object -First 20 | Format-Table -AutoSize

# Total records
$totalRecords = ($sortedResults | Measure-Object -Property Count -Sum).Sum
Write-Host "`nTotal records in database: $totalRecords" -ForegroundColor Green

# Clean up temporary file
Remove-Item -Path $TEMP_FILE -Force
