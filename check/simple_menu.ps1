# BTC Kline Monitor Menu
Clear-Host
Write-Host "BTC Kline Monitor Menu" -ForegroundColor Cyan
Write-Host "======================" -ForegroundColor Cyan
Write-Host

Write-Host "Select period to monitor:" -ForegroundColor Yellow
Write-Host
Write-Host "1. 1m period"
Write-Host "2. 5m period"
Write-Host "3. 15m period"
Write-Host "4. 30m period"
Write-Host "5. 4h period"
Write-Host "6. 1d period"
Write-Host "7. All periods"
Write-Host
Write-Host "0. Exit"
Write-Host

$choice = Read-Host "Enter option (0-7)"

switch ($choice) {
    "1" {
        Write-Host "Starting 1m period monitor..." -ForegroundColor Green
        $period = "1m"
    }
    "2" {
        Write-Host "Starting 5m period monitor..." -ForegroundColor Green
        $period = "5m"
    }
    "3" {
        Write-Host "Starting 15m period monitor..." -ForegroundColor Green
        $period = "15m"
    }
    "4" {
        Write-Host "Starting 30m period monitor..." -ForegroundColor Green
        $period = "30m"
    }
    "5" {
        Write-Host "Starting 4h period monitor..." -ForegroundColor Green
        $period = "4h"
    }
    "6" {
        Write-Host "Starting 1d period monitor..." -ForegroundColor Green
        $period = "1d"
    }
    "7" {
        Write-Host "Starting all periods monitor..." -ForegroundColor Green
        # Run all periods
        $env:BTC_DURATION = "600"

        # Supported periods
        $periods = @("1m", "5m", "15m", "30m", "4h", "1d")

        # Delete previous log files
        foreach ($p in $periods) {
            $logFile = "btc_${p}_monitor.log"
            if (Test-Path $logFile) {
                Remove-Item $logFile -Force
            }
        }

        # Create temp directory
        $tempDir = "temp"
        if (-not (Test-Path $tempDir)) {
            New-Item -ItemType Directory -Path $tempDir | Out-Null
        }

        # Start all period monitors
        foreach ($p in $periods) {
            Write-Host "Starting $p period monitor..." -ForegroundColor Green

            # Create temp batch files
            $dbBatContent = @"
@echo off
set BTC_PERIOD=$p
set BTC_DURATION=600
python check\db_monitor.py
"@

            $wsBatContent = @"
@echo off
set BTC_PERIOD=$p
set BTC_DURATION=600
python check\ws_monitor.py
"@

            $dbBatPath = "$tempDir\db_$p.bat"
            $wsBatPath = "$tempDir\ws_$p.bat"

            # Write temp batch files
            $dbBatContent | Out-File -FilePath $dbBatPath -Encoding ascii
            $wsBatContent | Out-File -FilePath $wsBatPath -Encoding ascii

            # Start scripts
            Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $dbBatPath -WindowStyle Normal
            Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $wsBatPath -WindowStyle Normal
        }

        Write-Host
        Write-Host "All monitor scripts started. Log files saved in btc_[period]_monitor.log files" -ForegroundColor Green
        Write-Host

        # Wait for user input then clean up temp files
        Read-Host "Press Enter to continue..."

        # Clean up temp files
        if (Test-Path $tempDir) {
            Remove-Item -Path $tempDir -Recurse -Force
        }

        exit
    }
    "0" {
        Write-Host "Exiting..." -ForegroundColor Red
        exit
    }
    default {
        Write-Host "Invalid option, please try again." -ForegroundColor Red
        Start-Sleep -Seconds 2
        & $PSCommandPath  # Re-run current script
        exit
    }
}

# If we get here, we're running a single period monitor
if ($period) {
    $env:BTC_PERIOD = $period
    $env:BTC_DURATION = "600"

    $logFile = "btc_${period}_monitor.log"

    # Delete previous log file
    if (Test-Path $logFile) {
        Remove-Item $logFile -Force
    }

    # Create temp batch files
    $dbBatContent = @"
@echo off
set BTC_PERIOD=$period
set BTC_DURATION=600
python check\db_monitor.py
"@

    $wsBatContent = @"
@echo off
set BTC_PERIOD=$period
set BTC_DURATION=600
python check\ws_monitor.py
"@

    $dbBatPath = "temp_db.bat"
    $wsBatPath = "temp_ws.bat"

    # Write temp batch files
    $dbBatContent | Out-File -FilePath $dbBatPath -Encoding ascii
    $wsBatContent | Out-File -FilePath $wsBatPath -Encoding ascii

    # Start scripts
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $dbBatPath -WindowStyle Normal
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $wsBatPath -WindowStyle Normal

    # Wait 1 second to ensure log file is created
    Start-Sleep -Seconds 1

    # Display log
    Write-Host "Displaying $period period log, press Ctrl+C to stop..." -ForegroundColor Yellow
    Write-Host

    try {
        Get-Content -Path $logFile -Wait -Tail 100
    }
    catch {
        Write-Host "Error reading log file: $_" -ForegroundColor Red
    }
    finally {
        # Clean up temp files
        if (Test-Path $dbBatPath) { Remove-Item $dbBatPath -Force }
        if (Test-Path $wsBatPath) { Remove-Item $wsBatPath -Force }

        Write-Host
        Write-Host "Monitoring complete. Full log saved in $logFile" -ForegroundColor Green
        Write-Host

        Read-Host "Press Enter to continue..."
    }
}
