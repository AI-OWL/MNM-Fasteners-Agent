# MNM Fasteners Agent - Uninstallation Script
# Run as Administrator

#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  MNM Fasteners Agent Uninstaller" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

$InstallPath = "C:\Program Files\MNMAgent"
$DataPath = "C:\ProgramData\MNMAgent"
$ServiceName = "MNMFastenersAgent"

# Confirm
$confirm = Read-Host "This will remove the MNM Agent. Continue? (y/N)"
if ($confirm -ne "y" -and $confirm -ne "Y") {
    Write-Host "Cancelled." -ForegroundColor Yellow
    exit 0
}

# Stop and remove service
Write-Host ""
Write-Host "Checking Windows service..." -ForegroundColor Yellow

try {
    $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($service) {
        if ($service.Status -eq "Running") {
            Write-Host "  Stopping service..." -ForegroundColor Gray
            Stop-Service -Name $ServiceName -Force
            Start-Sleep -Seconds 2
        }
        Write-Host "  Removing service..." -ForegroundColor Gray
        sc.exe delete $ServiceName | Out-Null
        Write-Host "  Service removed" -ForegroundColor Green
    } else {
        Write-Host "  Service not installed" -ForegroundColor Gray
    }
} catch {
    Write-Host "  Warning: Could not remove service: $_" -ForegroundColor Yellow
}

# Remove installation directory
Write-Host ""
Write-Host "Removing installation directory..." -ForegroundColor Yellow

if (Test-Path $InstallPath) {
    Remove-Item -Path $InstallPath -Recurse -Force
    Write-Host "  Removed: $InstallPath" -ForegroundColor Green
} else {
    Write-Host "  Directory not found" -ForegroundColor Gray
}

# Ask about data directory
Write-Host ""
$removeData = Read-Host "Remove data directory (logs, config)? (y/N)"
if ($removeData -eq "y" -or $removeData -eq "Y") {
    if (Test-Path $DataPath) {
        Remove-Item -Path $DataPath -Recurse -Force
        Write-Host "  Removed: $DataPath" -ForegroundColor Green
    }
} else {
    Write-Host "  Data directory preserved: $DataPath" -ForegroundColor Gray
}

# Remove from PATH
Write-Host ""
Write-Host "Cleaning up PATH..." -ForegroundColor Yellow

$envPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
if ($envPath -like "*$InstallPath*") {
    $newPath = ($envPath -split ";" | Where-Object { $_ -ne $InstallPath }) -join ";"
    [Environment]::SetEnvironmentVariable("Path", $newPath, "Machine")
    Write-Host "  Removed from PATH" -ForegroundColor Green
}

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  Uninstallation Complete!" -ForegroundColor Green
Write-Host "======================================" -ForegroundColor Cyan

