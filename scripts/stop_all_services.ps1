# =============================================================
# MigCockpit — Stop All Services (PowerShell)
# Usage: .\stop_all_services.ps1
# =============================================================

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $ScriptDir "..")

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  MigCockpit - Stopping All Services" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

function Stop-ByPort($Port) {
    Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
        Where-Object State -eq "Listen" |
        ForEach-Object {
            $proc = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host "  Stopping port $Port — PID $($proc.Id) ($($proc.ProcessName))" -ForegroundColor Yellow
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            }
        }
}

Write-Host "[1/3] Stopping by window title (MigCockpit*)..." -ForegroundColor Yellow
Get-Process | Where-Object { $_.MainWindowTitle -like "MigCockpit*" } |
    Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "[2/3] Stopping by port..." -ForegroundColor Yellow
foreach ($port in @(8000, 8001, 8003, 8004, 5173)) { Stop-ByPort $port }

Write-Host "[3/3] Cleaning up remaining Celery..." -ForegroundColor Yellow
Get-Process -Name "celery" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "[OK] All services stopped." -ForegroundColor Green
Write-Host ""
Read-Host "Press Enter to continue"
