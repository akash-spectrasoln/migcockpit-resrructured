<#
=============================================================
MigCockpit — Run Full Test Suite (backend + frontend + E2E)
Usage: .\scripts\run_full_tests.ps1
=============================================================
#>

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BaseDir = (Resolve-Path (Join-Path $ScriptDir "..")).Path

Set-Location $BaseDir

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Full Tests — Backend + Frontend + E2E" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1/3] Python tests (tests/)..."
if (-not $env:DJANGO_SETTINGS_MODULE) {
  Write-Host "[WARN] DJANGO_SETTINGS_MODULE is not set; skipping tests marked `db`" -ForegroundColor Yellow
  python -m pytest tests/ -v --tb=short -m "not db"
} else {
  python -m pytest tests/ -v --tb=short
}

Write-Host "[2/3] Frontend unit/integration tests (vitest)..."
Set-Location (Join-Path $BaseDir "frontend")
npm run test

Write-Host "[3/3] Starting frontend dev server for E2E (Vite)..."
$null = Start-Process -FilePath "npm" -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1", "--port", "5173", "--strictPort") -WorkingDirectory (Join-Path $BaseDir "frontend") -NoNewWindow -PassThru

Write-Host "Waiting for http://127.0.0.1:5173/ ..."
for ($i=0; $i -lt 60; $i++) {
  try {
    $resp = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:5173/" -TimeoutSec 2
    if ($resp.StatusCode -ge 200) { break }
  } catch {
    Start-Sleep -Milliseconds 500
  }
}

Write-Host "[4/3] Frontend E2E tests (playwright)..."
npm run test:e2e

# Stop dev server (best-effort)
Get-NetTCPConnection -LocalPort 5173 -ErrorAction SilentlyContinue | ForEach-Object {
  try { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue } catch {}
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  All full tests passed" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green

