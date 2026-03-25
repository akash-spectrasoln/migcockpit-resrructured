@echo off
REM =============================================================
REM MigCockpit — Run Full Test Suite (backend + frontend + E2E)
REM Usage: scripts\run_full_tests.bat
REM =============================================================

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set BASE_DIR=%SCRIPT_DIR%..
cd /d "%BASE_DIR%"

echo.
echo ============================================================
echo   Full Tests — Backend + Frontend + E2E
echo ============================================================
echo.

echo [1/3] Python tests (tests/)
set DJANGO_SETTINGS_MODULE_ENV=%DJANGO_SETTINGS_MODULE%
if "%DJANGO_SETTINGS_MODULE_ENV%"=="" (
  echo [WARN] DJANGO_SETTINGS_MODULE is not set; skipping tests marked `db`
  python -m pytest tests\ -v --tb=short -m "not db"
) else (
  python -m pytest tests\ -v --tb=short
)
if errorlevel 1 goto END

echo [2/3] Frontend unit/integration tests (vitest)
cd /d "%BASE_DIR%\frontend"
call npm run test
if errorlevel 1 goto END

echo [3/3] Starting frontend dev server for E2E (Vite)
start "" /b npm run dev -- --host 127.0.0.1 --port 5173 --strictPort

REM Wait for Vite to be reachable
powershell -NoProfile -Command "$ErrorActionPreference='SilentlyContinue'; for($i=0; $i -lt 60; $i++){ try{ $resp = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:5173/' -TimeoutSec 2; if($resp.StatusCode -ge 200){ exit 0 } } catch {} Start-Sleep -Milliseconds 500 }; exit 1"
if errorlevel 1 goto END

echo [4/3] Frontend E2E tests (playwright)
call npm run test:e2e
if errorlevel 1 goto END

REM Stop dev server (best-effort)
powershell -NoProfile -Command "$conns = Get-NetTCPConnection -LocalPort 5173 -ErrorAction SilentlyContinue; if($conns){ $pids = ($conns | Select-Object -ExpandProperty OwningProcess | Sort-Object -Unique); foreach($pid in $pids){ Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue } }"

echo.
echo ============================================================
echo   All full tests passed
echo ============================================================

:END
pause

