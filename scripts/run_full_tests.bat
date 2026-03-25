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

echo [3/3] Frontend E2E tests (playwright)
call npm run test:e2e
if errorlevel 1 goto END

echo.
echo ============================================================
echo   All full tests passed
echo ============================================================

:END
pause

