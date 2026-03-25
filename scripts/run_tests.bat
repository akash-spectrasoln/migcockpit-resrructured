@echo off
REM =============================================================
REM MigCockpit — Test Runner Tiers (Windows)
REM Usage:
REM   scripts\run_tests.bat pr
REM   scripts\run_tests.bat nightly
REM =============================================================

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%.."
set MODE=%1
if "%MODE%"=="" set MODE=pr

echo.
if /I "%MODE%"=="nightly" goto NIGHTLY
if /I "%MODE%"=="integration" goto NIGHTLY

echo Running Tier A (PR required): fast regression checks...
echo.
python -m pytest tests\unit\ tests\test_node_regression_backend_matrix.py -v --tb=short
if errorlevel 1 goto END
cd /d "%SCRIPT_DIR%..\frontend"
call npm run test -- --run src/pipeline-engine/__tests__/pipeline.integration.test.ts
cd /d "%SCRIPT_DIR%.."
goto END

:NIGHTLY
echo Running Tier B (merge/nightly): backend + frontend + E2E...
echo.
python -m pytest tests\ -v --tb=short
if errorlevel 1 goto END
cd /d "%SCRIPT_DIR%..\frontend"
call npm run test
if errorlevel 1 goto END
call npm run test:e2e
cd /d "%SCRIPT_DIR%.."

:END
echo.
pause
