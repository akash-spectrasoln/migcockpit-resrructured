@echo off
REM =============================================================
REM MigCockpit — Run Unit Tests (Windows)
REM Usage: scripts\run_tests.bat
REM =============================================================

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%.."

echo.
echo Running unit tests (no database required)...
echo.
python -m pytest tests\unit\ -v --tb=short
echo.
pause
