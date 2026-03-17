@echo off
REM MigCockpit — Lint Python code with Ruff
REM Install: pip install ruff
REM Usage: scripts\lint.bat
REM        scripts\lint.bat --fix

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%.."

where ruff >nul 2>&1
if errorlevel 1 (
    echo [ERROR] ruff not installed. Run: pip install ruff
    exit /b 1
)

echo Running Ruff linter...
ruff check . %*
echo.
echo Running Ruff formatter check...
ruff format . --check %*
