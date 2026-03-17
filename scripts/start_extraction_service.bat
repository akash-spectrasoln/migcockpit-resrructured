@echo off
REM =============================================================
REM MigCockpit — Start Extraction Service only (Windows)
REM Useful when debugging source connection issues separately.
REM =============================================================

set SCRIPT_DIR=%~dp0
set BASE_DIR=%SCRIPT_DIR%..
cd /d "%BASE_DIR%"

echo Starting Extraction Service on port 8001...
cd /d "%BASE_DIR%\services\extraction_service"
python main.py
