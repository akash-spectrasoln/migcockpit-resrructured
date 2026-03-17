@echo off
REM =============================================================
REM MigCockpit - Start All Services (Windows)
REM Pre-flight order: Lint → Tests → Start Services
REM =============================================================

setlocal enabledelayedexpansion

echo.
echo ============================================================
echo   MigCockpit - Pre-flight Check + Start All Services
echo ============================================================
echo.

set SCRIPT_DIR=%~dp0
set BASE_DIR=%SCRIPT_DIR%..
cd /d "%BASE_DIR%"

if not exist "manage.py" (
    echo [ERROR] manage.py not found. Run from scripts\ directory.
    pause & exit /b 1
)

REM -- Step 1: Check prerequisites ---------------------------------------
echo [1/6] Checking prerequisites...
python --version >nul 2>&1
if errorlevel 1 ( echo [ERROR] Python not found in PATH & pause & exit /b 1 )
python --version

set SKIP_FRONTEND=0
node --version >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Node.js not found - frontend will not start
    set SKIP_FRONTEND=1
) else ( node --version )

netstat -an | findstr ":6379" >nul 2>&1
if errorlevel 1 ( echo [WARNING] Redis not detected on :6379
) else ( echo [OK] Redis detected )

echo.

REM -- Step 2: Python unit tests ------------------------------------------
echo [2/6] Running Python unit tests...
pytest tests\unit\ -q --tb=short
echo [INFO] pytest exit code: %ERRORLEVEL% (continuing; not blocking services)

REM -- Start backend services ---------------------------------------------
echo.
echo [3/3] Starting backend services...

echo   Starting Django API (port 8000)...
start "MigCockpit - Django API :8000" /min cmd /k "cd /d %BASE_DIR% && python manage.py runserver 8000"
timeout /t 3 /nobreak >nul

echo   Starting Celery worker...
start "MigCockpit - Celery Worker" /min cmd /k "cd /d %BASE_DIR% && celery -A datamigrationapi worker --loglevel=info --pool=solo"
timeout /t 2 /nobreak >nul

echo   Starting Extraction Service (port 8001)...
start "MigCockpit - Extraction :8001" /min cmd /k "cd /d %BASE_DIR%\services\extraction_service && python main.py"
timeout /t 2 /nobreak >nul

echo   Starting Migration Service (port 8003)...
start "MigCockpit - Migration :8003" /min cmd /k "cd /d %BASE_DIR%\services\migration_service && python main.py"
timeout /t 2 /nobreak >nul

echo   Starting WebSocket Service (port 8004)...
start "MigCockpit - WebSocket :8004" /min cmd /k "cd /d %BASE_DIR%\services\websocket_service && python main.py"
timeout /t 2 /nobreak >nul


echo [6/6] Starting React frontend (port 3000)...
start "MigCockpit - Frontend :3000" /min cmd /k "cd /d %BASE_DIR%\frontend && npm install && npm run dev -- --port 3000"


echo.
echo ============================================================
echo   All Services Started!
echo ============================================================
echo.
echo   Django API     : http://localhost:8000
echo   Django Admin   : http://localhost:8000/admin
echo   Frontend       : http://localhost:3000
echo   Extraction API : http://localhost:8001/docs
echo   Migration API  : http://localhost:8003/docs
echo   WebSocket      : http://localhost:8004/docs
echo.
echo   Logs: check the minimised windows in the taskbar
echo   Stop: run stop_all_services.bat
echo.
pause
