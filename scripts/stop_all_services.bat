@echo off
REM =============================================================
REM MigCockpit — Stop All Services (Windows)
REM =============================================================

echo.
echo ============================================================
echo   MigCockpit - Stopping All Services
echo ============================================================
echo.

echo [1/3] Stopping by window title...
taskkill /FI "WINDOWTITLE eq MigCockpit*" /T /F >nul 2>&1

echo [2/3] Stopping by port...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000 " ^| findstr "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8001 " ^| findstr "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8003 " ^| findstr "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8004 " ^| findstr "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":5173 " ^| findstr "LISTENING"') do taskkill /F /PID %%a >nul 2>&1

echo [3/3] Cleaning up Celery processes...
taskkill /FI "IMAGENAME eq celery.exe" /T /F >nul 2>&1

echo.
echo [OK] All services stopped.
echo.
pause
