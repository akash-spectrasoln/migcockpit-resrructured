# =============================================================
# MigCockpit — Start All Services (PowerShell)
# Pre-flight order: Lint → Tests → Start Services
# Usage: .\start_all_services.ps1
# =============================================================

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BaseDir   = (Resolve-Path (Join-Path $ScriptDir "..")).Path
Set-Location $BaseDir

function Write-Step($n, $msg) {
    Write-Host ""
    Write-Host "[$n] $msg" -ForegroundColor Yellow
}
function Write-Ok($msg)   { Write-Host "  [OK]   $msg" -ForegroundColor Green }
function Write-Fail($msg) { Write-Host "  [FAIL] $msg" -ForegroundColor Red }
function Write-Skip($msg) { Write-Host "  [SKIP] $msg" -ForegroundColor Gray }
function Write-Warn($msg) { Write-Host "  [WARN] $msg" -ForegroundColor Yellow }

function Start-Window($Title, $Cmd, $Dir) {
    Start-Process "cmd.exe" `
        -ArgumentList "/k", "title $Title && cd /d `"$Dir`" && $Cmd" `
        -WindowStyle Minimized -PassThru | Out-Null
}

function Test-Port($Port) {
    (Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue).TcpTestSucceeded
}

function Prompt-Continue($msg) {
    $ans = Read-Host "$msg Continue anyway? (y/N)"
    if ($ans -ne 'y' -and $ans -ne 'Y') { exit 1 }
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  MigCockpit - Pre-flight Check + Start All Services" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

if (-not (Test-Path "manage.py")) {
    Write-Fail "manage.py not found. Run from scripts\ directory."
    Read-Host "Press Enter to exit"; exit 1
}

# ── Step 1: Prerequisites ──────────────────────────────────────
Write-Step "1/6" "Checking prerequisites..."
try { $v = python --version 2>&1; Write-Ok $v } catch { Write-Fail "Python not found"; exit 1 }

$hasFrontend = $true
try { $v = node --version 2>&1; Write-Ok "Node.js $v" }
catch { Write-Warn "Node.js not found — frontend will be skipped"; $hasFrontend = $false }

if (Test-Port 6379) { Write-Ok "Redis running on :6379" }
else { Write-Warn "Redis not detected on :6379 — Celery tasks will fail" }

if (Test-Port 5433) { Write-Ok "PostgreSQL running on :5433" }
else { Write-Warn "PostgreSQL not detected on :5433" }

# ── Step 2: Python lint ────────────────────────────────────────
Write-Step "2/6" "Python lint (Ruff)..."
$ruffOk = $false
try {
    $null = Get-Command ruff -ErrorAction Stop
    $result = ruff check . --quiet 2>&1
    if ($LASTEXITCODE -eq 0) { Write-Ok "Python lint passed"; $ruffOk = $true }
    else {
        Write-Fail "Ruff found errors:"
        Write-Host $result -ForegroundColor Red
        Prompt-Continue "Fix with: ruff check . --fix"
    }
} catch {
    Write-Skip "ruff not installed. Run: pip install ruff"
}

# ── Step 3: Frontend lint + type-check ────────────────────────
Write-Step "3/6" "Frontend checks (TypeScript + ESLint)..."
if ($hasFrontend) {
    Set-Location "$BaseDir\frontend"
    if (-not (Test-Path "node_modules")) {
        Write-Host "  Installing npm packages..." -ForegroundColor Gray
        npm install --silent
    }
    # Type-check
    $tc = npm run type-check 2>&1
    if ($LASTEXITCODE -eq 0) { Write-Ok "TypeScript check passed" }
    else { Write-Fail "TypeScript errors found"; Write-Host $tc; Prompt-Continue "cd frontend && npm run type-check" }
    # Lint
    $lint = npm run lint 2>&1
    if ($LASTEXITCODE -eq 0) { Write-Ok "ESLint passed" }
    else { Write-Warn "ESLint issues found. Run: cd frontend && npm run lint:fix" }
    Set-Location $BaseDir
} else {
    Write-Skip "Node.js not found"
}

# ── Step 4: Python unit tests ──────────────────────────────────
Write-Step "4/6" "Python unit tests..."
try {
    $null = Get-Command pytest -ErrorAction Stop
    $result = pytest tests\unit\ -q --tb=short 2>&1
    $output = $result | Out-String
    if ($LASTEXITCODE -eq 0) {
        $passed = if ($output -match '(\d+) passed') { $Matches[1] } else { '?' }
        Write-Ok "$passed unit tests passed"
    } else {
        Write-Fail "Unit tests failed:"
        Write-Host $output -ForegroundColor Red
        Prompt-Continue "Fix the failing tests before starting services."
    }
} catch {
    Write-Skip "pytest not installed. Run: pip install pytest"
}

# ── Step 5: Start backend services ────────────────────────────
Write-Step "5/6" "Starting backend services..."

Start-Window "MigCockpit - Django API :8000"  "python manage.py runserver 8000"  $BaseDir
Write-Host "  Django API starting on :8000..." -ForegroundColor Gray
Start-Sleep 3

Start-Window "MigCockpit - Celery Worker" "celery -A datamigrationapi worker --loglevel=info --pool=solo" $BaseDir
Write-Host "  Celery worker starting..." -ForegroundColor Gray
Start-Sleep 2

Start-Window "MigCockpit - Extraction :8001" "python main.py" "$BaseDir\services\extraction_service"
Write-Host "  Extraction Service starting on :8001..." -ForegroundColor Gray
Start-Sleep 2

Start-Window "MigCockpit - Migration :8003"  "python main.py" "$BaseDir\services\migration_service"
Write-Host "  Migration Service starting on :8003..." -ForegroundColor Gray
Start-Sleep 2

$wsReqs = Join-Path $BaseDir "services\websocket_service\requirements.txt"
if (Test-Path $wsReqs) { pip install -q -r $wsReqs 2>$null }
Start-Window "MigCockpit - WebSocket :8004"  "python main.py" "$BaseDir\services\websocket_service"
Write-Host "  WebSocket Service starting on :8004..." -ForegroundColor Gray
Start-Sleep 2

# ── Step 6: Start frontend ─────────────────────────────────────
Write-Step "6/6" "Starting React frontend..."
if ($hasFrontend) {
    Start-Window "MigCockpit - Frontend :5173" "npm install && npm run dev" "$BaseDir\frontend"
    Write-Host "  Frontend starting on :5173 (npm install + npm run dev)..." -ForegroundColor Gray
} else {
    Write-Skip "Node.js not found"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  All Services Started!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Django API     : http://localhost:8000" -ForegroundColor Cyan
Write-Host "  Django Admin   : http://localhost:8000/admin" -ForegroundColor Cyan
Write-Host "  Frontend       : http://localhost:5173" -ForegroundColor Cyan
Write-Host "  Extraction API : http://localhost:8001/docs" -ForegroundColor Cyan
Write-Host "  Migration API  : http://localhost:8003/docs" -ForegroundColor Cyan
Write-Host "  WebSocket      : http://localhost:8004/docs" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Logs: check minimised windows in taskbar" -ForegroundColor Gray
Write-Host "  Stop: .\stop_all_services.ps1" -ForegroundColor Gray
Write-Host ""
Read-Host "Press Enter to continue"
