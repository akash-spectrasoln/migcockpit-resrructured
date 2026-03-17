#!/bin/bash
# =============================================================
# MigCockpit — Start All Services (Linux / Mac)
# Pre-flight order: Lint → Tests → Start Services
# Usage: chmod +x scripts/start_services.sh && ./scripts/start_services.sh
# =============================================================

set -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BASE_DIR"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; GRAY='\033[0;37m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; }
skip() { echo -e "  ${GRAY}[SKIP]${NC} $1"; }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; }
step() { echo ""; echo -e "${YELLOW}[$1]${NC} $2"; }

prompt_continue() {
    echo ""
    read -p "  Continue anyway? (y/N): " ans
    [[ "$ans" =~ ^[Yy]$ ]] || exit 1
}

echo ""
echo "============================================================"
echo "  MigCockpit — Pre-flight Check + Start All Services"
echo "============================================================"

if [ ! -f "manage.py" ]; then
    fail "manage.py not found. Run from scripts/ directory."
    exit 1
fi

# ── Step 1: Prerequisites ──────────────────────────────────────
step "1/6" "Checking prerequisites..."
python3 --version && ok "Python found" || { fail "Python3 not found"; exit 1; }

SKIP_FRONTEND=0
if command -v npm &>/dev/null; then
    node --version && ok "Node.js found"
else
    warn "npm not found — frontend will be skipped"
    SKIP_FRONTEND=1
fi

if pgrep -x redis-server &>/dev/null || redis-cli ping &>/dev/null 2>&1; then
    ok "Redis running"
else
    if ! pgrep -x "redis-server" > /dev/null; then
        echo "  Starting Redis..."
        redis-server --daemonize yes 2>/dev/null && ok "Redis started" || warn "Could not start Redis"
    fi
fi

# ── Step 2: Python lint ────────────────────────────────────────
step "2/6" "Python lint (Ruff)..."
if command -v ruff &>/dev/null; then
    if ruff check . --quiet; then
        ok "Python lint passed"
    else
        fail "Ruff found lint errors"
        echo "  Fix with: ruff check . --fix"
        prompt_continue
    fi
else
    skip "ruff not installed. Run: pip install ruff"
fi

# ── Step 3: Frontend checks ────────────────────────────────────
step "3/6" "Frontend checks (TypeScript + ESLint)..."
if [ $SKIP_FRONTEND -eq 0 ]; then
    cd "$BASE_DIR/frontend"
    if [ ! -d "node_modules" ]; then
        echo "  Installing npm packages..."
        npm install --silent
    fi
    if npm run type-check --silent 2>/dev/null; then
        ok "TypeScript check passed"
    else
        fail "TypeScript errors found"
        echo "  Fix with: cd frontend && npm run type-check"
        prompt_continue
    fi
    if npm run lint --silent 2>/dev/null; then
        ok "ESLint passed"
    else
        warn "ESLint issues. Fix with: cd frontend && npm run lint:fix"
    fi
    cd "$BASE_DIR"
else
    skip "Node.js not found"
fi

# ── Step 4: Python unit tests ──────────────────────────────────
step "4/6" "Python unit tests..."
if command -v pytest &>/dev/null; then
    if pytest tests/unit/ -q --tb=short; then
        ok "All unit tests passed"
    else
        fail "Unit tests failed — fix before starting services"
        prompt_continue
    fi
else
    skip "pytest not installed. Run: pip install pytest"
fi

# ── Step 5: Start backend services ────────────────────────────
step "5/6" "Starting backend services..."

echo "  Django API on :8000..."
python3 manage.py runserver 8000 &
DJANGO_PID=$!

echo "  Celery worker..."
celery -A datamigrationapi worker --loglevel=info &
CELERY_PID=$!

echo "  Extraction Service on :8001..."
(cd "$BASE_DIR/services/extraction_service" && python3 main.py) &
EXTRACTION_PID=$!

echo "  Migration Service on :8003..."
(cd "$BASE_DIR/services/migration_service" && python3 main.py) &
MIGRATION_PID=$!

echo "  WebSocket Service on :8004..."
(cd "$BASE_DIR/services/websocket_service" && python3 main.py) &
WEBSOCKET_PID=$!

# ── Step 6: Frontend ───────────────────────────────────────────
step "6/6" "Starting React frontend..."
if [ $SKIP_FRONTEND -eq 0 ]; then
    echo "  npm install + npm run dev on :5173..."
    (cd "$BASE_DIR/frontend" && npm install --silent && npm run dev) &
    FRONTEND_PID=$!
else
    skip "Node.js not found"
fi

echo ""
echo "============================================================"
echo -e "  ${GREEN}All Services Started!${NC}"
echo "============================================================"
echo ""
echo "  Django API     : http://localhost:8000"
echo "  Django Admin   : http://localhost:8000/admin"
echo "  Frontend       : http://localhost:5173"
echo "  Extraction API : http://localhost:8001/docs"
echo "  Migration API  : http://localhost:8003/docs"
echo "  WebSocket      : http://localhost:8004/docs"
echo ""
echo "  Press Ctrl+C to stop all services"
echo ""

trap "
    echo ''
    echo 'Stopping all services...'
    kill \$DJANGO_PID \$CELERY_PID \$EXTRACTION_PID \$MIGRATION_PID \$WEBSOCKET_PID \$FRONTEND_PID 2>/dev/null
    echo 'Done.'
    exit 0
" INT TERM

wait
