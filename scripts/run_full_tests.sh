#!/bin/bash
# =============================================================
# MigCockpit — Run Full Test Suite (backend + frontend + E2E)
# Usage: ./scripts/run_full_tests.sh
# =============================================================

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$BASE_DIR"

echo ""
echo "============================================================"
echo "  Full Tests — Backend + Frontend + E2E"
echo "============================================================"
echo ""

echo "[1/3] Python tests (tests/)"
if [ -z "${DJANGO_SETTINGS_MODULE}" ]; then
  echo "[WARN] DJANGO_SETTINGS_MODULE is not set; skipping tests marked 'db'"
  python -m pytest tests/ -v --tb=short -m "not db"
else
  python -m pytest tests/ -v --tb=short
fi

echo "[2/3] Frontend unit/integration tests (vitest)"
cd "$BASE_DIR/frontend"
npm run test

echo "[3/3] Frontend E2E tests (playwright)"
npm run test:e2e

echo ""
echo "============================================================"
echo "  All full tests passed"
echo "============================================================"

