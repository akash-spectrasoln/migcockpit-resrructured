#!/bin/bash
# =============================================================
# MigCockpit — Run Test Suite
# Usage: ./scripts/run_tests.sh
#        ./scripts/run_tests.sh integration   (also run DB tests)
# =============================================================

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$(dirname "$SCRIPT_DIR")"

MODE="${1:-unit}"

echo ""
echo "============================================================"
echo "  MigCockpit — Test Runner"
echo "============================================================"
echo ""

if [ "$MODE" = "integration" ]; then
    echo "Running ALL tests (unit + integration)..."
    echo "Requires PostgreSQL at: ${TEST_PG_HOST:-localhost}:${TEST_PG_PORT:-5432}"
    echo ""
    python -m pytest tests/ -v --tb=short
else
    echo "Running unit tests (no database required)..."
    echo ""
    python -m pytest tests/unit/ -v --tb=short
fi

echo ""
echo "To run integration tests:"
echo "  export TEST_PG_HOST=localhost TEST_PG_PORT=5432"
echo "  export TEST_PG_USER=postgres TEST_PG_PASSWORD=yourpass"
echo "  ./scripts/run_tests.sh integration"
