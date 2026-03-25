#!/bin/bash
# =============================================================
# MigCockpit — Run Test Suite
# Usage:
#   ./scripts/run_tests.sh pr         (Tier A, fast)
#   ./scripts/run_tests.sh nightly    (Tier B, full)
#   ./scripts/run_tests.sh integration (legacy alias for nightly)
# =============================================================

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$(dirname "$SCRIPT_DIR")"

MODE="${1:-pr}"

echo ""
echo "============================================================"
echo "  MigCockpit — Test Runner"
echo "============================================================"
echo ""

if [ "$MODE" = "nightly" ] || [ "$MODE" = "integration" ]; then
    echo "Running Tier B (merge/nightly): backend + frontend + E2E..."
    echo "Requires PostgreSQL at: ${TEST_PG_HOST:-localhost}:${TEST_PG_PORT:-5432}"
    echo ""
    python -m pytest tests/ -v --tb=short || exit 1
    (cd frontend && npm run test) || exit 1
    (cd frontend && npm run test:e2e) || exit 1
else
    echo "Running Tier A (PR required): fast regression checks..."
    echo ""
    python -m pytest tests/unit/ tests/test_node_regression_backend_matrix.py -v --tb=short || exit 1
    (cd frontend && npm run test -- --run src/pipeline-engine/__tests__/pipeline.integration.test.ts) || exit 1
fi

echo ""
echo "Tiers:"
echo "  Tier A (PR):      ./scripts/run_tests.sh pr"
echo "  Tier B (nightly): ./scripts/run_tests.sh nightly"
