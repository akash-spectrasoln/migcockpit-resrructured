#!/bin/bash
# =============================================================
# MigCockpit — Lint Python code with Ruff
# Install Ruff once:  pip install ruff
# Usage: ./scripts/lint.sh
#        ./scripts/lint.sh --fix    (auto-fix safe issues)
# =============================================================
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$(dirname "$SCRIPT_DIR")"

if ! command -v ruff &> /dev/null; then
    echo "[ERROR] ruff not installed. Run: pip install ruff"
    exit 1
fi

echo "Running Ruff linter..."
ruff check . "$@"
echo ""
echo "Running Ruff formatter check..."
ruff format . --check "$@"
