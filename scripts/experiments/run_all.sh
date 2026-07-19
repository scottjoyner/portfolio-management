#!/usr/bin/env bash
# One-command backtesting suite + regression CI gate.
#
# Runs the standard experiment battery via run_suite.py and reports PASS/FAIL.
# NAS_FEED_ROOT is pointed at the big SSD cache when present, else the default.
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

SSD="/media/scott/SSD_4TB/feed_cache"
if [ -d "$SSD" ]; then
    export NAS_FEED_ROOT="$SSD"
    echo "NAS_FEED_ROOT=$NAS_FEED_ROOT"
else
    echo "SSD feed cache not mounted; using default NAS_FEED_ROOT"
fi

if [ -x .venv/bin/python ]; then
    PY=.venv/bin/python
else
    PY=python3
fi

echo "Running backtest suite..."
if "$PY" scripts/backtest_framework/run_suite.py --fail-on-regression; then
    echo "==================================="
    echo "SUITE RESULT: PASS"
    echo "==================================="
    exit 0
else
    echo "==================================="
    echo "SUITE RESULT: FAIL (regression gate)"
    echo "==================================="
    exit 1
fi
