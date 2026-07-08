#!/usr/bin/env bash
# Run all test suites for the trading system
set -e

ROOT="/home/scott/git/portfolio-management"
PASS=0
FAIL=0

run_test() {
    local name="$1"
    local cmd="$2"
    echo ""
    echo "======================================================================"
    echo "  $name"
    echo "======================================================================"
    if eval "$cmd" 2>&1; then
        echo ""
        echo "  >>> $name: PASSED <<<"
        PASS=$((PASS + 1))
    else
        echo ""
        echo "  >>> $name: FAILED <<<"
        FAIL=$((FAIL + 1))
    fi
}

cd "$ROOT"

run_test "Rust Core (strategies + confidence + streaming) Tests" "cd rust_core && cargo test 2>&1"
# Legacy Python tests archived — ported to Rust:
#   test_paper_trading_system.py (imported paper_trading_system → archived)
#   test_unified_signal_accumulator.py (imported backtester → archived)"

echo ""
echo "======================================================================"
echo "  COMPLETE: $PASS suites passed, $FAIL suites failed"
echo "======================================================================"
exit $FAIL
