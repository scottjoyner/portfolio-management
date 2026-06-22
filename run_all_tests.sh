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

run_test "Paper Trading System Tests" "python3 test_paper_trading_system.py"
run_test "Unified Signal Accumulator Tests" "python3 test_unified_signal_accumulator.py"
# run_test "Unified Signal Integration Tests" "python3 test_unified_signal_integration.py"
# run_test "Strategy Tests" "python3 test_strategies.py"

echo ""
echo "======================================================================"
echo "  COMPLETE: $PASS suites passed, $FAIL suites failed"
echo "======================================================================"
exit $FAIL
