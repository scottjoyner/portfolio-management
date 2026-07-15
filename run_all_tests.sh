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

run_test "Rust Core (strategies + confidence + streaming) Tests" "(cd rust_core && cargo test) 2>&1"

# Offline unit tests for the execution engine + orchestrator guard logic
# (no network required — catches endpoint-shape + position/PnL regressions in CI).
run_test "Execution Engine Unit Tests" "python3 -m unittest tests.test_execution_unit 2>&1"
run_test "Orchestrator Unit Tests" "python3 -m unittest tests.test_orchestrator_unit 2>&1"

# Live proof: real Coinbase CLI endpoints + full bracket lifecycle. Requires an
# authenticated CLI; skips gracefully (not a failure) when unavailable.
run_test "Execution Engine Live Proof" "python3 -m unittest tests.test_execution_proof 2>&1"

# Trader runtime harness: run_trader_v4 paper-mode scan loops + order-entry
# gating. Pure in-memory (no network); validates the live trading loop wiring.
run_test "Trader Runtime Harness (v4 extra6)" "python3 -m pytest tests/coverage/coinbase/test_run_trader_v4_extra6.py -q 2>&1"
run_test "Trader Runtime Harness (v4 extra7)" "python3 -m pytest tests/coverage/coinbase/test_run_trader_v4_extra7.py -q 2>&1"

# Optimizer integration smoke: proves the new order-flow / on-chain / stablecoin
# detection steps actually call their strategies and emit Opportunity objects.
run_test "Optimizer New-Signal Integration" "python3 -m pytest tests/coverage/test_optimizer_new_signals.py -q 2>&1"

# NAS feed cache: durable persistence (parquet append + dedup) for backtesting.
run_test "NAS Feed Cache" "python3 -m pytest tests/coverage/test_feed_cache.py -q 2>&1"

# Legacy Python tests archived — ported to Rust:
#   test_paper_trading_system.py (imported paper_trading_system → archived)
#   test_unified_signal_accumulator.py (imported backtester → archived)"

echo ""
echo "======================================================================"
echo "  COMPLETE: $PASS suites passed, $FAIL suites failed"
echo "======================================================================"
exit $FAIL
