#!/usr/bin/env bash
# Measure coverage for the 16 in-scope modules against the >=90% line / >=90% branch gate.
#
# Measurement environment notes:
#   * Real packages (trading_system.*, alembic) are measured from the repo root
#     WITHOUT --source so the repo .coveragerc (source = .) applies; the report
#     is filtered to the target module's basename.
#   * The tests.* helpers live under a `tests` namespace. The repo .coveragerc
#     omits */tests/*, so they are measured from a NEUTRAL cwd (/tmp) with an
#     explicit --source and the ABSOLUTE test path.
set -u
ROOT="/home/scott/git/portfolio-management"
COV="$ROOT/.venv/bin/coverage"
TMP="$(mktemp -d)"
GATE=90
pass=0
fail=0
RESULTS=()

# $1=label  $2=report-filter(grep)  $3=testpath  $4=cwd  $5=optional --source value
measure_one() {
  local label="$1" filt="$2" t="$3" wd="$4" src="${5:-}"
  local df="$TMP/$(echo "$label" | tr '/ .' '___').coverage"
  local runargs=(run --branch --data-file="$df" -m pytest "$t" -q)
  if [ -n "$src" ]; then runargs+=(--source="$src"); fi
  ( cd "$wd" && "$COV" "${runargs[@]}" >/dev/null 2>&1 )
  local line
  line=$("$COV" report --data-file="$df" 2>/dev/null | grep -E "$filt" | head -1)
  if [ -z "$line" ]; then
    RESULTS+=("FAIL  $label  (no data collected)")
    fail=$((fail+1)); return
  fi
  local cover brpart branch bpct
  cover=$(echo "$line" | awk '{print $NF}' | sed 's/%//')
  brpart=$(echo "$line" | awk '{print $(NF-1)}')
  branch=$(echo "$line" | awk '{print $(NF-2)}')
  bpct=100
  if [ "$branch" != "0" ] && [ -n "$branch" ]; then
    bpct=$(awk "BEGIN{printf \"%.1f\", ($branch - $brpart) / $branch * 100}")
  fi
  local ok
  ok=$(awk "BEGIN{print ($cover + 0 >= $GATE && $bpct + 0 >= $GATE) ? 1 : 0}")
  if [ "$ok" = "1" ]; then
    RESULTS+=("PASS  $label  line=${cover}% branch=${bpct}%")
    pass=$((pass+1))
  else
    RESULTS+=("FAIL  $label  line=${cover}% branch=${bpct}%  (gate=$GATE)")
    fail=$((fail+1))
  fi
}

echo "==================== REAL PACKAGES (repo root, .coveragerc) ===================="
measure_one "unified_execution/models.py" "models.py" "tests/coverage/unified_execution/test_models.py" "$ROOT"
measure_one "unified_execution/interfaces.py" "interfaces.py" "tests/coverage/unified_execution/test_interfaces.py" "$ROOT"
measure_one "unified_execution/adapters/mock.py" "adapters/mock.py" "tests/coverage/unified_execution/test_mock_adapter.py" "$ROOT"
measure_one "unified_execution/adapters/coinbase.py" "adapters/coinbase.py" "tests/coverage/unified_execution/test_coinbase_adapter.py" "$ROOT"
measure_one "database/queries/accounts.py" "queries/accounts.py" "tests/coverage/database/test_accounts.py" "$ROOT"
measure_one "database/queries/positions.py" "queries/positions.py" "tests/coverage/database/test_positions.py" "$ROOT"
measure_one "database/queries/trades.py" "queries/trades.py" "tests/coverage/database/test_trades.py" "$ROOT"
measure_one "database/queries/auto_approval_rules.py" "queries/auto_approval_rules.py" "tests/coverage/database/test_auto_approval_rules.py" "$ROOT"
measure_one "alembic/env.py" "alembic/env.py" "tests/coverage/alembic/test_env.py" "$ROOT"
measure_one "alembic/versions/0001_initial.py" "versions/0001_initial.py" "tests/coverage/alembic/test_versions.py" "$ROOT"
measure_one "alembic/versions/0002_onchain_runtime.py" "versions/0002_onchain_runtime.py" "tests/coverage/alembic/test_versions.py" "$ROOT"
measure_one "ui/dashboard_server.py" "ui/dashboard_server.py" "tests/coverage/ui/" "$ROOT"

echo "==================== TESTS.* HELPERS (via /tmp) ===================="
measure_one "tests.coverage.event_markets.em_helpers" "em_helpers" "$ROOT/tests/coverage/event_markets/test_em_helpers.py" "/tmp" "tests.coverage.event_markets.em_helpers"
measure_one "tests.coverage.strategies.strat_helpers" "strat_helpers" "$ROOT/tests/coverage/strategies/test_strat_helpers.py" "/tmp" "tests.coverage.strategies.strat_helpers"
measure_one "tests.coverage.graph_alpha_bot.conftest" "conftest.py" "$ROOT/tests/coverage/graph_alpha_bot/test_conftest_coverage.py" "/tmp" "tests.coverage.graph_alpha_bot.conftest"

echo "==================== SUMMARY ===================="
for r in "${RESULTS[@]}"; do echo "$r"; done
echo "PASS=$pass FAIL=$fail"
rm -rf "$TMP"
[ "$fail" -eq 0 ]
