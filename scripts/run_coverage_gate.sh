#!/usr/bin/env bash
# Orchestrates per-module 90% coverage gates for Python, Node, and Rust.
# Exit code is non-zero if ANY language gate fails.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
COV_BIN="${COV_BIN:-$ROOT/.venv/bin/coverage}"
GATE="$ROOT/scripts/coverage_gate.py"
COV_DIR="$ROOT/scripts/coverage"
THRESHOLD="${THRESHOLD:-90.0}"

overall=0

# ---------------------------------------------------------------------------
# PYTHON
# ---------------------------------------------------------------------------
echo "==================== PYTHON ===================="
if [ -d "$ROOT/.venv" ]; then
  # Combine any collected coverage data files (per-agent / root).
  rm -f "$COV_DIR/combined.coverage"
  # Gather data files: root .coverage and any /tmp/cov_*.coverage produced by agents.
  files=()
  [ -f "$ROOT/.coverage" ] && files+=("$ROOT/.coverage")
  shopt -s nullglob
  for f in /tmp/cov_*.coverage "$COV_DIR"/*.coverage; do
    [ "$f" != "$COV_DIR/combined.coverage" ] && files+=("$f")
  done
  shopt -u nullglob
  if [ "${#files[@]}" -gt 0 ]; then
    "$COV_BIN" combine --data-file="$COV_DIR/combined.coverage" "${files[@]}" >/dev/null 2>&1 || true
    "$COV_BIN" json --data-file="$COV_DIR/combined.coverage" -o "$COV_DIR/python_coverage.json" >/dev/null 2>&1 || true
    "$PYTHON_BIN" "$GATE" --lang python --manifest "$COV_DIR/python_manifest.txt" \
      --data "$COV_DIR/python_coverage.json" --threshold "$THRESHOLD" || overall=1
  else
    echo "No Python coverage data found. Run tests with coverage first."
    overall=1
  fi
else
  echo "Python venv not found; skipping Python gate."
  overall=1
fi

# ---------------------------------------------------------------------------
# NODE
# ---------------------------------------------------------------------------
echo ""
echo "==================== NODE ===================="
if command -v node >/dev/null 2>&1; then
  node --test --experimental-test-coverage tests/*.test.mjs > "$COV_DIR/node_cov.txt" 2>&1 || true
  # node coverage table goes to stderr in newer versions; capture both
  node --test --experimental-test-coverage tests/*.test.mjs 2>> "$COV_DIR/node_cov.txt" >/dev/null || true
  "$PYTHON_BIN" "$GATE" --lang node --manifest "$COV_DIR/node_manifest.txt" \
    --data "$COV_DIR/node_cov.txt" --threshold "$THRESHOLD" || overall=1
else
  echo "node not found; skipping Node gate."
  overall=1
fi

# ---------------------------------------------------------------------------
# RUST
# ---------------------------------------------------------------------------
echo ""
echo "==================== RUST ===================="
if command -v cargo >/dev/null 2>&1 && [ -d "$ROOT/rust_core" ]; then
  if command -v cargo-llvm-cov >/dev/null 2>&1; then
    (cd "$ROOT/rust_core" && cargo llvm-cov --json --output-path "$COV_DIR/rust_cov.json" >/dev/null 2>&1) || true
    if [ -f "$COV_DIR/rust_cov.json" ]; then
      "$PYTHON_BIN" "$GATE" --lang rust --manifest "$COV_DIR/rust_manifest.txt" \
        --data "$COV_DIR/rust_cov.json" --threshold "$THRESHOLD" || overall=1
    else
      echo "Rust coverage JSON not produced."
      overall=1
    fi
  else
    echo "cargo-llvm-cov not installed; skipping Rust gate."
    overall=1
  fi
else
  echo "cargo/rust_core not found; skipping Rust gate."
  overall=1
fi

echo ""
echo "==================== SUMMARY ===================="
if [ "$overall" -eq 0 ]; then
  echo "ALL GATES PASSED (>=${THRESHOLD}% per module)."
else
  echo "ONE OR MORE GATES FAILED (<${THRESHOLD}% on some modules)."
fi
exit $overall
