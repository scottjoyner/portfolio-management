#!/usr/bin/env bash
# Usage: measure.sh <dotted.module.path> <testpath> [unique_tag]
ROOT="/home/scott/git/portfolio-management"
COV="$ROOT/.venv/bin/coverage"
MOD="$1"; TP="$2"; TAG="${3:-cov}"; shift 3 2>/dev/null
mkdir -p "$ROOT/scripts/coverage"
DF="$ROOT/scripts/coverage/${TAG}.coverage"
cd "$ROOT"
rm -f "$DF"
timeout 90 "$COV" run --branch --source="$MOD" --data-file="$DF" -m pytest "$TP" -q >/tmp/${TAG}.out 2>&1
if [ $? -ne 0 ]; then echo "RUN FAILED (see /tmp/${TAG}.out)"; tail -20 /tmp/${TAG}.out; fi
"$COV" report --data-file="$DF" | grep -E "$MOD"
