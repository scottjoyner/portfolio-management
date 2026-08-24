#!/bin/bash
# Guard against committing generated runtime state (issue #32).
# Fails when any tracked file matches runtime/cache/state patterns outside
# the approved allowlist. Used by CI and the pre-commit hook.
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

VIOLATIONS=$(git ls-files | grep -E \
  '(^|/)(data/.*)\.(json|jsonl)$|(^|/)state_backups/|\.log$|\.pid$|\.sqlite$|\.db$' || true)

# Allowlist: intentional, versioned fixtures/artifacts (issue #32 step 4).
ALLOWED_RE='graph-alpha-bot/app/data/knowledge_graph.json'

FILTERED=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  if ! echo "$f" | grep -qE "$ALLOWED_RE"; then
    FILTERED="$FILTERED$f\n"
  fi
done <<< "$VIOLATIONS"
VIOLATIONS=$(echo -e "$FILTERED" | sed '/^$/d')

if [ -n "$VIOLATIONS" ]; then
  echo "ERROR: generated runtime state is tracked in git (issue #32):" >&2
  echo "$VIOLATIONS" | sed 's/^/  /' >&2
  echo "Untrack with: git rm --cached <file>" >&2
  exit 1
fi

echo "runtime-state guard: clean"
