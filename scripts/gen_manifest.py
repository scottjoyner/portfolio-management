#!/usr/bin/env python3
"""Generate the coverage manifest: every target Python source module.

Emitted as repo-relative paths (POSIX). Excludes tests, virtualenvs, broken
files, __init__.py, and non-source directories.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

EXCLUDE_DIRS = {
    ".git", ".venv", ".venv_test", "node_modules", "archive", "artifacts",
    "docs", "deploy", "scripts", "rust_core", "__pycache__", ".ruff_cache",
    ".pytest_cache", "data", "logs", "state", "historical_data", ".news_cache",
    ".hermes", ".github", "runtime", "storage", ".cb_sdk_env", "coinboard",
    "deathstar_sync", "deathstar-transfer", "infra", "cron", ".venv_test",
}

# Files that do not parse under the test interpreter (syntax errors / py2).
BROKEN_FILES = {
    "graph-alpha-bot/app/tools/demo_signals.py",
    "trading_system/strategies/trend/macd_crossover.py",
    "trading_system/backtest/test_suites/robust_backtest_suite.py",
    "trading_system/backtest/test_suites/robust_test_suite.py",
    "trading_system/backtest/strategies/kalshi_poly_arb_backtest.py",
}


def is_source(p: Path) -> bool:
    rel = p.relative_to(ROOT).as_posix()
    if rel in BROKEN_FILES:
        return False
    parts = set(p.relative_to(ROOT).parts)
    if parts & EXCLUDE_DIRS:
        return False
    name = p.name
    if name == "__init__.py":
        return False
    if name.startswith("test_") or name.endswith("_test.py"):
        return False
    # Only include files that parse.
    try:
        ast.parse(p.read_text(encoding="utf-8"))
    except Exception:
        return False
    return True


def main() -> int:
    out = []
    for p in sorted(ROOT.rglob("*.py")):
        if is_source(p):
            out.append(p.relative_to(ROOT).as_posix())
    Path(sys.argv[1] if len(sys.argv) > 1 else "scripts/coverage/python_manifest.txt").write_text("\n".join(out) + "\n")
    print(f"Wrote {len(out)} modules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
