#!/usr/bin/env python3
"""Mutation harness that refuses to report a result it did not earn.

Used repeatedly in this work and it produced five false "NOT CAUGHT" results,
every one because the mutation's anchor did not exist in the file, so nothing was
changed and the tests correctly still passed. Two more false results came from
tests that had been appended into the wrong class and were never collected.

This harness therefore distinguishes three outcomes and never collapses them:

  MUTATION APPLIED / caught      the file changed and a test failed
  MUTATION APPLIED / NOT CAUGHT  the file changed and the suite still passed
  MUTATION NO-OP                the file did not change; no conclusion is drawn

Usage:
  python3 scripts/mutation_check.py <spec.json>

The spec is a list of cases:

  {
    "description": "what this mutation simulates",
    "edits": [{"path": "relative/file.py", "find": "...", "replace": "..."}],
    "pytest": "tests/test_x.py -k Something"
  }
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def apply_edits(edits: list[dict]) -> list[str]:
    """Apply each edit, reporting which actually changed something.

    Returns a list of failures; empty means every edit landed.
    """
    problems = []
    for edit in edits:
        path = REPO_ROOT / edit["path"]
        original = path.read_text(encoding="utf-8")
        find = edit["find"]
        count = original.count(find)
        if count == 0:
            problems.append(
                f"{edit['path']}: anchor not found ({count} matches) -> "
                f"{find[:70]!r}")
            continue
        if count != 1 and not edit.get("replace_all"):
            problems.append(
                f"{edit['path']}: anchor matched {count} times, refusing to guess -> "
                f"{find[:70]!r}")
            continue
        updated = original.replace(find, edit["replace"])
        if updated == original:
            problems.append(f"{edit['path']}: replace produced no change")
            continue
        path.write_text(updated, encoding="utf-8")
    return problems


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    spec = json.loads(Path(sys.argv[1]).read_text())
    env = {**os.environ, "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'trading_system'}"}
    python = str(REPO_ROOT / ".venv" / "bin" / "python")
    if not Path(python).exists():
        python = sys.executable

    caught = not_caught = no_op = 0
    failures = []

    for case in spec:
        description = case["description"]
        backups = {}
        try:
            for edit in case["edits"]:
                path = REPO_ROOT / edit["path"]
                backups[edit["path"]] = path.read_text(encoding="utf-8")

            problems = apply_edits(case["edits"])
            if problems:
                no_op += 1
                print(f"  NO-OP   {description}")
                for problem in problems:
                    print(f"            {problem}")
                failures.append(f"{description}: no-op mutation, no conclusion")
                continue

            result = subprocess.run(
                [python, "-m", "pytest", *case["pytest"].split(), "-q"],
                capture_output=True, text=True, timeout=case.get("timeout", 600),
                cwd=str(REPO_ROOT), env=env,
            )
            if result.returncode != 0:
                caught += 1
                print(f"  caught  {description}")
            else:
                not_caught += 1
                print(f"  MISSED  {description}")
                failures.append(f"{description}: mutation applied but nothing failed")
        finally:
            for path, text in backups.items():
                (REPO_ROOT / path).write_text(text, encoding="utf-8")

    print()
    print(f"  caught {caught} | missed {not_caught} | no-op {no_op}")
    if not_caught or no_op:
        print("\n  Cases that did not earn a verdict must be fixed before the result means anything.")
        for failure in failures:
            print(f"    - {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())