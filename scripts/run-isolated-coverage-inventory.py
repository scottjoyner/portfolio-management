#!/usr/bin/env python3
"""Collect and run every generated coverage file in a fresh Python process.

The generated coverage corpus installs broad ``sys.modules`` stubs from its
root and per-directory conftests. Several test files also replace the same
module names with mutually incompatible fakes. Co-collecting a directory lets
one file's import-time state corrupt another file, producing order-dependent
failures that do not reproduce in isolation. This runner preserves every test
while giving each test file a clean interpreter.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def _discover_groups(coverage_root: Path) -> list[tuple[str, Path]]:
    groups: list[tuple[str, Path]] = []
    for test_file in sorted(coverage_root.rglob("test_*.py")):
        relative = test_file.relative_to(coverage_root)
        name = str(relative.with_suffix("")).replace(os.sep, "__")
        groups.append((name, test_file))
    return groups


def _pythonpath(repo: Path, test_file: Path) -> str:
    """Prefer canonical source packages, then file-local test helpers.

    ``tests/coverage`` itself is intentionally not added globally because it
    contains packages named ``alembic``, ``scripts``, ``strategies``, and other
    production names. The individual file's parent is sufficient for helper
    imports such as ``db_helpers`` without shadowing unrelated packages.
    """

    candidates = [
        repo,
        repo / "trading_system",
        repo / "graph-alpha-bot",
        repo / "graph-alpha-bot" / "scripts",
        repo / "graph-alpha-bot" / "app",
        test_file.parent,
    ]
    existing = os.environ.get("PYTHONPATH", "")
    ordered: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        value = str(candidate.resolve())
        if candidate.exists() and value not in seen:
            ordered.append(value)
            seen.add(value)
    for value in existing.split(os.pathsep):
        if value and value not in seen:
            ordered.append(value)
            seen.add(value)
    return os.pathsep.join(ordered)


def _safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-") or "group"


def _run(command: list[str], *, cwd: Path, env: dict[str, str], output: Path) -> int:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output.write_text(completed.stdout, encoding="utf-8")
    sys.stdout.write(completed.stdout)
    if completed.stdout and not completed.stdout.endswith("\n"):
        sys.stdout.write("\n")
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-total", type=int, required=True)
    parser.add_argument("--output-dir", default="full-python-inventory/coverage")
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Only collect each assigned test file; do not execute tests.",
    )
    args = parser.parse_args()

    if args.shard_total < 1 or not 0 <= args.shard_index < args.shard_total:
        parser.error("shard index must be within [0, shard total)")

    repo = Path(__file__).resolve().parents[1]
    coverage_root = repo / "tests" / "coverage"
    output_dir = repo / args.output_dir / f"shard-{args.shard_index}"
    output_dir.mkdir(parents=True, exist_ok=True)

    groups = _discover_groups(coverage_root)
    assigned = [group for index, group in enumerate(groups) if index % args.shard_total == args.shard_index]
    summary: dict[str, object] = {
        "shard_index": args.shard_index,
        "shard_total": args.shard_total,
        "discovered_groups": len(groups),
        "assigned_groups": [name for name, _ in assigned],
        "groups": [],
    }

    failed = False
    for name, test_file in assigned:
        relative_target = str(test_file.relative_to(repo))
        print(f"\n===== coverage file: {relative_target} =====", flush=True)
        env = dict(os.environ)
        env["PYTHONPATH"] = _pythonpath(repo, test_file)
        state_dir = output_dir / "state" / _safe_name(name)
        state_dir.mkdir(parents=True, exist_ok=True)
        env["TRADING_SYSTEM_STATE_DIR"] = str(state_dir)

        base_command = [
            sys.executable,
            "-m",
            "pytest",
            relative_target,
            "--import-mode=importlib",
            "-q",
        ]
        safe = _safe_name(name)
        collection_file = output_dir / f"{safe}-collection.txt"
        collection_rc = _run(
            [*base_command, "--collect-only"],
            cwd=repo,
            env=env,
            output=collection_file,
        )

        record: dict[str, object] = {
            "name": name,
            "target": relative_target,
            "collection_returncode": collection_rc,
            "collection_output": str(collection_file.relative_to(repo)),
        }
        if collection_rc != 0:
            failed = True
            record["test_returncode"] = None
            record["status"] = "collection_failed"
            summary["groups"].append(record)
            continue

        if args.collect_only:
            record["test_returncode"] = None
            record["status"] = "collected"
            summary["groups"].append(record)
            continue

        result_file = output_dir / f"{safe}-results.txt"
        test_rc = _run(base_command, cwd=repo, env=env, output=result_file)
        record["test_returncode"] = test_rc
        record["result_output"] = str(result_file.relative_to(repo))
        record["status"] = "passed" if test_rc == 0 else "failed"
        if test_rc != 0:
            failed = True
        summary["groups"].append(record)

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nInventory summary: {summary_path.relative_to(repo)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
