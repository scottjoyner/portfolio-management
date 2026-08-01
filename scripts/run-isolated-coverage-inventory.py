#!/usr/bin/env python3
"""Collect and run generated coverage tests in isolated domain processes.

The generated coverage corpus intentionally installs broad ``sys.modules``
stubs from ``tests/coverage/conftest.py``. Running every domain in a single
pytest interpreter lets those stubs and repeated package names contaminate
unrelated domains. This runner preserves the complete corpus while launching a
fresh pytest process for each top-level coverage domain.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Iterable


def _discover_groups(coverage_root: Path) -> list[tuple[str, list[Path]]]:
    groups: list[tuple[str, list[Path]]] = []
    root_tests = sorted(coverage_root.glob("test_*.py"))
    if root_tests:
        groups.append(("root", root_tests))

    for child in sorted(coverage_root.iterdir()):
        if not child.is_dir() or child.name.startswith("__"):
            continue
        if any(child.rglob("test_*.py")):
            groups.append((child.name, [child]))
    return groups


def _pythonpath(repo: Path, group_paths: Iterable[Path]) -> str:
    candidates: list[Path] = []
    for path in group_paths:
        candidates.append(path if path.is_dir() else path.parent)
    candidates.extend(
        [
            repo / "tests" / "coverage",
            repo,
            repo / "trading_system",
            repo / "graph-alpha-bot",
            repo / "graph-alpha-bot" / "scripts",
            repo / "graph-alpha-bot" / "app",
        ]
    )
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
        help="Only collect each assigned domain; do not execute tests.",
    )
    args = parser.parse_args()

    if args.shard_total < 1 or not 0 <= args.shard_index < args.shard_total:
        parser.error("shard index must be within [0, shard total)")

    repo = Path(__file__).resolve().parents[1]
    coverage_root = repo / "tests" / "coverage"
    output_dir = repo / args.output_dir / f"shard-{args.shard_index}"
    output_dir.mkdir(parents=True, exist_ok=True)

    groups = _discover_groups(coverage_root)
    assigned = [group for idx, group in enumerate(groups) if idx % args.shard_total == args.shard_index]
    summary: dict[str, object] = {
        "shard_index": args.shard_index,
        "shard_total": args.shard_total,
        "discovered_groups": len(groups),
        "assigned_groups": [name for name, _ in assigned],
        "groups": [],
    }

    failed = False
    for name, paths in assigned:
        print(f"\n===== coverage group: {name} =====", flush=True)
        env = dict(os.environ)
        env["PYTHONPATH"] = _pythonpath(repo, paths)
        # Keep any module-level state singleton inside a writable, group-local path.
        state_dir = output_dir / "state" / _safe_name(name)
        state_dir.mkdir(parents=True, exist_ok=True)
        env["TRADING_SYSTEM_STATE_DIR"] = str(state_dir)

        target_args = [str(path.relative_to(repo)) for path in paths]
        base_command = [sys.executable, "-m", "pytest", *target_args, "-q"]
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
            "targets": target_args,
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
