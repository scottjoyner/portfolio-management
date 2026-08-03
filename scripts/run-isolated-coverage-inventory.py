#!/usr/bin/env python3
"""Run active generated coverage files in fresh, bounded Python processes.

Generated coverage is treated as a catalog of snapshots rather than one Python
package: each active file gets a clean interpreter. Superseded snapshots must
be declared in ``tests/coverage/retired_tests.json`` with a concrete reason;
they remain visible in artifacts and counts instead of being silently ignored.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


def _discover_groups(coverage_root: Path) -> list[tuple[str, Path]]:
    groups: list[tuple[str, Path]] = []
    for test_file in sorted(coverage_root.rglob("test_*.py")):
        relative = test_file.relative_to(coverage_root)
        name = str(relative.with_suffix("")).replace(os.sep, "__")
        groups.append((name, test_file))
    return groups


def _load_retired(repo: Path, coverage_root: Path) -> dict[str, str]:
    registry_path = coverage_root / "retired_tests.json"
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("unsupported generated-test retirement schema")

    retired: dict[str, str] = {}
    for group in payload.get("groups", []):
        reason = str(group.get("reason", "")).strip()
        if not reason:
            raise ValueError("every retired generated-test group requires a reason")
        for raw_path in group.get("paths", []):
            path = str(raw_path)
            if path in retired:
                raise ValueError(f"duplicate retired generated test: {path}")
            if not (repo / path).is_file():
                raise ValueError(f"retired generated test does not exist: {path}")
            retired[path] = reason
    return retired


def _pythonpath(repo: Path, test_file: Path) -> str:
    """Expose file-local helpers without shadowing canonical packages."""

    candidates = [
        test_file.parent,
        repo,
        repo / "trading_system",
        repo / "graph-alpha-bot",
        repo / "graph-alpha-bot" / "scripts",
        repo / "graph-alpha-bot" / "app",
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


def _run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    output: Path,
    timeout_seconds: int,
) -> tuple[int, bool]:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=timeout_seconds,
        )
        text = completed.stdout
        returncode = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        text = (
            f"{stdout}\n"
            f"TIMEOUT: pytest exceeded {timeout_seconds} seconds for this file.\n"
        )
        returncode = 124
        timed_out = True

    output.write_text(text, encoding="utf-8")
    sys.stdout.write(text)
    if text and not text.endswith("\n"):
        sys.stdout.write("\n")
    return returncode, timed_out


def _status_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        status = str(record["status"])
        counts[status] = counts.get(status, 0) + 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-total", type=int, required=True)
    parser.add_argument("--output-dir", default="full-python-inventory/coverage")
    parser.add_argument("--file-timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Only collect each assigned active test file; do not execute tests.",
    )
    args = parser.parse_args()

    if args.shard_total < 1 or not 0 <= args.shard_index < args.shard_total:
        parser.error("shard index must be within [0, shard total)")
    if args.file_timeout_seconds < 1:
        parser.error("file timeout must be positive")

    repo = Path(__file__).resolve().parents[1]
    coverage_root = repo / "tests" / "coverage"
    output_dir = repo / args.output_dir / f"shard-{args.shard_index}"
    output_dir.mkdir(parents=True, exist_ok=True)

    groups = _discover_groups(coverage_root)
    discovered_targets = {str(path.relative_to(repo)) for _, path in groups}
    retired = _load_retired(repo, coverage_root)
    untracked_retired = set(retired) - discovered_targets
    if untracked_retired:
        raise ValueError(f"retirement registry contains non-tests: {sorted(untracked_retired)}")

    assigned = [
        group
        for index, group in enumerate(groups)
        if index % args.shard_total == args.shard_index
    ]
    records: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "shard_index": args.shard_index,
        "shard_total": args.shard_total,
        "file_timeout_seconds": args.file_timeout_seconds,
        "discovered_groups": len(groups),
        "retired_registry_count": len(retired),
        "assigned_groups": [name for name, _ in assigned],
        "groups": records,
    }

    failed = False
    for name, test_file in assigned:
        relative_target = str(test_file.relative_to(repo))
        retirement_reason = retired.get(relative_target)
        if retirement_reason is not None:
            print(
                f"\n===== retired coverage file: {relative_target} =====\n"
                f"{retirement_reason}",
                flush=True,
            )
            records.append(
                {
                    "name": name,
                    "target": relative_target,
                    "status": "retired",
                    "reason": retirement_reason,
                }
            )
            continue

        print(f"\n===== active coverage file: {relative_target} =====", flush=True)
        env = dict(os.environ)
        env["PYTHONPATH"] = _pythonpath(repo, test_file)
        state_dir = output_dir / "state" / _safe_name(name)
        state_dir.mkdir(parents=True, exist_ok=True)
        env["TRADING_SYSTEM_STATE_DIR"] = str(state_dir)

        command = [
            sys.executable,
            "-m",
            "pytest",
            relative_target,
            "--import-mode=importlib",
            "-q",
        ]
        if args.collect_only:
            command.append("--collect-only")

        safe = _safe_name(name)
        suffix = "collection" if args.collect_only else "results"
        result_file = output_dir / f"{safe}-{suffix}.txt"
        returncode, timed_out = _run(
            command,
            cwd=repo,
            env=env,
            output=result_file,
            timeout_seconds=args.file_timeout_seconds,
        )
        status = "passed" if returncode == 0 else ("timed_out" if timed_out else "failed")
        records.append(
            {
                "name": name,
                "target": relative_target,
                "returncode": returncode,
                "timed_out": timed_out,
                "output": str(result_file.relative_to(repo)),
                "status": status,
            }
        )
        if returncode != 0:
            failed = True

    summary["status_counts"] = _status_counts(records)
    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"\nInventory summary: {summary_path.relative_to(repo)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
