#!/usr/bin/env python3
"""Archive an old Hermes ledger and create a clean accounting-v2 book.

This command takes the same exclusive ledger lock used by the active Hermes
facade. It never rewrites legacy positions into the new schema.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

try:
    from scripts.hermes_agent_accounting import ACCOUNTING_VERSION, new_ledger
except ImportError:  # direct ``python scripts/reset_agent_competition.py``
    from hermes_agent_accounting import ACCOUNTING_VERSION, new_ledger

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LEDGER = ROOT / "data" / "hermes_agent_ledger.json"
DEFAULT_ARCHIVE = ROOT / "data" / "legacy_agent_ledgers"
DEFAULT_LOCK = ROOT / "data" / "hermes_agent_ledger.lock"


@contextlib.contextmanager
def exclusive_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def archive_and_reset(
    ledger_path: Path,
    archive_dir: Path,
    starting_capital: float,
    *,
    lock_path: Path = DEFAULT_LOCK,
) -> tuple[Path | None, dict]:
    with exclusive_lock(lock_path):
        archived = None
        if ledger_path.exists():
            archive_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            archived = archive_dir / f"hermes_agent_ledger_pre_v{ACCOUNTING_VERSION}_{stamp}.json"
            try:
                legacy = json.loads(ledger_path.read_text(encoding="utf-8") or "{}")
            except json.JSONDecodeError:
                legacy = {"unparsed_legacy_ledger": ledger_path.read_text(encoding="utf-8", errors="replace")}
            legacy["ranking_eligible"] = False
            legacy["invalidated_reason"] = "legacy_agent_accounting_can_apply_leverage_twice"
            legacy["invalidated_at"] = datetime.now(timezone.utc).isoformat()
            atomic_write(archived, legacy)

        ledger = new_ledger(starting_capital)
        ledger["reset_reason"] = "accounting_v2_clean_competition_start"
        ledger["previous_ledger_archive"] = str(archived) if archived else None
        atomic_write(ledger_path, ledger)
        return archived, ledger


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--starting-capital", type=float, default=10_000.0)
    parser.add_argument("--yes", action="store_true", help="Required confirmation for reset")
    args = parser.parse_args()
    if not args.yes:
        parser.error("refusing to reset without --yes")
    archived, ledger = archive_and_reset(
        args.ledger,
        args.archive_dir,
        args.starting_capital,
        lock_path=args.lock,
    )
    print(json.dumps({
        "ok": True,
        "accounting_version": ledger["accounting_version"],
        "ranking_eligible": ledger["ranking_eligible"],
        "ledger": str(args.ledger),
        "archive": str(archived) if archived else None,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
