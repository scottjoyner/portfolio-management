#!/usr/bin/env python3
"""Archive an old Hermes ledger and create a clean accounting-v2 competition book."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from hermes_agent_accounting import ACCOUNTING_VERSION, new_ledger

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LEDGER = ROOT / "data" / "hermes_agent_ledger.json"
DEFAULT_ARCHIVE = ROOT / "data" / "competition_archive"


def atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def archive_and_reset(ledger_path: Path, archive_dir: Path, starting_capital: float) -> tuple[Path | None, dict]:
    archived = None
    if ledger_path.exists():
        archive_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archived = archive_dir / f"hermes_agent_ledger_pre_v{ACCOUNTING_VERSION}_{stamp}.json"
        legacy = json.loads(ledger_path.read_text(encoding="utf-8") or "{}")
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--starting-capital", type=float, default=10_000.0)
    parser.add_argument("--yes", action="store_true", help="Required confirmation for reset")
    args = parser.parse_args()
    if not args.yes:
        parser.error("refusing to reset without --yes")
    archived, ledger = archive_and_reset(args.ledger, args.archive_dir, args.starting_capital)
    print(json.dumps({
        "ok": True,
        "accounting_version": ledger["accounting_version"],
        "ledger": str(args.ledger),
        "archive": str(archived) if archived else None,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
