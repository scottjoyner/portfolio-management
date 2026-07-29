#!/usr/bin/env python3
"""Start a fair bot-versus-agent competition epoch.

The command refuses to run while the EventTrader writer lock is held, requires a
fresh and flat bot book, archives/resets the Hermes ledger to accounting v2, and
captures both raw-equity baselines plus the paid-agent cost baseline. Ranking is
invalid until this manifest exists.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from competition_scoreboard import load_agent_cost, load_bot_book
from reset_agent_competition import (
    DEFAULT_ARCHIVE,
    DEFAULT_LEDGER,
    DEFAULT_LOCK as DEFAULT_AGENT_LOCK,
    archive_and_reset,
)

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BOT_STATE = ROOT / "data" / "paper_trader_v4_state.json"
DEFAULT_BOT_LOCK = ROOT / "data" / "trader-v4.lock"
DEFAULT_COST_LEDGER = ROOT / "data" / "agent_cost_ledger.json"
DEFAULT_EPOCH = ROOT / "data" / "competition_epoch.json"
DEFAULT_EPOCH_ARCHIVE = ROOT / "data" / "competition_epochs"
DEFAULT_SNAPSHOT = ROOT / "data" / "competition_state.json"


@contextlib.contextmanager
def require_writer_stopped(lock_path: Path) -> Iterator[None]:
    """Hold the bot writer lock or fail immediately when its process is active."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        if fcntl is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(
                    f"bot_writer_active:{lock_path}; stop EventTrader before starting an epoch"
                ) from exc
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid_json:{path}:{type(exc).__name__}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"invalid_json_object:{path}")
    return value


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


def archive_existing_epoch(epoch_path: Path, archive_dir: Path) -> Path | None:
    if not epoch_path.exists():
        return None
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = archive_dir / f"competition_epoch_{stamp}.json"
    shutil.copy2(epoch_path, destination)
    return destination


def start_epoch(
    *,
    agent_ledger: Path = DEFAULT_LEDGER,
    agent_archive: Path = DEFAULT_ARCHIVE,
    agent_lock: Path = DEFAULT_AGENT_LOCK,
    bot_state: Path = DEFAULT_BOT_STATE,
    bot_lock: Path = DEFAULT_BOT_LOCK,
    cost_ledger: Path = DEFAULT_COST_LEDGER,
    epoch_path: Path = DEFAULT_EPOCH,
    epoch_archive: Path = DEFAULT_EPOCH_ARCHIVE,
    snapshot_path: Path = DEFAULT_SNAPSHOT,
    normalized_capital: float = 10_000.0,
    stale_after: float = 900.0,
    now: float | None = None,
) -> dict:
    now = time.time() if now is None else now
    if normalized_capital <= 0:
        raise ValueError("normalized_capital must be > 0")

    with require_writer_stopped(bot_lock):
        bot, bot_warnings = load_bot_book(bot_state, now, stale_after)
        if bot.get("status") != "ok":
            raise RuntimeError(f"bot_book_not_ready:{','.join(sorted(bot_warnings))}")
        if bot.get("positions"):
            raise RuntimeError("bot_book_must_be_flat_before_epoch_start")
        bot_equity = bot.get("raw_equity_usd")
        if bot_equity is None:
            raise RuntimeError("bot_equity_unavailable")

        cost_payload = load_json(cost_ledger)
        current_agent_payload = load_json(agent_ledger)
        current_cost, cost_source = load_agent_cost(cost_payload, current_agent_payload)
        if current_cost is None:
            raise RuntimeError("agent_cost_baseline_unavailable")

        archived_ledger, agent = archive_and_reset(
            agent_ledger,
            agent_archive,
            normalized_capital,
            lock_path=agent_lock,
        )
        archived_epoch = archive_existing_epoch(epoch_path, epoch_archive)
        started_at = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        epoch = {
            "schema_version": 1,
            "epoch_id": f"competition-{datetime.fromtimestamp(now, tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}",
            "started_at": started_at,
            "started_epoch": now,
            "normalized_starting_capital_usd": float(normalized_capital),
            "agent_history_valid_from": agent["history_valid_from"],
            "baselines": {
                "agent_raw_equity_usd": float(agent["equity"]),
                "agent_realized_pnl_usd": float(agent.get("realized_pnl", 0.0)),
                "agent_cost_usd": float(current_cost),
                "bot_raw_equity_usd": float(bot_equity),
                "bot_realized_pnl_usd": float(bot.get("raw_realized_pnl_usd", 0.0)),
                "bot_fees_paid_usd": float(bot.get("raw_fees_paid_usd", 0.0)),
            },
            "evidence": {
                "agent_accounting_version": agent["accounting_version"],
                "agent_ledger_archive": str(archived_ledger) if archived_ledger else None,
                "previous_epoch_archive": str(archived_epoch) if archived_epoch else None,
                "bot_state": str(bot_state),
                "bot_trade_events_at_start": len(bot.get("trades", [])),
                "agent_cost_source": cost_source,
            },
        }
        atomic_write(epoch_path, epoch)
        try:
            snapshot_path.unlink()
        except FileNotFoundError:
            pass
        return epoch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--agent-archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--agent-lock", type=Path, default=DEFAULT_AGENT_LOCK)
    parser.add_argument("--bot-state", type=Path, default=DEFAULT_BOT_STATE)
    parser.add_argument("--bot-lock", type=Path, default=DEFAULT_BOT_LOCK)
    parser.add_argument("--cost-ledger", type=Path, default=DEFAULT_COST_LEDGER)
    parser.add_argument("--epoch", type=Path, default=DEFAULT_EPOCH)
    parser.add_argument("--epoch-archive", type=Path, default=DEFAULT_EPOCH_ARCHIVE)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--normalized-capital", type=float, default=10_000.0)
    parser.add_argument("--stale-after", type=float, default=900.0)
    parser.add_argument("--yes", action="store_true", help="Required destructive confirmation")
    args = parser.parse_args()
    if not args.yes:
        parser.error("refusing to archive/reset/start without --yes")
    epoch = start_epoch(
        agent_ledger=args.agent_ledger,
        agent_archive=args.agent_archive,
        agent_lock=args.agent_lock,
        bot_state=args.bot_state,
        bot_lock=args.bot_lock,
        cost_ledger=args.cost_ledger,
        epoch_path=args.epoch,
        epoch_archive=args.epoch_archive,
        snapshot_path=args.snapshot,
        normalized_capital=args.normalized_capital,
        stale_after=max(1.0, args.stale_after),
    )
    print(json.dumps({
        "ok": True,
        "epoch_id": epoch["epoch_id"],
        "started_at": epoch["started_at"],
        "normalized_starting_capital_usd": epoch["normalized_starting_capital_usd"],
        "next": "python scripts/competition_scoreboard.py --print-json",
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
