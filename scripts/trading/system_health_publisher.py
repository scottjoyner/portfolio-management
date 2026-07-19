#!/usr/bin/env python3
"""Publish host-only Portfolio OS system truth atomically into data/system-health.json.

Runs under the production supervisor. It never changes trading state, starts/stops
services, or probes a broker; it only observes local health and durable cache state.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA = ROOT / "data"
OUTPUT = DATA / "system-health.json"
TRADER_URL = "http://127.0.0.1:9090/health"
STALE_SECONDS = 180.0
PAPER_STATE = DATA / "paper_trader_v4_state.json"
MAX_PUBLISHED_POSITIONS = 20


def freshness(age: float | None) -> str:
    return "unknown" if age is None else ("fresh" if age <= STALE_SECONDS else "stale")


def trader_health() -> dict:
    try:
        with urllib.request.urlopen(TRADER_URL, timeout=0.25) as response:
            payload = json.loads(response.read().decode("utf-8"))
        mode = str(payload.get("mode", payload.get("trading_mode", ""))).lower()
        return {"available": True, "mode": mode if mode in {"paper", "live"} else None,
                "status": str(payload.get("status", "unknown")), "source": "127.0.0.1:9090"}
    except Exception as exc:
        return {"available": False, "mode": None, "status": "unknown", "source": "127.0.0.1:9090", "error": type(exc).__name__}


def unknown_paper_book() -> dict:
    return {
        "status": "unknown", "source": "paper_trader_v4_state.json", "state_age_sec": None,
        "mode": None, "schema_version": None, "cash_usd": None, "realized_pnl_usd": None,
        "fees_paid_usd": None, "open_positions": None, "gross_exposure_usd": None,
        "capital_in_play_usd": None, "positions": None,
    }


def finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def paper_book(path: Path = PAPER_STATE, now: float | None = None) -> dict:
    """Read the atomically-written paper ledger as bounded, fail-closed telemetry."""
    now = time.time() if now is None else now
    unknown = unknown_paper_book()
    try:
        state_age = max(0.0, now - path.stat().st_mtime)
        payload = json.loads(path.read_text())
    except (OSError, ValueError, TypeError):
        return unknown
    if state_age > STALE_SECONDS or not isinstance(payload, dict) or str(payload.get("mode", "")).lower() != "paper":
        return unknown

    cash = finite_number(payload.get("paper_cash"))
    realized = finite_number(payload.get("paper_realized_pnl", 0.0))
    fees = finite_number(payload.get("paper_fees_paid", 0.0))
    raw_positions = payload.get("paper_positions")
    if cash is None or realized is None or fees is None or not isinstance(raw_positions, (dict, list)):
        return unknown
    if isinstance(raw_positions, dict):
        position_items = list(raw_positions.items())
    else:
        position_items = [
            (str(position.get("product_id", position.get("symbol", ""))) if isinstance(position, dict) else "", position)
            for position in raw_positions
        ]

    published_positions = []
    gross = 0.0
    for product_id, position in position_items:
        if not product_id or not isinstance(position, dict):
            return unknown
        qty = finite_number(position.get("qty", position.get("quantity")))
        entry_price = finite_number(position.get("entry_price"))
        entry_notional = finite_number(position.get("entry_notional"))
        notional = abs(entry_notional) if entry_notional is not None else (abs(qty * entry_price) if qty is not None and entry_price is not None else None)
        if qty is None or entry_price is None or notional is None:
            return unknown
        gross += notional
        if len(published_positions) < MAX_PUBLISHED_POSITIONS:
            published_positions.append({
                "product_id": product_id, "qty": qty, "entry_price": entry_price,
                "entry_notional_usd": notional,
            })

    return {
        "status": "ok", "source": "paper_trader_v4_state.json", "state_age_sec": round(state_age, 1),
        "mode": "paper", "schema_version": payload.get("state_schema_version"), "cash_usd": cash,
        "realized_pnl_usd": realized, "fees_paid_usd": fees, "open_positions": len(position_items),
        "gross_exposure_usd": gross, "capital_in_play_usd": gross, "positions": published_positions,
    }


def service_state(name: str) -> str:
    try:
        return subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True, timeout=2).stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def build() -> dict:
    now = time.time()
    heartbeat = DATA / ".daemon_heartbeat"
    age = None
    try:
        age = max(0.0, now - float(heartbeat.read_text().strip()))
    except (OSError, ValueError):
        pass
    trader = trader_health()
    trader["paper_book"] = paper_book() if trader["available"] and trader["mode"] == "paper" else unknown_paper_book()
    try:
        from data.feed_cache import inspect_cache
        cache = inspect_cache()
    except Exception as exc:
        cache = {"status": "unknown", "source": "feed_cache", "error": type(exc).__name__}
    return {
        "generated_at": now,
        "trading_mode": trader["mode"],
        "trader": trader,
        "feed": {"heartbeat": {"age_sec": round(age, 1) if age is not None else None, "freshness": freshness(age)}},
        "cache": cache,
        "services": {"portfolio_trader": service_state("portfolio-trader.service")},
    }


def publish() -> None:
    DATA.mkdir(exist_ok=True)
    payload = json.dumps(build(), sort_keys=True, separators=(",", ":"))
    fd, temporary = tempfile.mkstemp(prefix=".system-health.", suffix=".tmp", dir=DATA)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, OUTPUT)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=15.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    while True:
        publish()
        if args.once:
            return
        time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    main()
