#!/usr/bin/env python3
"""Backfill `regime` onto the agent's closed (realized) trades.

Closed trades recorded before the regime-on-close fix (commit 020a6f75) have
no `regime` field, so hermes_expectancy collapsed every trade into the '?'
bucket and per-regime expectancy was blind. This joins each closed trade to the
most recent OPEN record for the same product_id (matching opening side) that
precedes it, and inherits that open's regime.

Join logic:
  closed.side == "SELL"        -> opened with side "BUY"
  closed.side == "SHORT_CLOSE" -> opened with side "SHORT_OPEN"
Pick the latest open with the matching side + product_id whose ts <= close ts.

Idempotent: only fills trades missing a truthy `regime`. Writes the ledger back.
"""
from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.hermes_agent_trader import load_ledger, save_ledger  # noqa: E402


def _parse_ts(ts: str) -> datetime:
    # ISO with possible 'Z' or offset; normalize
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def backfill() -> dict:
    led = load_ledger()
    trades = led.get("trades", [])
    opens = [t for t in trades if "realized_pnl" not in t]
    closed = [t for t in trades if "realized_pnl" in t]

    # index opens by (product_id, open_side) -> list of (ts, regime)
    by_key: dict[tuple[str, str], list[tuple[datetime, str]]] = {}
    note_key: dict[tuple[str, str, datetime], str] = {}
    for o in opens:
        pid = o.get("product_id", "")
        side = o.get("side", "")
        reg = o.get("regime") or ""
        try:
            t = _parse_ts(o["ts"])
        except Exception:
            continue
        by_key.setdefault((pid, side), []).append((t, reg))
        note_key[(pid, side, t)] = o.get("note", "")

    side_map = {"SELL": "BUY", "SHORT_CLOSE": "SHORT_OPEN"}
    filled = 0
    for c in closed:
        if c.get("regime"):
            continue
        pid = c.get("product_id", "")
        open_side = side_map.get(c.get("side", ""), "")
        if not open_side:
            continue
        try:
            ct = _parse_ts(c["ts"])
        except Exception:
            continue
        cands = by_key.get((pid, open_side), [])
        # latest open at or before the close
        match = None
        for ot, reg in sorted(cands, key=lambda x: x[0]):
            if ot <= ct and reg:
                match = reg
        # Fallback: BTC opens arrive via a separate local-momentum path whose
        # regime field is empty but whose note encodes it (btc-localUP / btc-localDOWN).
        if not match:
            for ot, _ in sorted(cands, key=lambda x: x[0]):
                if ot <= ct:
                    note = (note_key.get((pid, open_side, ot)) or "").lower()
                    if "localup" in note:
                        match = "LOCAL_UP"
                        break
                    if "localdown" in note:
                        match = "LOCAL_DOWN"
                        break
        if match:
            c["regime"] = match
            filled += 1

    save_ledger(led)
    return {"total_closed": len(closed), "filled": filled,
            "remaining_missing": sum(1 for c in closed if not c.get("regime"))}


if __name__ == "__main__":
    led_path = ROOT / "data" / "hermes_agent_ledger.json"
    shutil.copy(led_path, led_path.with_suffix(".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")))
    res = backfill()
    print(f"backfilled regime on {res['filled']}/{res['total_closed']} closed trades; "
          f"{res['remaining_missing']} still missing")
