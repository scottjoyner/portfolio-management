#!/usr/bin/env python3
"""
hermes_agent_loop.py — one iteration of the Hermes agent's PAPER trading competition.

Called periodically (cron). For each candidate pair it:
  1. pulls recent 1h candles (read-only, via CBClient)
  2. computes a naive momentum signal (last close vs N hours ago)
  3. if |momentum| exceeds threshold, records a PAPER fill via hermes_agent_trader
     (real exchange quote, NO money moved, capped at MAX_NOTIONAL)

All trading is paper/simulation. A live path does not exist here; hermes_agent_trader
refuses any live attempt unless HERMES_AGENT_LIVE is set by the operator.

ENV (read-only, inherited from .env):
  KILL_SWITCH, REQUIRE_MANUAL_APPROVAL, MAX_NOTIONAL_PER_TRADE_USD
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

KILL_SWITCH = os.getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")
try:
    MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "10"))
except ValueError:
    MAX_NOTIONAL = 10.0

PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD"]
HOURS = 5
MOMENTUM_THRESHOLD = 0.004  # 0.4% move over HOURS to trigger a paper signal
# Trade at 99% of the cap so base*price rounding never breaches MAX_NOTIONAL.
NOTIONAL_PER_SIGNAL = round(min(MAX_NOTIONAL * 0.99, 10.0), 2)


def _candles(client, product_id: str, hours: int) -> list[dict]:
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=hours)
    r = client._cli_json(
        "products", "candles", product_id, "granularity=1h",
        f"start={start.isoformat()}", f"end={end.isoformat()}",
    )
    if isinstance(r, dict) and "candles" in r:
        return r["candles"]
    return []


def momentum_signal(candles: list[dict]) -> tuple[str | None, float, float]:
    """Return (side, momentum_pct, last_close). side in {BUY,SELL,None}."""
    if len(candles) < 2:
        return None, 0.0, 0.0
    closes = [float(c["close"]) for c in candles if c.get("close")]
    if len(closes) < 2:
        return None, 0.0, closes[-1] if closes else 0.0
    first, last = closes[0], closes[-1]
    if first <= 0:
        return None, 0.0, last
    mom = (last - first) / first
    if mom > MOMENTUM_THRESHOLD:
        return "BUY", mom, last
    if mom < -MOMENTUM_THRESHOLD:
        return "SELL", mom, last
    return None, mom, last


def run_once(verbose: bool = True) -> dict:
    if KILL_SWITCH:
        msg = "KILL_SWITCH active — agent loop skipped"
        if verbose:
            print(msg)
        return {"skipped": True, "reason": "kill_switch"}
    from coinbase.src.cb_client import CBClient
    from scripts.hermes_agent_trader import record_signal
    client = CBClient(dry_run_cli=True)
    results = []
    for pair in PAIRS:
        try:
            candles = _candles(client, pair, HOURS)
            side, mom, last = momentum_signal(candles)
            if not side:
                results.append({"pair": pair, "signal": "HOLD", "momentum": round(mom, 5),
                                "last": round(last, 2)})
                continue
            if side == "BUY":
                rec = record_signal(pair, "BUY", quote_size=NOTIONAL_PER_SIGNAL,
                                    note=f"momentum+{mom:.4f}")
            else:
                base = round(NOTIONAL_PER_SIGNAL / last, 8)
                rec = record_signal(pair, "SELL", base_size=base,
                                    note=f"momentum-{mom:.4f}")
            results.append({"pair": pair, "signal": side, "momentum": round(mom, 5),
                           "result": rec.get("action", "?")})
        except Exception as exc:  # never crash the loop on a bad quote
            results.append({"pair": pair, "signal": "ERROR", "error": str(exc)[:120]})
    if verbose:
        ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        print(f"[{ts}] agent loop iteration: {json.dumps(results)}")
    return {"skipped": False, "iterations": results}


def main() -> int:
    ap = __import__("argparse").ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    run_once(verbose=not args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
