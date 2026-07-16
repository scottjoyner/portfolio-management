#!/usr/bin/env python3
"""
hermes_agent_loop.py — one iteration of the Hermes agent's PAPER trading competition.

REGIME-GATED MOMENTUM (Phase 1 edge over the bot):
  The bot trades each pair in isolation. This loop uses BTC-USD as a cross-asset
  oracle: it classifies the BTC regime (TREND_UP / TREND_DOWN / RANGE / CRISIS)
  and GATES alt signals on regime compatibility:
    - TREND_UP   -> only LONG/BUY momentum on alts
    - TREND_DOWN  -> only SHORT/SELL momentum on alts
    - RANGE       -> either side, but require a stronger extreme to "fade"
    - CRISIS      -> STAND DOWN (no trades; event/vol risk)
  BTC itself only trades its own momentum in TREND regimes (avoids range whipsaw).

All trading is paper/simulation. A live path does not exist here; hermes_agent_trader
refuses any live attempt unless HERMES_AGENT_LIVE is set by the operator.

ENV (read-only, inherited from .env): KILL_SWITCH, REQUIRE_MANUAL_APPROVAL,
MAX_NOTIONAL_PER_TRADE_USD
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

# alt universe: liquid majors + the bot's PROVEN-LOSING alts (where my regime
# method may have alpha the bot lacks). The meta-filter penalizes the latter.
ALTS = ["ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD", "ADA-USD",
         "DOT-USD", "NCT-USD", "PERP-USD", "STORJ-USD", "ZEC-USD", "GNO-USD"]
BTC = "BTC-USD"
HOURS = 5
# momentum thresholds: smaller in RANGE (fade extremes), larger in TREND (confirm)
MOM_TREND = 0.004   # 0.4%
MOM_RANGE = 0.007   # 0.7% (stronger extreme to fade in range)
# Trade at 99% of the cap so base*price rounding never breaches MAX_NOTIONAL.
NOTIONAL_PER_SIGNAL = round(min(MAX_NOTIONAL * 0.99, 10.0), 2)


def _candles(client, product_id: str, hours: int, granularity: str = "1h") -> list[dict]:
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=hours)
    r = client._cli_json(
        "products", "candles", product_id, f"granularity={granularity}",
        f"start={start.isoformat()}", f"end={end.isoformat()}",
    )
    if isinstance(r, dict) and "candles" in r:
        return r["candles"]
    return []


def momentum_signal(candles: list[dict]) -> tuple[str | None, float, float]:
    if len(candles) < 2:
        return None, 0.0, 0.0
    closes = [float(c["close"]) for c in candles if c.get("close")]
    if len(closes) < 2:
        return None, 0.0, closes[-1] if closes else 0.0
    first, last = closes[0], closes[-1]
    if first <= 0:
        return None, 0.0, last
    mom = (last - first) / first
    thr = MOM_TREND  # default; loop may pass a stricter threshold
    if mom > thr:
        return "BUY", mom, last
    if mom < -thr:
        return "SELL", mom, last
    return None, mom, last


def run_once(verbose: bool = True) -> dict:
    if KILL_SWITCH:
        msg = "KILL_SWITCH active — agent loop skipped"
        if verbose:
            print(msg)
        return {"skipped": True, "reason": "kill_switch"}

    from coinbase.src.cb_client import CBClient
    from scripts.hermes_agent_trader import record_signal, close_position
    from scripts.hermes_regime import classify_btc, compatible_side
    from scripts.hermes_meta import load_bot_edge, asset_edge

    client = CBClient(dry_run_cli=True)
    regime_info = classify_btc()
    regime = regime_info["regime"]
    if verbose:
        print(f"[regime] BTC={regime} ({regime_info['reason']}) last={regime_info.get('last')}")

    # Meta-layer: load the bot's own per-asset edge once per run. The bot cannot
    # analyze its own _records; we use it as a confirmation filter (Phase 2 edge).
    bot_edge = load_bot_edge()
    # --- close-pass: exit positions on CRISIS or stale hold ---
    from scripts.hermes_agent_trader import load_ledger
    led = load_ledger()
    HOLD_ITERS = 6
    now = dt.datetime.now(dt.timezone.utc)
    for pid, pos in list(led.get("positions", {}).items()):
        if pos.get("base", 0.0) <= 1e-12:
            continue
        entry = pos.get("entry_ts")
        stale = False
        if entry:
            try:
                age = (now - dt.datetime.fromisoformat(entry)).total_seconds() / 60.0
                stale = age > HOLD_ITERS * 15  # cron cadence = 15m
            except Exception:
                stale = False
        if regime == "CRISIS" or stale:
            res = close_position(pid, note=f"exit-{regime}{'-stale' if stale else ''}")
            if verbose and res.get("action") == "closed":
                print(f"[close] {pid} -> realized_pnl={res.get('realized_pnl')}")

    if regime == "CRISIS":
        return {"skipped": True, "reason": "crisis_standdown", "regime": regime}

    results = []

    # --- BTC itself: only trade its momentum in TREND regimes ---
    if regime in ("TREND_UP", "TREND_DOWN"):
        candles = _candles(client, BTC, HOURS)
        side, mom, last = momentum_signal(candles)
        if side:
            rec = record_signal(BTC, side, quote_size=NOTIONAL_PER_SIGNAL,
                                note=f"btc-{regime}-mom{mom:.4f}")
            results.append({"pair": BTC, "signal": side, "mom": round(mom, 5),
                           "result": rec.get("action", "?")})
    else:
        results.append({"pair": BTC, "signal": "HOLD", "reason": f"range:{regime}"})

    # --- Alts: LONG-ONLY, regime-gated, META-filtered (Phase 1 + 2) ---
    # TREND_UP   -> BUY momentum (ride the trend the oracle confirms)
    # TREND_DOWN  -> stand down alts (no shorts in paper v1)
    # RANGE       -> dip-buy: BUY only on a downside extreme (fade)
    # Meta-filter: if the bot BLEEDS this asset (verified negative edge), require a
    #   STRONGER own-signal before touching it; if the bot WINS here, normal gate.
    for pair in ALTS:
        try:
            ed = asset_edge(pair, bot_edge)
            candles = _candles(client, pair, HOURS)
            side, mom, last = momentum_signal(candles)
            want_buy = (regime == "TREND_UP" and side == "BUY") or \
                       (regime == "RANGE" and mom < -MOM_RANGE)
            if not want_buy:
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 5),
                               "regime": regime})
                continue
            # Meta penalty: bot bleeds here -> demand 1.5x stronger signal
            if ed["verdict"] == "bot_bleeds_here":
                thr_ok = (regime == "TREND_UP" and abs(mom) > MOM_TREND * 1.5) or \
                         (regime == "RANGE" and mom < -MOM_RANGE * 1.5)
                if not thr_ok:
                    results.append({"pair": pair, "signal": "HOLD",
                                   "mom": round(mom, 5),
                                   "reason": "bot_bleeds+weak",
                                   "bot_edge": ed["edge"]})
                    continue
            rec = record_signal(pair, "BUY", quote_size=NOTIONAL_PER_SIGNAL,
                                note=f"regime-{regime}-mom+{mom:.4f}-bot:{ed['verdict']}")
            results.append({"pair": pair, "signal": "BUY", "mom": round(mom, 5),
                           "bot_edge": ed["edge"], "result": rec.get("action", "?")})
        except Exception as exc:
            results.append({"pair": pair, "signal": "ERROR", "error": str(exc)[:120]})

    if verbose:
        ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        print(f"[{ts}] agent loop: {json.dumps(results)}")
    return {"skipped": False, "regime": regime, "iterations": results}


def main() -> int:
    ap = __import__("argparse").ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    run_once(verbose=not args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
