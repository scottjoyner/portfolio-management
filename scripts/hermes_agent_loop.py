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
    from scripts.hermes_agent_trader import (record_signal, close_position,
                                             open_short, close_short,
                                             drawdown_circuit, size_for, load_ledger)
    from scripts.hermes_regime import classify_candles, compatible_side
    from scripts.hermes_meta import load_bot_edge, asset_edge
    from scripts.hermes_mtf import multi_timeframe_regime, vol_regime, conviction
    from scripts.hermes_overlay import overlay_state

    client = CBClient(dry_run_cli=True)

    # --- Phase 6+7: MULTI-TIMEFRAME regime + VOL + SENTIMENT/EVENT overlay ---
    mtf = multi_timeframe_regime("BTC-USD", client)
    regime = mtf["regime"]  # MIXED if <2/3 timeframes agree (fakeout guard)
    # vol regime on the 4h BTC window (stand down new entries if EXTREME)
    from scripts.hermes_agent_loop import _candles as _c
    btc_4h = _c(client, "BTC-USD", 120, granularity="4h")
    vol = vol_regime(btc_4h)
    overlay = overlay_state()  # sentiment (F&G) + macro-event risk
    if verbose:
        print(f"[mtf] {regime} votes={mtf['votes']} vol={vol['bucket']} "
              f"fg={overlay['fear_greed'].get('bucket')} ev={overlay['event']['reason']}")

    # Composite NEW-ENTRY gate (any of these close the gate; open positions still managed):
    #  - MIXED regime (timeframes disagree)
    #  - EXTREME vol
    #  - drawdown circuit tripped (Phase 4)
    #  - EXTREME_GREED + macro event (sell-the-news, Phase 7)
    circuit = drawdown_circuit()
    circuit_open = circuit["open"]
    entry_gate_open = (regime != "MIXED" and not vol["stand_down"]
                      and circuit_open and not overlay["stand_down_new"])
    if verbose and not entry_gate_open:
        why = ("MIXED" if regime == "MIXED" else
               "EXTREME_VOL" if vol["stand_down"] else
               f"circuit:{circuit['reason']}" if not circuit_open else
               "sentiment_standdown")
        print(f"[gate] CLOSED new entries: {why}")

    # quick stand-down on CRISIS / MIXED / EXTREME_VOL
    if regime in ("CRISIS", "MIXED") or vol["stand_down"]:
        # still run close-pass below, but skip new entries
        pass
    # Meta-layer: load the bot's own per-asset edge once per run.
    bot_edge = load_bot_edge()
    from scripts.hermes_agent_trader import load_ledger, close_position, close_short
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
            if pid.startswith("SHORT:"):
                true_pid = pid.replace("SHORT:", "")
                res = close_short(true_pid, note=f"exit-{regime}{'-stale' if stale else ''}")
                if verbose and res.get("action") == "short_closed":
                    print(f"[close-short] {true_pid} -> realized_pnl={res.get('realized_pnl')}")
            else:
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

    # --- Alts: regime-gated, META-filtered, BOTH SIDES (Phase 1+2+3) ---
    # TREND_UP   -> LONG momentum (ride the trend the oracle confirms)
    # TREND_DOWN  -> SHORT momentum (profit from the downtrend)
    # RANGE       -> fade extremes: dip-BUY on downside, short on upside
    # Meta-filter: bot_bleeds_here demands 1.5x stronger own-signal.
    for pair in ALTS:
        try:
            ed = asset_edge(pair, bot_edge)
            candles = _candles(client, pair, HOURS)
            side, mom, last = momentum_signal(candles)

            # Decide desired action from regime + own momentum
            want_long = (regime == "TREND_UP" and side == "BUY") or \
                        (regime == "RANGE" and mom < -MOM_RANGE)
            want_short = (regime == "TREND_DOWN" and side == "SELL") or \
                         (regime == "RANGE" and mom > MOM_RANGE)
            if not (want_long or want_short):
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 5),
                               "regime": regime})
                continue

            # Composite gate (Phase 4+6+7): MIXED regime / EXTREME vol /
            # drawdown circuit / extreme-greed+event all block NEW entries.
            if not entry_gate_open:
                why = ("MIXED" if regime == "MIXED" else
                       "EXTREME_VOL" if vol["stand_down"] else
                       f"circuit:{circuit['reason']}" if not circuit_open else
                       "sentiment_standdown")
                results.append({"pair": pair, "signal": "HOLD", "mom": round(mom, 5),
                               "reason": f"gate:{why}"})
                continue

            # Meta penalty: bot bleeds here -> demand 1.5x stronger signal
            if ed["verdict"] == "bot_bleeds_here":
                thr_ok = (regime == "TREND_UP" and abs(mom) > MOM_TREND * 1.5) or \
                         (regime == "TREND_DOWN" and abs(mom) > MOM_TREND * 1.5) or \
                         (regime == "RANGE" and abs(mom) > MOM_RANGE * 1.5)
                if not thr_ok:
                    results.append({"pair": pair, "signal": "HOLD",
                                   "mom": round(mom, 5),
                                   "reason": "bot_bleeds+weak",
                                   "bot_edge": ed["edge"]})
                    continue

            # Phase 4+6+7 sizing: signal strength × vol-conviction × sentiment mult
            size_mult = conviction(mom, vol["bucket"]) * overlay["size_mult"]
            notional = round(size_for(mom) * size_mult, 2)
            if want_long:
                rec = record_signal(pair, "BUY", quote_size=notional,
                                    note=f"regime-{regime}-LONG-mom+{mom:.4f}-bot:{ed['verdict']}-sz{notional}")
                results.append({"pair": pair, "signal": "BUY", "mom": round(mom, 5),
                               "size": notional, "bot_edge": ed["edge"],
                               "result": rec.get("action", "?")})
            else:  # short
                rec = open_short(pair, notional,
                               note=f"regime-{regime}-SHORT-mom-{mom:.4f}-bot:{ed['verdict']}-sz{notional}")
                results.append({"pair": pair, "signal": "SHORT", "mom": round(mom, 5),
                               "size": notional, "bot_edge": ed["edge"],
                               "result": rec.get("action", "?")})
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
