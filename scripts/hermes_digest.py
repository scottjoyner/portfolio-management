#!/usr/bin/env python3
"""
hermes_digest.py — Phase 13: DAILY AGENT-vs-BOT DIGEST.

Compact standing snapshot for the operator: regime + sentiment (F&G + news) +
agent's own walk-forward expectancy table + universe tilt + live-promotion
trigger. Designed to be posted to the Hermes chat on a schedule (cron) so the
operator gets a daily view without running anything by hand.

Read-only: ledger + RSS + exchange candles. Writes nothing.
"""
from __future__ import annotations

import sys as _sys
from pathlib import Path as _P

_REPO = str(_P(__file__).resolve().parent.parent)
if _REPO not in _sys.path:
    _sys.path.insert(0, _REPO)

import datetime as dt

from scripts.hermes_mtf import multi_timeframe_regime, vol_regime
from scripts.hermes_agent_loop import _candles
from coinbase.src.cb_client import CBClient


def _client():
    return CBClient(dry_run_cli=True)
from scripts.hermes_overlay import overlay_state
from scripts.hermes_news import news_sentiment
from scripts.hermes_expectancy import (
    expectancy_table, universe_tilt, live_ready, unified_tilt, write_unified_expectancy,
)
from scripts.hermes_agent_trader import load_ledger

KILL_SWITCH = __import__("os").getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")


def digest() -> str:
    if KILL_SWITCH:
        return "Hermes agent digest: KILL_SWITCH active — no snapshot (trading halted)."
    lines = []
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"=== HERMES AGENT DIGEST — {now} ===")

    # Regime + vol + sentiment
    try:
        client = _client()
        mtf = multi_timeframe_regime("BTC-USD", client)
        btc_4h = _candles(client, "BTC-USD", 120, granularity="4h")
        vol = vol_regime(btc_4h)
        overlay = overlay_state()
        news = news_sentiment()
        lines.append(f"Regime: {mtf['regime']} (votes={mtf['votes']})  Vol: {vol['bucket']}")
        lines.append(f"Sentiment: F&G={overlay['fear_greed'].get('bucket')}  "
                     f"News={news['bucket']}({news['score']})  "
                     f"Event={overlay['event']['reason']}")
    except Exception as e:
        lines.append(f"Regime/sentiment: unavailable ({e})")

    # Live-promotion trigger
    lr = live_ready()
    lines.append(f"Live-ready: {'YES' if lr['ready'] else 'NO'} "
                 f"(closed={lr['n_closed']}, pos-regimes={lr['regimes_positive']}, "
                 f"circuit={lr['circuit_open']})")

    # Expectancy table (top cells)
    tbl = expectancy_table()
    if tbl:
        lines.append("Paper expectancy (top cells):")
        for k, c in sorted(tbl.items(), key=lambda x: -x[1]["expectancy"])[:6]:
            flag = "✓" if c["trusted"] else "~"
            lines.append(f"  {flag} {k} n={c['n']} wr={c['win_rate']:.0f}% "
                         f"exp={c['expectancy']:+.4f}")
    else:
        lines.append("Paper expectancy: no closed trades yet.")

    # Universe tilt (agent's own)
    tilt = universe_tilt()
    drops = [a for a, v in tilt.items() if v["tilt"] == "drop"]
    boosts = [a for a, v in tilt.items() if v["tilt"] == "boost"]
    if drops or boosts:
        if boosts:
            lines.append(f"Tilt boost: {', '.join(boosts)}")
        if drops:
            lines.append(f"Tilt drop : {', '.join(drops)}")

    # Cross-book unified tilt (agent + bot merged) — the feedback loop
    try:
        ut = write_unified_expectancy()  # persist so the BOT can read it
        u_drops = [a for a, v in ut.items() if v["tilt"] == "drop"]
        u_boosts = [a for a, v in ut.items() if v["tilt"] == "boost"]
        lines.append("Cross-book tilt (agent+bot):")
        if u_boosts:
            lines.append(f"  boost: {', '.join(u_boosts)}")
        if u_drops:
            lines.append(f"  drop : {', '.join(u_drops)}")
    except Exception as e:
        lines.append(f"Cross-book tilt: unavailable ({e})")

    lines.append("=== end digest ===")
    out = "\n".join(lines)
    # Persist the digest so it is observable without manual invocation.
    try:
        with open("data/learning_digest.md", "w") as fh:
            fh.write(out + "\n")
    except Exception:
        pass
    return out


if __name__ == "__main__":
    print(digest())
