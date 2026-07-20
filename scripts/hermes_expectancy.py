#!/usr/bin/env python3
"""
hermes_expectancy.py — Phase 10: WALK-FORWARD EXPECTANCY + UNIVERSE TILT.

The bot NEVER self-optimizes — it runs the same 20 strategies on the same
universe forever and bleeds on 11 of 13 assets. This module turns the agent's
OWN paper ledger into a per-(regime, asset, side) expectancy table and uses it
to auto-tilt:

  * expectancy(regime, asset, side) = avg_realized_pnl over closed paper trades
    in that cell, with win-rate. Cells with thin samples are 'unknown'.
  * universe_tilt() -> dict per asset: keep / drop / boost, based on whether the
    agent's own paper P&L is positive there across regimes. The loop can use
    this to down-weight assets the AGENT (not just the bot) keeps losing on.
  * live_ready(cycles) -> Phase 12 trigger: paper expectancy positive across
    >=2 regimes AND recent drawdown circuit healthy ->emit a promotion flag.

Read-only over the ledger; no network; no state writes.
"""
from __future__ import annotations

import sys as _sys
from pathlib import Path as _P
import json
from datetime import datetime

# Bootstrap so `from scripts...` works whether imported by the loop (cwd=repo)
# or run directly (python scripts/hermes_expectancy.py).
_REPO = str(_P(__file__).resolve().parent.parent)
if _REPO not in _sys.path:
    _sys.path.insert(0, _REPO)

from collections import defaultdict
from scripts.hermes_agent_trader import load_ledger, recent_stats, drawdown_circuit
from scripts.hermes_meta import load_bot_edge, asset_edge

# Minimum closed trades in a cell before we trust the sign of expectancy.
MIN_CELL_TRADES = 3


def expectancy_table(led: dict | None = None) -> dict:
    """Per (regime, asset, side) -> {n, win, loss, pnl, expectancy, win_rate}."""
    if led is None:
        led = load_ledger()
    cells: dict = defaultdict(lambda: {"n": 0, "win": 0, "loss": 0,
                                       "pnl": 0.0, "side": "", "asset": "",
                                       "regime": ""})
    for t in led.get("trades", []):
        if "realized_pnl" not in t:
            continue  # open-only trades carry no realized pnl yet
        reg = t.get("regime") or "?"
        asset = t.get("product_id", "?")
        side = t.get("side", "?")  # BUY / SHORT_CLOSE / SELL
        key = (reg, asset, side)
        c = cells[key]
        pnl = float(t.get("realized_pnl", 0.0))
        c["n"] += 1
        c["pnl"] += pnl
        c["side"] = side
        c["asset"] = asset
        c["regime"] = reg
        if pnl > 0:
            c["win"] += 1
        else:
            c["loss"] += 1
    out = {}
    for k, c in cells.items():
        n = c["n"]
        wr = (c["win"] / n * 100.0) if n else 0.0
        exp = c["pnl"] / n if n else 0.0
        out["|".join(k)] = {
            "regime": c["regime"], "asset": c["asset"], "side": c["side"],
            "n": n, "win_rate": round(wr, 1),
            "pnl": round(c["pnl"], 4), "expectancy": round(exp, 4),
            "trusted": n >= MIN_CELL_TRADES,
        }
    return out


def universe_tilt(led: dict | None = None) -> dict:
    """Per-asset agent tilt from its OWN paper P&L (independent of bot edge).
    Returns {asset: {agent_pnl, agent_trades, agent_wr, tilt, reason}} where
    tilt in {boost, keep, drop}."""
    if led is None:
        led = load_ledger()
    by_asset: dict = defaultdict(lambda: {"pnl": 0.0, "n": 0, "win": 0})
    for t in led.get("trades", []):
        if "realized_pnl" not in t:
            continue
        a = t["product_id"]
        pnl = float(t["realized_pnl"])
        by_asset[a]["pnl"] += pnl
        by_asset[a]["n"] += 1
        if pnl > 0:
            by_asset[a]["win"] += 1
    out = {}
    for a, v in by_asset.items():
        n = v["n"]
        if n < MIN_CELL_TRADES:
            tilt, reason = "keep", f"thin agent sample ({n})"
        elif v["pnl"] > 0:
            tilt, reason = "boost", f"agent paper +{v['pnl']:.2f} over {n}"
        else:
            tilt, reason = "drop", f"agent paper {v['pnl']:.2f} over {n}"
        wr = (v["win"] / n * 100.0) if n else 0.0
        out[a] = {"agent_pnl": round(v["pnl"], 4), "agent_trades": n,
                  "agent_wr": round(wr, 1), "tilt": tilt, "reason": reason}
    return out


def unified_tilt(min_trades: int = 8, drop_pnl: float = -25.0,
                 boost_pnl: float = 50.0) -> dict:
    """Cross-book tilt: merge the AGENT's own paper P&L (universe_tilt) with the
    BOT's live per-asset P&L (LivePerformanceTracker.asset_expectancy) into ONE
    per-asset view. This is the cross-book learning closure — each book sees the
    other's hard-won expectancy, so a winner (or loser) discovered by one side
    informs the other's entries without either re-deriving it.

    tilt rules (symmetric, conservative):
      • drop  — EITHER book shows clearly-negative P&L (<= drop_pnl) over >= min_trades
      • boost — EITHER book shows clearly-positive P&L (>= boost_pnl) over >= min_trades
                AND neither book is clearly negative
      • keep  — otherwise (thin / mixed / near-zero)

    Returns {asset: {agent_pnl, agent_trades, bot_pnl, bot_trades, tilt, reason}}.
    Pure read — no side effects; safe to call from loops, digests, or cron.
    """
    agent = universe_tilt()
    bot = {}
    try:
        import sys as _sys
        _sys.path.insert(0, str(_REPO))
        from coinbase.src.live_performance import LivePerformanceTracker
        _t = LivePerformanceTracker()
        bot = _t.asset_expectancy(min_trades=min_trades)
    except Exception:
        bot = {}

    assets = set(agent) | set(bot)
    out = {}
    for a in assets:
        ag = agent.get(a, {})
        bo = bot.get(a, {})
        ap = float(ag.get("agent_pnl", 0.0) or 0.0)
        an = int(ag.get("agent_trades", 0) or 0)
        bp = float(bo.get("total_pnl", 0.0) or 0.0)
        bn = int(bo.get("trades", 0) or 0)
        agent_neg = (an >= min_trades and ap <= drop_pnl)
        bot_neg = (bn >= min_trades and bp <= drop_pnl)
        agent_pos = (an >= min_trades and ap >= boost_pnl)
        bot_pos = (bn >= min_trades and bp >= boost_pnl)
        if agent_neg or bot_neg:
            tilt, reason = "drop", (
                f"neg: agent {ap:+.2f}/{an} or bot {bp:+.2f}/{bn}")
        elif agent_pos or bot_pos:
            tilt, reason = "boost", (
                f"pos: agent {ap:+.2f}/{an} or bot {bp:+.2f}/{bn}")
        else:
            tilt, reason = "keep", (
                f"mixed: agent {ap:+.2f}/{an}, bot {bp:+.2f}/{bn}")
        out[a] = {
            "agent_pnl": round(ap, 4), "agent_trades": an,
            "bot_pnl": round(bp, 4), "bot_trades": bn,
            "tilt": tilt, "reason": reason,
        }
    return out


def write_unified_expectancy(path: str | None = None) -> dict:
    """Compute unified_tilt() and persist to data/unified_expectancy.json so BOTH
    books can read the cross-book view on startup (the feedback loop). The bot's
    entry gate and the agent's tilt gate can consume this file to learn from each
    other. Returns the tilt dict.
    """
    tilt = unified_tilt()
    if path is None:
        path = str(_REPO) + "/data/unified_expectancy.json"
    try:
        with open(path, "w") as fh:
            json.dump({"generated_at": datetime.now().isoformat(),
                       "tilt": tilt}, fh, indent=2)
    except Exception:
        pass
    return tilt


def live_ready(led: dict | None = None, min_regimes: int = 2, min_closed: int = 12) -> dict:
    """Phase 12 trigger. Paper is ready to promote to a tiny live allocation
    iff: (a) positive expectancy in >= min_regimes distinct regimes,
    (b) enough closed trades, (c) drawdown circuit currently OPEN (healthy).
    Returns {ready, reasons[], regimes_positive[], n_closed, circuit_open}."""
    led = load_ledger()
    tbl = expectancy_table(led)
    closed = [t for t in led.get("trades", []) if "realized_pnl" in t]
    n_closed = len(closed)
    # positive-expectancy regimes (trusted cells only)
    pos_regimes = set()
    for k, c in tbl.items():
        if c["trusted"] and c["expectancy"] > 0:
            pos_regimes.add(c["regime"])
    circuit = drawdown_circuit()
    reasons = []
    ready = True
    if n_closed < min_closed:
        ready = False
        reasons.append(f"needs >= {min_closed} closed paper trades (have {n_closed})")
    if len(pos_regimes) < min_regimes:
        ready = False
        reasons.append(f"needs positive expectancy in >= {min_regimes} regimes "
                       f"(have {sorted(pos_regimes)})")
    if not circuit["open"]:
        ready = False
        reasons.append(f"drawdown circuit tripped: {circuit['reason']}")
    if ready:
        reasons.append("paper edge confirmed across regimes; operator may "
                       "enable HERMES_AGENT_LIVE with a tiny bounded allocation")
    return {"ready": ready, "reasons": reasons,
            "regimes_positive": sorted(pos_regimes), "n_closed": n_closed,
            "circuit_open": circuit["open"]}


if __name__ == "__main__":
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
    import sys
    led = load_ledger()
    print("=== Phase 10 expectancy table (agent's own paper, by regime/asset/side) ===")
    tbl = expectancy_table(led)
    if not tbl:
        print("  (no closed paper trades yet — run the loop to populate)")
    for k, c in sorted(tbl.items(), key=lambda x: -x[1]["expectancy"]):
        flag = "✓" if c["trusted"] else "~"
        print(f"  {flag} {k:42} n={c['n']:2} wr={c['win_rate']:5.1f}% "
              f"exp={c['expectancy']:+.4f} pnl={c['pnl']:+.2f}")
    print("\n=== universe tilt (agent's own P&L) ===")
    for a, v in sorted(universe_tilt(led).items(), key=lambda x: -x[1]["agent_pnl"]):
        print(f"  {a:12} {v['tilt']:5} {v['reason']}")
    print("\n=== Phase 12 live-promotion trigger ===")
    print("  ", live_ready())
