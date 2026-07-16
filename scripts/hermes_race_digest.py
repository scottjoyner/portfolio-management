#!/usr/bin/env python3
"""hermes_race_digest.py — head-to-head: Hermes AGENT vs the BOT, both from a
flat $10,000 paper start (bot's paper_starting_capital=10000; agent ledger
seeded with starting_capital=10000).

Pulls both books' equity and prints a fair comparison: return %, trades, win
rate, peak equity, current drawdown. Run after the cron has populated fills.

Usage:
    .venv/bin/python scripts/hermes_race_digest.py
"""
from __future__ import annotations
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AGENT_LEDGER = ROOT / "data" / "hermes_agent_ledger.json"
BOT_STATE = ROOT / "data" / "paper_trader_v4_state.json"


def _agent() -> dict:
    if not AGENT_LEDGER.exists():
        return {}
    led = json.loads(AGENT_LEDGER.read_text() or "{}")
    start = led.get("starting_capital", 10000.0)
    eq = led.get("equity", start)
    cash = led.get("cash", start)
    peak = led.get("peak_equity", eq)
    closed = [t for t in led.get("trades", []) if "realized_pnl" in t]
    wins = sum(1 for t in closed if t["realized_pnl"] >= 0)
    losses = len(closed) - wins
    pnl = sum(t["realized_pnl"] for t in closed)
    dd = (eq - peak) / peak * 100.0 if peak else 0.0
    open_pos = {k: v for k, v in led.get("positions", {}).items()
                if v.get("base", 0) > 1e-9}
    return {
        "name": "HERMES AGENT", "start": start, "equity": eq, "cash": cash,
        "peak": peak, "return_pct": (eq - start) / start * 100.0,
        "trades": len(closed), "wins": wins, "losses": losses,
        "win_rate": round(wins / len(closed) * 100.0, 2) if closed else 0.0,
        "realized_pnl": round(pnl, 2), "drawdown_pct": round(dd, 2),
        "open_positions": len(open_pos),
    }


def _bot() -> dict:
    if not BOT_STATE.exists():
        return {}
    b = json.loads(BOT_STATE.read_text() or "{}")
    start = b.get("paper_starting_capital", 10000.0)
    cash = b.get("paper_cash", start)
    eq = b.get("paper_equity", cash)
    peak = b.get("paper_peak_equity", eq)
    trades = b.get("paper_trades", [])
    # bot tracks per-trade pnl in its trade ledger
    pnls = [t.get("realized_pnl", t.get("pnl", 0)) for t in trades
            if "realized_pnl" in t or "pnl" in t]
    wins = sum(1 for p in pnls if p >= 0)
    losses = len(pnls) - wins
    dd = (eq - peak) / peak * 100.0 if peak else 0.0
    return {
        "name": "BOT (run_trader_v4)", "start": start, "equity": eq, "cash": cash,
        "peak": peak, "return_pct": (eq - start) / start * 100.0,
        "trades": len(trades), "wins": wins, "losses": losses,
        "win_rate": round(wins / len(trades) * 100.0, 2) if trades else 0.0,
        "realized_pnl": round(sum(pnls), 2), "drawdown_pct": round(dd, 2),
        "open_positions": b.get("paper_open_positions",
                                len(b.get("paper_positions", {}))),
    }


def main() -> int:
    a = _agent()
    b = _bot()
    bar = "=" * 64
    print(bar)
    print("PAPER RACE — both books start at $10,000")
    print(bar)
    for d in (a, b):
        if not d:
            print("(book not found)")
            continue
        print(f"\n{d['name']}")
        print(f"  start         ${d['start']:>12,.2f}")
        print(f"  equity        ${d['equity']:>12,.2f}  ({d['return_pct']:+.2f}%)")
        print(f"  peak          ${d['peak']:>12,.2f}")
        print(f"  drawdown      {d['drawdown_pct']:>12.2f}%")
        print(f"  cash          ${d['cash']:>12,.2f}")
        print(f"  trades        {d['trades']:>12}")
        print(f"  win rate      {d['win_rate']:>11.2f}%   ({d['wins']}W/{d['losses']}L)")
        print(f"  realized PnL  ${d['realized_pnl']:>12,.2f}")
        print(f"  open positions {d['open_positions']:>11}")
    if a and b:
        print("\n" + bar)
        edge = a["return_pct"] - b["return_pct"]
        print(f"  AGENT vs BOT return spread: {edge:+.2f} pts")
        if edge > 0:
            print("  >>> AGENT is ahead (on return %) <<<")
        elif edge < 0:
            print(f"  >>> BOT leads by {abs(edge):.2f} pts <<<")
        else:
            print("  >>> dead heat <<<")
        print(bar)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
