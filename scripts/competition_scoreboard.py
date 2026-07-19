#!/usr/bin/env python3
"""
Unified paper-trading competition scoreboard.

Normalizes BOTH books into one authoritative state file so we stop measuring
two different things in two different files:

  - Agent book : data/hermes_agent_ledger.json   (Hermes agent trades)
  - Bot book   : data/paper_trader_v4_state.json  (v4 EventTrader paper)

Writes a single source of truth: data/competition_state.json
and prints a side-by-side summary.

Run:  python3 scripts/competition_scoreboard.py
Cron: hook this after each book writes (or every 5m) to keep it live.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
AGENT_LEDGER = REPO / "data" / "hermes_agent_ledger.json"
BOT_STATE = REPO / "data" / "paper_trader_v4_state.json"
OUT = REPO / "data" / "competition_state.json"


def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load_agent() -> dict:
    if not AGENT_LEDGER.exists():
        return {}
    d = json.loads(AGENT_LEDGER.read_text())
    starting = _num(d.get("starting_capital"), 10000.0)
    equity = _num(d.get("equity"), starting)
    peak = _num(d.get("peak_equity"), equity)
    rpnl = _num(d.get("realized_pnl"))
    trades = d.get("trades", [])
    # Wins/losses: agent ledger marks realized_pnl per closing trade.
    wins = sum(1 for t in trades if isinstance(t, dict) and _num(t.get("realized_pnl")) > 0)
    losses = sum(1 for t in trades if isinstance(t, dict) and _num(t.get("realized_pnl")) < 0)
    return {
        "side": "agent",
        "label": "Hermes Agent",
        "starting_capital": starting,
        "equity": equity,
        "cash": _num(d.get("cash"), equity),
        "peak_equity": peak,
        "realized_pnl": rpnl,
        "return_pct": (_num(d.get("return_pct")) if d.get("return_pct") is not None
                        else (equity - starting) / starting * 100.0 if starting else 0.0),
        "max_drawdown_pct": (starting - peak) / starting * 100.0 if starting and peak < starting else 0.0,
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "open_positions": sum(1 for p in d.get("positions", {}).values()
                               if isinstance(p, dict) and _num(p.get("exposure")) != 0),
        "last_trade_ts": trades[-1].get("ts") if trades else None,
        "source": str(AGENT_LEDGER),
    }


def load_bot() -> dict:
    if not BOT_STATE.exists():
        return {}
    d = json.loads(BOT_STATE.read_text())
    starting = _num(d.get("paper_starting_capital"), 10000.0)
    cash = _num(d.get("paper_cash"), starting)
    rpnl = _num(d.get("paper_realized_pnl"))
    equity = cash + rpnl  # paper book: cash + realized (no open positions expected)
    peak = _num(d.get("paper_peak_equity"), equity)
    trades = d.get("trades", [])
    wins = _num(d.get("paper_wins"))
    losses = _num(d.get("paper_losses"))
    return {
        "side": "bot",
        "label": "v4 Bot (EventTraderV4)",
        "starting_capital": starting,
        "equity": equity,
        "cash": cash,
        "peak_equity": peak,
        "realized_pnl": rpnl,
        "return_pct": (equity - starting) / starting * 100.0 if starting else 0.0,
        "max_drawdown_pct": (starting - peak) / starting * 100.0 if starting and peak < starting else 0.0,
        "trades": len(trades) if trades else _num(d.get("paper_trades_total", 0)),
        "wins": int(wins),
        "losses": int(losses),
        "open_positions": len(d.get("paper_positions", [])),
        "last_trade_ts": None,
        "source": str(BOT_STATE),
    }


def rank(agent: dict, bot: dict) -> dict:
    # Leader = higher equity. Head-to-head delta in bps and USD.
    a_eq = agent.get("equity", 0.0)
    b_eq = bot.get("equity", 0.0)
    if a_eq > b_eq:
        leader, trailer, lead_eq, trail_eq = "agent", "bot", a_eq, b_eq
    else:
        leader, trailer, lead_eq, trail_eq = "bot", "agent", b_eq, a_eq
    edge_usd = abs(a_eq - b_eq)
    edge_bps = (edge_usd / min(a_eq, b_eq) * 10000.0) if min(a_eq, b_eq) > 0 else 0.0
    return {
        "leader": leader,
        "trailer": trailer,
        "edge_usd": round(edge_usd, 2),
        "edge_bps": round(edge_bps, 1),
        "agent_equity": round(a_eq, 2),
        "bot_equity": round(b_eq, 2),
    }


def main() -> None:
    agent = load_agent()
    bot = load_bot()
    standings = rank(agent, bot)
    state = {
        "schema_version": 1,
        "updated_at": time.time(),
        "updated_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "starting_capital": 10000.0,
        "competitors": {"agent": agent, "bot": bot},
        "standings": standings,
    }
    OUT.write_text(json.dumps(state, indent=2))
    print("=" * 64)
    print("  PAPER TRADING COMPETITION  —  updated", state["updated_iso"])
    print("=" * 64)
    for side in (agent, bot):
        if not side:
            continue
        print(f"\n  {side['label']}")
        print(f"    Equity        : ${side['equity']:,.2f}")
        print(f"    Return        : {side['return_pct']:+.2f}%")
        print(f"    Realized PnL  : ${side['realized_pnl']:,.2f}")
        print(f"    Peak equity   : ${side['peak_equity']:,.2f}")
        print(f"    Max DD        : {side['max_drawdown_pct']:.2f}%")
        print(f"    Trades        : {side['trades']}  (W {side['wins']} / L {side['losses']})")
        print(f"    Open positions: {side['open_positions']}")
    print("\n  " + "-" * 60)
    print(f"  LEADER: {standings['leader'].upper()}  "
          f"(+${standings['edge_usd']:,.2f} / +{standings['edge_bps']:.0f} bps)")
    print(f"  Agent ${standings['agent_equity']:,.2f}   vs   Bot ${standings['bot_equity']:,.2f}")
    print("=" * 64)
    print(f"\n  Wrote {OUT}")


if __name__ == "__main__":
    main()
