#!/usr/bin/env python3
"""
hermes_compare_report.py — side-by-side PAPER competition report.

Compares the Hermes agent's paper track record (data/hermes_agent_ledger.json)
against the strategy engine's live paper records (data/live_performance.json -> _records).

NOTE on scale: the strategy engine's per-strategy _records use realistic small-dollar
P&L (e.g. -$5 on $2k volume) — directly comparable to the agent's $10/trade book.
The aggregate strategy_analytics.json numbers are backtest-scale ($M) and are NOT used
for the head-to-head; only trade-level _records are compared. We report both raw and
normalized (P&L %, profit factor) so the contest is fair.

Read-only: reads JSON, prints. Writes nothing.
"""
from __future__ import annotations

import sys as _sys
from pathlib import Path as _P

# Bootstrap so `from scripts...` works whether imported by the loop (cwd=repo)
# or run directly (python scripts/hermes_compare_report.py).
_REPO = str(_P(__file__).resolve().parent.parent)
if _REPO not in _sys.path:
    _sys.path.insert(0, _REPO)

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "data" / "hermes_agent_ledger.json"
LIVE_PERF = ROOT / "data" / "live_performance.json"


def _agent_stats() -> dict:
    if not LEDGER.exists():
        return {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
                "fees": 0.0, "volume": 0.0, "win_rate": 0.0,
                "pnl_pct": 0.0, "profit_factor": 0.0, "unreal": 0.0}
    led = json.loads(LEDGER.read_text() or "{}")
    trades = led.get("trades", [])
    pnl = float(led.get("realized_pnl", 0.0))
    fees = sum(float(t.get("commission", 0)) for t in trades)
    volume = sum(float(t.get("quote_size", 0)) for t in trades)
    # wins/losses from closed trades (those that carry a realized_pnl field)
    wins = losses = 0
    sum_wins = sum_losses = 0.0
    for t in trades:
        if "realized_pnl" in t:
            rp = float(t["realized_pnl"])
            if rp >= 0:
                wins += 1
                sum_wins += rp
            else:
                losses += 1
                sum_losses += -rp
    closed = wins + losses
    win_rate = (wins / closed * 100.0) if closed else 0.0
    pnl_pct = (pnl / volume * 100.0) if volume else 0.0
    pf = (sum_wins / sum_losses) if sum_losses else (float("inf") if sum_wins else 0.0)
    return {"trades": len(trades), "wins": wins, "losses": losses, "pnl": round(pnl, 4),
            "fees": round(fees, 4), "volume": round(volume, 2),
            "win_rate": round(win_rate, 2), "pnl_pct": round(pnl_pct, 4),
            "profit_factor": round(pf, 3) if pf != float("inf") else "inf",
            "unreal": 0.0}


def _engine_stats() -> dict:
    if not LIVE_PERF.exists():
        return {"strategies": 0, "trades": 0, "wins": 0, "losses": 0,
                "pnl": 0.0, "fees": 0.0, "volume": 0.0, "win_rate": 0.0,
                "pnl_pct": 0.0, "profit_factor": 0.0}
    d = json.loads(LIVE_PERF.read_text() or "{}")
    recs = d.get("_records", {})
    trades = wins = losses = 0
    pnl = fees = volume = sum_wins = sum_losses = 0.0
    for v in recs.values():
        if not isinstance(v, dict):
            continue
        trades += int(v.get("trades", 0))
        wins += int(v.get("wins", 0))
        losses += int(v.get("losses", 0))
        pnl += float(v.get("total_pnl", 0) or 0)
        fees += float(v.get("total_fees", 0) or 0)
        volume += float(v.get("total_volume", 0) or 0)
        sum_wins += float(v.get("sum_wins", 0) or 0)
        sum_losses += float(v.get("sum_losses", 0) or 0)
    win_rate = (wins / trades * 100.0) if trades else 0.0
    pnl_pct = (pnl / volume * 100.0) if volume else 0.0
    pf = (sum_wins / abs(sum_losses)) if sum_losses else (float("inf") if sum_wins else 0.0)
    return {"strategies": len(recs), "trades": trades, "wins": wins, "losses": losses,
            "pnl": round(pnl, 2), "fees": round(fees, 2), "volume": round(volume, 2),
            "win_rate": round(win_rate, 2), "pnl_pct": round(pnl_pct, 4),
            "profit_factor": round(pf, 3) if pf != float("inf") else "inf"}


def main() -> int:
    a = _agent_stats()
    e = _engine_stats()
    print("=" * 64)
    print(" HERMES AGENT vs STRATEGY ENGINE — PAPER COMPETITION")
    print("=" * 64)
    print(f"{'metric':<22}{'HERMES AGENT':>20}{'STRATEGY ENG':>20}")
    print("-" * 64)
    rows = [
        ("paper trades", a["trades"], e["trades"]),
        ("strategies", "-", e["strategies"]),
        ("realized P&L ($)", f'{a["pnl"]:.4f}', f'{e["pnl"]:.2f}*'),
        ("volume ($)", f'{a["volume"]:.2f}', f'{e["volume"]:.2f}*'),
        ("fees ($)", f'{a["fees"]:.4f}', f'{e["fees"]:.2f}*'),
        ("win rate (%)", f'{a["win_rate"]:.1f}', f'{e["win_rate"]:.1f}'),
        ("P&L % of vol", f'{a["pnl_pct"]:.4f}', f'{e["pnl_pct"]:.4f}'),
        ("profit factor", str(a["profit_factor"]), str(e["profit_factor"])),
    ]
    for name, av, ev in rows:
        print(f"{name:<22}{str(av):>20}{str(ev):>20}")
    print("-" * 64)
    print("Scale note: agent capped at $10/trade (MAX_NOTIONAL); engine _records are")
    print("BACKTEST-SCALE ($M) per strategy, NOT trade-level small-$. Raw-$ rows marked")
    print("'*' are NOT comparable. Fair contest = win rate %, P&L % of vol, profit")
    print("factor (all scale-invariant). Agent per-trade win/loss is round-trip net.")
    print("=" * 64)

    # --- Phase 10: agent's own walk-forward expectancy (by regime/asset/side) ---
    try:
        from scripts.hermes_expectancy import expectancy_table, universe_tilt
        tbl = expectancy_table()
        tilt = universe_tilt()
        if tbl:
            print()
            print("AGENT PAPER EXPECTANCY (Phase 10 — own ledger, by regime|asset|side)")
            print("-" * 64)
            print(f"{'cell':<46}{'n':>3}{'wr%':>6}{'exp$':>9}")
            for k, c in sorted(tbl.items(), key=lambda x: -x[1]["expectancy"]):
                flag = "✓" if c["trusted"] else "~"
                print(f"{flag} {k:<44}{c['n']:>3}{c['win_rate']:>6.1f}{c['expectancy']:>+9.4f}")
            drops = [a for a, v in tilt.items() if v["tilt"] == "drop"]
            boosts = [a for a, v in tilt.items() if v["tilt"] == "boost"]
            if drops or boosts:
                print("-" * 64)
                if boosts:
                    print(f"  TILT boost: {', '.join(boosts)}")
                if drops:
                    print(f"  TILT drop : {', '.join(drops)} (agent keeps losing here)")
        else:
            print("\n(Phase 10: no closed paper trades yet — run the loop to populate)")
    except Exception as exc:
        print(f"\n[expectancy] skipped: {exc}")

    # --- Phase 12: live-promotion trigger ---
    try:
        from scripts.hermes_expectancy import live_ready
        lr = live_ready()
        print()
        print("PHASE 12 — LIVE PROMOTION TRIGGER (paper-readiness gate)")
        print("-" * 64)
        print(f"  READY TO PROMOTE: {'YES ✅' if lr['ready'] else 'NO'}")
        print(f"  closed paper trades: {lr['n_closed']}")
        print(f"  positive-expectancy regimes: {lr['regimes_positive'] or 'none'}")
        print(f"  drawdown circuit open: {lr['circuit_open']}")
        for r in lr["reasons"]:
            print(f"   - {r}")
        print("=" * 64)
    except Exception as exc:
        print(f"\n[live_ready] skipped: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
