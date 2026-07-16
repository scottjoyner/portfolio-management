#!/usr/bin/env python3
"""
hermes_agent_trader.py — Hermes agent's paper/simulation trading harness for the
portfolio-management system. Lets the agent COMPETE against the strategy engine's
paper trades using REAL exchange quotes, with ZERO live money at risk.

=== SAFETY MODEL (non-negotiable) ===
* This module is PAPER/SIMULATION ONLY by default. It uses CBClient with
  dry_run_cli=True and only ever calls `orders preview` (a read-only quote) to
  value trades. It NEVER submits a live `orders create`.
* A live path exists (propose_live / _execute_live) but is gated behind the env
  var HERMES_AGENT_LIVE=true. The agent MUST NOT set that var itself; it requires
  an explicit, separate operator authorization. If unset (or any safety gate is
  active), every live attempt is refused and logged.
* Inherited gates (read from .env, never modified):
    KILL_SWITCH            -> if true, ALL trading refused (paper + live)
    REQUIRE_MANUAL_APPROVAL-> paper fills still simulated, but live blocked
    MAX_NOTIONAL_PER_TRADE_USD -> caps simulated trade quote_size
* The agent's job per operator: "don't lose any money." Paper competition only.

=== HOW IT COMPETES ===
The strategy engine runs paper trades (portfolio_optimizer / run_trader_v4 paper
mode). This harness independently evaluates signals using REAL exchange quotes
(CBClient.preview_order) and tracks a parallel simulated book in
data/hermes_agent_ledger.json. Later we diff the two paper P&Ls.

=== CLI USAGE ===
  .venv/bin/python scripts/hermes_agent_trader.py quote BTC-USD BUY --quote-size 10
  .venv/bin/python scripts/hermes_agent_trader.py signal BTC-USD BUY --quote-size 10
        (records a simulated fill at the preview price into the ledger)
  .venv/bin/python scripts/hermes_agent_trader.py ledger
  .venv/bin/python scripts/hermes_agent_trader.py status
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "data" / "hermes_agent_ledger.json"

# Safety env (read-only — never written by this module)
KILL_SWITCH = os.getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")
REQUIRE_MANUAL_APPROVAL = os.getenv("REQUIRE_MANUAL_APPROVAL", "").lower() in ("1", "true", "yes")
try:
    MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "10"))
except ValueError:
    MAX_NOTIONAL = 10.0
HERMES_AGENT_LIVE = os.getenv("HERMES_AGENT_LIVE", "").lower() in ("1", "true", "yes")


def _refuse(reason: str) -> dict:
    return {"action": "refused", "reason": reason, "live": False}


def get_client():
    # Force dry-run so create_market_order is always simulated. preview_order is
    # read-only regardless. Import lazily to keep the module importable anywhere.
    sys.path.insert(0, str(ROOT))
    from coinbase.src.cb_client import CBClient
    return CBClient(dry_run_cli=True)


def load_ledger() -> dict:
    if LEDGER.exists():
        try:
            return json.loads(LEDGER.read_text() or "{}")
        except Exception:
            pass
    return {"positions": {}, "trades": [], "realized_pnl": 0.0,
            "created_at": datetime.now(timezone.utc).isoformat()}


def save_ledger(led: dict) -> None:
    LEDGER.write_text(json.dumps(led, indent=2))


def quote(product_id: str, side: str, quote_size: float | None = None,
          base_size: float | None = None) -> dict:
    """Read-only real quote from the exchange via CBClient.preview_order."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is not None and quote_size > MAX_NOTIONAL:
        return _refuse(f"quote_size {quote_size} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    if base_size is not None and (base_size * 0) != 0:
        pass  # base_size notional checked at fill time via preview
    client = get_client()
    r = client.preview_order(side, product_id, quote_size=str(quote_size) if quote_size else None,
                             base_size=str(base_size) if base_size else None)
    if isinstance(r, dict) and r.get("status") == "preview_error":
        return {"action": "quote_error", "error": r.get("error"), "raw": r}
    return {"action": "quote", "live": False, "product_id": product_id, "side": side.upper(),
            "quote": r}


def record_signal(product_id: str, side: str, quote_size: float | None = None,
                  base_size: float | None = None, note: str = "") -> dict:
    """Simulate a paper fill at the real preview price and log it to the ledger.
    This is the agent 'competing' — a paper position, no money moves."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is None and base_size is None:
        return _refuse("need quote_size or base_size")
    if quote_size is not None and quote_size > MAX_NOTIONAL:
        return _refuse(f"quote_size {quote_size} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    q = quote(product_id, side, quote_size=quote_size, base_size=base_size)
    if q.get("action") != "quote":
        return q
    price = float(q["quote"].get("est_average_filled_price", 0))
    base = float(q["quote"].get("base_size", 0))
    commission = float(q["quote"].get("commission_total", 0))
    if price <= 0 or base <= 0:
        return {"action": "signal_error", "error": "bad preview price/base", "quote": q["quote"]}
    # Notional for the gating check when base-only
    notional = quote_size if quote_size is not None else base * price
    # tolerance for float/rounding drift (e.g. 10.08 vs cap 10.0 from base rounding)
    if notional > MAX_NOTIONAL + 0.01:
        return _refuse(f"notional {notional:.2f} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    led = load_ledger()
    ts = datetime.now(timezone.utc).isoformat()
    trade = {
        "ts": ts, "product_id": product_id, "side": side.upper(),
        "quote_size": notional, "fill_price": price, "base_size": base,
        "commission": commission, "live": False, "note": note,
    }
    led["trades"].append(trade)
    # Update a simple net position (base-weighted) per product
    pos = led["positions"].get(product_id, {"base": 0.0, "cost_basis": 0.0, "entries": 0})
    if side.upper() == "BUY":
        pos["base"] += base
        pos["cost_basis"] += notional
    else:  # SELL reduces; realize pnl vs cost basis (avg)
        avg_cost = (pos["cost_basis"] / pos["base"]) if pos["base"] > 0 else price
        proceeds = base * price
        led["realized_pnl"] += (proceeds - avg_cost * base)
        pos["base"] = max(0.0, pos["base"] - base)
        pos["cost_basis"] = max(0.0, pos["cost_basis"] - avg_cost * base)
    pos["entries"] += 1
    led["positions"][product_id] = pos
    save_ledger(led)
    return {"action": "signal_recorded", "live": False, "trade": trade,
            "position": pos, "realized_pnl": round(led["realized_pnl"], 6)}


def propose_live(product_id: str, side: str, quote_size: float,
                 note: str = "") -> dict:
    """Live proposal. REFUSED unless HERMES_AGENT_LIVE=true AND no safety gate active.
    The agent never sets HERMES_AGENT_LIVE itself — operator must authorize."""
    if not HERMES_AGENT_LIVE:
        return _refuse("HERMES_AGENT_LIVE not set — agent may not trade live")
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if REQUIRE_MANUAL_APPROVAL:
        return _refuse("REQUIRE_MANUAL_APPROVAL active — needs human gate")
    if quote_size > MAX_NOTIONAL:
        return _refuse(f"quote_size {quote_size} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    # Gated path exists but is intentionally not auto-executed. Log intent only.
    return {"action": "live_proposal_blocked",
            "reason": "live execution requires explicit operator confirmation step",
            "would_preview": quote(product_id, side, quote_size=quote_size)}


def ledger_summary() -> dict:
    led = load_ledger()
    return {"trades": len(led["trades"]), "positions": led["positions"],
            "realized_pnl": round(led["realized_pnl"], 6),
            "unrealized_note": "mark-to-market requires live quotes; compute on demand"}


def status() -> dict:
    return {
        "kill_switch": KILL_SWITCH,
        "require_manual_approval": REQUIRE_MANUAL_APPROVAL,
        "max_notional": MAX_NOTIONAL,
        "hermes_agent_live": HERMES_AGENT_LIVE,
        "mode": "LIVE-PROPOSED" if HERMES_AGENT_LIVE else "PAPER/SIM (no money at risk)",
        "ledger": ledger_summary(),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Hermes agent paper-trading harness (read-only quotes)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pq = sub.add_parser("quote"); pq.add_argument("product"); pq.add_argument("side")
    pq.add_argument("--quote-size", type=float); pq.add_argument("--base-size", type=float)
    ps = sub.add_parser("signal"); ps.add_argument("product"); ps.add_argument("side")
    ps.add_argument("--quote-size", type=float); ps.add_argument("--base-size", type=float)
    ps.add_argument("--note", default="")
    pl = sub.add_parser("ledger")
    pp = sub.add_parser("propose-live"); pp.add_argument("product"); pp.add_argument("side")
    pp.add_argument("--quote-size", type=float, required=True); pp.add_argument("--note", default="")
    pst = sub.add_parser("status")
    args = ap.parse_args()

    if args.cmd == "quote":
        print(json.dumps(quote(args.product, args.side, args.quote_size, args.base_size), indent=2))
    elif args.cmd == "signal":
        print(json.dumps(record_signal(args.product, args.side, args.quote_size,
                                        args.base_size, args.note), indent=2))
    elif args.cmd == "ledger":
        print(json.dumps(ledger_summary(), indent=2))
    elif args.cmd == "propose-live":
        print(json.dumps(propose_live(args.product, args.side, args.quote_size, args.note), indent=2))
    elif args.cmd == "status":
        print(json.dumps(status(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
