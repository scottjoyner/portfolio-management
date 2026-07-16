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
import datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "data" / "hermes_agent_ledger.json"

# Safety env (read-only — never written by this module)
KILL_SWITCH = os.getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")
REQUIRE_MANUAL_APPROVAL = os.getenv("REQUIRE_MANUAL_APPROVAL", "").lower() in ("1", "true", "yes")
try:
    MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "250"))
except ValueError:
    MAX_NOTIONAL = 10.0
# Phase 17: LEVERAGE — the agent is the aggressive moonshot book. Operator
# authorized leverage (bot runs --max-leverage 2.0; agent goes harder). Margin
# per trade is capped at MAX_NOTIONAL; EXPOSURE = margin * AGENT_LEVERAGE.
# Paper-only: no borrow, no liquidation — PnL just scales by leverage.
try:
    AGENT_LEVERAGE = float(os.getenv("AGENT_LEVERAGE", "3.0"))
except ValueError:
    AGENT_LEVERAGE = 3.0
HERMES_AGENT_LIVE = os.getenv("HERMES_AGENT_LIVE", "").lower() in ("1", "true", "yes")


def _refuse(reason: str) -> dict:
    return {"action": "refused", "reason": reason, "live": False}


def get_client():
    # Force dry-run so create_market_order is always simulated. preview_order is
    # read-only regardless. Import lazily to keep the module importable anywhere.
    sys.path.insert(0, str(ROOT))
    from coinbase.src.cb_client import CBClient
    return CBClient(dry_run_cli=True)


def _current_price(product_id: str) -> float:
    """Current mark price via latest 1h candle close (read-only, per-product
    accurate). NOTE: the `products best-bid-ask product_id=X` CLI call ignores
    the product_id and always returns ETH-USD, so we mark from candles instead."""
    from coinbase.src.cb_client import CBClient
    c = CBClient(dry_run_cli=True)
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=1)
    r = c._cli_json("products", "candles", product_id, "granularity=1h",
                    f"start={start.isoformat()}", f"end={end.isoformat()}")
    if isinstance(r, dict) and "candles" in r:
        cs = [x for x in r["candles"] if x.get("close")]
        if cs:
            return float(cs[-1]["close"])
    return 0.0


def load_ledger() -> dict:
    if LEDGER.exists():
        try:
            return json.loads(LEDGER.read_text() or "{}")
        except Exception:
            pass
    return {"positions": {}, "trades": [], "realized_pnl": 0.0,
            "cash": 10000.0, "equity": 10000.0, "peak_equity": 10000.0,
            "equity_curve": [10000.0], "starting_capital": 10000.0,
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
                  base_size: float | None = None, note: str = "",
                  regime: str = "", setup: str = "", price: float | None = None) -> dict:
    """Simulate a paper fill at the real preview price and log it to the ledger.
    This is the agent 'competing' — a paper position, no money moves.

    `price` (optional) is the decision price (e.g. candle last close) used for the
    paper fill. When passed, NO live quote is fetched — the paper sim is fully
    self-contained from candles (Phase 9c: removes dry_run preview dependency so
    signals actually record in paper competition). Falls back to live preview."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is None and base_size is None:
        return _refuse("need quote_size or base_size")
    if quote_size is not None and quote_size > MAX_NOTIONAL:
        return _refuse(f"quote_size {quote_size} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    # If a decision price was supplied, simulate the fill directly (no live quote).
    if price is not None and price > 0:
        margin = quote_size if quote_size is not None else (base_size or 0) * price
        if margin > MAX_NOTIONAL + 0.01:
            return _refuse(f"margin {margin:.2f} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
        lev = AGENT_LEVERAGE
        exposure = margin * lev  # leveraged notional (PnL scales by lev)
        base = exposure / price if price > 0 else 0.0
        commission = margin * 0.0012  # fee on margin deployed
        led = load_ledger()
        ts = datetime.now(timezone.utc).isoformat()
        trade = {
            "ts": ts, "product_id": product_id, "side": side.upper(),
            "quote_size": round(margin, 4), "exposure": round(exposure, 4),
            "leverage": lev, "fill_price": round(price, 8),
            "base_size": round(base, 8), "commission": round(commission, 6),
            "live": False, "note": note, "regime": regime, "setup": setup,
        }
        led["trades"].append(trade)
        pos = led["positions"].get(product_id, {"base": 0.0, "cost_basis": 0.0,
                                                "exposure": 0.0, "entries": 0})
        if side.upper() == "BUY":
            if pos.get("base", 0.0) <= 1e-12:
                pos["entry_ts"] = ts
                pos["entry_price"] = price
            else:
                # weighted-average entry price
                prev_base = pos["base"]
                pos["entry_price"] = ((pos["entry_price"] * prev_base) + (price * base)) / (prev_base + base)
            pos["base"] += base
            pos["cost_basis"] += margin  # margin tied up (not full exposure)
            pos["exposure"] += exposure
            led["cash"] = led.get("cash", 10000.0) - (margin + commission)
        else:
            avg_entry = (pos["entry_price"] if pos.get("entry_price")
                         else price)
            avg_exposure = pos.get("exposure", 0.0)
            # close a fraction `base` of the position; scale pnl by leverage
            frac = (base / pos["base"]) if pos["base"] > 0 else 1.0
            pnl = (price - avg_entry) * base * lev - commission
            led["realized_pnl"] += pnl
            led["cash"] = led.get("cash", 10000.0) + (margin + pnl)
            pos["base"] = max(0.0, pos["base"] - base)
            pos["cost_basis"] = max(0.0, pos["cost_basis"] - margin)
            pos["exposure"] = max(0.0, pos["exposure"] - exposure)
        pos["entries"] += 1
        led["positions"][product_id] = pos
        save_ledger(led)
        return {"action": "signal_recorded", "live": False, "trade": trade,
                "position": pos, "realized_pnl": round(led["realized_pnl"], 6)}
    # SELL via base_size: some products reject fine precision. Retry coarser if the
    # exchange complains, so a valid signal isn't dropped on a rounding artifact.
    tried = set()
    base_attempts = [base_size] + [round(base_size, d) for d in (6, 5, 4, 3, 2)] if base_size else [None]
    last_err = None
    q = None
    for b_att in base_attempts:
        if b_att in tried:
            continue
        tried.add(b_att)
        q = quote(product_id, side, quote_size=quote_size, base_size=b_att)
        if q.get("action") == "quote":
            break
        err = str(q.get("error", ""))
        if "precision" in err:
            last_err = err
            continue  # try coarser base
        if "too small" in err or "minimum" in err or "min_size" in err:
            # Asset's minimum base size exceeds our MAX_NOTIONAL cap — untradeable
            # at this size. Skip cleanly (not an error worth retrying).
            return {"action": "min_size_skip", "product_id": product_id,
                    "reason": "min base size > MAX_NOTIONAL notional"}
        # non-precision error (e.g. kill switch upstream) -> surface it
        return q
    else:
        return {"action": "quote_error", "error": last_err or "all base-precision attempts failed",
                "raw": q}
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
        "regime": regime, "setup": setup,
    }
    led["trades"].append(trade)
    # Update a simple net position (base-weighted) per product
    pos = led["positions"].get(product_id, {"base": 0.0, "cost_basis": 0.0, "entries": 0})
    if side.upper() == "BUY":
        if pos.get("base", 0.0) <= 1e-12:
            pos["entry_ts"] = ts  # fresh entry start time
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


def close_position(product_id: str, note: str = "close", price: float | None = None) -> dict:
    """Simulated SELL of the full held base at the current real quote.
    Realizes P&L vs cost basis. Paper-only. No money moves.
    `price` (optional) lets the caller pass the decision price (loop passes the
    same candle mark used for the TP/SL decision, so execution == decision)."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    led = load_ledger()
    pos = led["positions"].get(product_id)
    if not pos or pos.get("base", 0.0) <= 1e-12:
        return {"action": "no_position", "product_id": product_id}
    base = pos["base"]
    if price is None or price <= 0:
        price = _current_price(product_id)
    if price <= 0:
        return {"action": "quote_error", "error": "no bid", "product_id": product_id}
    commission = base * price * 0.0012  # ~0.12% taker, matches preview scale
    entry = pos.get("entry_price", price)
    lev = pos.get("leverage", AGENT_LEVERAGE)
    pnl = (price - entry) * base * lev - commission  # PnL scales by leverage
    margin = pos.get("cost_basis", 0.0)  # margin tied up, released on close
    led["cash"] = led.get("cash", 10000.0) + (margin + pnl)
    ts = datetime.now(timezone.utc).isoformat()
    trade = {"ts": ts, "product_id": product_id, "side": "SELL",
             "quote_size": round(base * price, 4), "exposure": round(base * price * lev, 4),
             "leverage": lev, "fill_price": price,
             "base_size": base, "commission": round(commission, 6), "live": False,
             "note": note, "realized_pnl": round(pnl, 6)}
    led["trades"].append(trade)
    led["realized_pnl"] += pnl
    led["positions"][product_id] = {"base": 0.0, "cost_basis": 0.0, "entries": pos["entries"] + 1}
    save_ledger(led)
    return {"action": "closed", "live": False, "trade": trade,
            "realized_pnl": round(led["realized_pnl"], 6)}


def open_short(product_id: str, quote_size: float, note: str = "short",
               regime: str = "", setup: str = "", price: float | None = None) -> dict:
    """Open a SIMULATED SHORT (paper). Stores magnitude as a SHORT: keyed position
    with entry_price = current candle mark. No real borrow/sell; no money moves.
    P&L on close = (entry - exit) * magnitude - commission.

    `price` (optional) overrides the decision mark (Phase 9c: candle-derived, no
    live fetch needed for paper competition)."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if quote_size is None or quote_size <= 0:
        return _refuse("need quote_size for short")
    if quote_size > MAX_NOTIONAL:
        return _refuse(f"quote_size {quote_size} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    if price is None or price <= 0:
        price = _current_price(product_id)
    if price <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    magnitude = quote_size / price  # positive = size of short
    commission = quote_size * 0.0012
    led = load_ledger()
    led["cash"] = led.get("cash", 10000.0) + (quote_size - commission)
    ts = datetime.now(timezone.utc).isoformat()
    key = f"SHORT:{product_id}"
    pos = led["positions"].get(key, {"base": 0.0, "entry_price": 0.0,
                                    "entries": 0, "entry_ts": ts})
    pos["base"] += magnitude
    pos["entry_price"] = price  # simple: latest entry price
    pos["entry_ts"] = ts
    pos["exposure"] = pos.get("exposure", 0.0) + (magnitude * price * AGENT_LEVERAGE)
    pos["leverage"] = AGENT_LEVERAGE
    pos["cost_basis"] = pos.get("cost_basis", 0.0) + quote_size  # margin tied up
    pos["entries"] += 1
    led["positions"][key] = pos
    trade = {"ts": ts, "product_id": product_id, "side": "SHORT_OPEN",
             "quote_size": round(quote_size, 4),
             "exposure": round(magnitude * price * AGENT_LEVERAGE, 4),
             "leverage": AGENT_LEVERAGE, "fill_price": price,
             "base_size": round(magnitude, 8), "commission": round(commission, 6),
             "live": False, "note": note, "regime": regime, "setup": setup}
    led["trades"].append(trade)
    save_ledger(led)
    return {"action": "short_opened", "live": False, "trade": trade, "position": pos}


def close_short(product_id: str, note: str = "close-short", price: float | None = None) -> dict:
    """Close a simulated short: buy back at current candle mark. Realizes P&L.
    `price` (optional) lets the caller pass the decision price (loop passes the
    same candle mark used for the TP/SL decision, so execution == decision)."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    led = load_ledger()
    key = f"SHORT:{product_id}"
    pos = led["positions"].get(key)
    if not pos or pos.get("base", 0.0) <= 1e-12:
        return {"action": "no_short", "product_id": product_id}
    magnitude = pos["base"]
    entry = pos["entry_price"]
    if price is None or price <= 0:
        price = _current_price(product_id)
    exit_px = price
    if exit_px <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    commission = magnitude * exit_px * 0.0012
    lev = pos.get("leverage", AGENT_LEVERAGE)
    pnl = (entry - exit_px) * magnitude * lev - commission  # short wins if exit < entry
    margin = pos.get("cost_basis", 0.0)
    led["cash"] = led.get("cash", 10000.0) + (pnl - margin)
    ts = datetime.now(timezone.utc).isoformat()
    trade = {"ts": ts, "product_id": product_id, "side": "SHORT_CLOSE",
             "quote_size": round(magnitude * exit_px, 4),
             "exposure": round(magnitude * exit_px * lev, 4),
             "leverage": lev, "fill_price": exit_px,
             "base_size": round(magnitude, 8), "commission": round(commission, 6),
             "live": False, "note": note, "realized_pnl": round(pnl, 6)}
    led["trades"].append(trade)
    led["realized_pnl"] += pnl
    led["positions"][key] = {"base": 0.0, "entry_price": 0.0, "entries": pos["entries"] + 1}
    save_ledger(led)
    return {"action": "short_closed", "live": False, "trade": trade,
            "realized_pnl": round(led["realized_pnl"], 6)}


def add_to_position(product_id: str, add_margin: float, price: float | None = None,
                     note: str = "add-to-winner") -> dict:
    """Pyramid into an EXISTING paper position (long or short) by averaging the
    entry and increasing exposure. Margin accounting mirrors record_signal/
    open_short: longs debit margin+commission, shorts credit margin-commission.
    Paper-only. Used by the agent's 'add-to-winner' edge (let winners run harder
    when the setup is still confirmed)."""
    if KILL_SWITCH:
        return _refuse("KILL_SWITCH is active")
    if add_margin is None or add_margin <= 0:
        return _refuse("need add_margin > 0")
    if add_margin > MAX_NOTIONAL:
        return _refuse(f"add_margin {add_margin} exceeds MAX_NOTIONAL {MAX_NOTIONAL}")
    is_short = product_id.startswith("SHORT:") or led_short_key(product_id)
    key = f"SHORT:{product_id}" if is_short else product_id
    led = load_ledger()
    pos = led["positions"].get(key)
    if not pos or pos.get("base", 0.0) <= 1e-12:
        return {"action": "no_position", "product_id": product_id}
    if price is None or price <= 0:
        price = _current_price(product_id if not is_short else product_id)
    if price <= 0:
        return {"action": "quote_error", "error": "no mark", "product_id": product_id}
    lev = pos.get("leverage", AGENT_LEVERAGE)
    commission = add_margin * 0.0012
    ts = datetime.now(timezone.utc).isoformat()
    base = 0.0
    magnitude = 0.0
    if is_short:
        magnitude = add_margin / price
        # average entry price across the (now larger) short
        prev_base = pos["base"]
        pos["entry_price"] = ((pos["entry_price"] * prev_base) + (price * magnitude)) / (prev_base + magnitude)
        pos["base"] += magnitude
        pos["exposure"] = pos.get("exposure", 0.0) + (magnitude * price * lev)
        pos["cost_basis"] = pos.get("cost_basis", 0.0) + add_margin  # margin tied up
        led["cash"] = led.get("cash", 10000.0) + (add_margin - commission)
        side_tag = "SHORT_ADD"
    else:
        base = (add_margin * lev) / price
        prev_base = pos["base"]
        pos["entry_price"] = ((pos["entry_price"] * prev_base) + (price * base)) / (prev_base + base)
        pos["base"] += base
        pos["cost_basis"] = pos.get("cost_basis", 0.0) + add_margin
        pos["exposure"] = pos.get("exposure", 0.0) + (base * price * lev)
        led["cash"] = led.get("cash", 10000.0) - (add_margin + commission)
        side_tag = "BUY_ADD"
    pos["entries"] = pos.get("entries", 0) + 1
    pos["adds"] = pos.get("adds", 0) + 1
    led["positions"][key] = pos
    trade = {"ts": ts, "product_id": product_id, "side": side_tag,
             "quote_size": round(add_margin, 4),
             "exposure": round((base if not is_short else magnitude) * price * lev, 4),
             "leverage": lev, "fill_price": price,
             "base_size": round(base if not is_short else magnitude, 8),
             "commission": round(commission, 6), "live": False, "note": note}
    led["trades"].append(trade)
    save_ledger(led)
    return {"action": "position_added", "live": False, "trade": trade, "position": pos}


def led_short_key(product_id: str) -> bool:
    """True if product_id already exists as a SHORT: key in the ledger."""
    return load_ledger()["positions"].get(f"SHORT:{product_id}") is not None


def bot_recent_fills(minutes: int = 15) -> dict:
    """Same-team intel: read the bot's LIVE paper fills from its state file and
    return {product_id: {'side': 'BUY'|'SELL', 'ts': float, 'strategy': str}}
    for fills within the last `minutes`. The bot is the conservative book; when
    it just opened the SAME side on the SAME asset as the agent, that's a free
    confirmation signal (it's literally trading the same alpha). Paper-only read."""
    out: dict = {}
    try:
        import json as _json
        from pathlib import Path as _P
        p = _P(__file__).resolve().parent.parent / "data" / "paper_trader_v4_state.json"
        if not p.exists():
            return out
        d = _json.loads(p.read_text())
        trades = d.get("paper_trades", [])
        cutoff = (datetime.now(timezone.utc).timestamp()) - minutes * 60.0
        for t in trades:
            ts = t.get("ts", 0.0)
            if ts < cutoff:
                continue
            pid = t.get("product_id", "")
            side = "BUY" if t.get("side", "").upper() == "BUY" else "SELL"
            out[pid] = {"side": side, "ts": ts, "strategy": t.get("strategy", "")}
    except Exception:
        return out
    return out


def recent_stats(led: dict | None = None, n: int = 10) -> dict:
    if led is None:
        led = load_ledger()
    closed = [t for t in led.get("trades", []) if "realized_pnl" in t]
    window = closed[-n:] if n else closed
    wins = sum(1 for t in window if t["realized_pnl"] >= 0)
    losses = len(window) - wins
    pnl = sum(t["realized_pnl"] for t in window)
    peak = 0.0
    dd = 0.0
    run = 0.0
    for t in window:
        run += t["realized_pnl"]
        peak = max(peak, run)
        dd = min(dd, run - peak)
    return {
        "n": len(window), "wins": wins, "losses": losses,
        "win_rate": round(wins / len(window) * 100.0, 2) if window else 0.0,
        "pnl": round(pnl, 4), "max_drawdown": round(dd, 4),
    }


def size_for(mom: float, cap: float | None = None) -> float:
    """Kelly-lite sizing (Phase 4): stronger |momentum| -> larger notional,
    clamped to [30%, 100%] of the cap. Small signals stay small."""
    if cap is None:
        cap = MAX_NOTIONAL
    MOM_REF = 0.02  # 2% move -> ~full size
    frac = max(0.3, min(1.0, abs(mom) / MOM_REF))
    return round(cap * frac * 0.99, 2)  # 99% of cap to avoid notional breach


def drawdown_circuit(led: dict | None = None, n: int = 10,
                     min_win_rate: float = 40.0,
                     max_dd: float = -5.0) -> dict:
    """Return whether NEW entries should be blocked (circuit open) + reason.
    Opens when recent win-rate < min_win_rate OR max_drawdown < max_dd."""
    s = recent_stats(led, n)
    if s["n"] < 4:
        return {"open": True, "reason": "insufficient_samples", "stats": s}
    if s["win_rate"] < min_win_rate:
        return {"open": False, "reason": f"win_rate {s['win_rate']}% < {min_win_rate}%", "stats": s}
    if s["max_drawdown"] < max_dd:
        return {"open": False, "reason": f"drawdown {s['max_drawdown']} < {max_dd}", "stats": s}
    return {"open": True, "reason": "ok", "stats": s}


def close_all(note: str = "close-all") -> dict:
    """Close every open long and short (used on CRISIS / shutdown)."""
    led = load_ledger()
    longs = [p for p, v in led["positions"].items()
             if not p.startswith("SHORT:") and v.get("base", 0) > 1e-12]
    shorts = [p.replace("SHORT:", "") for p, v in led["positions"].items()
              if p.startswith("SHORT:") and v.get("base", 0) > 1e-12]
    res = {"longs": [], "shorts": []}
    for p in longs:
        res["longs"].append({p: close_position(p, note=note).get("action")})
    for p in shorts:
        res["shorts"].append({p: close_short(p, note=note).get("action")})
    return res


def mark_to_market() -> dict:
    """Value open positions at current real quotes (read-only). Returns unrealized P&L."""
    if KILL_SWITCH:
        return {"error": "KILL_SWITCH active"}
    led = load_ledger()
    out = {}
    total_unreal = 0.0
    for pid, pos in led["positions"].items():
        if pos.get("base", 0.0) <= 1e-12:
            continue
        try:
            true_pid = pid.replace("SHORT:", "") if pid.startswith("SHORT:") else pid
            bid = _current_price(true_pid)  # candles mark (best-bid-ask ignores product_id)
            if bid <= 0:
                continue
            if pid.startswith("SHORT:"):
                # short: unrealized = (entry - mark) * magnitude * leverage
                entry = pos.get("entry_price", bid)
                lev = pos.get("leverage", AGENT_LEVERAGE)
                unreal = (entry - bid) * pos["base"] * lev
                out[true_pid] = {"side": "SHORT", "magnitude": round(pos["base"], 6),
                                "entry": round(entry, 4), "mark": bid, "leverage": lev,
                                "unrealized_pnl": round(unreal, 4)}
            else:
                avg_cost = pos.get("entry_price", bid)
                lev = pos.get("leverage", AGENT_LEVERAGE)
                unreal = (bid - avg_cost) * pos["base"] * lev
                out[pid] = {"side": "LONG", "base": pos["base"],
                            "avg_cost": round(avg_cost, 4), "bid": bid, "leverage": lev,
                            "unrealized_pnl": round(unreal, 4)}
            total_unreal += unreal
        except Exception:
            continue
    return {"positions": out, "total_unrealized_pnl": round(total_unreal, 4)}


def update_equity() -> dict:
    """Recompute agent EQUITY (cash + open mark-to-market) and persist it the
    same way the bot tracks paper_equity_curve / paper_peak_equity, so the
    head-to-head digest compares apples-to-apples from a $10k start. Returns the
    current equity snapshot."""
    led = load_ledger()
    cash = led.get("cash", 10000.0)
    mtm = mark_to_market()
    unreal = mtm.get("total_unrealized_pnl", 0.0)
    equity = cash + unreal
    start = led.get("starting_capital", 10000.0)
    peak = max(led.get("peak_equity", equity), equity)
    curve = led.get("equity_curve", [start])
    curve.append(round(equity, 2))
    if len(curve) > 5000:
        curve = curve[-5000:]
    led["cash"] = round(cash, 2)
    led["equity"] = round(equity, 2)
    led["peak_equity"] = round(peak, 2)
    led["equity_curve"] = curve
    led["return_pct"] = round((equity - start) / start * 100.0, 4)
    save_ledger(led)
    return {"cash": round(cash, 2), "unrealized": round(unreal, 2),
            "equity": round(equity, 2), "peak_equity": round(peak, 2),
            "return_pct": led["return_pct"]}


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
    pc = sub.add_parser("close"); pc.add_argument("product")
    psh = sub.add_parser("short"); psh.add_argument("product")
    psh.add_argument("--quote-size", type=float, required=True); psh.add_argument("--note", default="short")
    pcs = sub.add_parser("closeshort"); pcs.add_argument("product")
    pca = sub.add_parser("closeall")
    pm = sub.add_parser("mtm")
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
    elif args.cmd == "close":
        print(json.dumps(close_position(args.product), indent=2))
    elif args.cmd == "short":
        print(json.dumps(open_short(args.product, args.quote_size, args.note), indent=2))
    elif args.cmd == "closeshort":
        print(json.dumps(close_short(args.product), indent=2))
    elif args.cmd == "closeall":
        print(json.dumps(close_all(), indent=2))
    elif args.cmd == "mtm":
        print(json.dumps(mark_to_market(), indent=2))
    elif args.cmd == "propose-live":
        print(json.dumps(propose_live(args.product, args.side, args.quote_size, args.note), indent=2))
    elif args.cmd == "status":
        print(json.dumps(status(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
