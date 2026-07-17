#!/usr/bin/env python3
"""
hermes_agent_watch.py — high-frequency EXIT-ONLY watcher for the Hermes agent
paper book.

WHY THIS EXISTS
  The strategic loop (hermes_agent_loop.py) only runs every ~15 min. Crypto can
  gap several percent in seconds, so a stop that is only *checked* every 15 min is
  a suggestion, not a stop. This watcher does ONE cheap thing on a tight cadence
  (default 20s): pull the LIVE bid/ask for each open position and evaluate the
  exact same _exit_check logic the loop uses (TP / SL / hard stop / trailing /
  breakeven / regime-flip / timeout). If a level is hit, it closes the position
  immediately — bounding intraday loss between strategic ticks.

WHAT IT DOES *NOT* DO
  * It never OPENS positions. Entries stay 100% with the 15-min strategic loop.
  * It never touches sizing, leverage, or the drawdown circuit.
  * It writes only via the same close_position / close_short trader functions the
    loop uses, so the ledger stays consistent and single-writer-safe (the loop and
    watcher both mutate data/hermes_agent_ledger.json but only ever through the
    trader's load->mutate->save, and closes are idempotent on an already-flat pos).

SAFETY
  * Honors KILL_SWITCH (env) — exits immediately, does nothing.
  * Paper-only: close_position/close_short are simulation calls; no real orders.
  * On any per-asset error it logs and continues (never crashes the loop).
  * Uses the LIVE order-book mid (best bid/ask) — the real-time mark — not the
    lagging 120h candle close the strategic loop uses.

USAGE
  # one pass over open positions, then exit (for cron @ 1-min, or a smoke test):
  python scripts/hermes_agent_watch.py --once

  # continuous daemon, check every 20s:
  python scripts/hermes_agent_watch.py --interval 20

  # quiet mode (only print when it actually closes something):
  python scripts/hermes_agent_watch.py --interval 20 --quiet
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Reuse the loop's EXACT exit logic + constants so watcher and loop never diverge.
from scripts import hermes_agent_loop as L  # noqa: E402
from scripts.hermes_agent_trader import (  # noqa: E402
    load_ledger, close_position, close_short, update_equity,
)

KILL_SWITCH = os.getenv("KILL_SWITCH", "").lower() in ("1", "true", "yes")
# Matches HOLD_ITERS in hermes_agent_loop.run_once (15m cadence * this = timeout).
HOLD_ITERS = 6


def _mid_price(client, product_id: str) -> float | None:
    """Live order-book mid (best bid/ask average). This is the REAL-TIME mark,
    unlike the loop's 120h candle close. Falls back to bid or ask alone, then
    None if the book can't be read."""
    try:
        book = client.best_bid_ask(product_id)
        pbs = book.get("pricebooks", []) if isinstance(book, dict) else []
        for pb in pbs:
            if not isinstance(pb, dict):
                continue
            # match product (remap-tolerant): accept the only book if single
            bids = pb.get("bids") or []
            asks = pb.get("asks") or []
            bid = float(bids[0]["price"]) if bids and bids[0].get("price") else 0.0
            ask = float(asks[0]["price"]) if asks and asks[0].get("price") else 0.0
            if bid > 0 and ask > 0:
                return (bid + ask) / 2.0
            if bid > 0:
                return bid
            if ask > 0:
                return ask
    except Exception:
        return None
    return None


def _atr_for(client, product_id: str) -> float:
    """Fetch ATR for the position's asset (used by _exit_check's ATR-stop branch).
    Returns 0.0 on any failure so the watcher falls back to the flat SL constant."""
    try:
        cd = L._candles(client, product_id, 120)
        return L.atr_from_candles(cd) if cd else 0.0
    except Exception:
        return 0.0


def watch_once(client, verbose: bool = True) -> dict:
    """Single pass over all open positions. Closes any that hit an exit level at
    the LIVE mid price. Returns a summary dict."""
    if KILL_SWITCH:
        if verbose:
            print("[watch] KILL_SWITCH active — no action")
        return {"skipped": True, "reason": "kill_switch"}

    led = load_ledger()
    positions = led.get("positions", {})
    open_pids = [pid for pid, pos in positions.items()
                 if isinstance(pos, dict) and pos.get("base", 0.0) > 1e-12]

    if not open_pids:
        if verbose:
            print("[watch] no open positions")
        return {"checked": 0, "closed": []}

    now = dt.datetime.now(dt.timezone.utc)
    book_tp = L.book_tp_active(led.get("equity", 10000.0),
                              led.get("starting_capital", 10000.0))

    closed = []
    checked = 0
    for pid in open_pids:
        pos = positions.get(pid, {})
        true_pid = pid.replace("SHORT:", "")
        is_short = pid.startswith("SHORT:")
        try:
            cur = _mid_price(client, true_pid)
            if not cur or cur <= 0:
                continue
            checked += 1
            atr = _atr_for(client, true_pid)
            # The watcher fires ONLY on PRICE-based exits (hard stop / SL / TP /
            # trailing / breakeven). Regime-flip and timeout exits are strategic
            # decisions left to the 15-min loop, which has the full MTF regime the
            # watcher can't cheaply compute. So we pass regime="" and stale=False
            # to suppress those two branches — the watcher is a pure risk stop.
            close_now, reason = L._exit_check(
                pid, pos, cur, "", False, "",
                vol_bucket="NORMAL", bot_coholds=False, atr=atr, book_tp=book_tp,
            )
            if close_now:
                if is_short:
                    res = close_short(true_pid, note=f"watch-{reason}", price=cur)
                    ok = res.get("action") == "short_closed"
                else:
                    res = close_position(pid, note=f"watch-{reason}", price=cur)
                    ok = res.get("action") == "closed"
                if ok:
                    closed.append({"pid": pid, "reason": reason,
                                   "price": cur, "pnl": res.get("realized_pnl")})
                    if verbose:
                        print(f"[watch-close] {pid} {reason} @ {cur:.6g} "
                              f"-> pnl={res.get('realized_pnl')}")
        except Exception as exc:
            if verbose:
                print(f"[watch] {pid} error: {exc}")
            continue

    # Refresh equity mark after any closes so the drawdown circuit sees truth.
    try:
        update_equity()
    except Exception:
        pass

    if verbose and not closed:
        print(f"[watch] checked {checked} position(s), nothing to close")
    return {"checked": checked, "closed": closed}


def main() -> int:
    ap = argparse.ArgumentParser(description="Hermes agent exit-only watcher")
    ap.add_argument("--interval", type=float, default=20.0,
                    help="seconds between passes (daemon mode). Default 20.")
    ap.add_argument("--once", action="store_true",
                    help="single pass then exit (for cron or smoke test)")
    ap.add_argument("--quiet", action="store_true",
                    help="only print when a position is actually closed")
    args = ap.parse_args()

    if KILL_SWITCH:
        print("[watch] KILL_SWITCH active — exiting")
        return 0

    # Lazy client init (reuses the loop's CBClient path).
    from coinbase.src.cb_client import CBClient
    client = CBClient()

    verbose = not args.quiet

    if args.once:
        res = watch_once(client, verbose=verbose)
        # In quiet mode, still surface closes to stdout for cron delivery.
        if args.quiet and res.get("closed"):
            for c in res["closed"]:
                print(f"[watch-close] {c['pid']} {c['reason']} @ "
                      f"{c['price']:.6g} -> pnl={c['pnl']}")
        return 0

    # Daemon loop.
    print(f"[watch] exit-only watcher started — interval={args.interval}s "
          f"(paper-only, never opens positions)")
    while True:
        try:
            res = watch_once(client, verbose=verbose)
            if args.quiet and res.get("closed"):
                for c in res["closed"]:
                    print(f"[watch-close] {c['pid']} {c['reason']} @ "
                          f"{c['price']:.6g} -> pnl={c['pnl']}", flush=True)
        except KeyboardInterrupt:
            print("\n[watch] stopped")
            return 0
        except Exception as exc:
            print(f"[watch] pass error: {exc}", flush=True)
        time.sleep(max(2.0, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
