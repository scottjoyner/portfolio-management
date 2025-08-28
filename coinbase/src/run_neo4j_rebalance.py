#!/usr/bin/env python3
from __future__ import annotations
import os, math, time
from typing import Dict, List, Tuple
from dotenv import load_dotenv
from src.cb_client import CBClient
from src.portfolio.neo4j_alloc import fetch_allocations

load_dotenv(override=False)

DRY_RUN       = (os.getenv("DRY_RUN", "true").lower() == "true")
CASH_CCY      = os.getenv("CASH", "USD").upper()
MIN_NOTIONAL  = float(os.getenv("MIN_NOTIONAL", "50"))
TAKER_FEE_BPS = float(os.getenv("TAKER_FEE_BPS", "8.0"))
SLIPPAGE_BPS  = float(os.getenv("SLIPPAGE_BPS", "0.0"))

def _list_holdings(cb: CBClient) -> Tuple[Dict[str,float], float]:
    """Return base currency balances (symbol->amount) and USD cash balance."""
    acc = cb.list_accounts()
    hold = {}
    usd = 0.0
    # SDK may return dict or obj; normalize to dict-of-dicts
    items = acc.get("accounts") or acc.get("data") or acc
    for a in items:
        cur = (a.get("currency") or a.get("currency_code") or "").upper()
        avail = float((a.get("available_balance") or a.get("available") or {}).get("value", a.get("available", 0)) or 0)
        if cur == CASH_CCY:
            usd += avail
        else:
            hold[cur] = hold.get(cur, 0.0) + avail
    return hold, usd

def _mid_from_pricebook(books: List[dict]) -> Dict[str,float]:
    mids = {}
    for b in books:
        pid = b.get("product_id")
        bids = b.get("bids") or []
        asks = b.get("asks") or []
        if bids and asks:
            bid = float(bids[0]["price"])
            ask = float(asks[0]["price"])
            mids[pid] = (bid + ask) / 2.0
    return mids

def _usd_equity(hold: Dict[str,float], usd: float, prices: Dict[str,float]) -> float:
    eq = usd
    for sym, amt in hold.items():
        pid = f"{sym}-USD"
        p = prices.get(pid, 0.0)
        eq += amt * p
    return eq

def main():
    cb = CBClient()
    alloc = fetch_allocations()
    products = [a["product_id"] for a in alloc]
    pricebooks = cb.best_bid_ask(products)
    books = pricebooks.get("pricebooks") or []
    mids = _mid_from_pricebook(books)

    hold, usd = _list_holdings(cb)
    equity = _usd_equity(hold, usd, mids)

    print(f"Equity ≈ ${equity:,.2f}  (USD: ${usd:,.2f})")
    for a in alloc:
        pid = a["product_id"]
        a["price"] = mids.get(pid, 0.0)

    # compute targets and diffs
    orders = []
    for a in alloc:
        pid = a["product_id"]; sym = a["symbol"]; w = a["weight"]; px = a.get("price", 0.0)
        if px <= 0: 
            print(f"skip {pid}: no price")
            continue
        target_notional = equity * w
        target_base = target_notional / px
        have_base = hold.get(sym, 0.0)
        diff_base = target_base - have_base
        diff_usd  = diff_base * px
        if abs(diff_usd) < MIN_NOTIONAL:
            continue
        side = "buy" if diff_usd > 0 else "sell"
        if side == "buy":
            orders.append({"side": "buy", "product_id": pid, "quote_size": f"{abs(diff_usd):.2f}"})
        else:
            orders.append({"side": "sell", "product_id": pid, "base_size": f"{abs(diff_base):.8f}"})

    # Print plan
    if not orders:
        print("No rebalance needed (all within thresholds).")
        return

    print("\nPlanned orders:")
    for o in orders:
        if o["side"] == "buy":
            print(f"  BUY  {o['product_id']:>12}   ~${o['quote_size']}")
        else:
            print(f"  SELL {o['product_id']:>12}   ~{o['base_size']} base")

    # Execute
    if DRY_RUN:
        print("\nDRY_RUN=true → previewing each order:")
        for o in orders:
            if o["side"] == "buy":
                r = cb.preview_order("buy", o["product_id"], quote_size=o["quote_size"])
            else:
                r = cb.preview_order("sell", o["product_id"], base_size=o["base_size"])
            pid = (r.get("preview_id") or (r.get("success_response") or {}).get("preview_id"))
            print(f"  preview {o['side']} {o['product_id']} -> preview_id={pid}")
    else:
        print("\nDRY_RUN=false → placing live market IOC orders:")
        for o in orders:
            if o["side"] == "buy":
                prev = cb.preview_order("buy", o["product_id"], quote_size=o["quote_size"])
                pid = (prev.get("preview_id") or (prev.get("success_response") or {}).get("preview_id"))
                r = cb.market_order("buy", o["product_id"], quote_size=o["quote_size"], preview_id=pid or None)
            else:
                prev = cb.preview_order("sell", o["product_id"], base_size=o["base_size"])
                pid = (prev.get("preview_id") or (prev.get("success_response") or {}).get("preview_id"))
                r = cb.market_order("sell", o["product_id"], base_size=o["base_size"], preview_id=pid or None)
            print(f"  placed {o['side']} {o['product_id']}: ok")
    print("\nDone.")

if __name__ == "__main__":
    main()
