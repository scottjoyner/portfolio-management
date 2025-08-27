from __future__ import annotations
import time, uuid, logging
from typing import Dict, List
from .cb_client import CBClient
from .state import add_bracket, load_state, save_state, remove_bracket_by_id
from .analytics import log_trade

log = logging.getLogger(__name__)

def place_bracket_long(cb: CBClient, product_id: str, base_size: float, entry: float, stop: float, target: float, dry_run: bool) -> dict:
    """
    Enter long (market) and register a synthetic OCO (stop or target) managed by the bot.
    """
    cid = str(uuid.uuid4())
    if dry_run:
        prev = cb.preview_order(side="buy", product_id=product_id, quote_size=str(base_size*entry))
        order_res = {"preview": prev, "client_order_id": cid}
    else:
        order = cb.market_order("buy", product_id=product_id, quote_size=str(base_size*entry), client_order_id=cid)
        order_res = {"order": order, "client_order_id": cid}
    add_bracket({
        "client_order_id": cid,
        "product_id": product_id,
        "side": "long",
        "base_size": base_size,
        "entry": entry,
        "stop": stop,
        "target": target,
        "active": True,
        "created_ts": int(time.time())
    })
    return order_res

def current_mid(best: dict, product_id: str) -> float:
    for b in best.get("pricebooks", best if isinstance(best, list) else []):
        if b.get("product_id") == product_id:
            bid = float(b.get("bids", [{}])[0].get("price", 0)) if b.get("bids") else 0.0
            ask = float(b.get("asks", [{}])[0].get("price", 0)) if b.get("asks") else 0.0
            return (bid + ask)/2 if bid and ask else max(bid, ask, 0.0)
    return 0.0

def manage_brackets(cb: CBClient, poll_secs: int = 5, trail_atr_mult: float = 0.0, break_even_after_r: float = 1.0, dry_run: bool = False):
    """
    Poll prices and execute exits when stop/target levels are crossed.
    Also advances stop to breakeven after +1R and optionally trails by ATR multiple (requires caller to update ATR in state if desired).
    """
    st = load_state()
    changed = False
    best = cb.best_bid_ask([b["product_id"] for b in st.get("brackets", []) if b.get("active")])
    now = int(time.time())
    for b in st.get("brackets", []):
        if not b.get("active"):
            continue
        pid = b["product_id"]
        mid = current_mid(best, pid)
        if mid <= 0: 
            continue
        entry, stop, target = b["entry"], b["stop"], b["target"]
        base_size = b["base_size"]
        r = entry - stop  # R distance for long
        # Advance to breakeven after 1R
        if break_even_after_r > 0 and mid >= entry + break_even_after_r * r and b["stop"] < entry:
            b["stop"] = entry
            changed = True
        # Trail (requires atr in record)
        if trail_atr_mult > 0 and "atr" in b and b["atr"] > 0:
            trail_stop = mid - trail_atr_mult * b["atr"]
            if trail_stop > b["stop"]:
                b["stop"] = trail_stop
                changed = True
        # Check exits
        exit_reason = None
        if mid <= b["stop"]:
            exit_reason = "stop"
        elif mid >= b["target"]:
            exit_reason = "target"
        if exit_reason:
            if dry_run:
                log.info(f"[DRY] Exiting {pid} via {exit_reason} @ ~{mid} for {base_size} base")
            else:
                # sell to close
                cb.market_order("sell", product_id=pid, base_size=str(base_size), client_order_id=str(uuid.uuid4()))
            b["active"] = False
            b["closed_ts"] = now
            b["exit_reason"] = exit_reason
            changed = True
    if changed:
        save_state(st)
    time.sleep(poll_secs)


def place_bracket_short(cb: CBClient, product_id: str, base_size: float, entry: float, stop: float, target: float, dry_run: bool) -> dict:
    """
    Enter short (market sell) and register a synthetic OCO. NOTE: True net shorts require margin/derivatives.
    On spot-only accounts, this function only sells existing holdings. If holdings are insufficient, skip.
    """
    cid = str(uuid.uuid4())
    if dry_run:
        prev = cb.preview_order(side="sell", product_id=product_id, base_size=str(base_size))
        order_res = {"preview": prev, "client_order_id": cid}
    else:
        order = cb.market_order("sell", product_id=product_id, base_size=str(base_size), client_order_id=cid)
        order_res = {"order": order, "client_order_id": cid}
    add_bracket({
        "client_order_id": cid,
        "product_id": product_id,
        "side": "short",
        "base_size": base_size,
        "entry": entry,
        "stop": stop,
        "target": target,
        "active": True,
        "created_ts": int(time.time())
    })
    return order_res
