from __future__ import annotations
import time, uuid, logging
from .cb_client import CBClient
from .state import add_bracket, load_state, save_state
from .analytics import log_trade

log = logging.getLogger(__name__)

def place_bracket_long(cb: CBClient, product_id: str, base_size: float, entry: float, stop: float, target: float, dry_run: bool) -> dict:
    cid = str(uuid.uuid4())
    if dry_run:
        prev = cb.preview_order(side="buy", product_id=product_id, quote_size=str(base_size*entry))
        order_res = {"preview": prev, "client_order_id": cid}
    else:
        order = cb.market_order("buy", product_id=product_id, quote_size=str(base_size*entry), client_order_id=cid)
        order_res = {"order": order, "client_order_id": cid}
    add_bracket({"client_order_id": cid, "product_id": product_id, "side": "long", "base_size": base_size, "entry": entry, "stop": stop, "target": target, "active": True, "created_ts": int(time.time())})
    return order_res

def place_bracket_short(cb: CBClient, product_id: str, base_size: float, entry: float, stop: float, target: float, dry_run: bool) -> dict:
    cid = str(uuid.uuid4())
    if dry_run:
        prev = cb.preview_order(side="sell", product_id=product_id, base_size=str(base_size))
        order_res = {"preview": prev, "client_order_id": cid}
    else:
        order = cb.market_order("sell", product_id=product_id, base_size=str(base_size), client_order_id=cid)
        order_res = {"order": order, "client_order_id": cid}
    add_bracket({"client_order_id": cid, "product_id": product_id, "side": "short", "base_size": base_size, "entry": entry, "stop": stop, "target": target, "active": True, "created_ts": int(time.time())})
    return order_res

def manage_brackets(cb: CBClient, poll_secs: int = 5, trail_atr_mult: float = 0.0, break_even_after_r: float = 1.0, dry_run: bool = False):
    st = load_state()
    changed = False
    best = cb.best_bid_ask([b["product_id"] for b in st.get("brackets", []) if b.get("active")])
    now = int(time.time())
    for b in st.get("brackets", []):
        if not b.get("active"): continue
        pid = b["product_id"]
        # derive mid
        mid = 0.0
        for pb in best.get("pricebooks", best if isinstance(best, list) else []):
            if pb.get("product_id") == pid:
                bid = float(pb.get("bids", [{}])[0].get("price", 0)) if pb.get("bids") else 0.0
                ask = float(pb.get("asks", [{}])[0].get("price", 0)) if pb.get("asks") else 0.0
                mid = (bid + ask)/2 if bid and ask else max(bid, ask, 0.0)
                break
        if mid <= 0: continue
        entry, stop, target, base_size = b["entry"], b["stop"], b["target"], b["base_size"]
        # Breakeven & trailing
        if b["side"] == "long":
            rdist = entry - stop
            if break_even_after_r > 0 and mid >= entry + break_even_after_r * rdist and b["stop"] < entry:
                b["stop"] = entry; changed = True
        else:
            rdist = stop - entry
            if break_even_after_r > 0 and mid <= entry - break_even_after_r * rdist and b["stop"] > entry:
                b["stop"] = entry; changed = True
        # Exit conditions
        exit_reason = None
        if b['side'] == 'long':
            if mid <= b['stop']: exit_reason = 'stop'
            elif mid >= b['target']: exit_reason = 'target'
        else:
            if mid >= b['stop']: exit_reason = 'stop'
            elif mid <= b['target']: exit_reason = 'target'
        if exit_reason:
            if dry_run:
                log.info(f"[DRY] Exit {pid} via {exit_reason} @ ~{mid} for {base_size} base ({b['side']})")
            else:
                if b['side'] == 'long':
                    cb.market_order("sell", product_id=pid, base_size=str(base_size), client_order_id=str(uuid.uuid4()))
                else:
                    cb.market_order("buy", product_id=pid, base_size=str(base_size), client_order_id=str(uuid.uuid4()))
            b["active"] = False; b["closed_ts"] = now; b["exit_reason"] = exit_reason
            # Rough R & PnL
            if b['side'] == 'long':
                r = (mid - entry) / max(1e-9, (entry - b['stop'])); pnl = (mid - entry) * base_size
            else:
                r = (entry - mid) / max(1e-9, (b['stop'] - entry)); pnl = (entry - mid) * base_size
            log_trade({'ts_open': b.get('created_ts'), 'ts_close': now, 'product_id': pid, 'setup': b.get('setup','unknown'), 'side': b['side'],
                       'entry': entry, 'stop': b['stop'], 'target': b['target'], 'exit_price': mid, 'exit_reason': exit_reason, 'r_multiple': r, 'pnl_usd': pnl})
            changed = True
    if changed: save_state(st)
