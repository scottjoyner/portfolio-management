from __future__ import annotations
import time, uuid, logging
from typing import List, Dict, Any
from .cb_client import CBClient
from .state import add_bracket, load_state, save_state
from .analytics import log_trade

log = logging.getLogger(__name__)


def _fmt_base(v: float) -> str:
    return f"{float(v):.8f}".rstrip("0").rstrip(".") or "0"


def _fmt_quote(v: float) -> str:
    return f"{float(v):.2f}".rstrip("0").rstrip(".") or "0"


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Access attribute from both pydantic models and dicts."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _set(obj: Any, key: str, value: Any) -> None:
    """Set attribute on both pydantic models and dicts."""
    if isinstance(obj, dict):
        obj[key] = value
    else:
        setattr(obj, key, value)


def _bracket_dict(product_id: str, side: str, base_size: float,
                  entry: float, stop: float, target: float,
                  strategy_id: str = "rr_trade", cid: str | None = None) -> dict:
    """Build a dict matching the pydantic Bracket model."""
    return {
        "client_order_id": cid or str(uuid.uuid4()),
        "product_id": product_id,
        "side": side,
        "base_size": base_size,
        "quote_size": base_size * entry,
        "entry_price": entry,
        "stop_loss": stop,
        "take_profit": target,
        "status": "OPEN",
        "strategy_id": strategy_id,
        "timestamp": int(time.time()),
        "metadata": {},
    }


def place_bracket_long(cb: CBClient, product_id: str, base_size: float,
                       entry: float, stop: float, target: float,
                       dry_run: bool, strategy_id: str = "rr_trade") -> dict:
    cid = str(uuid.uuid4())
    if dry_run:
        prev = cb.preview_order(side="buy", product_id=product_id, quote_size=_fmt_quote(base_size * entry))
        order_res = {"preview": prev, "client_order_id": cid}
    else:
        try:
            order = cb.market_order("buy", product_id=product_id,
                                    quote_size=_fmt_quote(base_size * entry), client_order_id=cid)
            order_res = {"order": order, "client_order_id": cid}
        except Exception as e:
            log.error("Entry order failed for %s: %s", product_id, e)
            return {"success": False, "error": str(e), "client_order_id": cid}
    add_bracket(_bracket_dict(product_id, "long", base_size, entry, stop, target, cid=cid))
    return order_res


def place_bracket_short(cb: CBClient, product_id: str, base_size: float,
                        entry: float, stop: float, target: float,
                        dry_run: bool, strategy_id: str = "rr_trade") -> dict:
    cid = str(uuid.uuid4())
    if dry_run:
        prev = cb.preview_order(side="sell", product_id=product_id, base_size=_fmt_base(base_size))
        order_res = {"preview": prev, "client_order_id": cid}
    else:
        try:
            order = cb.market_order("sell", product_id=product_id,
                                    base_size=_fmt_base(base_size), client_order_id=cid)
            order_res = {"order": order, "client_order_id": cid}
        except Exception as e:
            log.error("Entry order failed for %s: %s", product_id, e)
            return {"success": False, "error": str(e), "client_order_id": cid}
    add_bracket(_bracket_dict(product_id, "short", base_size, entry, stop, target, cid=cid))
    return order_res


def manage_brackets(cb: CBClient, poll_secs: int = 5,
                    trail_atr_mult: float = 0.0, break_even_after_r: float = 1.0,
                    dry_run: bool = False) -> None:
    """Monitor active brackets, move stops to breakeven, trail, and close on stop/target."""
    while True:
        st = load_state()
        active = [b for b in st if _get(b, "status") == "OPEN"]
        changed = False
        now = int(time.time())

        if active:
            pids = [_get(b, "product_id") for b in active]
            best = cb.best_bid_ask(pids)

            for b in active:
                pid = _get(b, "product_id")
                mid = _get_mid(best, pid)
                if mid <= 0:
                    continue

                entry = _get(b, "entry_price")
                stop = _get(b, "stop_loss")
                target = _get(b, "take_profit")
                base_size = _get(b, "base_size")
                side = _get(b, "side")

                rdist = abs(entry - stop) if stop else 0
                if rdist <= 0:
                    continue

                # Breakeven: move stop to entry after R multiple gained
                if break_even_after_r > 0:
                    stop_changed = False
                    if side == "long" and mid >= entry + break_even_after_r * rdist and _get(b, "stop_loss", 0) < entry:
                        _set(b, "stop_loss", entry)
                        stop_changed = True
                    elif side == "short" and mid <= entry - break_even_after_r * rdist and _get(b, "stop_loss", 0) > entry:
                        _set(b, "stop_loss", entry)
                        stop_changed = True
                    if stop_changed:
                        changed = True
                        log.info(f"[BRK] {pid} stop moved to breakeven @ {entry}")

                # Trailing stop
                if trail_atr_mult > 0:
                    new_stop = _trail_stop(mid, entry, side, trail_atr_mult, rdist)
                    if new_stop and ((side == "long" and new_stop > _get(b, "stop_loss", 0)) or
                                     (side == "short" and new_stop < _get(b, "stop_loss", 0))):
                        _set(b, "stop_loss", new_stop)
                        changed = True
                        log.info(f"[TRAIL] {pid} stop trailed to {new_stop:.2f}")

                # Exit conditions
                exit_reason = None
                if side == "long":
                    if _get(b, "stop_loss") and mid <= _get(b, "stop_loss"):
                        exit_reason = "stop"
                    elif _get(b, "take_profit") and mid >= _get(b, "take_profit"):
                        exit_reason = "target"
                else:
                    if _get(b, "stop_loss") and mid >= _get(b, "stop_loss"):
                        exit_reason = "stop"
                    elif _get(b, "take_profit") and mid <= _get(b, "take_profit"):
                        exit_reason = "target"

                if exit_reason:
                    if dry_run:
                        log.info(f"[DRY] Exit {pid} via {exit_reason} @ ~{mid} for {base_size} base ({side})")
                    else:
                        try:
                            if side == "long":
                                cb.market_order("sell", product_id=pid, base_size=_fmt_base(base_size),
                                                client_order_id=str(uuid.uuid4()))
                            else:
                                cb.market_order("buy", product_id=pid, base_size=_fmt_base(base_size),
                                                client_order_id=str(uuid.uuid4()))
                        except Exception as e:
                            log.error("Exit order failed for %s: %s — will retry", pid, e)
                            continue

                    _set(b, "status", "CLOSED")
                    _set(b, "metadata", {**_get(b, "metadata", {}), "exit_price": mid,
                                          "exit_reason": exit_reason, "closed_ts": now})

                    # Compute R multiple and PnL
                    if side == "long":
                        r = (mid - entry) / max(1e-9, rdist)
                        pnl = (mid - entry) * base_size
                    else:
                        r = (entry - mid) / max(1e-9, rdist)
                        pnl = (entry - mid) * base_size

                    log_trade({
                        "ts_open": _get(b, "timestamp"),
                        "ts_close": now,
                        "product_id": pid,
                        "setup": _get(b, "strategy_id", "unknown"),
                        "side": side,
                        "entry": entry,
                        "stop": _get(b, "stop_loss"),
                        "target": _get(b, "take_profit"),
                        "exit_price": mid,
                        "exit_reason": exit_reason,
                        "r_multiple": r,
                        "pnl_usd": pnl,
                    })
                    changed = True
                    log.info(f"[EXIT] {pid} {exit_reason} @ {mid:.2f} R={r:.2f} PnL=${pnl:.2f}")

            if changed:
                save_state(st)

        if poll_secs <= 0:
            break
        time.sleep(poll_secs)


def _get_mid(best: Any, pid: str) -> float:
    """Extract mid price from best_bid_ask response."""
    if best is None:
        return 0.0
    pricebooks = best.get("pricebooks", best if isinstance(best, list) else [])
    for pb in pricebooks:
        if pb.get("product_id") == pid:
            bids = pb.get("bids", [])
            asks = pb.get("asks", [])
            bid = float(bids[0].get("price", 0)) if bids else 0.0
            ask = float(asks[0].get("price", 0)) if asks else 0.0
            return (bid + ask) / 2 if bid and ask else max(bid, ask, 0.0)
    return 0.0


def _trail_stop(mid: float, entry: float, side: str, trail_atr_mult: float, rdist: float) -> float | None:
    """Compute a trailing stop level. Returns new stop price or None."""
    if trail_atr_mult <= 0 or rdist <= 0:
        return None
    trail_dist = rdist * trail_atr_mult
    if side == "long":
        new_stop = mid - trail_dist
        return new_stop if new_stop > entry else None
    else:
        new_stop = mid + trail_dist
        return new_stop if new_stop < entry else None
