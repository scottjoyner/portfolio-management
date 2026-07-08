from __future__ import annotations
import time
import uuid
import logging
import threading
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from enum import Enum
from .cb_client import CBClient

log = logging.getLogger(__name__)


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_LIMIT = "stop_limit"
    STOP_MARKET = "stop_market"


class OrderStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


@dataclass
class OrderIntent:
    side: str
    product_id: str
    order_type: OrderType
    base_size: str = ""
    quote_size: str = ""
    limit_price: str = ""
    stop_price: str = ""
    stop_direction: str = "stop_direction_stop_up"
    time_in_force: str = "GTC"
    post_only: bool = False
    client_order_id: str = ""
    preview_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderResult:
    success: bool
    order_id: str = ""
    client_order_id: str = ""
    status: OrderStatus = OrderStatus.PENDING
    fill_price: float = 0.0
    filled_size: float = 0.0
    fees: float = 0.0
    error: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


def _fmt_base(v: float) -> str:
    return f"{float(v):.8f}".rstrip("0").rstrip(".") or "0"


def _fmt_quote(v: float) -> str:
    return f"{float(v):.2f}".rstrip("0").rstrip(".") or "0"


def _fmt_price(v: float) -> str:
    return f"{float(v):.2f}"


class NativeExecutionEngine:
    def __init__(self, cb: CBClient, dry_run: bool = True):
        self.cb = cb
        self.dry_run = dry_run
        self._orders: Dict[str, OrderResult] = {}

    def place(self, intent: OrderIntent) -> OrderResult:
        if not intent.client_order_id:
            intent.client_order_id = str(uuid.uuid4())

        if self.dry_run:
            return self._preview(intent)

        try:
            return self._execute(intent)
        except Exception as e:
            log.error(f"Order failed: {e}")
            return OrderResult(success=False, error=str(e), status=OrderStatus.FAILED)

    def _preview(self, intent: OrderIntent) -> OrderResult:
        try:
            if intent.order_type == OrderType.MARKET:
                kw = {}
                if intent.side.upper() == "BUY":
                    if intent.quote_size:
                        kw["quote_size"] = intent.quote_size
                    elif intent.base_size:
                        kw["base_size"] = intent.base_size
                    else:
                        raise ValueError("buy preview needs quote_size or base_size")
                else:
                    kw["base_size"] = intent.base_size
                prev = self.cb.preview_order(intent.side, intent.product_id, **kw)
            else:
                prev = {"preview_id": f"sim-{intent.client_order_id}", "status": "preview_simulated"}
            return OrderResult(
                success=True,
                client_order_id=intent.client_order_id,
                status=OrderStatus.PENDING,
                raw={"preview": prev},
            )
        except Exception as e:
            return OrderResult(success=False, error=str(e), status=OrderStatus.FAILED)

    def _execute(self, intent: OrderIntent) -> OrderResult:
        cid = intent.client_order_id
        pid = intent.product_id
        side = intent.side

        if intent.order_type == OrderType.MARKET:
            kw = {"client_order_id": cid}
            if intent.preview_id:
                kw["preview_id"] = intent.preview_id
            if side.upper() == "BUY":
                if intent.quote_size:
                    kw["quote_size"] = intent.quote_size
                elif intent.base_size:
                    kw["base_size"] = intent.base_size
                else:
                    return OrderResult(success=False, error="buy order needs quote_size or base_size")
            else:
                kw["base_size"] = intent.base_size
            preview = self.cb.preview_order(
                side,
                pid,
                base_size=kw.get("base_size"),
                quote_size=kw.get("quote_size"),
            )
            if preview.get("error") or preview.get("status") == "preview_error":
                return OrderResult(success=False, error=str(preview.get("error", "preview failed")), status=OrderStatus.FAILED, raw={"preview": preview})
            preview_id = preview.get("preview_id") or preview.get("id") or ""
            if preview_id:
                kw["preview_id"] = preview_id
            raw = self.cb.market_order(side, pid, **kw)

        elif intent.order_type == OrderType.LIMIT:
            raw = self.cb.create_limit_order(
                side, pid, base_size=intent.base_size, price=intent.limit_price,
                client_order_id=cid, time_in_force=intent.time_in_force,
                post_only=intent.post_only,
            )

        elif intent.order_type == OrderType.STOP_LIMIT:
            raw = self.cb.create_stop_limit_order(
                side, pid, base_size=intent.base_size,
                limit_price=intent.limit_price, stop_price=intent.stop_price,
                client_order_id=cid, time_in_force=intent.time_in_force,
                stop_direction=intent.stop_direction,
            )

        elif intent.order_type == OrderType.STOP_MARKET:
            raw = self.cb.create_stop_market_order(
                side, pid, base_size=intent.base_size, stop_price=intent.stop_price,
                client_order_id=cid, stop_direction=intent.stop_direction,
            )
        else:
            return OrderResult(success=False, error=f"Unknown order type: {intent.order_type}")

        order_id = raw.get("order_id", raw.get("id", ""))
        status_str = raw.get("status", "OPEN")
        try:
            status = OrderStatus(status_str)
        except ValueError:
            status = OrderStatus.OPEN

        fill_price = float(raw.get("average_filled_price", raw.get("avg_price", 0)))
        filled_size = float(raw.get("filled_size", raw.get("filled_base_size", 0)))
        fees = float(raw.get("total_fees", raw.get("fees", 0)))

        result = OrderResult(
            success=True,
            order_id=order_id,
            client_order_id=cid,
            status=status,
            fill_price=fill_price,
            filled_size=filled_size,
            fees=fees,
            raw=raw,
        )
        self._orders[cid] = result
        log.info(f"[EXEC] {side} {pid} {intent.order_type.value} size={intent.base_size} id={order_id} status={status.value}")
        return result

    def cancel(self, order_id: str) -> bool:
        try:
            self.cb.cancel_order(order_id)
            return True
        except Exception:
            return False

    def poll_status(self, order_id: str) -> Optional[OrderResult]:
        # First check local cache
        for cached_result in self._orders.values():
            if cached_result.order_id == order_id:
                return cached_result
        
        # Fallback to API polling
        try:
            cursor = None
            while True:
                args = ["orders", "list", f"order_status=ALL"]
                if cursor:
                    args.append(f"cursor={cursor}")
                resp = self.cb._cli_json(*args)
                orders = resp.get("orders", []) if isinstance(resp, dict) else []
                for o in orders:
                    if o.get("order_id") == order_id:
                        return self._parse_listed_order(o)
                if isinstance(resp, dict):
                    cursor = resp.get("cursor") or resp.get("next_cursor") or None
                    if not cursor:
                        break
                else:
                    break
            return None
        except Exception:
            return None

    @staticmethod
    def _parse_listed_order(o: dict) -> OrderResult:
        status_str = o.get("status", "OPEN")
        try:
            status = OrderStatus(status_str)
        except ValueError:
            status = OrderStatus.OPEN
        return OrderResult(
            success=True,
            order_id=o.get("order_id", ""),
            client_order_id=o.get("client_order_id", ""),
            status=status,
            fill_price=float(o.get("average_filled_price", 0)),
            filled_size=float(o.get("filled_size", 0)),
            fees=float(o.get("total_fees", o.get("fees", 0))),
            raw=o,
        )


class BracketManager:
    def __init__(self, engine: NativeExecutionEngine):
        self.engine = engine
        self._brackets: Dict[str, Dict[str, Any]] = {}
        self._stop_polling = threading.Event()

    def stop_polling(self) -> None:
        """Signal the poll_brackets loop to stop gracefully."""
        self._stop_polling.set()

    def place_bracket(
        self, product_id: str, side: str, base_size: float,
        entry_price: float, stop_price: float, target_price: float,
        strategy_id: str = "bracket",
    ) -> Dict[str, Any]:
        cid = str(uuid.uuid4())
        base = _fmt_base(base_size)

        entry_intent = OrderIntent(
            side=side, product_id=product_id,
            order_type=OrderType.MARKET,
            base_size=base, client_order_id=cid,
            metadata={"strategy": strategy_id, "type": "entry"},
        )
        entry_result = self.engine.place(entry_intent)

        actual_entry = entry_result.fill_price if entry_result.fill_price else entry_price

        # Validate stop/target sanity
        s = side.upper()
        if s == "BUY":
            if stop_price <= 0 or stop_price >= actual_entry:
                raise ValueError(f"BUY stop_price ({stop_price}) must be > 0 and < entry ({actual_entry})")
            if target_price <= 0 or target_price <= actual_entry:
                raise ValueError(f"BUY target_price ({target_price}) must be > entry ({actual_entry})")
        elif s == "SELL":
            if stop_price <= 0 or stop_price <= actual_entry:
                raise ValueError(f"SELL stop_price ({stop_price}) must be > entry ({actual_entry})")
            if target_price <= 0 or target_price >= actual_entry:
                raise ValueError(f"SELL target_price ({target_price}) must be < entry ({actual_entry})")
        else:
            raise ValueError(f"Invalid side: {side}")

        if base_size <= 0:
            raise ValueError(f"base_size must be > 0, got {base_size}")

        bracket_id = cid
        self._brackets[bracket_id] = {
            "product_id": product_id,
            "side": side,
            "base_size": base_size,
            "entry_price": actual_entry,
            "stop_price": stop_price,
            "target_price": target_price,
            "strategy_id": strategy_id,
            "status": "OPEN",
            "entry_order": entry_result,
            "stop_order_id": None,
            "target_order_id": None,
            "trailing_stop": None,
            "breakeven_set": False,
            "initial_stop_dist": abs(actual_entry - stop_price),
            "highest_price": actual_entry,
            "lowest_price": actual_entry,
            "timestamp": int(time.time()),
        }

        if not entry_result.success:
            self._brackets[bracket_id]["status"] = "FAILED"
            self._brackets[bracket_id]["entry_error"] = entry_result.error
        elif not self.engine.dry_run:
            self._place_stop_loss(bracket_id)
            self._place_take_profit(bracket_id)

        return self._brackets[bracket_id]

    def _place_stop_loss(self, bracket_id: str):
        b = self._brackets[bracket_id]
        side = "SELL" if b["side"].upper() == "BUY" else "BUY"
        stop_dir = "stop_direction_stop_down" if b["side"].upper() == "BUY" else "stop_direction_stop_up"
        stop_intent = OrderIntent(
            side=side, product_id=b["product_id"],
            order_type=OrderType.STOP_MARKET,
            base_size=_fmt_base(b["base_size"]),
            stop_price=_fmt_price(b["stop_price"]),
            stop_direction=stop_dir,
            metadata={"strategy": b["strategy_id"], "type": "stop_loss", "parent": bracket_id},
        )
        result = self.engine.place(stop_intent)
        if result.success:
            b["stop_order_id"] = result.order_id
        else:
            log.warning("Stop loss placement failed for bracket %s: %s", bracket_id, result.error)

    def _place_take_profit(self, bracket_id: str):
        b = self._brackets[bracket_id]
        side = "SELL" if b["side"].upper() == "BUY" else "BUY"
        limit_intent = OrderIntent(
            side=side, product_id=b["product_id"],
            order_type=OrderType.LIMIT,
            base_size=_fmt_base(b["base_size"]),
            limit_price=_fmt_price(b["target_price"]),
            time_in_force="GTC",
            metadata={"strategy": b["strategy_id"], "type": "take_profit", "parent": bracket_id},
        )
        result = self.engine.place(limit_intent)
        if result.success:
            b["target_order_id"] = result.order_id
        else:
            log.warning("Take-profit placement failed for bracket %s: %s", bracket_id, result.error)

    def poll_brackets(self, poll_secs: int = 5) -> None:
        while not self._stop_polling.is_set():
            for bid, b in list(self._brackets.items()):
                if b["status"] != "OPEN":
                    continue
                self._check_bracket_status(bid, b)
            if poll_secs <= 0:
                break
            # Wait for either timeout or stop signal
            self._stop_polling.wait(timeout=poll_secs)

    def active_brackets(self) -> Dict[str, Dict[str, Any]]:
        return {bid: b for bid, b in self._brackets.items() if b.get("status") == "OPEN"}

    def reconcile_open_brackets(self, stale_after_s: int = 15, force_flatten_after_s: int = 60) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        now = int(time.time())
        for bid, b in list(self._brackets.items()):
            if b.get("status") != "OPEN":
                continue
            age = now - int(b.get("timestamp", now))
            if self._check_bracket_status(bid, b):
                events.append({"bracket_id": bid, "event": "closed", "status": b.get("status"), "age_s": age})
                continue
            if age >= stale_after_s:
                self._cancel_bracket_orders(b)
            if age >= force_flatten_after_s:
                events.append(self.force_flatten_bracket(bid, reason=f"timeout_{age}s"))
        return events

    def _cancel_bracket_orders(self, b: dict) -> None:
        for key in ("stop_order_id", "target_order_id"):
            order_id = b.get(key)
            if not order_id:
                continue
            try:
                self.engine.cancel(order_id)
            except Exception:
                pass

    def force_flatten_bracket(self, bracket_id: str, reason: str = "forced_flatten") -> Dict[str, Any]:
        b = self._brackets.get(bracket_id)
        if not b:
            return {"bracket_id": bracket_id, "status": "MISSING"}
        if b.get("status") != "OPEN":
            return {"bracket_id": bracket_id, "status": b.get("status"), "reason": "not_open"}

        self._cancel_bracket_orders(b)
        flatten_side = "SELL" if b["side"].upper() == "BUY" else "BUY"
        close_intent = OrderIntent(
            side=flatten_side,
            product_id=b["product_id"],
            order_type=OrderType.MARKET,
            base_size=_fmt_base(b["base_size"]),
            metadata={"strategy": b["strategy_id"], "type": "forced_flatten", "parent": bracket_id},
        )
        result = self.engine.place(close_intent)
        if result.success:
            b["status"] = "CLOSED"
            b["exit_reason"] = reason
            b["exit_price"] = result.fill_price or b.get("entry_price", 0.0)
            b["exit_order_id"] = result.order_id
            return {
                "bracket_id": bracket_id,
                "status": "CLOSED",
                "reason": reason,
                "exit_price": b["exit_price"],
                "order_id": result.order_id,
            }
        b["status"] = "FAILED"
        b["exit_reason"] = "force_flatten_failed"
        b["flatten_error"] = result.error
        return {
            "bracket_id": bracket_id,
            "status": "FAILED",
            "reason": "force_flatten_failed",
            "error": result.error,
        }

    def update_trailing_stop(
        self,
        bracket_id: str,
        current_price: float,
        highest_price: float,
        lowest_price: float,
        initial_stop_dist: float,
        r_multiple: float,
        max_hold_s: float,
        age_s: float,
        regime: str = "unknown",
    ) -> bool:
        """Update trailing stop for an open bracket using paper-mode logic.
        
        Returns True if stop was updated (cancelled old + placed new).
        """
        b = self._brackets.get(bracket_id)
        if not b or b.get("status") != "OPEN":
            return False

        side = b["side"].upper()
        if side not in ("BUY", "SELL"):
            return False

        # Only works in live mode (dry_run=False means we can actually cancel/replace)
        if self.engine.dry_run:
            return False

        stop_order_id = b.get("stop_order_id")
        if not stop_order_id:
            return False

        # Compute new stop using paper logic
        vol_mult = 1.5 if regime == "high_volatility" else 1.0
        trailing_dist = initial_stop_dist * vol_mult
        new_stop = None

        if side == "BUY":
            # Long position trailing logic
            current_stop = highest_price - trailing_dist
            current_stop = max(current_stop, b.get("stop_price", 0.0) or 0.0)

            # Breakeven at 1.5R
            if r_multiple >= 1.5 and not b.get("breakeven_set", False):
                current_stop = max(current_stop, b["entry_price"])
                b["breakeven_set"] = True
                b["trailing_activated"] = True

            # Tight trail at 2.5R
            if r_multiple >= 2.5:
                tight_trail = initial_stop_dist * 0.8
                current_stop = max(current_stop, highest_price - tight_trail)
                b["trailing_activated"] = True

            # Age tightening
            age_ratio = age_s / max(max_hold_s, 1.0)
            age_tighten = 1.0
            if age_ratio >= 0.90:
                age_tighten = 0.2
            elif age_ratio >= 0.75:
                age_tighten = 0.4
            elif age_ratio >= 0.50:
                age_tighten = 0.6
            elif age_ratio >= 0.25:
                age_tighten = 0.8

            if age_tighten < 1.0:
                age_stop = highest_price - initial_stop_dist * age_tighten
                current_stop = max(current_stop, age_stop)

            new_stop = current_stop

            # Only tighten (move up), never loosen
            old_stop = b.get("stop_price", 0.0)
            if old_stop > 0 and new_stop <= old_stop:
                return False

        else:
            # Short position trailing logic
            current_stop = lowest_price + trailing_dist
            current_stop = min(current_stop, b.get("stop_price", float("inf")))

            # Breakeven at 1.5R
            if r_multiple >= 1.5 and not b.get("breakeven_set", False):
                current_stop = min(current_stop, b["entry_price"])
                b["breakeven_set"] = True
                b["trailing_activated"] = True

            # Age tightening
            age_ratio = age_s / max(max_hold_s, 1.0)
            age_tighten = 1.0
            if age_ratio >= 0.90:
                age_tighten = 0.2
            elif age_ratio >= 0.75:
                age_tighten = 0.4
            elif age_ratio >= 0.50:
                age_tighten = 0.6
            elif age_ratio >= 0.25:
                age_tighten = 0.8

            if age_tighten < 1.0:
                age_stop = lowest_price + initial_stop_dist * age_tighten
                current_stop = min(current_stop, age_stop)

            new_stop = current_stop

            # Only tighten (move down for shorts), never loosen
            old_stop = b.get("stop_price", 0.0)
            if old_stop > 0 and new_stop >= old_stop:
                return False

        # Place new stop order and cancel old one
        try:
            # Cancel old stop order
            if stop_order_id:
                self.engine.cancel(stop_order_id)
            
            # Place new stop order
            stop_side = "SELL" if side == "BUY" else "BUY"
            stop_dir = "stop_direction_stop_down" if side == "BUY" else "stop_direction_stop_up"
            stop_intent = OrderIntent(
                side=stop_side,
                product_id=b["product_id"],
                order_type=OrderType.STOP_MARKET,
                base_size=_fmt_base(b["base_size"]),
                stop_price=_fmt_price(new_stop),
                stop_direction=stop_dir,
                metadata={"strategy": b["strategy_id"], "type": "stop_loss", "parent": bracket_id, "trailing_update": True},
            )
            result = self.engine.place(stop_intent)
            
            if result.success:
                b["stop_order_id"] = result.order_id
                b["stop_price"] = new_stop
                log.info(f"[BRK-TRAIL] {b['product_id']} trailing stop updated: {old_stop:.4f} -> {new_stop:.4f} (r={r_multiple:.1f}, age_ratio={age_s/max(max_hold_s,1.0):.2f})")
                return True
            else:
                log.warning(f"[BRK-TRAIL] Failed to place new trailing stop for {b['product_id']}: {result.error}")
                return False
        except Exception as e:
            log.error(f"[BRK-TRAIL] Error updating trailing stop for {b['product_id']}: {e}")
            return False

    def _check_bracket_status(self, bracket_id: str, b: dict) -> bool:
        stop_filled = False
        target_filled = False
        
        if b.get("stop_order_id"):
            result = self.engine.poll_status(b["stop_order_id"])
            if result and result.status == OrderStatus.FILLED:
                stop_filled = True
                b["status"] = "CLOSED"
                b["exit_reason"] = "stop"
                b["exit_price"] = result.fill_price
                log.info(f"[BRK] {b['product_id']} stop filled @ {result.fill_price}")
        
        if b.get("target_order_id"):
            result = self.engine.poll_status(b["target_order_id"])
            if result and result.status == OrderStatus.FILLED:
                target_filled = True
                b["status"] = "CLOSED"
                b["exit_reason"] = "target"
                b["exit_price"] = result.fill_price
                log.info(f"[BRK] {b['product_id']} target filled @ {result.fill_price}")
        
        # If both filled in same tick, prefer target as the exit reason
        if stop_filled and target_filled:
            b["exit_reason"] = "target"
        
        return stop_filled or target_filled
