from __future__ import annotations
import time
import uuid
import logging
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
                    kw["quote_size"] = intent.quote_size or _fmt_quote(
                        float(intent.base_size) * 50000)
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
                    kw["quote_size"] = _fmt_quote(float(intent.base_size) * 50000)
            else:
                kw["base_size"] = intent.base_size
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

        result = OrderResult(
            success=True,
            order_id=order_id,
            client_order_id=cid,
            status=status,
            fill_price=fill_price,
            filled_size=filled_size,
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
        try:
            orders = self.cb.list_orders(status="OPEN")
            for o in orders:
                if o.get("order_id") == order_id:
                    return self._parse_listed_order(o)
            return None
        except Exception:
            return None

    @staticmethod
    def _parse_listed_order(o: dict) -> OrderResult:
        return OrderResult(
            success=True,
            order_id=o.get("order_id", ""),
            client_order_id=o.get("client_order_id", ""),
            status=OrderStatus(o.get("status", "OPEN")),
            fill_price=float(o.get("average_filled_price", 0)),
            filled_size=float(o.get("filled_size", 0)),
            raw=o,
        )


class BracketManager:
    def __init__(self, engine: NativeExecutionEngine):
        self.engine = engine
        self._brackets: Dict[str, Dict[str, Any]] = {}

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

        bracket_id = cid
        self._brackets[bracket_id] = {
            "product_id": product_id,
            "side": side,
            "base_size": base_size,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "strategy_id": strategy_id,
            "status": "OPEN",
            "entry_order": entry_result,
            "stop_order_id": None,
            "target_order_id": None,
            "trailing_stop": None,
            "breakeven_set": False,
            "timestamp": int(time.time()),
        }

        if not self.engine.dry_run and entry_result.success:
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
        b["stop_order_id"] = result.order_id

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
        b["target_order_id"] = result.order_id

    def poll_brackets(self, poll_secs: int = 5) -> None:
        while True:
            for bid, b in list(self._brackets.items()):
                if b["status"] != "OPEN":
                    continue
                self._check_bracket_status(bid, b)
            if poll_secs <= 0:
                break
            time.sleep(poll_secs)

    def _check_bracket_status(self, bracket_id: str, b: dict):
        if b.get("stop_order_id"):
            result = self.engine.poll_status(b["stop_order_id"])
            if result and result.status == OrderStatus.FILLED:
                b["status"] = "CLOSED"
                b["exit_reason"] = "stop"
                b["exit_price"] = result.fill_price
                log.info(f"[BRK] {b['product_id']} stop filled @ {result.fill_price}")
                return
        if b.get("target_order_id"):
            result = self.engine.poll_status(b["target_order_id"])
            if result and result.status == OrderStatus.FILLED:
                b["status"] = "CLOSED"
                b["exit_reason"] = "target"
                b["exit_price"] = result.fill_price
                log.info(f"[BRK] {b['product_id']} target filled @ {result.fill_price}")
                return
