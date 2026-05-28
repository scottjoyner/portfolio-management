from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import uuid4

from execution.queue_model.models import SimpleQueueModel
from core.events.ws_hub import hub


@dataclass
class PaperOrder:
    order_id: str
    strategy_id: str
    portfolio_id: str
    product_id: str
    side: str
    order_type: str
    size: Decimal
    price: Decimal | None
    status: str = "pending"
    filled_size: Decimal = Decimal("0")
    filled_value: Decimal = Decimal("0")
    remaining_size: Decimal = Decimal("0")
    avg_fill_price: Decimal = Decimal("0")
    created_at: float = 0.0
    filled_at: float | None = None
    fee: Decimal = Decimal("0")
    slippage_bps: float = 0.0


@dataclass
class PaperFill:
    fill_id: str
    order_id: str
    product_id: str
    side: str
    size: Decimal
    price: Decimal
    fee: Decimal
    liquidity: str
    created_at: float


@dataclass
class PaperPosition:
    product_id: str
    size: Decimal = Decimal("0")
    cost_basis: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")


class PaperExchangeEngine:
    FEE_RATE_MAKER = Decimal("0.0004")
    FEE_RATE_TAKER = Decimal("0.0008")

    def __init__(self, starting_cash: Decimal = Decimal("10000"), products: list[str] | None = None) -> None:
        self.cash: Decimal = starting_cash
        self.products: list[str] = products or ["BTC-USD"]
        self.orders: dict[str, PaperOrder] = {}
        self.fills: list[PaperFill] = []
        self.positions: dict[str, PaperPosition] = {}
        self.spreads: dict[str, Decimal] = {}
        self.mid_prices: dict[str, Decimal] = {}
        self.queue_model = SimpleQueueModel()
        self._running = False

        for p in self.products:
            self.positions[p] = PaperPosition(product_id=p)

    def set_market_price(self, product_id: str, mid: Decimal, spread_bps: Decimal = Decimal("5")) -> None:
        self.mid_prices[product_id] = mid
        self.spreads[product_id] = spread_bps

    def _generate_order_id(self) -> str:
        return f"paper-{uuid4().hex[:12]}"

    def _generate_fill_id(self) -> str:
        return f"fill-{uuid4().hex[:12]}"

    def place_order(self, strategy_id: str, portfolio_id: str, product_id: str, side: str, order_type: str, size: Decimal, price: Decimal | None = None, limit_price: Decimal | None = None) -> PaperOrder:
        order = PaperOrder(
            order_id=self._generate_order_id(),
            strategy_id=strategy_id,
            portfolio_id=portfolio_id,
            product_id=product_id,
            side=side,
            order_type=order_type,
            size=size,
            price=limit_price or price,
            remaining_size=size,
            status="open",
            created_at=time.time(),
        )
        self.orders[order.order_id] = order

        if order_type == "market":
            self._fill_market(order)
        else:
            self._queue_limit(order)

        hub.publish_sync("orders", {
            "event": "order_placed",
            "order_id": order.order_id,
            "product_id": product_id,
            "side": side,
            "order_type": order_type,
            "size": str(size),
            "strategy_id": strategy_id,
        })

        return order

    def _fill_market(self, order: PaperOrder) -> None:
        mid = self.mid_prices.get(order.product_id, Decimal("100"))
        spread_bps = self.spreads.get(order.product_id, Decimal("5"))
        offset = spread_bps / Decimal("2") / Decimal("10000")
        exec_price = mid * (Decimal("1") + offset if order.side == "buy" else Decimal("1") - offset)
        fee_rate = self.FEE_RATE_TAKER
        notional = order.size * exec_price
        fee = notional * fee_rate
        order.fee = fee
        order.slippage_bps = float(spread_bps) / 2.0

        self._apply_fill(order, exec_price, "taker")

    def _quote_limit(self, product_id: str, side: str) -> tuple[Decimal, Decimal]:
        mid = self.mid_prices.get(product_id, Decimal("100"))
        spread_bps = self.spreads.get(product_id, Decimal("5"))
        half_spread = spread_bps / Decimal("2") / Decimal("10000")
        if side == "buy":
            return mid * (Decimal("1") - half_spread), mid * (Decimal("1") + half_spread)
        return mid * (Decimal("1") + half_spread), mid * (Decimal("1") - half_spread)

    def _queue_limit(self, order: PaperOrder) -> None:
        bid, ask = self._quote_limit(order.product_id, order.side)
        limit_px = bid if order.side == "buy" else ask
        if order.price is not None:
            if (order.side == "buy" and order.price < limit_px) or (order.side == "sell" and order.price > limit_px):
                limit_px = order.price
        order.price = limit_px

        queue_ahead = 2.5
        trade_rate = 0.8
        cancel_rate = 0.3
        estimate = self.queue_model.estimate(queue_ahead, trade_rate, cancel_rate, float(order.size))

        if estimate.fill_probability > 0.3:
            time_to_fill_ms = estimate.expected_queue_time_ms
            fill_ratio = min(1.0, estimate.fill_probability)
            fill_size = order.size * Decimal(str(fill_ratio))
            fee = fill_size * limit_px * self.FEE_RATE_MAKER
            slippage = estimate.adverse_selection_bps

            order.fee = fee
            order.slippage_bps = slippage
            order.filled_at = time.time() + time_to_fill_ms / 1000.0
            self._apply_fill(order, limit_px, "maker", fill_size=fill_size)
        else:
            order.status = "cancelled"

    def _apply_fill(self, order: PaperOrder, price: Decimal, liquidity: str, fill_size: Decimal | None = None) -> None:
        fill_qty = fill_size or order.size
        notional = fill_qty * price

        if order.side == "buy":
            self.cash -= notional + order.fee
            pos = self.positions[order.product_id]
            pos.cost_basis = ((pos.cost_basis * pos.size) + notional) / (pos.size + fill_qty) if pos.size + fill_qty > 0 else price
            pos.size += fill_qty
        else:
            self.cash += notional - order.fee
            pos = self.positions[order.product_id]
            avg_cost = fill_qty * pos.cost_basis
            pos.realized_pnl += notional - avg_cost
            pos.size -= fill_qty

        total_filled = order.filled_size + fill_qty
        order.avg_fill_price = ((order.avg_fill_price * order.filled_size) + (price * fill_qty)) / total_filled if total_filled > 0 else price
        order.filled_size = total_filled
        order.filled_value += notional
        order.remaining_size = order.size - total_filled
        order.status = "filled" if order.remaining_size <= 0 else "partially_filled"

        fill = PaperFill(
            fill_id=self._generate_fill_id(),
            order_id=order.order_id,
            product_id=order.product_id,
            side=order.side,
            size=fill_qty,
            price=price,
            fee=order.fee,
            liquidity=liquidity,
            created_at=time.time(),
        )
        self.fills.append(fill)

        hub.publish_sync("orders", {
            "event": "order_filled",
            "order_id": order.order_id,
            "fill_id": fill.fill_id,
            "product_id": order.product_id,
            "side": order.side,
            "size": str(fill_qty),
            "price": str(price),
            "liquidity": liquidity,
            "fee": str(order.fee),
        })

    def cancel_order(self, order_id: str) -> bool:
        order = self.orders.get(order_id)
        if order and order.status in ("open", "pending"):
            order.status = "cancelled"
            return True
        return False

    def get_portfolio_summary(self) -> dict[str, Any]:
        total_equity = self.cash
        for pos in self.positions.values():
            mid = self.mid_prices.get(pos.product_id, Decimal("0"))
            pos_value = pos.size * mid
            total_equity += pos_value

        return {
            "cash": float(self.cash),
            "positions": {pid: float(p.size) for pid, p in self.positions.items()},
            "total_equity": float(total_equity),
            "open_orders": len([o for o in self.orders.values() if o.status == "open"]),
            "total_fills": len(self.fills),
        }

    def get_open_orders(self) -> list[PaperOrder]:
        return [o for o in self.orders.values() if o.status == "open"]

    async def run(self, config: dict[str, Any] | None = None) -> None:
        self._running = True
        while self._running:
            await asyncio.sleep(0.1)

    def stop(self) -> None:
        self._running = False
