from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import uuid4

from execution.queue_model.models import SimpleQueueModel
from core.events.ws_hub import hub


def _decimal(value: Decimal | str | int | float) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


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
    fee: Decimal = Decimal("0")
    liquidity: str = "taker"
    created_at: float = 0.0


@dataclass
class PaperPosition:
    product_id: str
    size: Decimal = Decimal("0")
    cost_basis: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")

    @property
    def side(self) -> str:
        if self.size > 0:
            return "long"
        if self.size < 0:
            return "short"
        return "flat"


class PaperExchangeEngine:
    FEE_RATE_MAKER = Decimal("0.0004")
    FEE_RATE_TAKER = Decimal("0.0008")

    def __init__(
        self,
        starting_cash: Decimal | str | int | float = Decimal("10000"),
        products: list[str] | None = None,
    ) -> None:
        self.cash = _decimal(starting_cash)
        self.products = list(products) if products is not None else ["BTC-USD"]
        self.orders: dict[str, PaperOrder] = {}
        self.fills: list[PaperFill] = []
        self.positions: dict[str, PaperPosition] = {}
        self.spreads: dict[str, Decimal] = {}
        self.mid_prices: dict[str, Decimal] = {}
        self.queue_model = SimpleQueueModel()
        self._running = False

        for product_id in self.products:
            self.positions[product_id] = PaperPosition(product_id=product_id)

    def set_market_price(
        self,
        product_id: str,
        mid: Decimal | str | int | float,
        spread_bps: Decimal | str | int | float = Decimal("5"),
    ) -> None:
        if product_id not in self.products:
            self.products.append(product_id)
        self.positions.setdefault(product_id, PaperPosition(product_id=product_id))
        self.mid_prices[product_id] = _decimal(mid)
        self.spreads[product_id] = _decimal(spread_bps)

    def _generate_order_id(self) -> str:
        return f"paper-{uuid4().hex[:12]}"

    def _generate_fill_id(self) -> str:
        return f"fill-{uuid4().hex[:12]}"

    def place_order(
        self,
        strategy_id: str,
        portfolio_id: str,
        product_id: str,
        side: str,
        order_type: str,
        size: Decimal | str | int | float,
        price: Decimal | str | int | float | None = None,
        limit_price: Decimal | str | int | float | None = None,
    ) -> PaperOrder:
        normalized_size = _decimal(size)
        if normalized_size <= 0:
            raise ValueError("size must be positive")
        normalized_side = side.lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        normalized_type = order_type.lower()
        if normalized_type not in {"market", "limit"}:
            raise ValueError("order_type must be market or limit")

        selected_price = limit_price if limit_price is not None else price
        normalized_price = _decimal(selected_price) if selected_price is not None else None
        if product_id not in self.products:
            self.products.append(product_id)
        self.positions.setdefault(product_id, PaperPosition(product_id=product_id))

        order = PaperOrder(
            order_id=self._generate_order_id(),
            strategy_id=strategy_id,
            portfolio_id=portfolio_id,
            product_id=product_id,
            side=normalized_side,
            order_type=normalized_type,
            size=normalized_size,
            price=normalized_price,
            remaining_size=normalized_size,
            status="open",
            created_at=time.time(),
        )
        self.orders[order.order_id] = order

        if normalized_type == "market":
            self._fill_market(order)
        else:
            self._queue_limit(order)

        hub.publish_sync(
            "orders",
            {
                "event": "order_placed",
                "order_id": order.order_id,
                "product_id": product_id,
                "side": normalized_side,
                "order_type": normalized_type,
                "size": str(normalized_size),
                "strategy_id": strategy_id,
            },
        )
        return order

    def _fill_market(self, order: PaperOrder) -> None:
        mid = self.mid_prices.get(order.product_id, Decimal("100"))
        spread_bps = self.spreads.get(order.product_id, Decimal("5"))
        offset = spread_bps / Decimal("2") / Decimal("10000")
        exec_price = mid * (
            Decimal("1") + offset if order.side == "buy" else Decimal("1") - offset
        )
        notional = order.size * exec_price
        order.fee = notional * self.FEE_RATE_TAKER
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
        limit_price = bid if order.side == "buy" else ask
        if order.price is not None:
            if (
                order.side == "buy" and order.price < limit_price
            ) or (
                order.side == "sell" and order.price > limit_price
            ):
                limit_price = order.price
        order.price = limit_price

        estimate = self.queue_model.estimate(2.5, 0.8, 0.3, float(order.size))
        if estimate.fill_probability > 0.3:
            fill_ratio = min(1.0, estimate.fill_probability)
            fill_size = order.size * Decimal(str(fill_ratio))
            order.fee = fill_size * limit_price * self.FEE_RATE_MAKER
            order.slippage_bps = estimate.adverse_selection_bps
            order.filled_at = time.time() + estimate.expected_queue_time_ms / 1000.0
            self._apply_fill(order, limit_price, "maker", fill_size=fill_size)
        else:
            order.status = "cancelled"

    def _apply_fill(
        self,
        order: PaperOrder,
        price: Decimal,
        liquidity: str,
        fill_size: Decimal | None = None,
    ) -> None:
        fill_quantity = fill_size or order.size
        notional = fill_quantity * price
        position = self.positions.setdefault(
            order.product_id,
            PaperPosition(product_id=order.product_id),
        )

        if order.side == "buy":
            self.cash -= notional + order.fee
            new_size = position.size + fill_quantity
            position.cost_basis = (
                ((position.cost_basis * position.size) + notional) / new_size
                if new_size > 0
                else price
            )
            position.size = new_size
        else:
            self.cash += notional - order.fee
            average_cost = fill_quantity * position.cost_basis
            position.realized_pnl += notional - average_cost
            position.size -= fill_quantity

        total_filled = order.filled_size + fill_quantity
        order.avg_fill_price = (
            ((order.avg_fill_price * order.filled_size) + (price * fill_quantity)) / total_filled
            if total_filled > 0
            else price
        )
        order.filled_size = total_filled
        order.filled_value += notional
        order.remaining_size = order.size - total_filled
        order.status = "filled" if order.remaining_size <= 0 else "partially_filled"

        current_mid = self.mid_prices.get(order.product_id, price)
        position.unrealized_pnl = (current_mid - position.cost_basis) * position.size
        fill = PaperFill(
            fill_id=self._generate_fill_id(),
            order_id=order.order_id,
            product_id=order.product_id,
            side=order.side,
            size=fill_quantity,
            price=price,
            fee=order.fee,
            liquidity=liquidity,
            created_at=time.time(),
        )
        self.fills.append(fill)

        hub.publish_sync(
            "orders",
            {
                "event": "order_filled",
                "order_id": order.order_id,
                "fill_id": fill.fill_id,
                "product_id": order.product_id,
                "side": order.side,
                "size": str(fill_quantity),
                "price": str(price),
                "liquidity": liquidity,
                "fee": str(order.fee),
            },
        )

    def cancel_order(self, order_id: str) -> bool:
        order = self.orders.get(order_id)
        if order and order.status in {"open", "pending"}:
            order.status = "cancelled"
            return True
        return False

    def get_portfolio_summary(self) -> dict[str, Any]:
        total_equity = self.cash
        for position in self.positions.values():
            mid = self.mid_prices.get(position.product_id, Decimal("0"))
            total_equity += position.size * mid
        return {
            "cash": float(self.cash),
            "positions": {product_id: float(position.size) for product_id, position in self.positions.items()},
            "total_equity": float(total_equity),
            "open_orders": sum(order.status == "open" for order in self.orders.values()),
            "total_fills": len(self.fills),
        }

    def get_open_orders(self) -> list[PaperOrder]:
        return [order for order in self.orders.values() if order.status == "open"]

    async def run(self, config: dict[str, Any] | None = None) -> None:
        self._running = True
        while self._running:
            await asyncio.sleep(0.1)

    def stop(self) -> None:
        self._running = False
