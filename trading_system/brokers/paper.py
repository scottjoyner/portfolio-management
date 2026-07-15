from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from apps.paper_exchange.engine import PaperExchangeEngine
from trading_system.brokers.base import (
    BrokerAccount, BrokerAdapter, BrokerFill, BrokerOrder, BrokerPosition, OrderStatus, TimeInForce,
)


class PaperBrokerAdapter(BrokerAdapter):
    def __init__(self, engine: PaperExchangeEngine | None = None) -> None:
        from apps.paper_exchange.engine import PaperExchangeEngine as PE
        self._engine = engine or PE(starting_cash=Decimal("100000"), products=[])

    def broker_name(self) -> str:
        return "paper"

    @property
    def engine(self) -> PaperExchangeEngine:
        return self._engine

    async def get_accounts(self) -> list[BrokerAccount]:
        total = self._engine.cash
        return [
            BrokerAccount(
                account_id="paper-001",
                name="Paper Wallet",
                currency="USD",
                available_balance=total,
                total_balance=total,
                buying_power=total,
            )
        ]

    async def get_account(self, account_id: str) -> BrokerAccount:
        return (await self.get_accounts())[0]

    async def preview_order(self, order: BrokerOrder) -> tuple[bool, str]:
        if order.size <= 0:
            return False, "size must be positive"
        return True, "preview passed"

    async def submit_order(self, order: BrokerOrder) -> BrokerOrder:
        pe_order = self._engine.place_order(
            strategy_id=order.client_order_id,
            portfolio_id=order.account_id,
            product_id=order.product_id,
            side=order.side,
            order_type=order.order_type,
            size=order.size,
            limit_price=order.price,
        )
        order.broker_order_id = pe_order.order_id
        order.status = OrderStatus.OPEN
        order.created_at = pe_order.created_at
        return order

    async def cancel_order(self, broker_order_id: str) -> bool:
        return self._engine.cancel_order(broker_order_id)

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        po = self._engine.orders.get(broker_order_id)
        if not po:
            return None
        return BrokerOrder(
            broker_order_id=po.order_id,
            client_order_id=po.strategy_id,
            account_id=po.portfolio_id,
            product_id=po.product_id,
            side=po.side,
            order_type=po.order_type,
            size=po.size,
            price=po.price,
            status=OrderStatus(po.status) if po.status else OrderStatus.OPEN,
            filled_size=po.filled_size,
            remaining_size=po.remaining_size,
            fee=po.fee if hasattr(po, 'fee') else Decimal("0"),
            created_at=po.created_at,
        )

    async def list_orders(
        self, product_id: str | None = None, status: OrderStatus | None = None,
    ) -> list[BrokerOrder]:
        results = []
        for oid, po in self._engine.orders.items():
            if product_id and po.product_id != product_id:
                continue
            if status and po.status != status.value:
                continue
            results.append(await self.get_order(oid))  # type: ignore[misc]
        return results

    async def get_fills(self, broker_order_id: str) -> list[BrokerFill]:
        return [
            BrokerFill(
                fill_id=pf.fill_id,
                broker_order_id=pf.order_id,
                product_id=pf.product_id,
                side=pf.side or "buy",
                size=pf.size,
                price=pf.price,
                notional=pf.size * pf.price,
                fee=pf.fee,
                filled_at=datetime.utcnow(),
            )
            for pf in self._engine.fills
            if pf.order_id == broker_order_id
        ]

    async def get_positions(self, product_id: str | None = None) -> list[BrokerPosition]:
        return [
            BrokerPosition(
                product_id=p.product_id,
                side=p.side if hasattr(p, 'side') else "long",
                size=p.size,
                entry_price=p.cost_basis,
                current_price=self._engine.mid_prices.get(p.product_id, p.cost_basis),
                unrealized_pnl=p.unrealized_pnl if hasattr(p, 'unrealized_pnl') else Decimal("0"),
                realized_pnl=p.realized_pnl if hasattr(p, 'realized_pnl') else Decimal("0"),
            )
            for pid, p in self._engine.positions.items()
            if not product_id or pid == product_id
        ]

    async def list_products(self) -> list[dict[str, Any]]:
        return [
            {"product_id": pid, "base_currency": pid.split("-")[0], "quote_currency": pid.split("-")[1]}
            for pid in self._engine.products
        ]

    async def get_product(self, product_id: str) -> dict[str, Any] | None:
        if product_id in self._engine.products:
            price = self._engine.mid_prices.get(product_id, Decimal("0"))
            return {"product_id": product_id, "price": float(price)}
        return None

    async def get_market_price(self, product_id: str) -> Decimal | None:
        return self._engine.mid_prices.get(product_id)

    async def health_check(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "broker": "paper",
            "order_count": len(self._engine.orders),
            "position_count": len(self._engine.positions),
            "capital": float(self._engine.cash),
        }
