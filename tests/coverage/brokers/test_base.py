import asyncio
from decimal import Decimal

from trading_system.brokers.base import (
    BrokerAccount,
    BrokerAdapter,
    BrokerFill,
    BrokerOrder,
    BrokerPosition,
    OrderStatus,
)


class _ConcreteAdapter(BrokerAdapter):
    def broker_name(self) -> str:
        return "concrete"

    async def get_accounts(self):
        return [BrokerAccount("a", "n", "USD", Decimal("1"))]

    async def get_account(self, account_id):
        return BrokerAccount(account_id, "n", "USD", Decimal("1"))

    async def preview_order(self, order):
        return True, "ok"

    async def submit_order(self, order):
        return order

    async def cancel_order(self, broker_order_id):
        return True

    async def get_order(self, broker_order_id):
        return None

    async def list_orders(self, product_id=None, status=None):
        return []

    async def get_fills(self, broker_order_id):
        return []

    async def get_positions(self, product_id=None):
        return []

    async def list_products(self):
        return []

    async def get_product(self, product_id):
        return None


def test_enums_values():
    assert OrderStatus.FILLED.value == "filled"
    assert OrderStatus.PARTIALLY_FILLED.value == "partially_filled"
    assert BrokerOrder(
        broker_order_id="b",
        client_order_id="c",
        account_id="a",
        product_id="BTC-USD",
        side="buy",
        order_type="limit",
        size=Decimal("1"),
    ).status == OrderStatus.PENDING


def test_dataclass_defaults():
    order = BrokerOrder("b", "c", "a", "BTC-USD", "buy", "limit", Decimal("1"))
    assert order.filled_size == Decimal("0")
    assert order.fee == Decimal("0")
    assert order.extra == {}


def test_base_default_market_price_and_health():
    adapter = _ConcreteAdapter()

    async def exercise():
        assert await adapter.get_market_price("BTC-USD") is None
        assert await adapter.health_check() == {"status": "unknown"}

    asyncio.run(exercise())


def test_broker_fill_and_position_dataclasses():
    fill = BrokerFill("f1", "o1", "BTC-USD", "buy", Decimal("1"), Decimal("100"), Decimal("100"))
    assert fill.notional == Decimal("100")
    assert fill.liquidity == "TAKER"
    position = BrokerPosition("BTC-USD", "long", Decimal("1"), Decimal("100"))
    assert position.unrealized_pnl == Decimal("0")
    assert position.realized_pnl == Decimal("0")
