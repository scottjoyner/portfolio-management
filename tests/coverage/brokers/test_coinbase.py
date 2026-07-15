from decimal import Decimal

import pytest
from unittest.mock import AsyncMock

from trading_system.brokers.base import BrokerOrder, OrderStatus
from trading_system.brokers.coinbase import CoinbaseBrokerAdapter


def make_order(size=1, price=Decimal("100"), product_id="BTC-USD", side="buy",
               account_id="acc1", client_order_id="c1", order_type="limit"):
    return BrokerOrder(
        broker_order_id="b1", client_order_id=client_order_id, account_id=account_id,
        product_id=product_id, side=side, order_type=order_type, size=Decimal(str(size)),
        price=price,
    )


class FakeClient:
    def __init__(self, accounts=None, products=None, order=None, fills=None,
                 positions=None, single=None, ticker=None):
        self._accounts = accounts or []
        self._products = products or [{"product_id": "BTC-USD", "base_increment": "0.00000001",
                                        "quote_increment": "0.01", "price": "100"}]
        self._order = order or {"order_id": "o1"}
        self._fills = fills or []
        self._positions = positions or []
        self._single = single or {"product_id": "BTC-USD", "price": "100"}
        self._ticker = ticker or {"product_id": "BTC-USD", "price": "105"}
        self.create_order = AsyncMock(return_value=self._order)
        self.cancel_order = AsyncMock(return_value=True)
        self.get_product = AsyncMock(return_value=self._single)
        self.get_product_ticker = AsyncMock(return_value=self._ticker)
        self.get_accounts = AsyncMock(return_value=self._accounts)
        self.get_account = AsyncMock(return_value=self._single)
        self.list_products = AsyncMock(return_value=self._products)
        self.get_order = AsyncMock(return_value=self._single)
        self.list_orders = AsyncMock(return_value=[self._order])
        self.get_fills = AsyncMock(return_value=self._fills)
        self.get_positions = AsyncMock(return_value=self._positions)

    async def get_product(self, pid):
        return self._single


@pytest.fixture
def adapter():
    return CoinbaseBrokerAdapter(client=FakeClient())


def test_broker_name(adapter):
    assert adapter.broker_name() == "coinbase"


@pytest.mark.asyncio
async def test_get_accounts_list_and_dict(adapter):
    a = CoinbaseBrokerAdapter(client=FakeClient(accounts=[
        {"uuid": "u1", "name": "a", "currency": "USD", "available_balance": {"value": "10"},
         "hold": {"value": "2"}},
    ]))
    accs = await a.get_accounts()
    assert accs[0].account_id == "u1"
    assert accs[0].total_balance == Decimal("12")

    b = CoinbaseBrokerAdapter(client=FakeClient(accounts={"accounts": [
        {"id": "i1", "name": "b", "currency": "USD", "available_balance": {"value": "5"},
         "hold": {"value": "1"}},
    ]}))
    accs2 = await b.get_accounts()
    assert accs2[0].account_id == "i1"


@pytest.mark.asyncio
async def test_get_account_found_and_missing(adapter):
    a = CoinbaseBrokerAdapter(client=FakeClient(accounts=[
        {"uuid": "u1", "name": "a", "currency": "USD", "available_balance": {"value": "10"}, "hold": {"value": "0"}},
    ]))
    acc = await a.get_account("u1")
    assert acc.account_id == "u1"
    with pytest.raises(ValueError):
        await a.get_account("nope")


@pytest.mark.asyncio
async def test_preview_order_branches():
    a = CoinbaseBrokerAdapter(client=FakeClient(single={
        "product_id": "BTC-USD", "base_increment": "0.0003", "quote_increment": "0.01"}))
    assert (await a.preview_order(make_order(size=0))) == (False, "size must be positive")
    assert (await a.preview_order(make_order(size=Decimal("0.0001")))) == (
        False, "size must be multiple of 0.0003")
    assert (await a.preview_order(make_order(size=Decimal("0.0003"), price=Decimal("0.005")))) == (
        False, "price must be multiple of 0.01")
    assert (await a.preview_order(make_order(size=Decimal("0.0003"), price=Decimal("100")))) == (
        True, "preview passed")


@pytest.mark.asyncio
async def test_submit_order(adapter):
    order = make_order(size=1, price=Decimal("100"))
    res = await adapter.submit_order(order)
    assert res.broker_order_id == "o1"
    assert res.status == OrderStatus.OPEN
    adapter._client.create_order.assert_awaited()


@pytest.mark.asyncio
async def test_cancel_order(adapter):
    assert await adapter.cancel_order("o1") is True
    adapter._client.cancel_order.assert_awaited_with("o1")


@pytest.mark.asyncio
async def test_get_order_none_and_mapped():
    cli = FakeClient(single={"order_id": "o9", "product_id": "BTC-USD",
                             "side": "BUY", "status": "filled", "size": "2",
                             "price": "100"})
    cli.get_order = AsyncMock(side_effect=lambda oid: None if oid == "x" else cli._single)
    a = CoinbaseBrokerAdapter(client=cli)
    assert await a.get_order("x") is None
    o = await a.get_order("o9")
    assert o.broker_order_id == "o9"
    assert o.status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_list_orders_filters():
    a = CoinbaseBrokerAdapter(client=FakeClient())
    res = await a.list_orders(product_id="BTC-USD", status=OrderStatus.OPEN)
    assert isinstance(res, list)
    a._client.list_orders.assert_awaited_with(product_id="BTC-USD", order_status="OPEN")


@pytest.mark.asyncio
async def test_get_fills():
    a = CoinbaseBrokerAdapter(client=FakeClient(fills=[
        {"fill_id": "f1", "product_id": "BTC-USD", "side": "BUY", "size": "1", "price": "100",
         "fee": "0.1", "timestamp": "2024-01-01T00:00:00Z"},
    ]))
    fills = await a.get_fills("o1")
    assert fills[0].fill_id == "f1"
    assert fills[0].notional == Decimal("100")


@pytest.mark.asyncio
async def test_get_positions_long_short():
    a = CoinbaseBrokerAdapter(client=FakeClient(positions=[
        {"product_id": "BTC-USD", "position_size": "2", "entry_price": "100", "current_price": "110",
         "unrealized_pnl": "20", "realized_pnl": "0"},
        {"product_id": "ETH-USD", "position_size": "-1", "entry_price": "50", "current_price": "40",
         "unrealized_pnl": "10", "realized_pnl": "1"},
    ]))
    positions = await a.get_positions()
    sides = {p.product_id: p.side for p in positions}
    assert sides["BTC-USD"] == "long"
    assert sides["ETH-USD"] == "short"


@pytest.mark.asyncio
async def test_list_products_and_capability():
    a = CoinbaseBrokerAdapter(client=FakeClient(products=[
        {"product_id": "BTC-USD", "base_increment": "0.00000001", "quote_increment": "0.01"},
    ]))
    prods = await a.list_products()
    assert prods[0]["product_id"] == "BTC-USD"
    assert a.get_capability("BTC-USD")["product_id"] == "BTC-USD"


@pytest.mark.asyncio
async def test_get_product_and_market_price():
    a = CoinbaseBrokerAdapter(client=FakeClient(single={"product_id": "BTC-USD", "price": "100"}))
    assert (await a.get_product("BTC-USD"))["price"] == "100"
    # product has price
    assert await a.get_market_price("BTC-USD") == Decimal("100")
    # product no price -> ticker
    a2 = CoinbaseBrokerAdapter(client=FakeClient(single={"product_id": "BTC-USD"}))
    assert await a2.get_market_price("BTC-USD") == Decimal("105")
    # neither -> None
    a3 = CoinbaseBrokerAdapter(client=FakeClient(single={"product_id": "BTC-USD"},
                                                  ticker={"product_id": "BTC-USD"}))
    assert await a3.get_market_price("BTC-USD") is None


@pytest.mark.asyncio
async def test_health_check_healthy_and_unhealthy():
    a = CoinbaseBrokerAdapter(client=FakeClient(products=[{"product_id": "BTC-USD"}]))
    assert (await a.health_check())["status"] == "healthy"
    bad = CoinbaseBrokerAdapter(client=FakeClient())
    bad._client.list_products = AsyncMock(side_effect=RuntimeError("boom"))
    h = await bad.health_check()
    assert h["status"] == "unhealthy"


@pytest.mark.asyncio
async def test_get_exchange_capability_matrix():
    a = CoinbaseBrokerAdapter(client=FakeClient(products=[
        {"product_id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD",
         "base_increment": "0.00000001", "quote_increment": "0.01", "base_min_size": "0.0001",
         "base_max_size": "1000", "quote_decimals": 2, "supported_order_types": ["limit"],
         "maker_fee_rate": "0.001", "taker_fee_rate": "0.002", "trading_disabled": False},
    ]))
    matrix = await a.get_exchange_capability_matrix()
    assert matrix[0]["status"] == "active"
    assert matrix[0]["fees"]["maker_rate"] == "0.001"


def test_build_order_payload():
    a = CoinbaseBrokerAdapter(client=FakeClient())
    payload = a._build_order_payload(make_order(size=Decimal("1"), price=Decimal("100")))
    assert payload["side"] == "BUY"
    assert payload["order_configuration"]["limit_limit_gtc"]["base_size"] == "1"


def test_raw_to_order_mapping():
    a = CoinbaseBrokerAdapter(client=FakeClient())
    raw = {"order_id": "o1", "client_order_id": "c1", "account_id": "acc", "product_id": "BTC-USD",
           "side": "BUY", "type": "limit", "size": "2", "limit_price": "100", "status": "filled",
           "filled_size": "2", "fees": "0.1", "created_at": "2024-01-01T00:00:00Z"}
    o = a._raw_to_order(raw)
    assert o.broker_order_id == "o1"
    assert o.status == OrderStatus.FILLED
    # status None -> OPEN, price None -> None
    raw2 = {"order_id": "o2", "product_id": "BTC-USD", "side": "buy", "size": "1"}
    o2 = a._raw_to_order(raw2)
    assert o2.status == OrderStatus.OPEN
    assert o2.price is None


@pytest.mark.asyncio
async def test_list_orders_no_filter():
    a = CoinbaseBrokerAdapter(client=FakeClient())
    res = await a.list_orders()
    assert isinstance(res, list)


@pytest.mark.asyncio
async def test_get_positions_filtered():
    a = CoinbaseBrokerAdapter(client=FakeClient(positions=[
        {"product_id": "BTC-USD", "position_size": "2", "entry_price": "100", "current_price": "110",
         "unrealized_pnl": "20", "realized_pnl": "0"},
        {"product_id": "ETH-USD", "position_size": "1", "entry_price": "50", "current_price": "40",
         "unrealized_pnl": "10", "realized_pnl": "1"},
    ]))
    filtered = await a.get_positions(product_id="ETH-USD")
    assert [p.product_id for p in filtered] == ["ETH-USD"]
