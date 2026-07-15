from decimal import Decimal

import pytest

from trading_system.brokers.paper import PaperBrokerAdapter
from trading_system.brokers.base import BrokerOrder, OrderStatus
from apps.paper_exchange.engine import PaperExchangeEngine, PaperOrder


@pytest.fixture
def engine():
    e = PaperExchangeEngine(starting_cash=Decimal("100000"), products=["BTC-USD", "ETH-USD"])
    e.set_market_price("BTC-USD", Decimal("100"))
    e.set_market_price("ETH-USD", Decimal("50"))
    # market buy fills -> creates a position + a fill
    o = e.place_order(strategy_id="s1", portfolio_id="p1", product_id="BTC-USD",
                      side="buy", order_type="market", size=Decimal("1"),
                      limit_price=Decimal("100"))
    return e, o


def test_broker_name():
    assert PaperBrokerAdapter().broker_name() == "paper"


def test_engine_property():
    e = PaperExchangeEngine(starting_cash=Decimal("1"), products=[])
    assert PaperBrokerAdapter(e).engine is e


@pytest.mark.asyncio
async def test_get_accounts_and_account():
    e = PaperExchangeEngine(starting_cash=Decimal("50000"), products=[])
    a = PaperBrokerAdapter(e)
    accs = await a.get_accounts()
    assert accs[0].total_balance == Decimal("50000")
    acc = await a.get_account("paper-001")
    assert acc.account_id == "paper-001"


@pytest.mark.asyncio
async def test_preview_order():
    a = PaperBrokerAdapter()
    assert (await a.preview_order(BrokerOrder("b", "c", "p", "BTC-USD", "buy", "limit",
                                              size=Decimal("0")))) == (False, "size must be positive")
    assert (await a.preview_order(BrokerOrder("b", "c", "p", "BTC-USD", "buy", "limit",
                                              size=Decimal("1")))) == (True, "preview passed")


@pytest.mark.asyncio
async def test_submit_order(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    order = BrokerOrder("b", "s1", "p1", "BTC-USD", "buy", "market", size=Decimal("1"),
                         price=Decimal("100"))
    res = await a.submit_order(order)
    assert res.broker_order_id
    assert res.status == OrderStatus.OPEN


@pytest.mark.asyncio
async def test_cancel_order(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    from apps.paper_exchange.engine import PaperOrder
    po = PaperOrder(order_id="lim1", strategy_id="s2", portfolio_id="p1",
                    product_id="BTC-USD", side="buy", order_type="limit",
                    size=Decimal("1"), price=Decimal("90"), status="open")
    e.orders["lim1"] = po
    assert await a.cancel_order("lim1") is True
    assert await a.cancel_order("nope") is False


@pytest.mark.asyncio
async def test_get_order_present_and_missing(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    bo = await a.get_order(o.order_id)
    assert bo is not None and bo.broker_order_id == o.order_id
    assert await a.get_order("nope") is None


@pytest.mark.asyncio
async def test_list_orders_filters(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    all_orders = await a.list_orders()
    assert len(all_orders) >= 1
    filtered = await a.list_orders(product_id="BTC-USD")
    assert all(p.product_id == "BTC-USD" for p in filtered)
    by_status = await a.list_orders(status=OrderStatus.FILLED)
    assert all(p.status == OrderStatus.FILLED for p in by_status)


@pytest.mark.asyncio
async def test_get_fills(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    fills = await a.get_fills(o.order_id)
    assert len(fills) >= 1
    assert fills[0].notional == fills[0].size * fills[0].price


@pytest.mark.asyncio
async def test_get_positions_and_filter(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    positions = await a.get_positions()
    assert any(p.product_id == "BTC-USD" for p in positions)
    filtered = await a.get_positions(product_id="BTC-USD")
    assert [p.product_id for p in filtered] == ["BTC-USD"]


@pytest.mark.asyncio
async def test_list_products_get_product_market_price(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    prods = await a.list_products()
    assert {"BTC-USD", "ETH-USD"}.issubset({p["product_id"] for p in prods})
    assert (await a.get_product("BTC-USD"))["price"] == 100.0
    assert await a.get_product("DOGE-USD") is None
    assert await a.get_market_price("BTC-USD") == Decimal("100")
    assert await a.get_market_price("MISSING") is None


@pytest.mark.asyncio
async def test_health_check(engine):
    e, o = engine
    a = PaperBrokerAdapter(e)
    h = await a.health_check()
    assert h["status"] == "healthy"
    assert h["order_count"] >= 1


@pytest.mark.asyncio
async def test_list_orders_filter_continues():
    e = PaperExchangeEngine(starting_cash=Decimal("100000"), products=["BTC-USD", "ETH-USD"])
    e.set_market_price("BTC-USD", Decimal("100"))
    e.set_market_price("ETH-USD", Decimal("50"))
    filled = e.place_order(strategy_id="s1", portfolio_id="p1", product_id="BTC-USD",
                           side="buy", order_type="market", size=Decimal("1"),
                           limit_price=Decimal("100"))
    # Add a non-matching product + non-matching status order to drive the
    # `continue` branches in list_orders.
    other = PaperOrder(order_id="eth1", strategy_id="s2", portfolio_id="p1",
                       product_id="ETH-USD", side="buy", order_type="limit",
                       size=Decimal("1"), price=Decimal("50"), status="open")
    e.orders["eth1"] = other
    a = PaperBrokerAdapter(e)
    by_product = await a.list_orders(product_id="BTC-USD")
    assert all(p.product_id == "BTC-USD" for p in by_product)
    by_status = await a.list_orders(status=OrderStatus.FILLED)
    assert all(p.status == OrderStatus.FILLED for p in by_status)
    assert filled.order_id in [p.broker_order_id for p in by_status]


@pytest.mark.asyncio
async def test_get_order_empty_status_and_missing_fee():
    e = PaperExchangeEngine(starting_cash=Decimal("100000"), products=["BTC-USD"])
    e.set_market_price("BTC-USD", Decimal("100"))
    # status="" (falsy) and no `fee` attribute -> exercises the False branches
    # in get_order's status/fee ternaries.
    po = PaperOrder(order_id="x1", strategy_id="s", portfolio_id="p",
                    product_id="BTC-USD", side="buy", order_type="limit",
                    size=Decimal("1"), price=Decimal("100"), status="")
    del po.fee
    e.orders["x1"] = po
    a = PaperBrokerAdapter(e)
    bo = await a.get_order("x1")
    assert bo.status == OrderStatus.OPEN
    assert bo.fee == Decimal("0")


@pytest.mark.asyncio
async def test_get_product_no_price_and_missing():
    e = PaperExchangeEngine(starting_cash=Decimal("100000"), products=[])
    a = PaperBrokerAdapter(e)
    # product not in engine -> None
    assert await a.get_product("BTC-USD") is None
    assert await a.get_market_price("BTC-USD") is None


@pytest.mark.asyncio
async def test_get_positions_no_filter_and_mid_price():
    e = PaperExchangeEngine(starting_cash=Decimal("100000"), products=["BTC-USD"])
    e.set_market_price("BTC-USD", Decimal("100"))
    e.place_order(strategy_id="s1", portfolio_id="p1", product_id="BTC-USD",
                  side="buy", order_type="market", size=Decimal("1"),
                  limit_price=Decimal("100"))
    a = PaperBrokerAdapter(e)
    all_pos = await a.get_positions()
    assert all_pos[0].current_price == Decimal("100")
    filtered = await a.get_positions(product_id="BTC-USD")
    assert [p.product_id for p in filtered] == ["BTC-USD"]
    none = await a.get_positions(product_id="ETH-USD")
    assert none == []


@pytest.mark.asyncio
async def test_cancel_order_missing():
    e = PaperExchangeEngine(starting_cash=Decimal("100000"), products=["BTC-USD"])
    a = PaperBrokerAdapter(e)
    assert await a.cancel_order("nope") is False

