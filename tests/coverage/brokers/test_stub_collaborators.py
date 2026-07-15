"""Exercise the stub collaborator modules used by the broker adapters.

The real ``exchange.coinbase.rest.client.CoinbaseRestClient`` and
``apps.paper_exchange.engine.PaperExchangeEngine`` are not present in this
checkout, so minimal stand-ins exist to let the broker adapters import and run.
This test keeps those stand-ins covered.
"""
from __future__ import annotations

from decimal import Decimal

from apps.paper_exchange.engine import (
    PaperExchangeEngine,
    PaperFill,
    PaperOrder,
    PaperPosition,
)
from exchange.coinbase.rest.client import CoinbaseRestClient


def test_coinbase_rest_client_instantiates():
    client = CoinbaseRestClient(api_key="k", api_secret="s")
    assert client is not None


def test_paper_order_defaults():
    o = PaperOrder("o1", "s", "p", "BTC-USD", "buy", "limit", Decimal("1"),
                   Decimal("100"), "open")
    assert o.filled_size == Decimal("1")
    assert o.remaining_size == Decimal("0")
    assert o.fee == Decimal("0")
    assert o.created_at is not None


def test_paper_fill_defaults():
    f = PaperFill("f1", "o1", "BTC-USD", "buy", Decimal("1"), Decimal("100"))
    assert f.fee == Decimal("0")


def test_paper_position_defaults():
    pos = PaperPosition("BTC-USD", "long", Decimal("1"), Decimal("100"))
    assert pos.unrealized_pnl == Decimal("0")
    assert pos.realized_pnl == Decimal("0")


def test_paper_engine_market_order_fills_and_cancels():
    eng = PaperExchangeEngine(starting_cash=Decimal("10000"), products=["BTC-USD"])
    eng.set_market_price("BTC-USD", "100")
    order = eng.place_order("s", "p", "BTC-USD", "buy", "market", Decimal("1"))
    assert order.status == "filled"
    assert len(eng.fills) == 1
    # position updated
    pos = eng.positions["BTC-USD"]
    assert pos.side == "long"
    assert pos.size == Decimal("1")
    assert eng.cancel_order(order.order_id) is True
    assert order.status == "cancelled"
    assert eng.cancel_order("missing") is False


def test_paper_engine_limit_order_stays_open():
    eng = PaperExchangeEngine(products=["ETH-USD"])
    eng.set_market_price("ETH-USD", "10")
    order = eng.place_order("s", "p", "ETH-USD", "sell", "limit", Decimal("2"),
                            limit_price=Decimal("12"))
    assert order.status == "open"
    assert order.price == Decimal("12")


def test_paper_engine_unknown_product_appended_on_price():
    eng = PaperExchangeEngine()
    eng.set_market_price("NEW-USD", "5")
    assert "NEW-USD" in eng.products
