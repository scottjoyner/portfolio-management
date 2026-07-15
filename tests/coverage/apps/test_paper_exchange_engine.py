import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from _helpers import install_fakes

install_fakes({
    "execution.queue_model.models": None,
    "core.events.ws_hub": None,
})

from trading_system.apps.paper_exchange import engine as engine_mod
from trading_system.apps.paper_exchange.engine import PaperExchangeEngine


def run(coro):
    return asyncio.run(coro)


def make_engine(prices=None):
    e = PaperExchangeEngine(starting_cash=Decimal("10000"), products=["BTC-USD"])
    for pid, mid in (prices or {}).items():
        e.set_market_price(pid, Decimal(str(mid)))
    return e


def test_set_market_price():
    e = make_engine()
    e.set_market_price("BTC-USD", Decimal("50000"), Decimal("10"))
    assert e.mid_prices["BTC-USD"] == Decimal("50000")
    assert e.spreads["BTC-USD"] == Decimal("10")


def test_place_order_market_buy():
    e = make_engine({"BTC-USD": 50000})
    order = e.place_order("s", "p", "BTC-USD", "buy", "market", Decimal("0.1"))
    assert order.status == "filled"
    assert order.filled_size == Decimal("0.1")
    assert e.cash < Decimal("10000")
    assert e.positions["BTC-USD"].size == Decimal("0.1")
    assert engine_mod.hub.publish_sync.called


def test_place_order_market_sell():
    e = make_engine({"BTC-USD": 50000})
    e.place_order("s", "p", "BTC-USD", "buy", "market", Decimal("0.1"))
    e.cash = Decimal("10000")
    order = e.place_order("s", "p", "BTC-USD", "sell", "market", Decimal("0.1"))
    assert order.status == "filled"
    assert e.cash > Decimal("10000")


def test_place_order_limit_partial_fill():
    e = make_engine({"BTC-USD": 50000})
    e.queue_model.estimate.return_value.fill_probability = 0.5
    e.queue_model.estimate.return_value.expected_queue_time_ms = 100.0
    e.queue_model.estimate.return_value.adverse_selection_bps = 1.0
    order = e.place_order("s", "p", "BTC-USD", "buy", "limit", Decimal("0.2"))
    assert order.status == "partially_filled"
    assert order.filled_size < Decimal("0.2")


def test_place_order_limit_no_fill():
    e = make_engine({"BTC-USD": 50000})
    e.queue_model.estimate.return_value.fill_probability = 0.1
    e.queue_model.estimate.return_value.expected_queue_time_ms = 100.0
    e.queue_model.estimate.return_value.adverse_selection_bps = 1.0
    order = e.place_order("s", "p", "BTC-USD", "buy", "limit", Decimal("0.2"))
    assert order.status == "cancelled"


def test_place_order_limit_price_override():
    e = make_engine({"BTC-USD": 50000})
    e.queue_model.estimate.return_value.fill_probability = 0.9
    e.queue_model.estimate.return_value.expected_queue_time_ms = 100.0
    e.queue_model.estimate.return_value.adverse_selection_bps = 1.0
    # buy with price below computed limit -> limit_px becomes price
    order = e.place_order("s", "p", "BTC-USD", "buy", "limit", Decimal("0.2"),
                          limit_price=Decimal("1.0"))
    assert order.price == Decimal("1.0")


def test_place_order_limit_price_no_override():
    e = make_engine({"BTC-USD": 50000})
    e.queue_model.estimate.return_value.fill_probability = 0.9
    e.queue_model.estimate.return_value.expected_queue_time_ms = 100.0
    e.queue_model.estimate.return_value.adverse_selection_bps = 1.0
    # buy with price above computed limit -> keep computed limit_px
    order = e.place_order("s", "p", "BTC-USD", "buy", "limit", Decimal("0.2"),
                          limit_price=Decimal("999999"))
    assert order.price != Decimal("999999")


def test_quote_limit_both_sides():
    e = make_engine({"BTC-USD": 50000})
    bid, ask = e._quote_limit("BTC-USD", "buy")
    assert bid < ask
    bid2, ask2 = e._quote_limit("BTC-USD", "sell")
    assert (bid2, ask2) != (bid, ask)


def test_cancel_order():
    e = make_engine({"BTC-USD": 50000})
    # fill a market order first (status filled)
    o = e.place_order("s", "p", "BTC-USD", "buy", "market", Decimal("0.1"))
    o.status = "open"
    assert e.cancel_order(o.order_id) is True
    # already cancelled
    assert e.cancel_order(o.order_id) is False
    # missing
    assert e.cancel_order("nope") is False


def test_get_portfolio_summary_and_open_orders():
    e = make_engine({"BTC-USD": 50000})
    e.place_order("s", "p", "BTC-USD", "buy", "market", Decimal("0.1"))
    summary = e.get_portfolio_summary()
    assert summary["total_equity"] > 0
    assert summary["open_orders"] == 0
    assert e.get_open_orders() == []


def test_run_with_body(monkeypatch):
    e = make_engine({"BTC-USD": 50000})
    e._running = True

    async def fake_sleep(t):
        e._running = False

    monkeypatch.setattr(engine_mod, "asyncio", SimpleNamespace(sleep=fake_sleep))
    run(e.run())
    assert e._running is False


def test_stop():
    e = make_engine()
    e._running = True
    e.stop()
    assert e._running is False
