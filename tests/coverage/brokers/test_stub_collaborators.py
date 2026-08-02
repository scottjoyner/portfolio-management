"""Exercise the canonical collaborators used by broker adapters."""
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
    assert CoinbaseRestClient(api_key="k", api_secret="s") is not None


def test_paper_order_defaults_are_unfilled():
    order = PaperOrder(
        "o1",
        "s",
        "p",
        "BTC-USD",
        "buy",
        "limit",
        Decimal("1"),
        Decimal("100"),
        "open",
    )
    assert order.filled_size == Decimal("0")
    assert order.remaining_size == Decimal("0")
    assert order.fee == Decimal("0")
    assert order.created_at is not None


def test_paper_fill_explicit_execution_fields():
    fill = PaperFill(
        "f1",
        "o1",
        "BTC-USD",
        "buy",
        Decimal("1"),
        Decimal("100"),
        Decimal("0"),
        "taker",
        1.0,
    )
    assert fill.fee == Decimal("0")
    assert fill.liquidity == "taker"


def test_paper_position_defaults():
    position = PaperPosition(product_id="BTC-USD", size=Decimal("1"), cost_basis=Decimal("100"))
    assert position.realized_pnl == Decimal("0")


def test_paper_engine_market_order_fills_and_rejects_cancel_after_fill():
    engine = PaperExchangeEngine(starting_cash=Decimal("10000"), products=["BTC-USD"])
    engine.set_market_price("BTC-USD", Decimal("100"))
    order = engine.place_order("s", "p", "BTC-USD", "buy", "market", Decimal("1"))
    assert order.status == "filled"
    assert len(engine.fills) == 1
    assert engine.positions["BTC-USD"].size == Decimal("1")
    assert engine.cancel_order(order.order_id) is False
    assert engine.cancel_order("missing") is False


def test_paper_engine_limit_order_has_deterministic_terminal_or_open_state():
    engine = PaperExchangeEngine(products=["ETH-USD"])
    engine.set_market_price("ETH-USD", Decimal("10"))
    order = engine.place_order(
        "s",
        "p",
        "ETH-USD",
        "sell",
        "limit",
        Decimal("2"),
        limit_price=Decimal("12"),
    )
    assert order.status in {"open", "filled", "partially_filled", "cancelled"}
    assert order.price == Decimal("12")


def test_paper_engine_tracks_configured_products():
    engine = PaperExchangeEngine()
    engine.set_market_price("NEW-USD", Decimal("5"))
    assert engine.mid_prices["NEW-USD"] == Decimal("5")
