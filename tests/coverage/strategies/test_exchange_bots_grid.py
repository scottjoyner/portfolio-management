"""Tests for SpotGridStrategy (exchange-bot spot grid)."""
from __future__ import annotations

import pytest

from trading_system.strategies.exchange_bots.spot_grid import (
    SpotGridConfig,
    SpotGridStrategy,
)


def make_ms(price, product="BTC-USD", warmup=True, position=None):
    return {
        "product_id": product,
        "price": price,
        "warmup_complete": warmup,
        "position": position,
    }


def make_strategy(**overrides):
    kwargs = dict(
        enabled=True,
        lower=100.0,
        upper=200.0,
        grids=5,
        investment=1000.0,
    )
    kwargs.update(overrides)
    cfg = SpotGridConfig(**kwargs)
    return SpotGridStrategy(bot_config=cfg)


def test_disabled_returns_none():
    s = make_strategy(enabled=False)
    assert s.is_disabled(make_ms(150))[0] is True
    assert s.generate_signal(make_ms(150)) is None


def test_warmup_incomplete_returns_none():
    s = make_strategy()
    assert s.generate_signal(make_ms(150, warmup=False)) is None


def test_invalid_upper_lower_returns_none():
    s = make_strategy(upper=100.0)
    assert s.generate_signal(make_ms(150)) is None


def test_invalid_grids_returns_none():
    s = make_strategy(grids=1)
    assert s._lines == []
    assert s.generate_signal(make_ms(150)) is None


def test_invalid_investment_returns_none():
    s = make_strategy(investment=0.0)
    assert s.generate_signal(make_ms(150)) is None


def test_invalid_trigger_normalized():
    cfg = SpotGridConfig(lower=100, upper=200, grids=5, investment=1000, trigger="foo")
    assert cfg.trigger == "both"


def test_buy_on_way_down():
    s = make_strategy()
    sig = s.generate_signal(make_ms(200.0))
    assert sig is not None
    assert sig.score > 0
    assert s._filled["BTC-USD"] == {0}
    intents = s.order_intents(sig, make_ms(200.0))
    assert intents[0]["side"] == "BUY"
    assert intents[0]["price"] == 100.0


def test_zero_price_returns_none():
    s = make_strategy()
    assert s.generate_signal(make_ms(0.0)) is None


def test_price_above_upper_no_action():
    s = make_strategy()
    sig = s.generate_signal(make_ms(250.0))
    assert sig is None
    assert s._filled["BTC-USD"] == set()


def test_price_above_upper_triggers_sell_of_filled():
    s = make_strategy()
    s._filled["BTC-USD"] = {0, 1, 2, 3, 4}
    sig = s.generate_signal(make_ms(250.0))
    assert sig is not None
    assert sig.score < 0
    assert 4 not in s._filled["BTC-USD"]
    assert s._filled["BTC-USD"] == {0, 1, 2, 3}


def test_sell_on_way_up():
    s = make_strategy()
    s._filled["BTC-USD"] = {0, 1, 2}
    sig = s.generate_signal(make_ms(160.0))
    assert sig is not None
    assert sig.score < 0
    assert 2 not in s._filled["BTC-USD"]
    assert s._filled["BTC-USD"] == {0, 1}


def test_trigger_buy_only_no_sell():
    s = make_strategy(trigger="buy")
    s._filled["BTC-USD"] = {0, 1, 2}
    sig = s.generate_signal(make_ms(160.0))
    assert sig is None


def test_trigger_buy_only_buys():
    s = make_strategy(trigger="buy")
    sig = s.generate_signal(make_ms(200.0))
    assert sig is not None
    assert sig.score > 0
    assert s._filled["BTC-USD"] == {0}


def test_trigger_sell_only_no_buy():
    s = make_strategy(trigger="sell")
    sig = s.generate_signal(make_ms(200.0))
    assert sig is None


def test_trigger_sell_only_sells():
    s = make_strategy(trigger="sell")
    s._filled["BTC-USD"] = {0, 1, 2}
    sig = s.generate_signal(make_ms(160.0))
    assert sig is not None
    assert sig.score < 0
    assert 2 not in s._filled["BTC-USD"]


def test_multiple_products_independent():
    s = make_strategy()
    s.generate_signal(make_ms(200.0, product="BTC-USD"))
    s.generate_signal(make_ms(200.0, product="ETH-USD"))
    assert s._filled["BTC-USD"] == {0}
    assert s._filled["ETH-USD"] == {0}


def test_partial_fill_tracking():
    s = make_strategy(grids=3)
    sig1 = s.generate_signal(make_ms(200.0))
    assert sig1.score > 0 and 0 in s._filled["BTC-USD"]
    sig2 = s.generate_signal(make_ms(150.0))
    assert sig2.score > 0 and s._filled["BTC-USD"] == {0, 1}
    sig3 = s.generate_signal(make_ms(150.0))
    assert sig3.score < 0 and s._filled["BTC-USD"] == {0}
    sig4 = s.generate_signal(make_ms(100.0))
    assert sig4.score < 0 and s._filled["BTC-USD"] == set()
