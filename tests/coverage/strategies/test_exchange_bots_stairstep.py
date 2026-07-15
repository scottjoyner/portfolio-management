"""Tests for StairStepTakeProfitStrategy."""
from __future__ import annotations

from trading_system.strategies.exchange_bots.stair_step_tp import (
    StairStepTakeProfitConfig,
    StairStepTakeProfitStrategy,
)


def _long_ms(product_id="BTC-USD", price=100.0, size=10.0, warmup=True):
    return {
        "product_id": product_id,
        "price": price,
        "position": {
            "side": "LONG",
            "size": size,
            "entry_price": price,
            "unrealized_pnl": 0.0,
        },
        "warmup_complete": warmup,
    }


def _no_pos_ms(product_id="BTC-USD", warmup=True):
    return {
        "product_id": product_id,
        "price": 100.0,
        "position": None,
        "warmup_complete": warmup,
    }


def _make(trailing=False, **overrides):
    cfg = StairStepTakeProfitConfig(trailing=trailing, **overrides)
    return StairStepTakeProfitStrategy(
        strategy_id="stair_tp",
        strategy_type="stair_step_tp",
        bot_config=cfg,
    )


def test_disabled_returns_none():
    strat = _make(enabled=False)
    assert strat.generate_signal(_long_ms()) is None


def test_warmup_not_complete_returns_none():
    strat = _make()
    assert strat.generate_signal(_long_ms(warmup=False)) is None


def test_no_position_returns_none_trailing_true_resets():
    strat = _make(trailing=True)
    assert strat.generate_signal(_no_pos_ms()) is None
    assert "BTC-USD" not in strat._registered


def test_no_position_returns_none_trailing_false_no_reset():
    strat = _make(trailing=False)
    assert strat.generate_signal(_no_pos_ms()) is None
    assert "BTC-USD" not in strat._registered


def test_first_registration_emits_buy():
    strat = _make(low=50.0, high=100.0, steps=5, take_profit_pct=0.02,
                  base_size_pct=0.2)
    sig = strat.generate_signal(_long_ms(price=100.0))
    assert sig is not None
    assert sig.score > 0
    assert "BTC-USD" in strat._registered
    assert strat._decisions["BTC-USD"][0] == "BUY"


def test_sell_at_step():
    strat = _make(low=50.0, high=100.0, steps=5, take_profit_pct=0.02,
                  base_size_pct=0.2)
    strat.generate_signal(_long_ms(price=100.0))  # buy level 0
    strat.generate_signal(_long_ms(price=90.0))   # buy level 1
    sig = strat.generate_signal(_long_ms(price=97.0))  # take profit
    assert sig is not None
    assert sig.score < 0
    assert strat._decisions["BTC-USD"][0] == "SELL"
    assert "stair-step SELL" in sig.reason


def test_price_outside_band_no_signal():
    # Fresh engine above the high with no inventory -> engine returns None.
    strat = _make(low=50.0, high=100.0, steps=5, take_profit_pct=0.02,
                  base_size_pct=0.2)
    assert strat.generate_signal(_long_ms(price=200.0)) is None


def test_zero_price_uses_zero_size():
    strat = _make(low=50.0, high=100.0, steps=5, take_profit_pct=0.02,
                  base_size_pct=0.2)
    strat.generate_signal(_long_ms(price=100.0))  # register with budget > 0
    sig = strat.generate_signal(_long_ms(price=0.0))  # already registered
    assert sig is not None
    side, price, size = strat._decisions["BTC-USD"]
    assert size == 0.0


def test_multiple_products():
    strat = _make(low=50.0, high=100.0, steps=3, take_profit_pct=0.01,
                  base_size_pct=0.2)
    s1 = strat.generate_signal(_long_ms(product_id="BTC-USD", price=100.0))
    s2 = strat.generate_signal(_long_ms(product_id="ETH-USD", price=100.0))
    assert s1 is not None and s2 is not None
    assert "BTC-USD" in strat._registered
    assert "ETH-USD" in strat._registered


def test_edge_params():
    strat = _make(low=99.0, high=100.0, steps=2, take_profit_pct=0.01,
                  base_size_pct=0.2)
    strat.generate_signal(_long_ms(price=100.0))  # buy level 0
    sig = strat.generate_signal(_long_ms(price=101.0))  # take profit
    assert sig is not None
    assert sig.score < 0


def test_trailing_true_with_position_no_reset_side_effect():
    strat = _make(trailing=True, low=50.0, high=100.0, steps=3,
                  take_profit_pct=0.01, base_size_pct=0.2)
    strat.generate_signal(_long_ms(price=100.0))
    assert "BTC-USD" in strat._registered
