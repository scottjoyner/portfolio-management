import math

import pytest

from trading_system.strategies.microstructure.amihud_illiquidity import (
    AmihudIlliquidityProxyStrategy,
)
from trading_system.strategies.microstructure.roll_microprice_bias import (
    RollMicropriceBiasStrategy,
)
from trading_system.strategies.microstructure.vpin_proxy import VpinProxyStrategy


def _make_ohlc(n, drift=0.001, vol=1.0, start=100.0):
    closes = [start]
    highs = [start]
    lows = [start]
    volumes = []
    price = start
    for i in range(n):
        price = price * (1.0 + drift + (0.5 - (i % 2)) * 0.0005)
        c = price
        h = c * (1.0 + 0.001)
        l = c * (1.0 - 0.001)
        closes.append(c)
        highs.append(h)
        lows.append(l)
        volumes.append(vol)
    return closes, highs, lows, volumes


def test_instantiate_and_metadata_flags():
    for cls in (
        AmihudIlliquidityProxyStrategy,
        RollMicropriceBiasStrategy,
        VpinProxyStrategy,
    ):
        s = cls()
        meta = s.metadata()
        assert meta["strategy_id"] == cls.__name__
        assert meta["strategy_type"] == "microstructure"
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert meta["paper_mode"] is True
        assert "product_id" in meta["data_requirements"]


def test_generate_signal_returns_signal_with_book():
    strat = RollMicropriceBiasStrategy(window=5, bias_threshold=0.0001)
    closes, highs, lows, volumes = _make_ohlc(30)
    state = {
        "product_id": "BTC-USD",
        "best_bid": 99.9,
        "best_ask": 100.2,
        "mid_price": 100.05,
        "highs": highs,
        "lows": lows,
        "close": closes[-1],
        "warmup_complete": True,
    }
    sig = strat.generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "RollMicropriceBiasStrategy"
    assert -1.0 <= sig.score <= 1.0
    assert sig.product_id == "BTC-USD"


def test_returns_none_before_warmup():
    closes, highs, lows, volumes = _make_ohlc(5)
    state = {
        "product_id": "BTC-USD",
        "closes": closes,
        "volumes": volumes,
        "warmup_complete": False,
    }
    strat = AmihudIlliquidityProxyStrategy(window=20)
    assert strat.generate_signal(state) is None

    strat2 = VpinProxyStrategy(window=50)
    assert strat2.generate_signal(state) is None


def test_cooldown_blocks_resignal():
    strat = RollMicropriceBiasStrategy(window=5, bias_threshold=0.0001, )
    strat.config.cooldown_seconds = 60.0
    state = {
        "product_id": "BTC-USD",
        "best_bid": 99.9,
        "best_ask": 100.2,
        "mid_price": 100.05,
        "highs": _make_ohlc(30)[1],
        "lows": _make_ohlc(30)[2],
        "close": 100.0,
        "warmup_complete": True,
    }
    first = strat.generate_signal(state)
    assert first is not None
    second = strat.generate_signal(state)
    assert second is None
