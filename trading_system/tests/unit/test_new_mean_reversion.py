"""
Unit tests for the new mean-reversion strategies.

Run with:
    python3 -m pytest trading_system/tests/unit/test_new_mean_reversion.py -q
"""
from __future__ import annotations

import math

from strategies.mean_reversion.bollinger_reversion_signal import BollingerBandReversionStrategy
from strategies.mean_reversion.rsi_bounce_reversion import RsiBounceReversionStrategy
from strategies.mean_reversion.donchian_mean_reversion import DonchianMeanReversionStrategy


def _ohlc(n: int, price_fn) -> list[dict]:
    bars = []
    for i in range(n):
        close = price_fn(i)
        bars.append({"open": close, "high": close * 1.01, "low": close * 0.99, "close": close, "volume": 100.0})
    return bars


def test_metadata_mode_flags():
    for strat in (BollingerBandReversionStrategy(), RsiBounceReversionStrategy(), DonchianMeanReversionStrategy()):
        meta = strat.metadata()
        assert meta["paper_mode"] is True
        assert meta["backtest_supported"] is True
        assert meta["strategy_id"] in (
            "BollingerBandReversionStrategy",
            "RsiBounceReversionStrategy",
            "DonchianMeanReversionStrategy",
        )


def test_generate_signal_on_oversold_condition():
    # Bollinger: build a flat series then a sharp drop -> pierces lower band.
    base = _ohlc(40, lambda i: 100.0)
    base[-1] = {"open": 80.0, "high": 81.0, "low": 79.0, "close": 80.0, "volume": 100.0}
    state = {"product_id": "BTC-USD", "ohlc_history": base, "close": 80.0}
    sig = BollingerBandReversionStrategy().generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "BollingerBandReversionStrategy"
    assert sig.score > 0
    assert sig.product_id == "BTC-USD"

    # RSI: monotonic drop -> oversold bounce.
    drop = _ohlc(40, lambda i: 100.0 - i * 1.0)
    rsi_state = {"product_id": "BTC-USD", "ohlc_history": drop, "close": drop[-1]["close"]}
    rsi_sig = RsiBounceReversionStrategy().generate_signal(rsi_state)
    assert rsi_sig is not None
    assert rsi_sig.score > 0

    # Donchian: last bar tags the channel low.
    don = _ohlc(30, lambda i: 100.0 + (i % 2) * 5.0)
    don[-1] = {"open": 90.0, "high": 91.0, "low": 89.0, "close": 89.0, "volume": 100.0}
    don_state = {"product_id": "BTC-USD", "ohlc_history": don, "close": 89.0}
    don_sig = DonchianMeanReversionStrategy().generate_signal(don_state)
    assert don_sig is not None
    assert don_sig.score > 0


def test_cooldown_blocks_immediate_resignal():
    from strategies.base import StrategyConfig

    strat = BollingerBandReversionStrategy()
    strat.config = StrategyConfig(threshold=0.05, cooldown_seconds=60.0, warmup_period=20)
    base = _ohlc(40, lambda i: 100.0)
    base[-1] = {"open": 80.0, "high": 81.0, "low": 79.0, "close": 80.0, "volume": 100.0}
    state = {"product_id": "BTC-USD", "ohlc_history": base, "close": 80.0}

    first = strat.generate_signal(state)
    assert first is not None
    # Immediate second call must be blocked by cooldown.
    second = strat.generate_signal(state)
    assert second is None
