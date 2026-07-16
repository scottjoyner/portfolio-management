"""
Tests for the new volatility-surface / options-style (spot-vol proxied) strategies.
"""
import math
import time

import pytest

from trading_system.strategies.volatility.bollinger_bandwidth_reversion import (
    BollingerBandwidthReversionStrategy,
)
from trading_system.strategies.volatility.vol_filtered_breakout import (
    VolFilteredBreakoutStrategy,
)
from trading_system.strategies.volatility.vol_term_structure_carry import (
    VolTermStructureCarryStrategy,
)


def _make_closes(n: int, drift: float = 0.0, vol: float = 0.01) -> list[float]:
    closes = [100.0]
    for i in range(1, n):
        ret = drift + vol * math.sin(i / 3.0)
        closes.append(closes[-1] * math.exp(ret))
    return closes


def _make_regime_closes(n: int) -> list[float]:
    """Quiet long history then a volatile burst at the end (vol expansion)."""
    closes = _make_closes(n, drift=0.0, vol=0.003)
    for i in range(1, 30):
        closes.append(closes[-1] * math.exp(0.02 * math.sin(i)))
    return closes


def _base_state(extra: dict | None = None) -> dict:
    state = {
        "product_id": "BTC-USD",
        "score": 0.0,
        "warmup_complete": True,
    }
    if extra:
        state.update(extra)
    return state


STRATEGIES = [
    VolTermStructureCarryStrategy,
    VolFilteredBreakoutStrategy,
    BollingerBandwidthReversionStrategy,
]


def test_instantiate_and_metadata_flags():
    for cls in STRATEGIES:
        s = cls()
        meta = s.metadata()
        assert meta["strategy_id"] == s.strategy_id
        assert meta["strategy_type"] == "volatility"
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert s.supports_mode("backtest")
        assert s.supports_mode("replay")


def test_generate_signal_returns_signal_when_conditions_met():
    s = VolTermStructureCarryStrategy()
    closes = _make_regime_closes(80)
    state = _base_state({"closes": closes})
    sig = s.generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "VolTermStructureCarryStrategy"
    assert -1.0 <= sig.score <= 1.0
    assert isinstance(sig.product_id, str)


def test_cooldown_blocks_resignal():
    s = VolFilteredBreakoutStrategy()
    closes = _make_closes(90, drift=0.0, vol=0.03)
    highs = [c * 1.002 for c in closes]
    lows = [c * 0.998 for c in closes]
    state = _base_state({"closes": closes, "highs": highs, "lows": lows})
    first = s.generate_signal(state)
    # immediate repeat should be blocked by cooldown
    second = s.generate_signal(state)
    if first is not None:
        assert second is None
    time.sleep(0.01)  # no-op guard; cooldown is 8s so still blocked


def test_returns_none_before_warmup():
    s = BollingerBandwidthReversionStrategy()
    # not enough history for warmup_period (60) + history (40)
    closes = _make_closes(30)
    state = _base_state({"closes": closes, "warmup_complete": False})
    sig = s.generate_signal(state)
    assert sig is None
