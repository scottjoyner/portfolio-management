import math

import pytest

from trading_system.strategies.derivatives_proxy.options_greeks_proxy import (
    OptionsGreeksProxyStrategy,
)
from trading_system.strategies.derivatives_proxy.cross_exchange_basis_proxy import (
    CrossExchangeBasisProxyStrategy,
)
from trading_system.strategies.derivatives_proxy.onchain_flow_proxy import (
    OnchainFlowProxyStrategy,
)


def _rising_closes(n=60, start=100.0, step=0.5):
    return [start + i * step for i in range(n)]


def _flat_volumes(n=60, base=1000.0):
    return [base] * n


# 1) instantiate + metadata flags
def test_metadata_flags():
    for cls in (OptionsGreeksProxyStrategy, CrossExchangeBasisProxyStrategy, OnchainFlowProxyStrategy):
        s = cls()
        m = s.metadata()
        assert m["strategy_id"] == cls.__name__
        assert m["live_supported"] is False
        assert m["backtest_supported"] is True
        assert m["replay_supported"] is True
        assert "product_id" in m["data_requirements"]
        assert "closes" in m["data_requirements"]


# 2) generate_signal returns a StrategySignal when conditions met (options greeks)
def test_generate_signal_emits():
    s = OptionsGreeksProxyStrategy(lookback=30)
    closes = _rising_closes(40)
    # push last close far from band mean to force extreme pseudo-delta
    closes[-1] = closes[-2] * 1.10
    sig = s.generate_signal({
        "product_id": "BTC-USD",
        "closes": closes,
        "funding_rate": 0.0005,
        "warmup_complete": True,
    })
    assert sig is not None
    assert sig.strategy_id == "OptionsGreeksProxyStrategy"
    assert -1.0 <= sig.score <= 1.0
    assert isinstance(sig.product_id, str)


# 3) returns None before warmup
def test_returns_none_before_warmup():
    s = CrossExchangeBasisProxyStrategy(lookback=20)
    short = _rising_closes(5)
    assert s.generate_signal({
        "product_id": "BTC-USD",
        "closes": short,
        "warmup_complete": False,
    }) is None


# 4) cooldown blocks resignal
def test_cooldown_blocks_resignal():
    s = OnchainFlowProxyStrategy(lookback=10, volume_z_threshold=1.0)
    closes = _rising_closes(20)
    volumes = _flat_volumes(20, base=1.0)
    # make a huge volume spike so vol_z is large and impact is ~0 -> exhaustion fade
    volumes[-1] = 1e9
    closes[-1] = closes[-2] * 1.05
    state = {"product_id": "BTC-USD", "closes": closes, "volumes": volumes, "warmup_complete": True}
    first = s.generate_signal(state)
    assert first is not None
    # immediate second call must be blocked by cooldown
    assert s.generate_signal(state) is None


# 5) returns None gracefully on missing optional fields
def test_graceful_missing_optional_fields():
    s = OnchainFlowProxyStrategy(lookback=20)
    # missing volumes entirely -> required field absent -> None
    assert s.generate_signal({
        "product_id": "BTC-USD",
        "closes": _rising_closes(25),
        "warmup_complete": True,
    }) is None

    # cross-exchange basis falls back to range when volumes absent
    b = CrossExchangeBasisProxyStrategy(lookback=15)
    closes = _rising_closes(20)
    highs = [c * 1.01 for c in closes]
    lows = [c * 0.99 for c in closes]
    out = b.generate_signal({
        "product_id": "BTC-USD",
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "warmup_complete": True,
    })
    assert out is None or out.strategy_id == "CrossExchangeBasisProxyStrategy"
