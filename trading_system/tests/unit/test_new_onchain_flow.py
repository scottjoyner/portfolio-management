from __future__ import annotations

import time

import pytest

from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.microstructure.volume_flow_accdist import (
    VolumeFlowAccDistStrategy,
)
from trading_system.strategies.microstructure.exchange_netflow_proxy import (
    ExchangeNetflowProxyStrategy,
)
from trading_system.strategies.microstructure.stablecoin_flow_proxy import (
    StablecoinFlowProxyStrategy,
)


def _rising_closes(n=12, start=100.0, step=0.5):
    return [start + step * i for i in range(n)]


def _steady_volumes(n=12, base=1000.0):
    return [base] * n


def _build_state(closes, volumes, warmup_complete=True):
    return {
        "product_id": "BTC-USD",
        "closes": closes,
        "volumes": volumes,
        "warmup_complete": warmup_complete,
        "score": 0.0,
    }


def test_instantiate_and_metadata_flags():
    for cls in (
        VolumeFlowAccDistStrategy,
        ExchangeNetflowProxyStrategy,
        StablecoinFlowProxyStrategy,
    ):
        s = cls()
        meta = s.metadata()
        assert meta["strategy_id"] == cls.__name__
        assert meta["strategy_type"] == "microstructure"
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert set(["closes", "volumes"]).issubset(set(meta["data_requirements"]))


def test_generate_signal_returns_signal_when_conditions_met():
    s = VolumeFlowAccDistStrategy()
    closes = _rising_closes()
    volumes = [1000.0] * (len(closes) - 1) + [5000.0]
    state = _build_state(closes, volumes)
    sig = s.generate_signal(state)
    assert isinstance(sig, StrategySignal)
    assert sig.strategy_id == "VolumeFlowAccDistStrategy"
    assert -1.0 <= sig.score <= 1.0


def test_netflow_proxy_signal():
    s = ExchangeNetflowProxyStrategy()
    closes = _rising_closes(start=100.0, step=3.0)
    volumes = [1000.0] * (len(closes) - 1) + [8000.0]
    state = _build_state(closes, volumes)
    sig = s.generate_signal(state)
    assert isinstance(sig, StrategySignal)
    assert sig.score > 0


def test_cooldown_blocks_resignal():
    s = ExchangeNetflowProxyStrategy()
    closes = _rising_closes(start=100.0, step=3.0)
    volumes = [1000.0] * (len(closes) - 1) + [8000.0]
    state = _build_state(closes, volumes)
    sig1 = s.generate_signal(state)
    assert sig1 is not None
    sig2 = s.generate_signal(state)
    assert sig2 is None
    time.sleep(1.1)
    sig3 = s.generate_signal(state)
    assert isinstance(sig3, StrategySignal)


def test_returns_none_before_warmup():
    s = StablecoinFlowProxyStrategy()
    closes = _rising_closes(n=6)
    volumes = _steady_volumes(n=6)[:-1] + [5000.0]
    state = _build_state(closes, volumes, warmup_complete=False)
    sig = s.generate_signal(state)
    assert sig is None or sig.warmup_passed is False


def test_returns_none_on_short_history():
    s = VolumeFlowAccDistStrategy()
    state = _build_state([100.0, 101.0], [1000.0, 1000.0])
    assert s.generate_signal(state) is None
