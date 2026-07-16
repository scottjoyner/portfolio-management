from __future__ import annotations

import math

import pytest

from trading_system.strategies.risk.vol_target_overlay import VolTargetOverlayStrategy
from trading_system.strategies.risk.risk_parity_zscore import RiskParityZScoreStrategy
from trading_system.strategies.risk.ewma_var_breakout import EwmaVarBreakoutStrategy
from trading_system.strategies.base.interfaces import StrategySignal


def _warm_closes(n: int = 60, trend: float = 0.001) -> list[float]:
    base = 100.0
    out = []
    for i in range(n):
        base *= math.exp(trend + 0.002 * math.sin(i / 5.0))
        out.append(base)
    return out


def test_instantiate_and_metadata_flags():
    for cls in (VolTargetOverlayStrategy, RiskParityZScoreStrategy, EwmaVarBreakoutStrategy):
        s = cls()
        meta = s.metadata()
        assert meta["strategy_id"] == s.strategy_id
        assert meta["strategy_type"] == "risk"
        assert meta["backtest_supported"] is True
        assert meta["paper_mode"] is True
        assert "closes" in meta["data_requirements"]
        assert "volumes" in meta["data_requirements"]


def test_generate_signal_returns_signal_when_conditions_met():
    s = VolTargetOverlayStrategy()
    ms = {
        "product_id": "BTC-USD",
        "closes": _warm_closes(),
        "volumes": [1.0] * 60,
        "warmup_complete": True,
    }
    sig = s.generate_signal(ms)
    assert isinstance(sig, StrategySignal)
    assert -1.0 <= sig.score <= 1.0
    assert sig.strategy_id == "VolTargetOverlay"


def test_returns_none_before_warmup():
    s = RiskParityZScoreStrategy()
    ms = {
        "product_id": "BTC-USD",
        "closes": _warm_closes(10),
        "volumes": [1.0] * 10,
        "warmup_complete": False,
    }
    assert s.generate_signal(ms) is None


def test_cooldown_blocks_resignal():
    s = EwmaVarBreakoutStrategy(window=40)
    closes = _warm_closes(60, trend=0.01)
    ms = {
        "product_id": "BTC-USD",
        "closes": closes,
        "volumes": [1.0] * 60,
        "warmup_complete": True,
    }
    first = s.generate_signal(ms)
    assert isinstance(first, StrategySignal)
    # immediate re-call should be blocked by cooldown
    assert s.generate_signal(ms) is None
