"""Unit tests for the regime-adaptive (batch 5b) novelty strategies.

Covers:
  * VolRegimeKalmanSwitchStrategy   -- hidden 2-state volatility Kalman filter.
  * KatzFractalBreakoutStrategy     -- Katz fractal-dimension breakout.
  * HurstAdaptiveLookbackStrategy   -- persistence-driven adaptive EMA lookback.

Each strategy gets a bespoke input series that puts it into a regime where it
should emit, plus shared tests for metadata flags, warmup guard and cooldown.
"""
from __future__ import annotations

import os
import random
import sys

import pytest

# Mirror the PYTHONPATH=trading_system:. convention used by verify commands.
_HERE = os.path.dirname(__file__)
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
_TS = os.path.join(_REPO, "trading_system")
for _p in (_REPO, _TS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from trading_system.strategies.base.interfaces import StrategySignal  # noqa: E402
from trading_system.strategies.regime_adaptive_2.fractal_breakout import (  # noqa: E402
    KatzFractalBreakoutStrategy,
)
from trading_system.strategies.regime_adaptive_2.hurst_adaptive_lookback import (  # noqa: E402
    HurstAdaptiveLookbackStrategy,
)
from trading_system.strategies.regime_adaptive_2.vol_kalman_switch import (  # noqa: E402
    VolRegimeKalmanSwitchStrategy,
)

ALL_STRATEGY_CLASSES = [
    VolRegimeKalmanSwitchStrategy,
    KatzFractalBreakoutStrategy,
    HurstAdaptiveLookbackStrategy,
]


# ---------------------------------------------------------------------------
# Per-strategy series that reliably drive a signal
# ---------------------------------------------------------------------------
def _persistent_trend(n: int = 120, seed: int = 7) -> list[float]:
    rng = random.Random(seed)
    prices = [100.0]
    step = 0.006
    for _ in range(n):
        step = 0.9 * step + 0.1 * rng.uniform(0.003, 0.012)
        prices.append(prices[-1] * (1.0 + step))
    return prices


def _turbulent_end(seed: int = 3) -> list[float]:
    """Calm trend followed by a turbulent, alternating-shock phase."""
    rng = random.Random(seed)
    prices = [100.0]
    for _ in range(40):
        prices.append(prices[-1] * (1.0 + rng.uniform(0.001, 0.004)))
    for i in range(30):
        shock = rng.uniform(0.02, 0.05) * (1 if i % 2 == 0 else -1)
        prices.append(prices[-1] * (1.0 + shock))
    return prices


def _emitting_series(cls) -> list[float]:
    if cls is VolRegimeKalmanSwitchStrategy:
        return _turbulent_end()
    return _persistent_trend()


# ---------------------------------------------------------------------------
# Test 1 -- instantiation + metadata flags
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_instantiate_and_metadata(cls):
    strat = cls()
    meta = strat.metadata()

    assert isinstance(meta, dict)
    assert isinstance(meta["strategy_id"], str) and meta["strategy_id"]
    assert meta["paper_mode"] is True
    assert meta["replay_supported"] is True
    assert meta["backtest_supported"] is True
    assert meta["live_supported"] is False
    assert "closes" in meta["data_requirements"]
    assert "config" in meta
    assert meta["config"]["warmup_period"] >= 40


def test_strategy_ids_unique_and_exact():
    ids = [cls().metadata()["strategy_id"] for cls in ALL_STRATEGY_CLASSES]
    assert ids == [
        "VolRegimeKalmanSwitch",
        "KatzFractalBreakout",
        "HurstAdaptiveLookbackTrend",
    ]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Test 2 -- generate_signal returns a StrategySignal when conditions are met
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_generate_signal_emits(cls):
    strat = cls()
    closes = _emitting_series(cls)
    sig = strat.generate_signal(
        {"product_id": "BTC-USD", "closes": closes, "warmup_complete": True}
    )
    assert isinstance(sig, StrategySignal)
    assert sig.strategy_id == strat.metadata()["strategy_id"]
    assert sig.product_id == "BTC-USD"
    assert -1.0 <= sig.score <= 1.0
    assert abs(sig.score) > strat.config.threshold
    assert 0.0 <= sig.confidence <= 1.0
    assert sig.features  # non-empty diagnostics


def test_fractal_and_hurst_are_long_on_uptrend():
    for cls in (KatzFractalBreakoutStrategy, HurstAdaptiveLookbackStrategy):
        strat = cls()
        sig = strat.generate_signal(
            {"product_id": "BTC-USD", "closes": _persistent_trend(), "warmup_complete": True}
        )
        assert isinstance(sig, StrategySignal)
        assert sig.score > 0  # trending up => long bias


# ---------------------------------------------------------------------------
# Test 3 -- returns None before warmup (insufficient bars) / empty input
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_none_before_warmup(cls):
    strat = cls()
    short = _emitting_series(cls)[:10]
    assert strat.generate_signal({"product_id": "BTC-USD", "closes": short}) is None


@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_none_on_empty_input(cls):
    strat = cls()
    assert strat.generate_signal({"product_id": "BTC-USD", "closes": []}) is None
    assert strat.generate_signal({"product_id": "BTC-USD"}) is None


# ---------------------------------------------------------------------------
# Test 4 -- cooldown blocks an immediate re-signal
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_cooldown_blocks_resignal(cls):
    strat = cls()
    assert strat.config.cooldown_seconds > 0
    state = {"product_id": "BTC-USD", "closes": _emitting_series(cls)}

    first = strat.generate_signal(state)
    assert isinstance(first, StrategySignal)

    assert strat.in_cooldown() is True
    assert strat.generate_signal(state) is None

    strat._last_emit_ts = 0.0
    assert isinstance(strat.generate_signal(state), StrategySignal)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
