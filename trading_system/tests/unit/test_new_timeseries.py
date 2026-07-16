"""Unit tests for the pure-Python time-series / statistical novelty strategies.

Covers:
  * HurstRegimeStrategy            (rescaled-range Hurst exponent regime)
  * DFAAlphaRegimeStrategy         (detrended fluctuation analysis exponent)
  * SampleEntropyRegimeStrategy    (sample-entropy complexity regime)
"""
from __future__ import annotations

import os
import random
import sys

import pytest

# Make ``strategies`` (alias) and ``trading_system`` both importable, mirroring
# the PYTHONPATH=trading_system:. convention used by the verify commands.
_HERE = os.path.dirname(__file__)
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
_TS = os.path.join(_REPO, "trading_system")
for _p in (_REPO, _TS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from trading_system.strategies.base.interfaces import StrategySignal  # noqa: E402
from trading_system.strategies.timeseries.dfa_alpha import (  # noqa: E402
    DFAAlphaRegimeStrategy,
)
from trading_system.strategies.timeseries.hurst_regime import (  # noqa: E402
    HurstRegimeStrategy,
)
from trading_system.strategies.timeseries.sample_entropy import (  # noqa: E402
    SampleEntropyRegimeStrategy,
)

ALL_STRATEGY_CLASSES = [
    HurstRegimeStrategy,
    DFAAlphaRegimeStrategy,
    SampleEntropyRegimeStrategy,
]


def _persistent_series(n: int = 90, seed: int = 11) -> list[float]:
    """Build a strongly persistent (trending) close series with autocorrelated
    increments so Hurst/DFA exponents sit well above 0.5 and returns are
    regular (low sample entropy)."""
    rng = random.Random(seed)
    prices = [100.0]
    step = 0.008
    for _ in range(n):
        step = 0.9 * step + 0.1 * rng.uniform(0.004, 0.012)
        prices.append(prices[-1] * (1.0 + step))
    return prices


@pytest.fixture
def persistent_closes() -> list[float]:
    return _persistent_series()


# ---------------------------------------------------------------------------
# Test 1 — instantiation + metadata flags
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_instantiate_and_metadata(cls):
    strat = cls()
    meta = strat.metadata()

    assert isinstance(meta, dict)
    assert isinstance(meta["strategy_id"], str) and meta["strategy_id"]
    # paper/replay/backtest supported; live not supported by default
    assert meta["paper_mode"] is True
    assert meta["replay_supported"] is True
    assert meta["backtest_supported"] is True
    assert meta["live_supported"] is False
    # declares closes in data requirements
    assert "closes" in meta["data_requirements"]
    # config carried through
    assert "config" in meta
    assert meta["config"]["warmup_period"] >= 40


def test_strategy_ids_unique():
    ids = [cls().metadata()["strategy_id"] for cls in ALL_STRATEGY_CLASSES]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Test 2 — generate_signal returns a StrategySignal when conditions are met
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_generate_signal_emits(cls, persistent_closes):
    strat = cls()
    sig = strat.generate_signal(
        {"product_id": "BTC-USD", "closes": persistent_closes, "warmup_complete": True}
    )
    assert isinstance(sig, StrategySignal)
    assert sig.strategy_id == strat.metadata()["strategy_id"]
    assert sig.product_id == "BTC-USD"
    # persistent/trending series => long (positive) bias
    assert sig.score > 0
    assert -1.0 <= sig.score <= 1.0
    assert 0.0 <= sig.confidence <= 1.0
    assert abs(sig.score) > strat.config.threshold


# ---------------------------------------------------------------------------
# Test 3 — returns None before warmup (insufficient bars)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_none_before_warmup(cls):
    strat = cls()
    short = _persistent_series()[:10]  # far fewer than warmup_period
    sig = strat.generate_signal({"product_id": "BTC-USD", "closes": short})
    assert sig is None


@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_none_on_empty_input(cls):
    strat = cls()
    assert strat.generate_signal({"product_id": "BTC-USD", "closes": []}) is None
    assert strat.generate_signal({"product_id": "BTC-USD"}) is None


# ---------------------------------------------------------------------------
# Test 4 — cooldown blocks an immediate re-signal
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cls", ALL_STRATEGY_CLASSES)
def test_cooldown_blocks_resignal(cls, persistent_closes):
    strat = cls()
    assert strat.config.cooldown_seconds > 0
    state = {"product_id": "BTC-USD", "closes": persistent_closes}

    first = strat.generate_signal(state)
    assert isinstance(first, StrategySignal)

    # Immediately after emitting, cooldown must suppress the next signal.
    assert strat.in_cooldown() is True
    second = strat.generate_signal(state)
    assert second is None

    # Clearing the cooldown timer allows a fresh signal again.
    strat._last_emit_ts = 0.0
    third = strat.generate_signal(state)
    assert isinstance(third, StrategySignal)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
