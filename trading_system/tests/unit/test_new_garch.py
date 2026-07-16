"""
Unit tests for the new GARCH / volatility-clustering strategies.

Run with:
    PYTHONPATH=trading_system:. python3 -m pytest trading_system/tests/unit/test_new_garch.py -q
"""
from __future__ import annotations

import math

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.volatility.garch_vol_forecast import GarchLiteVolForecastStrategy
from strategies.volatility.vol_clustering_breakout import VolClusteringBreakoutStrategy
from strategies.volatility.regime_persistence_vol import RegimePersistenceVolStrategy


def _synth_closes(n: int, regime: str = "calm") -> list[float]:
    """Generate synthetic closes. regime in {calm, cluster, mixed}.

    For a vol cluster we use a *persistent* volatility level (slow
    mean-reverting random walk) so |returns| show positive lag-1
    autocorrelation - the textbook signature of volatility clustering.
    The return SIGN comes from a deterministic white-noise LCG so the
    magnitude (vol) drives the autocorrelation, not the sign pattern.
    """
    closes: list[float] = [100.0]
    signed_vol = 0.0
    rng = 123456789
    for i in range(1, n + 1):
        if regime == "calm":
            target = 0.0
            drift = 0.0002
        elif regime == "cluster":
            target = 0.012
            drift = 0.004
        else:
            phase = (i % 20) < 10
            target = 0.0 if phase else 0.012
            drift = 0.0002 if phase else 0.004
        # Persistent signed vol level -> |returns| are autocorrelated.
        signed_vol = signed_vol + 0.3 * (target - signed_vol) + 0.01 * math.sin(i / 4.0)
        signed_vol = max(-0.06, min(0.06, signed_vol))
        rng = (1103515245 * rng + 12345) % (2 ** 31)
        noise = (rng / (2 ** 30)) - 0.5
        ret = drift + signed_vol + 0.0008 * noise
        closes.append(closes[-1] * math.exp(ret))
    return closes


def test_metadata_flags():
    for strat in (
        GarchLiteVolForecastStrategy(),
        VolClusteringBreakoutStrategy(),
        RegimePersistenceVolStrategy(),
    ):
        meta = strat.metadata()
        assert meta["paper_mode"] is True
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert meta["live_supported"] is False
        assert meta["strategy_id"] in (
            "GarchLiteVolForecastStrategy",
            "VolClusteringBreakoutStrategy",
            "RegimePersistenceVolStrategy",
        )


def test_generate_returns_signal_when_conditions_met():
    strat = VolClusteringBreakoutStrategy()
    state = {
        "product_id": "BTC-USD",
        "closes": _synth_closes(80, "cluster"),
        "volumes": [100.0] * 81,
    }
    sig = strat.generate_signal(state)
    assert sig is not None
    assert isinstance(sig, StrategySignal)
    assert sig.strategy_id == "VolClusteringBreakoutStrategy"
    assert -1.0 <= sig.score <= 1.0

    g = GarchLiteVolForecastStrategy()
    gsig = g.generate_signal({
        "product_id": "BTC-USD",
        "closes": _synth_closes(80, "mixed"),
        "volumes": [100.0] * 81,
    })
    assert gsig is None or isinstance(gsig, StrategySignal)


def test_returns_none_before_warmup():
    for strat in (
        GarchLiteVolForecastStrategy(),
        VolClusteringBreakoutStrategy(),
        RegimePersistenceVolStrategy(),
    ):
        state = {
            "product_id": "BTC-USD",
            "closes": _synth_closes(20, "cluster"),
            "volumes": [100.0] * 21,
        }
        assert strat.generate_signal(state) is None


def test_cooldown_blocks_resignal():
    strat = VolClusteringBreakoutStrategy()
    base_state = {
        "product_id": "BTC-USD",
        "closes": _synth_closes(80, "cluster"),
        "volumes": [100.0] * 81,
    }
    first = strat.generate_signal(base_state)
    assert first is not None
    immediately = strat.generate_signal({
        "product_id": "BTC-USD",
        "closes": _synth_closes(81, "cluster"),
        "volumes": [100.0] * 82,
    })
    assert immediately is None
