from __future__ import annotations

import time

from strategies.base import StrategyConfig, StrategyMetadata
from trading_system.strategies.ml.kalman_mean_reversion import KalmanAdaptiveMeanReversionStrategy
from trading_system.strategies.ml.online_linear_regression import OnlineLinearRegressionMomentumStrategy
from trading_system.strategies.ml.volatility_regime_adaptive import VolatilityRegimeAdaptiveStrategy


def _trend_closes(n=60, drift=1.0):
    return [100.0 + drift * i + (i % 3) for i in range(n)]


def test_metadata_flags():
    for cls in (
        KalmanAdaptiveMeanReversionStrategy,
        OnlineLinearRegressionMomentumStrategy,
        VolatilityRegimeAdaptiveStrategy,
    ):
        s = cls()
        md = s.metadata()
        assert md["strategy_id"] == s.strategy_id
        assert md["backtest_supported"] is True
        assert md["replay_supported"] is True
        assert md["paper_mode"] is True


def test_generate_signal_returns_signal():
    s = OnlineLinearRegressionMomentumStrategy()
    closes = _trend_closes(60, drift=2.0)
    volumes = [1000.0] * 60
    sig = s.generate_signal(
        {"product_id": "BTC-USD", "closes": closes, "volumes": volumes, "warmup_complete": True}
    )
    assert sig is not None
    assert sig.strategy_id == "OnlineLinearRegressionMomentum"
    assert -1.0 <= sig.score <= 1.0


def test_returns_none_before_warmup():
    s = KalmanAdaptiveMeanReversionStrategy(window=50)
    sig = s.generate_signal(
        {"product_id": "BTC-USD", "closes": [100.0, 101.0, 99.0], "warmup_complete": False}
    )
    assert sig is None


def test_cooldown_blocks_resignal():
    s = VolatilityRegimeAdaptiveStrategy(window=40)
    closes = [100.0 + 0.5 * i for i in range(60)]
    state = {"product_id": "BTC-USD", "closes": closes, "warmup_complete": True}
    first = s.generate_signal(state)
    assert first is not None
    second = s.generate_signal({**state, "closes": closes[:-1] + [closes[-1] + 5.0]})
    assert second is None
