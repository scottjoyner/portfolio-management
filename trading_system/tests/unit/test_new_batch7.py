"""
Unit tests for the new cross_asset_2 strategy family (batch 7).
"""
from __future__ import annotations

import time

from strategies.base import StrategyConfig, StrategySignal
from strategies.cross_asset_2.etf_equity_stat_arb import EtfEquityStatArbProxyStrategy
from strategies.cross_asset_2.sentiment_news_fusion import SentimentNewsFusionStrategy
from strategies.cross_asset_2.regime_ensemble import RegimeConditionedEnsembleStrategy


def _series(base: float, n: int, drift: float = 0.0) -> list[float]:
    out = []
    v = base
    for i in range(n):
        out.append(v)
        v = v * (1.0 + drift)
    return out


def test_metadata_flags():
    for cls in (
        EtfEquityStatArbProxyStrategy,
        SentimentNewsFusionStrategy,
        RegimeConditionedEnsembleStrategy,
    ):
        s = cls()
        md = s.metadata()
        assert md["strategy_id"] == cls.__name__
        assert md["paper_mode"] is True
        assert md["replay_supported"] is True
        assert md["backtest_supported"] is True
        assert s.supports_mode("paper")
        assert s.supports_mode("backtest")
        assert not s.supports_mode("live")


def test_generate_signal_returns_signal():
    # ETF/equity stat-arb: crypto drifts up while equity proxy stays flat -> rich -> short.
    s1 = EtfEquityStatArbProxyStrategy(window=40, entry_z=1.5)
    closes = _series(30000.0, 60, drift=0.0005)
    peers = [_series(2000.0, 60, drift=0.0)]
    sig1 = s1.generate_signal({"product_id": "BTC-USD", "closes": closes, "peer_closes": peers, "warmup_complete": True})
    assert isinstance(sig1, StrategySignal)
    assert -1.0 <= sig1.score <= 1.0
    assert sig1.score < 0  # crypto rich vs flat equity -> fade short

    # Sentiment-news fusion: bullish news + positive momentum -> long.
    s2 = SentimentNewsFusionStrategy(lookback=20)
    bull = _series(100.0, 30, drift=0.004)
    sig2 = s2.generate_signal({"product_id": "BTC-USD", "closes": bull, "news_score": 0.6, "warmup_complete": True})
    assert isinstance(sig2, StrategySignal)
    assert sig2.score > 0

    # Regime-conditioned ensemble emits a blended score.
    s3 = RegimeConditionedEnsembleStrategy(window=60)
    trend = _series(100.0, 120, drift=0.003)
    sig3 = s3.generate_signal({"product_id": "BTC-USD", "closes": trend, "warmup_complete": True})
    assert isinstance(sig3, StrategySignal)
    assert -1.0 <= sig3.score <= 1.0


def test_none_when_optional_fields_missing():
    # ETF/equity: missing peer_closes.
    s1 = EtfEquityStatArbProxyStrategy()
    assert s1.generate_signal({"product_id": "BTC-USD", "closes": _series(30000.0, 60)}) is None

    # Sentiment fusion: missing news_score.
    s2 = SentimentNewsFusionStrategy()
    assert s2.generate_signal({"product_id": "BTC-USD", "closes": _series(100.0, 30)}) is None

    # Regime ensemble: missing closes is fine (covered by data_requirements) but
    # empty closes must not crash.
    s3 = RegimeConditionedEnsembleStrategy()
    assert s3.generate_signal({"product_id": "BTC-USD", "closes": []}) is None


def test_cooldown_blocks_resignal():
    s1 = EtfEquityStatArbProxyStrategy(window=40, entry_z=1.2)
    closes = _series(30000.0, 60, drift=0.0008)
    peers = [_series(2000.0, 60, drift=0.0)]
    ms = {"product_id": "BTC-USD", "closes": closes, "peer_closes": peers, "warmup_complete": True}
    first = s1.generate_signal(ms)
    assert isinstance(first, StrategySignal)
    assert s1.generate_signal(ms) is None  # cooldown blocks immediate resignal

    s2 = SentimentNewsFusionStrategy(lookback=20)
    s2.config.cooldown_seconds = 3600
    bull = _series(100.0, 30, drift=0.004)
    ms2 = {"product_id": "BTC-USD", "closes": bull, "news_score": 0.7, "warmup_complete": True}
    assert isinstance(s2.generate_signal(ms2), StrategySignal)
    assert s2.generate_signal(ms2) is None
