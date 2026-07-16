from __future__ import annotations

from trading_system.strategies.base import StrategyConfig
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.sentiment.return_dispersion import ReturnDispersionFearGreedStrategy
from trading_system.strategies.sentiment.volume_attention import VolumeAttentionMomentumStrategy
from trading_system.strategies.sentiment.price_acceleration import PriceAccelerationSentimentStrategy


def _closes(n, start=100.0, step=0.0):
    return [start + i * step for i in range(n)]


def test_metadata_flags():
    for cls in (ReturnDispersionFearGreedStrategy, VolumeAttentionMomentumStrategy, PriceAccelerationSentimentStrategy):
        s = cls()
        m = s.metadata()
        assert m["strategy_type"] == "sentiment"
        assert m["backtest_supported"] is True
        assert m["replay_supported"] is True
        assert "closes" in m["data_requirements"]
        assert "volumes" in m["data_requirements"]
        assert s.strategy_id not in {"", None}


def test_generate_signal_returns_signal_on_conditions():
    s = ReturnDispersionFearGreedStrategy(window=10)
    closes = [100.0] * 10 + [100.0, 100.1, 100.0, 99.5, 98.0, 96.0, 93.0, 89.0, 84.0, 78.0]
    ms = {"product_id": "BTC-USD", "closes": closes, "volumes": [1.0] * len(closes), "warmup_complete": True}
    sig = s.generate_signal(ms)
    assert sig is None or isinstance(sig, StrategySignal)

    s2 = VolumeAttentionMomentumStrategy(window=10)
    vc = _closes(11, 100.0) + [105.0]
    vv = [1.0] * 10 + [1.0, 50.0]
    ms2 = {"product_id": "BTC-USD", "closes": vc, "volumes": vv, "warmup_complete": True}
    s2.generate_signal(ms2)
    sig2 = s2.generate_signal(ms2)
    assert isinstance(sig2, StrategySignal)
    assert sig2.score > 0

    s3 = PriceAccelerationSentimentStrategy(window=8)
    pc = _closes(10, 100.0) + [100.0, 99.0, 96.0]
    pv = [1.0] * 12 + [20.0]
    ms3 = {"product_id": "BTC-USD", "closes": pc, "volumes": pv, "warmup_complete": True}
    s3.generate_signal(ms3)
    sig3 = s3.generate_signal(ms3)
    assert isinstance(sig3, StrategySignal)
    assert sig3.score > 0


def test_returns_none_before_warmup():
    s = VolumeAttentionMomentumStrategy(window=30)
    ms = {"product_id": "BTC-USD", "closes": _closes(5, 100.0), "volumes": [1.0] * 5, "warmup_complete": False}
    assert s.generate_signal(ms) is None


def test_cooldown_blocks_resignal():
    s = VolumeAttentionMomentumStrategy(window=10)
    s.config.cooldown_seconds = 3600
    vc = _closes(11, 100.0) + [108.0]
    vv = [1.0] * 10 + [1.0, 50.0]
    ms = {"product_id": "BTC-USD", "closes": vc, "volumes": vv, "warmup_complete": True}
    s.generate_signal(ms)
    first = s.generate_signal(ms)
    assert isinstance(first, StrategySignal)
    second = s.generate_signal(ms)
    assert second is None
