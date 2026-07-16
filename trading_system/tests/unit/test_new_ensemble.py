from __future__ import annotations

from strategies.base import StrategyConfig, StrategyMetadata
from trading_system.strategies.ensemble.conviction_weighted import ConvictionWeightedCompositeStrategy
from trading_system.strategies.ensemble.majority_vote import MajorityVoteEnsembleStrategy
from trading_system.strategies.ensemble.regime_switching_blend import RegimeSwitchingBlendStrategy


def _trend_closes(n=60, drift=1.0):
    return [100.0 + drift * i + (i % 3) for i in range(n)]


def _trend_highs_lows(closes):
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    return highs, lows


def test_metadata_flags():
    for cls, sid in (
        (MajorityVoteEnsembleStrategy, "MajorityVoteEnsemble"),
        (ConvictionWeightedCompositeStrategy, "ConvictionWeightedComposite"),
        (RegimeSwitchingBlendStrategy, "RegimeSwitchingBlend"),
    ):
        s = cls()
        md = s.metadata()
        assert md["strategy_id"] == sid
        assert md["backtest_supported"] is True
        assert md["replay_supported"] is True
        assert md["paper_mode"] is True
        assert "closes" in md["data_requirements"]


def _breakout_closes(n=60):
    base = [100.0 + 0.1 * i for i in range(40)]
    last = base[-1]
    out = list(base)
    for j in range(n - len(base)):
        out.append(last + 0.4 * j + 2.0 * ((j % 6) - 3))
    return out


def test_generate_signal_returns_signal_when_agree():
    s = MajorityVoteEnsembleStrategy()
    closes = _breakout_closes(60)
    volumes = [1000.0 + 50.0 * i for i in range(60)]
    sig = s.generate_signal(
        {"product_id": "BTC-USD", "closes": closes, "volumes": volumes, "warmup_complete": True}
    )
    assert sig is not None
    assert sig.strategy_id == "MajorityVoteEnsemble"
    assert -1.0 <= sig.score <= 1.0


def test_conviction_and_regime_emit():
    c = ConvictionWeightedCompositeStrategy()
    closes = _trend_closes(60, drift=2.0)
    sig = c.generate_signal(
        {"product_id": "BTC-USD", "closes": closes, "volumes": [1000.0] * 60, "warmup_complete": True}
    )
    assert sig is not None
    r = RegimeSwitchingBlendStrategy()
    highs, lows = _trend_highs_lows(closes)
    sig2 = r.generate_signal(
        {"product_id": "BTC-USD", "closes": closes, "highs": highs, "lows": lows,
         "volumes": [1000.0] * 60, "warmup_complete": True}
    )
    assert sig2 is not None


def test_returns_none_before_warmup():
    s = ConvictionWeightedCompositeStrategy(warmup_period=30)
    sig = s.generate_signal(
        {"product_id": "BTC-USD", "closes": [100.0, 101.0, 99.0], "volumes": [10.0], "warmup_complete": False}
    )
    assert sig is None


def test_cooldown_blocks_resignal():
    s = RegimeSwitchingBlendStrategy(warmup_period=30)
    closes = _trend_closes(60, drift=2.0)
    highs, lows = _trend_highs_lows(closes)
    state = {"product_id": "BTC-USD", "closes": closes, "highs": highs, "lows": lows,
             "volumes": [1000.0] * 60, "warmup_complete": True}
    first = s.generate_signal(state)
    assert first is not None
    second = s.generate_signal({**state, "closes": closes[:-1] + [closes[-1] + 5.0]})
    assert second is None
