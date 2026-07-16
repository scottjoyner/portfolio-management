"""
Unit tests for the new cross-asset / stat-arb strategies.
"""
from __future__ import annotations

import time

from strategies.cross_asset.spread_zscore import SpreadZScoreReversionStrategy
from strategies.cross_asset.momentum_divergence import CrossAssetMomentumDivergenceStrategy
from strategies.cross_asset.beta_reversion import BetaAdjustedCointegrationReversionStrategy


def _series(base: float, n: int, drift: float = 0.0, noise: float = 0.0) -> list[float]:
    out = []
    v = base
    for i in range(n):
        out.append(v)
        v = v * (1.0 + drift + noise * (0.5 - (i % 3) / 3.0))
    return out


def test_metadata_mode_flags():
    for cls in (
        SpreadZScoreReversionStrategy,
        CrossAssetMomentumDivergenceStrategy,
        BetaAdjustedCointegrationReversionStrategy,
    ):
        s = cls()
        md = s.metadata()
        assert md["strategy_id"] == cls.__name__
        assert md["paper_mode"] is True
        assert md["replay_supported"] is True
        assert md["backtest_supported"] is True
        assert "peer_closes" in md["data_requirements"]
        assert s.supports_mode("paper")
        assert s.supports_mode("backtest")
        assert not s.supports_mode("live")


def test_signal_emitted_with_peer_closes():
    s = SpreadZScoreReversionStrategy(window=30, entry_z=1.5)
    closes = _series(30000.0, 60, drift=0.0002)
    # Peer flat -> primary looks rich vs peer -> short spread signal.
    peer = _series(2000.0, 60, drift=0.0)
    ms = {"product_id": "BTC-USD", "closes": closes, "peer_closes": peer}
    sig = s.generate_signal(ms)
    assert sig is not None
    assert sig.product_id == "BTC-USD"
    assert -1.0 <= sig.score <= 1.0
    assert sig.score > 0

    # Beta-adjusted reversion: peer tracks primary (high corr) but last point
    # deviates so the residual z exceeds entry.
    peer_corr = [c * 0.066 + (5.0 if i == len(closes) - 1 else 0.0) for i, c in enumerate(closes)]
    b = BetaAdjustedCointegrationReversionStrategy(window=30, entry_z=1.5, min_corr=0.1)
    sig2 = b.generate_signal({"product_id": "BTC-USD", "closes": closes, "peer_closes": peer_corr})
    assert sig2 is not None

    # Momentum divergence: primary strongly outperforms peer.
    m = CrossAssetMomentumDivergenceStrategy(lookback=20, confirm_bars=3, min_divergence=0.005)
    strong = _series(30000.0, 40, drift=0.004)
    weak = _series(2000.0, 40, drift=0.0001)
    sig3 = m.generate_signal({"product_id": "BTC-USD", "closes": strong, "peer_closes": weak})
    assert sig3 is not None
    assert sig3.score > 0


def test_none_when_peer_closes_missing():
    for cls, kwargs in (
        (SpreadZScoreReversionStrategy, {}),
        (CrossAssetMomentumDivergenceStrategy, {}),
        (BetaAdjustedCointegrationReversionStrategy, {}),
    ):
        s = cls(**kwargs)
        ms = {"product_id": "BTC-USD", "closes": _series(30000.0, 60)}
        assert s.generate_signal(ms) is None


def test_cooldown_blocks_resignal():
    s = SpreadZScoreReversionStrategy(window=30, entry_z=1.0, min_corr=0.0) \
        if False else SpreadZScoreReversionStrategy(window=30, entry_z=1.0)
    closes = _series(30000.0, 60, drift=0.0005)
    peer = _series(2000.0, 60, drift=0.0)
    ms = {"product_id": "BTC-USD", "closes": closes, "peer_closes": peer}
    first = s.generate_signal(ms)
    assert first is not None
    # Immediate re-call within cooldown must be blocked.
    assert s.generate_signal(ms) is None
