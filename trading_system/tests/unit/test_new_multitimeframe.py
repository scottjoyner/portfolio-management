"""Unit tests for the novel multi-timeframe confluence strategies."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from trading_system.strategies.base.interfaces import StrategySignal  # noqa: E402
from trading_system.strategies.trend.multitf_confluence import (  # noqa: E402
    MultiTFTrendConfluenceStrategy,
)
from trading_system.strategies.trend.timeframe_momentum_divergence import (  # noqa: E402
    TimeframeMomentumDivergenceStrategy,
)
from trading_system.strategies.trend.volatility_cycle_align import (  # noqa: E402
    VolatilityCycleAlignStrategy,
)

STRATS = [
    MultiTFTrendConfluenceStrategy,
    TimeframeMomentumDivergenceStrategy,
    VolatilityCycleAlignStrategy,
]

UPTREND = [100 + i * 0.8 for i in range(160)]
DOWNTREND = [220 - i * 0.8 for i in range(160)]
# Long downtrend with a sharp recent bounce -> fast overbought vs slow down.
FADE_SERIES = [300 - i * 1.0 for i in range(140)] + [160 + i * 5 for i in range(12)]
# Sharp spike above the slow Bollinger band -> reversion.
SPIKE_SERIES = [100 + i * 0.05 for i in range(140)] + [107 + i * 5 for i in range(10)]


def test_metadata_flags():
    """(1) instantiate + metadata() flags are correct/consistent."""
    for cls in STRATS:
        s = cls()
        m = s.metadata()
        assert m["strategy_type"] == "trend"
        assert m["live_supported"] is False
        assert m["paper_mode"] is True
        assert m["backtest_supported"] is True
        assert "closes" in m["data_requirements"]
        # Each strategy declares a unique, clearly-new id.
        assert m["strategy_id"] == cls().strategy_id
    ids = {cls().strategy_id for cls in STRATS}
    assert ids == {
        "MultiTFTrendConfluenceStrategy",
        "TimeframeMomentumDivergenceStrategy",
        "VolatilityCycleAlignStrategy",
    }


def test_confluence_generates_signal():
    """(2) generate_signal returns a StrategySignal on confluence."""
    # Multi-TF: all horizons up -> strong positive score.
    mtf = MultiTFTrendConfluenceStrategy().generate_signal(
        {"product_id": "BTC-USD", "closes": UPTREND, "close": UPTREND[-1]}
    )
    assert isinstance(mtf, StrategySignal)
    assert mtf.score > 0

    # Multi-TF: all horizons down -> strong negative score.
    mtf_dn = MultiTFTrendConfluenceStrategy().generate_signal(
        {"product_id": "BTC-USD", "closes": DOWNTREND, "close": DOWNTREND[-1]}
    )
    assert isinstance(mtf_dn, StrategySignal)
    assert mtf_dn.score < 0

    # Divergence: fast overbought vs slow downtrend -> fade (negative).
    div = TimeframeMomentumDivergenceStrategy().generate_signal(
        {"product_id": "BTC-USD", "closes": FADE_SERIES, "close": FADE_SERIES[-1]}
    )
    assert isinstance(div, StrategySignal)
    assert div.score < 0
    assert "fade" in div.reason

    # Cycle: sharp spike above band -> reversion (negative).
    cyc = VolatilityCycleAlignStrategy().generate_signal(
        {"product_id": "BTC-USD", "closes": SPIKE_SERIES, "close": SPIKE_SERIES[-1]}
    )
    assert isinstance(cyc, StrategySignal)
    assert cyc.score < 0
    assert cyc.features["mode"] == "reversion"


def test_returns_none_before_warmup():
    """(3) returns None before enough bars for the slow timeframe."""
    short = UPTREND[:40]  # < 60 bars required
    for cls in STRATS:
        assert cls().generate_signal(
            {"product_id": "BTC-USD", "closes": short, "close": short[-1]}
        ) is None
    # Empty closes also yields None.
    for cls in STRATS:
        assert cls().generate_signal({"product_id": "BTC-USD", "closes": []}) is None


def test_cooldown_blocks_resignal():
    """(4) cooldown blocks an immediate re-signal."""
    s = MultiTFTrendConfluenceStrategy()
    state = {"product_id": "BTC-USD", "closes": UPTREND, "close": UPTREND[-1]}
    first = s.generate_signal(state)
    assert isinstance(first, StrategySignal)
    # Immediately calling again -> cooldown active (30s) -> None.
    assert s.generate_signal(state) is None


def test_continuation_near_band_mean():
    """Cycle strategy trades continuation when price sits near slow band mean."""
    from trading_system.strategies.trend.volatility_cycle_align import (
        _bollinger,
        _subsample,
    )

    slow = _subsample(UPTREND, 6)
    mean = _bollinger(slow, 20, 2.0)[0]
    sig = VolatilityCycleAlignStrategy().generate_signal(
        {"product_id": "BTC-USD", "closes": UPTREND, "close": mean}
    )
    assert isinstance(sig, StrategySignal)
    assert sig.features["mode"] == "continuation"
    assert sig.score > 0
