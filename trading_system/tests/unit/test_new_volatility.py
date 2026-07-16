import pytest

from strategies.volatility.keltner_channel_breakout import (
    KeltnerVolBreakoutStrategy,
    _atr,
    _sma,
)
from strategies.volatility.bollinger_squeeze_expansion import (
    BollingerSqueezeVolExpansionStrategy,
    _stdev,
)
from strategies.volatility.donchian_choppiness_breakout import (
    DonchianChoppinessVolBreakoutStrategy,
    _choppiness,
)


def _series(n=60, start=100.0, drift=0.0):
    return [start + i * drift for i in range(n)]


# 1. metadata mode flags
def test_metadata_mode_flags():
    for strat in (
        KeltnerVolBreakoutStrategy(),
        BollingerSqueezeVolExpansionStrategy(),
        DonchianChoppinessVolBreakoutStrategy(),
    ):
        meta = strat.metadata()
        assert meta["paper_mode"] is True
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert meta["live_supported"] is True
        assert meta["status"] == "implemented"
        assert meta["strategy_type"] == "volatility"


# 2. generate_signal returns a StrategySignal when breakout conditions met
def test_generate_signal_breakout():
    # Keltner: sharp up-move closes above EMA + 2*ATR
    closes = _series(60, 100.0, 0.0)
    closes[-1] = 130.0
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    kelt = KeltnerVolBreakoutStrategy()
    sig = kelt.generate_signal({
        "product_id": "BTC-USD",
        "close": closes[-1],
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "warmup_complete": True,
    })
    assert sig is not None
    assert sig.strategy_id == "KeltnerVolBreakoutStrategy"
    assert sig.score > kelt.config.threshold
    assert 0.0 <= sig.confidence <= 1.0
    assert sig.warmup_passed is True

    # Bollinger squeeze: tight band then expansion breakout
    sq = _series(60, 100.0, 0.0)
    sq[-6:-1] = [100.0] * 5  # compress
    sq[-1] = 112.0
    sq_highs = [c + 0.2 for c in sq]
    sq_lows = [c - 0.2 for c in sq]
    boll = BollingerSqueezeVolExpansionStrategy()
    sig2 = boll.generate_signal({
        "product_id": "BTC-USD",
        "close": sq[-1],
        "closes": sq,
        "highs": sq_highs,
        "lows": sq_lows,
        "warmup_complete": True,
    })
    assert sig2 is not None
    assert sig2.strategy_id == "BollingerSqueezeVolExpansionStrategy"
    assert sig2.score > boll.config.threshold

    # Donchian + low choppiness breakout up (steady uptrend -> low choppiness)
    dc = _series(60, 100.0, 0.3)
    dc[-1] = dc[-1] + 15.0  # breakout impulse clearly above prior 20-bar high
    dc_highs = [c + 0.5 for c in dc]
    dc_highs[-1] = dc[-1] + 0.5
    dc_lows = [c - 0.5 for c in dc]
    don = DonchianChoppinessVolBreakoutStrategy()
    sig3 = don.generate_signal({
        "product_id": "BTC-USD",
        "close": dc[-1],
        "closes": dc,
        "highs": dc_highs,
        "lows": dc_lows,
        "warmup_complete": True,
    })
    assert sig3 is not None
    assert sig3.strategy_id == "DonchianChoppinessVolBreakoutStrategy"
    assert sig3.score > don.config.threshold


# 3. cooldown blocks immediate re-signal
def test_cooldown_blocks_resignal():
    closes = _series(60, 100.0, 0.0)
    closes[-1] = 130.0
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    kelt = KeltnerVolBreakoutStrategy()
    state = {
        "product_id": "BTC-USD",
        "close": closes[-1],
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "warmup_complete": True,
    }
    first = kelt.generate_signal(state)
    assert first is not None
    second = kelt.generate_signal(state)
    assert second is None


def test_indicators_sanity():
    closes = _series(40, 100.0, 0.5)
    assert _sma(closes, 20) is not None
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    assert _atr(highs, lows, closes, 14) is not None
    assert _stdev(closes[-20:], 100.0) >= 0.0
    assert _choppiness(highs, lows, closes, 14) is not None
