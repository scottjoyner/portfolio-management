import math

import pytest

from strategies.momentum.ema_macd_momentum import EmaMacdMomentumStrategy, _ema, _macd_histogram
from strategies.momentum.adx_di_strength import AdxDiStrengthStrategy, _adx_di
from strategies.momentum.aroon_breakout import AroonBreakoutMomentumStrategy, _aroon


def _bull_closes(n=60, start=100.0, step=0.5):
    # convex uptrend so EMA12 leads EMA26 -> positive MACD histogram
    return [start + (i * step) ** 1.3 for i in range(n)]


def _bear_closes(n=60, start=200.0, step=0.5):
    return [start - (i * step) ** 1.3 for i in range(n)]


# 1. metadata mode flags
def test_metadata_mode_flags():
    for strat in (EmaMacdMomentumStrategy(), AdxDiStrengthStrategy(), AroonBreakoutMomentumStrategy()):
        meta = strat.metadata()
        assert meta["paper_mode"] is True
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert meta["live_supported"] is True
        assert meta["status"] == "implemented"


# 2. generate_signal returns a StrategySignal when trend conditions met
def test_generate_signal_bullish():
    strat = EmaMacdMomentumStrategy()
    closes = _bull_closes(60)
    state = {
        "product_id": "BTC-USD",
        "close": closes[-1],
        "closes": closes,
        "warmup_complete": True,
    }
    sig = strat.generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "EmaMacdMomentumStrategy"
    assert sig.score > strat.config.threshold
    assert 0.0 <= sig.confidence <= 1.0
    assert sig.warmup_passed is True

    adx_strat = AdxDiStrengthStrategy()
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    sig2 = adx_strat.generate_signal({
        "product_id": "BTC-USD",
        "highs": highs, "lows": lows, "closes": closes,
        "warmup_complete": True,
    })
    assert sig2 is not None
    assert sig2.score > 0

    aroon_strat = AroonBreakoutMomentumStrategy()
    sig3 = aroon_strat.generate_signal({
        "product_id": "BTC-USD",
        "highs": highs, "lows": lows, "warmup_complete": True,
    })
    assert sig3 is not None
    assert sig3.score > 0


# 3. cooldown blocks immediate re-signal
def test_cooldown_blocks_resignal():
    strat = EmaMacdMomentumStrategy()
    closes = _bull_closes(60)
    state = {
        "product_id": "BTC-USD",
        "close": closes[-1],
        "closes": closes,
        "warmup_complete": True,
    }
    first = strat.generate_signal(state)
    assert first is not None
    # immediate call should be blocked by cooldown
    second = strat.generate_signal(state)
    assert second is None


def test_indicators_sanity():
    closes = _bull_closes(40)
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    assert ema12 is not None and ema26 is not None
    assert ema12 > ema26  # uptrend
    hist = _macd_histogram(closes)
    assert hist is not None and hist > 0

    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    adx, pdi, mdi = _adx_di(highs, lows, closes, period=14)
    assert adx is not None and pdi is not None and mdi is not None
    up, down = _aroon(highs, lows, period=25)
    assert up is not None and down is not None
