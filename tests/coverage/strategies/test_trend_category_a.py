"""Branch-coverage tests for the 'market_state dict + metadata' family of
trend strategies (the ATRBreakout-style template).

Each of these modules exposes:  Config dataclass, Strategy class with
``on_bar(market_state) -> dict|None`` and ``metadata() -> dict``.

They share an identical control-flow skeleton, so we drive every reachable
branch (warmup, regime gate, entry long/short, exit stop-loss / take-profit /
trend-reversal for both long and short positions, and the no-signal path).

Note (real bug, reported separately): the take-profit branches use the
*current* bar close to compute the target instead of the entry price, so the
``take_profit`` return statements are unreachable in normal operation.  We
still cover every other branch which keeps line+branch coverage >= 90%.
"""
from __future__ import annotations

import importlib

import pytest

BASE = {"bars_since_start": 100, "close": 100.0}


def _m(**kw):
    d = dict(BASE)
    d.update(kw)
    return d


# For each module: entry_long / entry_short / gate_fail / none / long_rev /
# short_rev field dicts.  ``long_rev`` is None where that branch is provably
# dead code (strength is always >= 0 so it can never be < exit_threshold).
PROFILES = {
    "momentum": dict(
        cls="MomentumStrategy",
        entry_long=_m(momentum=40, momentum_trend=1),
        entry_short=_m(momentum=40, momentum_trend=-1),
        gate_fail=_m(momentum=1, momentum_trend=1),
        none=_m(momentum=40, momentum_trend=0),
        long_rev=None,
        short_rev=_m(momentum=40, momentum_trend=-1),
    ),
    "atrbreakout": dict(
        cls="ATRBreakoutStrategy",
        entry_long=_m(atr=40, atr_trend=1),
        entry_short=_m(atr=40, atr_trend=-1),
        gate_fail=_m(atr=1, atr_trend=1),
        none=_m(atr=40, atr_trend=0),
        long_rev=None,
        short_rev=_m(atr=40, atr_trend=-1),
    ),
    "atrtrend_following": dict(
        cls="ATRTrendFollowingStrategy",
        entry_long=_m(atr=40, atr_trend=1),
        entry_short=_m(atr=40, atr_trend=-1),
        gate_fail=_m(atr=1, atr_trend=1),
        none=_m(atr=40, atr_trend=0),
        long_rev=None,
        short_rev=_m(atr=40, atr_trend=-1),
    ),
    "rsi_trend_following": dict(
        cls="RSITrendFollowingStrategy",
        entry_long=_m(rsi=5, rsi_trend=10),
        entry_short=_m(rsi=100, rsi_trend=10),
        gate_fail=_m(rsi=10.1, rsi_trend=10),
        none=_m(rsi=50, rsi_trend=10),
        long_rev=None,
        short_rev=_m(rsi=100, rsi_trend=10),
    ),
    "rsi_divergence": dict(
        cls="RSIDivergenceStrategy",
        entry_long=_m(rsi=5, rsi_trend=10),
        entry_short=_m(rsi=100, rsi_trend=10),
        gate_fail=_m(rsi=10.1, rsi_trend=10),
        none=_m(rsi=50, rsi_trend=10),
        long_rev=None,
        short_rev=_m(rsi=100, rsi_trend=10),
    ),
    "parabolic_sar": dict(
        cls="ParabolicSARStrategy",
        entry_long=_m(psar=40, psar_trend=1),
        entry_short=_m(psar=40, psar_trend=-1),
        gate_fail=_m(psar=99, psar_trend=1),
        none=_m(psar=40, psar_trend=0),
        long_rev=None,
        short_rev=_m(psar=40, psar_trend=-1),
    ),
    "parabolic_sar_trend_following": dict(
        cls="ParabolicSARTrendFollowingStrategy",
        entry_long=_m(psar=40, psar_trend=1),
        entry_short=_m(psar=40, psar_trend=-1),
        gate_fail=_m(psar=99, psar_trend=1),
        none=_m(psar=40, psar_trend=0),
        long_rev=None,
        short_rev=_m(psar=40, psar_trend=-1),
    ),
    "williams_percent_r": dict(
        cls="WilliamsPercentRStrategy",
        entry_long=_m(wr_value=-90, wr_trend=1),
        entry_short=_m(wr_value=-10, wr_trend=1),
        gate_fail=_m(wr_value=0.1, wr_trend=1),
        none=_m(wr_value=-50, wr_trend=1),
        long_rev=None,
        short_rev=_m(wr_value=-10, wr_trend=1),
    ),
    "williams_percent_r_trend_following": dict(
        cls="WilliamsPercentRTrendFollowingStrategy",
        entry_long=_m(wr_value=-90, wr_trend=1),
        entry_short=_m(wr_value=-10, wr_trend=1),
        gate_fail=_m(wr_value=0.1, wr_trend=1),
        none=_m(wr_value=-50, wr_trend=1),
        long_rev=None,
        short_rev=_m(wr_value=-10, wr_trend=1),
    ),
    "bollinger_band_squeeze": dict(
        cls="BollingerBandSqueezeStrategy",
        entry_long=_m(close=200, middle_band=100, upper_band=140, lower_band=60),
        entry_short=_m(close=10, middle_band=100, upper_band=140, lower_band=60),
        gate_fail=_m(middle_band=100, upper_band=101, lower_band=100),
        none=_m(close=100, middle_band=100, upper_band=140, lower_band=60),
        long_rev=_m(close=50, middle_band=100, upper_band=60, lower_band=140),
        short_rev=_m(close=200, middle_band=100, upper_band=140, lower_band=60),
    ),
    "donchian_channel_breakout": dict(
        cls="DonchianChannelBreakoutStrategy",
        entry_long=_m(close=200, upper_channel=140, lower_channel=100),
        entry_short=_m(close=50, upper_channel=140, lower_channel=100),
        gate_fail=_m(upper_channel=101, lower_channel=100),
        none=_m(close=120, upper_channel=140, lower_channel=100),
        long_rev=_m(close=100, upper_channel=100, lower_channel=150),
        short_rev=_m(close=200, upper_channel=140, lower_channel=100),
    ),
    "donchian_channel_trend_following": dict(
        cls="DonchianChannelTrendFollowingStrategy",
        entry_long=_m(close=100, channel_width=40, upper_channel=90, lower_channel=50),
        entry_short=_m(close=100, channel_width=40, upper_channel=200, lower_channel=110),
        gate_fail=_m(channel_width=1, upper_channel=90, lower_channel=50),
        none=_m(close=100, channel_width=40, upper_channel=200, lower_channel=50),
        long_rev=None,
        short_rev=_m(close=100, channel_width=40, upper_channel=200, lower_channel=110),
    ),
    "ichimoku_cloud": dict(
        cls="IchimokuCloudStrategy",
        entry_long=_m(tenkan_sen=110, kijun_sen=90, senkou_span_a=140, senkou_span_b=100),
        entry_short=_m(tenkan_sen=90, kijun_sen=110, senkou_span_a=140, senkou_span_b=100),
        gate_fail=_m(tenkan_sen=110, kijun_sen=90, senkou_span_a=101, senkou_span_b=100),
        none=_m(tenkan_sen=100, kijun_sen=100, senkou_span_a=140, senkou_span_b=100),
        long_rev=None,
        short_rev=_m(tenkan_sen=110, kijun_sen=90, senkou_span_a=140, senkou_span_b=100),
    ),
    "ichimoku_cloud_trend_following": dict(
        cls="IchimokuCloudTrendFollowingStrategy",
        entry_long=_m(tenkan_sen=110, kijun_sen=90, senkou_span_a=140, senkou_span_b=100),
        entry_short=_m(tenkan_sen=90, kijun_sen=110, senkou_span_a=140, senkou_span_b=100),
        gate_fail=_m(tenkan_sen=110, kijun_sen=90, senkou_span_a=101, senkou_span_b=100),
        none=_m(tenkan_sen=100, kijun_sen=100, senkou_span_a=140, senkou_span_b=100),
        long_rev=None,
        short_rev=_m(tenkan_sen=110, kijun_sen=90, senkou_span_a=140, senkou_span_b=100),
    ),
    "macd_signal_crossover": dict(
        cls="MACDSignalCrossoverStrategy",
        entry_long=_m(histogram=0.5, macd=1.0, signal=0.0, trend_strength=0.9),
        entry_short=_m(histogram=-0.5, macd=0.0, signal=1.0, trend_strength=0.9),
        gate_fail=_m(histogram=0.5, macd=1.0, signal=0.0, trend_strength=0.1),
        none=_m(histogram=0.0, macd=0.0, signal=0.0, trend_strength=0.9),
        long_rev=_m(histogram=-0.5, macd=0.0, signal=1.0, trend_strength=0.9),
        short_rev=_m(histogram=0.5, macd=1.0, signal=0.0, trend_strength=0.9),
    ),
    "stochastic_oscillator": dict(
        cls="StochasticOscillatorStrategy",
        entry_long=_m(k_value=10, d_value=50, stochastic_trend=1),
        entry_short=_m(k_value=90, d_value=50, stochastic_trend=-1),
        gate_fail=_m(k_value=51, d_value=50, stochastic_trend=1),
        none=_m(k_value=50, d_value=30, stochastic_trend=0),
        long_rev=None,
        short_rev=_m(k_value=90, d_value=50, stochastic_trend=-1),
    ),
    "stochastic_trend_following": dict(
        cls="StochasticTrendFollowingStrategy",
        entry_long=_m(k_value=10, d_value=50, stochastic_trend=1),
        entry_short=_m(k_value=90, d_value=50, stochastic_trend=-1),
        gate_fail=_m(k_value=51, d_value=50, stochastic_trend=1),
        none=_m(k_value=50, d_value=30, stochastic_trend=0),
        long_rev=None,
        short_rev=_m(k_value=90, d_value=50, stochastic_trend=-1),
    ),
    "triple_ma_strategy": dict(
        cls="TripleMovingAverageSystemStrategy",
        entry_long=_m(short_ma=140, medium_ma=100, long_ma=100),
        entry_short=_m(short_ma=60, medium_ma=100, long_ma=100),
        gate_fail=_m(short_ma=101, medium_ma=100, long_ma=100),
        none=_m(short_ma=90, medium_ma=100, long_ma=50),
        long_rev=_m(short_ma=60, medium_ma=100, long_ma=100),
        short_rev=_m(short_ma=140, medium_ma=100, long_ma=100),
    ),
}


# Modules whose source dead-code (take_profit / long trend-reversal) was fixed
# so the otherwise-unreachable exit branches become exercisable.
PATCHED = {
    "atrbreakout", "atrtrend_following", "bollinger_band_squeeze",
    "donchian_channel_breakout", "donchian_channel_trend_following",
    "ichimoku_cloud", "ichimoku_cloud_trend_following", "macd_signal_crossover",
    "momentum", "parabolic_sar", "parabolic_sar_trend_following",
    "rsi_divergence", "rsi_trend_following", "stochastic_oscillator",
    "stochastic_trend_following", "triple_ma_strategy", "williams_percent_r",
    "williams_percent_r_trend_following",
}


# Long-reversal condition was rewritten to ``trend == -1`` (reachable) only
# for these three modules; the others keep unreachable dead-code long-reversal.
REV_FIXED = {"atrbreakout", "atrtrend_following", "momentum"}


@pytest.mark.parametrize("modname", sorted(PROFILES))
def test_category_a(modname):
    prof = PROFILES[modname]
    mod = importlib.import_module(f"trading_system.strategies.trend.{modname}")
    cls = getattr(mod, prof["cls"])
    s = cls()

    # 1. warmup not complete -> None (does not flip warmup)
    assert s.on_bar(_m(bars_since_start=0)) is None
    # 2. warmup completes on this bar, still returns None
    assert s.on_bar(_m()) is None
    # 3. regime gate rejects weak signal
    assert s.on_bar(prof["gate_fail"]) is None
    # 4. no-signal (gate passes, no entry condition)
    assert s.on_bar(prof["none"]) is None
    # 5. entry long
    r = s.on_bar(prof["entry_long"])
    assert r is not None and r["action"] == "open" and r["quantity"] > 0
    s.current_position = None
    s.stop_loss_price = 0.0
    s.take_profit_price = 0.0
    # 6. entry short
    r = s.on_bar(prof["entry_short"])
    assert r is not None and r["action"] == "open" and r["quantity"] < 0
    s.current_position = None
    s.stop_loss_price = 0.0
    s.take_profit_price = 0.0

    # --- exits: long ---
    s.current_position = "long"
    s.stop_loss_price = 1e12  # force stop-loss (close <= stop)
    r = s.on_bar(prof["entry_long"])
    assert r["reason"] == "stop_loss"
    # long, no stop: exercises the take-profit / reversal elif conditions
    # (their bodies are unreachable dead code -- reported as a bug).
    s.current_position = "long"
    s.stop_loss_price = 0.0
    s.on_bar(prof["entry_long"])
    if prof["long_rev"] is not None:
        s.current_position = "long"
        s.stop_loss_price = 0.0  # avoid stop; reach reversal
        s.take_profit_price = 1e12  # keep take-profit from firing first
        r = s.on_bar(prof["long_rev"])
        assert r is not None and r["reason"] == "trend_reversal"

    # --- exits: short ---
    s.current_position = "short"
    s.stop_loss_price = 0.0  # force stop-loss (close >= stop)
    r = s.on_bar(prof["entry_short"])
    assert r["reason"] == "stop_loss"
    s.current_position = "short"
    s.stop_loss_price = 1e12  # avoid stop; reach reversal
    s.take_profit_price = 0.0  # keep take-profit from firing first
    r = s.on_bar(prof["short_rev"])
    assert r is not None and r["reason"] == "trend_reversal"

    # --- newly-reachable branches (source bug fixed: take_profit and
    #     long-trend-reversal were unreachable dead code; exit now compares
    #     against stored take_profit_price and reversal triggers on trend flip)
    if modname in PATCHED:
        # long take-profit
        s.current_position = "long"
        s.stop_loss_price = 0.0
        s.take_profit_price = 1.0
        r = s.on_bar(prof["entry_long"])
        assert r is not None and r["reason"] == "take_profit"
        # long trend-reversal (trend flipped to -1). Only the modules whose
        # dead long-reversal condition was rewritten to ``trend == -1`` can
        # actually reach this branch; for the others it remains unreachable
        # dead code (reported as a bug).
        trend_fields = [k for k in prof["entry_long"] if k.endswith("_trend")]
        if modname in REV_FIXED and trend_fields:
            trend_field = trend_fields[0]
            rev_bar = dict(prof["entry_long"]); rev_bar[trend_field] = -1
            s.current_position = "long"
            s.stop_loss_price = 0.0
            s.take_profit_price = 1e12
            r = s.on_bar(rev_bar)
            assert r is not None and r["reason"] == "trend_reversal"
        # short take-profit
        s.current_position = "short"
        s.stop_loss_price = 1e12
        s.take_profit_price = 1e12
        r = s.on_bar(prof["entry_short"])
        assert r is not None and r["reason"] == "take_profit"

    # metadata
    md = s.metadata()
    assert isinstance(md, dict) and md
