"""
Shared test driver for the standard trend-following strategy modules that share
the identical on_bar() branch structure (warmup -> trend gate -> long/short exits
-> reversal -> no-position entries).  Each module's "long reversal" branch was a
dead (unreachable) branch because it compared a non-negative value against a
NEGATIVE exit_threshold; that has been fixed in the source so both sides of
every branch can now be exercised.
"""
import math

WARMUP = 60

from trading_system.strategies.trend.donchian_channel_trend_following import (
    DonchianChannelTrendFollowingStrategy,
)
from trading_system.strategies.trend.ichimoku_cloud import IchimokuCloudStrategy
from trading_system.strategies.trend.ichimoku_cloud_trend_following import (
    IchimokuCloudTrendFollowingStrategy,
)
from trading_system.strategies.trend.parabolic_sar import ParabolicSARStrategy
from trading_system.strategies.trend.parabolic_sar_trend_following import (
    ParabolicSARTrendFollowingStrategy,
)
from trading_system.strategies.trend.rsi_divergence import RSIDivergenceStrategy
from trading_system.strategies.trend.rsi_trend_following import (
    RSITrendFollowingStrategy,
)
from trading_system.strategies.trend.stochastic_oscillator import (
    StochasticOscillatorStrategy,
)
from trading_system.strategies.trend.stochastic_trend_following import (
    StochasticTrendFollowingStrategy,
)
from trading_system.strategies.trend.williams_percent_r import WilliamsPercentRStrategy
from trading_system.strategies.trend.williams_percent_r_trend_following import (
    WilliamsPercentRTrendFollowingStrategy,
)

CLASSES = {
    "donchian_channel_trend_following": DonchianChannelTrendFollowingStrategy,
    "ichimoku_cloud": IchimokuCloudStrategy,
    "ichimoku_cloud_trend_following": IchimokuCloudTrendFollowingStrategy,
    "parabolic_sar": ParabolicSARStrategy,
    "parabolic_sar_trend_following": ParabolicSARTrendFollowingStrategy,
    "rsi_divergence": RSIDivergenceStrategy,
    "rsi_trend_following": RSITrendFollowingStrategy,
    "stochastic_oscillator": StochasticOscillatorStrategy,
    "stochastic_trend_following": StochasticTrendFollowingStrategy,
    "williams_percent_r": WilliamsPercentRStrategy,
    "williams_percent_r_trend_following": WilliamsPercentRTrendFollowingStrategy,
}


# Per-module, per-scenario overrides.  Values that scale with the close price are
# expressed as fractions (e.g. cw) and resolved in build().
SPEC = {
    "donchian_channel_trend_following": {
        "general":    {"uc": 0.9, "lc": 1.1, "cw": 0.35},
        "trend_low":  {"uc": 0.9, "lc": 1.1, "cw": 0.10},
        "long_rev":   {"uc": 0.9, "lc": 1.1, "cw": 0.35},
        "long_none":  {"uc": 0.9, "lc": 1.1, "cw": 0.50},
        "short_rev":  {"uc": 0.9, "lc": 1.1, "cw": 0.35},
        "short_none": {"uc": 0.9, "lc": 1.1, "cw": 0.50},
        "entry_long": {"uc": 0.9, "lc": 1.1, "cw": 0.35},
        "entry_short": {"uc": 2.0, "lc": 1.5, "cw": 0.35},
        "none":       {"uc": 2.0, "lc": 0.5, "cw": 0.35},
    },
    "ichimoku_cloud": {
        "general":    {"tenkan": 10, "kijun": 20, "sa": 100, "sb": 145},
        "trend_low":  {"tenkan": 10, "kijun": 20, "sa": 120, "sb": 145},
        "long_rev":   {"tenkan": 10, "kijun": 20, "sa": 100, "sb": 145},
        "long_none":  {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "short_rev":  {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "short_none": {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "entry_long": {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "entry_short": {"tenkan": 10, "kijun": 20, "sa": 100, "sb": 145},
        "none":       {"tenkan": 20, "kijun": 20, "sa": 100, "sb": 145},
    },
    "ichimoku_cloud_trend_following": {
        "general":    {"tenkan": 10, "kijun": 20, "sa": 100, "sb": 145},
        "trend_low":  {"tenkan": 10, "kijun": 20, "sa": 120, "sb": 145},
        "long_rev":   {"tenkan": 10, "kijun": 20, "sa": 100, "sb": 145},
        "long_none":  {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "short_rev":  {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "short_none": {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "entry_long": {"tenkan": 30, "kijun": 20, "sa": 100, "sb": 145},
        "entry_short": {"tenkan": 10, "kijun": 20, "sa": 100, "sb": 145},
        "none":       {"tenkan": 20, "kijun": 20, "sa": 100, "sb": 145},
    },
    "parabolic_sar": {
        "general":    {"psar_frac": 0.63, "psar_trend": 1},
        "trend_low":  {"psar_frac": 1.0, "psar_trend": 1},
        "long_rev":   {"psar_frac": 0.63, "psar_trend": 1},
        "long_none":  {"psar_frac": 0.55, "psar_trend": 1},
        "short_rev":  {"psar_frac": 1.40, "psar_trend": -1},
        "short_none": {"psar_frac": 0.63, "psar_trend": 0},
        "entry_long": {"psar_frac": 0.63, "psar_trend": 1},
        "entry_short": {"psar_frac": 1.40, "psar_trend": -1},
        "none":       {"psar_frac": 0.63, "psar_trend": 0},
    },
    "parabolic_sar_trend_following": {
        "general":    {"psar_frac": 0.63, "psar_trend": 1},
        "trend_low":  {"psar_frac": 1.0, "psar_trend": 1},
        "long_rev":   {"psar_frac": 0.63, "psar_trend": 1},
        "long_none":  {"psar_frac": 0.55, "psar_trend": 1},
        "short_rev":  {"psar_frac": 1.40, "psar_trend": -1},
        "short_none": {"psar_frac": 0.63, "psar_trend": 0},
        "entry_long": {"psar_frac": 0.63, "psar_trend": 1},
        "entry_short": {"psar_frac": 1.40, "psar_trend": -1},
        "none":       {"psar_frac": 0.63, "psar_trend": 0},
    },
    "rsi_divergence": {
        "general":    {"rsi": 29, "rsi_trend": 39},
        "trend_low":  {"rsi": 50, "rsi_trend": 50},
        "long_rev":   {"rsi": 29, "rsi_trend": 39},
        "long_none":  {"rsi": 40, "rsi_trend": 80},
        "short_rev":  {"rsi": 75, "rsi_trend": 50},
        "short_none": {"rsi": 40, "rsi_trend": 80},
        "entry_long": {"rsi": 29, "rsi_trend": 39},
        "entry_short": {"rsi": 75, "rsi_trend": 50},
        "none":       {"rsi": 40, "rsi_trend": 80},
    },
    "rsi_trend_following": {
        "general":    {"rsi": 29, "rsi_trend": 39},
        "trend_low":  {"rsi": 50, "rsi_trend": 50},
        "long_rev":   {"rsi": 29, "rsi_trend": 39},
        "long_none":  {"rsi": 40, "rsi_trend": 80},
        "short_rev":  {"rsi": 75, "rsi_trend": 50},
        "short_none": {"rsi": 40, "rsi_trend": 80},
        "entry_long": {"rsi": 29, "rsi_trend": 39},
        "entry_short": {"rsi": 75, "rsi_trend": 50},
        "none":       {"rsi": 40, "rsi_trend": 80},
    },
    "stochastic_oscillator": {
        "general":    {"k": 10, "d": 7.4},
        "trend_low":  {"k": 50, "d": 50},
        "long_rev":   {"k": 10, "d": 7.4},
        "long_none":  {"k": 30, "d": 7.4},
        "short_rev":  {"k": 90, "d": 68},
        "short_none": {"k": 30, "d": 7.4},
        "entry_long": {"k": 10, "d": 7.4},
        "entry_short": {"k": 90, "d": 68},
        "none":       {"k": 30, "d": 7.4},
    },
    "stochastic_trend_following": {
        "general":    {"k": 10, "d": 7.4},
        "trend_low":  {"k": 50, "d": 50},
        "long_rev":   {"k": 10, "d": 7.4},
        "long_none":  {"k": 30, "d": 7.4},
        "short_rev":  {"k": 90, "d": 68},
        "short_none": {"k": 30, "d": 7.4},
        "entry_long": {"k": 10, "d": 7.4},
        "entry_short": {"k": 90, "d": 68},
        "none":       {"k": 30, "d": 7.4},
    },
    "williams_percent_r": {
        "general":    {"wr": -85, "wr_trend": -1},
        "trend_low":  {"wr": -0.1, "wr_trend": 1},
        "long_rev":   {"wr": -85, "wr_trend": -1},
        "long_none":  {"wr": -85, "wr_trend": 1},
        "short_rev":  {"wr": -10, "wr_trend": 1},
        "short_none": {"wr": -50, "wr_trend": 1},
        "entry_long": {"wr": -85, "wr_trend": -1},
        "entry_short": {"wr": -10, "wr_trend": 1},
        "none":       {"wr": -50, "wr_trend": 1},
    },
    "williams_percent_r_trend_following": {
        "general":    {"wr": -85, "wr_trend": -1},
        "trend_low":  {"wr": -0.1, "wr_trend": 1},
        "long_rev":   {"wr": -85, "wr_trend": -1},
        "long_none":  {"wr": -85, "wr_trend": 1},
        "short_rev":  {"wr": -10, "wr_trend": 1},
        "short_none": {"wr": -50, "wr_trend": 1},
        "entry_long": {"wr": -85, "wr_trend": -1},
        "entry_short": {"wr": -10, "wr_trend": 1},
        "none":       {"wr": -50, "wr_trend": 1},
    },
}


def build(key, scenario, close, bars):
    ov = SPEC[key][scenario]
    state = {"close": close, "bars_since_start": bars}
    if key == "donchian_channel_trend_following":
        state["upper_channel"] = ov["uc"] * close
        state["lower_channel"] = ov["lc"] * close
        state["channel_width"] = ov["cw"] * close
    elif key in ("ichimoku_cloud", "ichimoku_cloud_trend_following"):
        state["tenkan_sen"] = ov["tenkan"]
        state["kijun_sen"] = ov["kijun"]
        state["senkou_span_a"] = ov["sa"]
        state["senkou_span_b"] = ov["sb"]
    elif key in ("parabolic_sar", "parabolic_sar_trend_following"):
        state["psar"] = ov["psar_frac"] * close
        state["psar_trend"] = ov["psar_trend"]
    elif key in ("rsi_divergence", "rsi_trend_following"):
        state["rsi"] = ov["rsi"]
        state["rsi_trend"] = ov["rsi_trend"]
    elif key in ("stochastic_oscillator", "stochastic_trend_following"):
        state["k_value"] = ov["k"]
        state["d_value"] = ov["d"]
        state["stochastic_trend"] = 1
    elif key in ("williams_percent_r", "williams_percent_r_trend_following"):
        state["wr_value"] = ov["wr"]
        state["wr_trend"] = ov["wr_trend"]
    return state


def exercise(key):
    cls = CLASSES[key]
    s = cls()
    # Warmup: incomplete (inner False) then complete (inner True).
    s.on_bar(build(key, "general", 100, WARMUP - 1))
    assert s.on_bar(build(key, "general", 100, WARMUP)) is None
    # Trend below minimum -> gate returns None.
    s.on_bar(build(key, "trend_low", 100, WARMUP))

    # Long position: stop-loss, take-profit, reversal, no-exit.
    s.current_position = "long"
    s.stop_loss_price = 50.0
    s.take_profit_price = 150.0
    s.on_bar(build(key, "long_rev", 40, WARMUP))
    s.on_bar(build(key, "long_rev", 160, WARMUP))
    s.on_bar(build(key, "long_rev", 100, WARMUP))
    s.on_bar(build(key, "long_none", 100, WARMUP))

    # Short position: stop-loss, take-profit, reversal, no-exit.
    s.current_position = "short"
    s.stop_loss_price = 200.0
    s.take_profit_price = 50.0
    s.on_bar(build(key, "short_rev", 210, WARMUP))
    s.on_bar(build(key, "short_rev", 40, WARMUP))
    s.on_bar(build(key, "short_rev", 100, WARMUP))
    s.on_bar(build(key, "short_none", 100, WARMUP))

    # No position: entry long, entry short, no entry.  Each must start with no
    # position because an entry sets current_position as a side effect.
    s.current_position = None
    s.on_bar(build(key, "entry_long", 100, WARMUP))
    s.current_position = None
    s.on_bar(build(key, "entry_short", 100, WARMUP))
    s.current_position = None
    s.on_bar(build(key, "none", 100, WARMUP))

    # Metadata branch.
    assert isinstance(s.metadata(), dict)
    return s
