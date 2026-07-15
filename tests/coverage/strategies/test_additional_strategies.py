"""Coverage tests for trend.additional_strategies (was on the stale SKIP list)."""
import pytest

from trading_system.strategies.trend.additional_strategies import (
    AdaptiveMABands,
    AdditionalTrendStrategiesUnitTests,
    EMACrossover,
    IchimokuCloudBreakout,
    KeltnerChannelBreakout,
    TripleEMASystem,
    TrendStrategyFactory,
    VolumeProfileMomentum,
)


def _d(prices, vol=None):
    n = len(prices)
    return {
        "close": prices,
        "high": [p * 1.01 for p in prices],
        "low": [p * 0.99 for p in prices],
        "volume": vol if vol is not None else [1000.0] * n,
    }


RISE = [100 + i for i in range(60)]
FALL = [160 - i for i in range(60)]


def test_ema_crossover():
    s = EMACrossover()
    assert s.on_bar(_d([1, 2, 3])) is None  # too short
    assert s.on_bar(_d(RISE)) == "LONG"
    assert s.on_bar(_d(FALL)) == "SHORT"
    # compute_ema fallback (len < period)
    assert s.compute_ema([1.0, 2.0], 9) == pytest.approx(1.5)
    assert s.get_performance_metrics()["win_rate"] == 0.52


def test_triple_ema():
    s = TripleEMASystem()
    assert s.on_bar(_d([1, 2, 3])) is None
    assert s.on_bar(_d(RISE)) == "LONG"
    assert s.on_bar(_d(FALL)) == "SHORT"
    # neither aligned -> None
    zig = [100, 105, 95, 110, 90] * 12
    s.on_bar(_d(zig))
    # compute_ema short branch
    assert s.compute_ema([10.0], 9) == pytest.approx(10.0 / 9)
    assert s.get_performance_metrics()["profit_factor"] == 1.58


def test_ichimoku():
    s = IchimokuCloudBreakout()
    assert s.on_bar(_d([1, 2, 3])) is None
    # sharp late spike -> tenkan > kijun and price > tenkan -> LONG
    long_prices = [10.0] * 30 + [200.0, 300.0]
    assert s.on_bar(_d(long_prices)) == "LONG"
    # sharp late drop -> price < tenkan < kijun -> SHORT
    short_prices = [300.0] * 30 + [20.0, 10.0]
    assert s.on_bar(_d(short_prices)) == "SHORT"
    # scalar close branch in compute_tenkan_kijun
    t, k = s.compute_tenkan_kijun([100], {"close": 100})
    assert t is not None and k is not None
    assert s.get_performance_metrics()["max_drawdown"] == 0.22


def test_keltner():
    s = KeltnerChannelBreakout()
    assert s.on_bar(_d([1, 2, 3])) is None
    # sharp spike above the channel -> LONG
    spike = [100.0] * 30 + [100.0, 300.0]
    assert s.on_bar(_d(spike)) == "LONG"
    # flat -> no breakout
    assert s.on_bar(_d([100.0] * 40)) is None
    # compute_atr fallback branch (few bars)
    assert s.compute_atr([10, 11], [9, 10], [9.5, 10.5]) >= 0
    assert s.get_performance_metrics()["sharpe_ratio"] == 1.14


def test_volume_profile():
    s = VolumeProfileMomentum()
    assert s.on_bar(_d([1, 2, 3])) is None  # too short
    # long enough for momentum but too short for profile_range -> empty profile
    assert s.on_bar(_d(list(range(100, 125)))) is None
    # LONG: rising price with a final volume surge
    prices = list(range(100, 140))
    vols = [1000.0] * 39 + [5000.0]
    assert s.on_bar(_d(prices, vol=vols)) == "LONG"
    # SHORT: price collapses below the high-volume POC with a volume surge
    short_prices = [200.0] * 38 + [100.0, 90.0]
    short_vols = [5000.0] * 38 + [100.0, 6000.0]
    assert s.on_bar(_d(short_prices, vol=short_vols)) == "SHORT"
    # bucket_size <= 0 branch (all equal prices)
    flat = [100.0] * 40
    s.compute_volume_profile(_d(flat))
    assert s.get_performance_metrics()["win_rate"] == 0.54


def test_adaptive_ma_bands():
    s = AdaptiveMABands()
    assert s.on_bar(_d([1, 2, 3])) is None
    assert s.on_bar(_d(RISE)) is None  # always None by design, exercises compute_atr
    assert s.compute_atr([10, 11], [9, 10], [9.5, 10.5]) >= 0
    assert s.get_performance_metrics()["max_drawdown"] == 0.17


def test_factory():
    f = TrendStrategyFactory()
    assert len(f.get_all()) == 6
    assert f.get_all("ema_crossover") is EMACrossover
    inst = f.instantiate("triple_ema_system", fast=5, medium=10, slow=20)
    assert isinstance(inst, TripleEMASystem)
    with pytest.raises(ValueError):
        f.instantiate("nope")


def test_unit_test_helpers():
    AdditionalTrendStrategiesUnitTests.test_triple_ema()
    assert AdditionalTrendStrategiesUnitTests.run_all_tests()["all_tests_passed"] is True
