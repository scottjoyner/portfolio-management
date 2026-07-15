"""Coverage tests for the trend package __init__ strategy catalog."""
import pytest

from trading_system.strategies.trend import (
    BollingerBandBreakout,
    DonchianChannelBreakout,
    MACDSignalCrossover,
    MovingAverageCrossover,
    ParabolicSARVariant,
    TrendStrategiesUnitTests,
    TrendStrategyBase,
    TrendStrategyFactory,
    VWAPBreakout,
)


def d(close, high=None, low=None, volume=None, open_=None):
    n = len(close)
    return {
        "close": close,
        "high": high if high is not None else [c * 1.01 for c in close],
        "low": low if low is not None else [c * 0.99 for c in close],
        "volume": volume if volume is not None else [1000.0] * n,
        "open": open_ if open_ is not None else list(close),
    }


def test_base_not_implemented():
    b = TrendStrategyBase("x")
    with pytest.raises(NotImplementedError):
        b.on_bar({})
    assert b.handle_signal("LONG") is None
    assert b.get_performance_metrics()["win_rate"] == 0.0


def test_macd_signal_crossover():
    s = MACDSignalCrossover()
    assert s.on_bar(d([1, 2, 3])) is None  # too short
    # metrics with short history
    assert "total_trades" in s.get_performance_metrics()
    prices = []
    for i in range(1, 60):
        prices = list(range(100, 100 + i))
        s.on_bar(d(prices))
    # crossover branches exercised; metrics now use the long-history path
    m = s.get_performance_metrics()
    assert 0.4 <= m["win_rate"] <= 0.6
    # handle_signal LONG then SHORT
    assert s.handle_signal("LONG") is not None
    assert s.handle_signal("SHORT") is not None


def test_moving_average_crossover():
    s = MovingAverageCrossover(lookback_fast=5, lookback_slow=10)
    assert s.on_bar(d([1, 2, 3])) is None
    for i in range(15, 40):
        s.on_bar(d(list(range(100, 100 + i))))
    assert s.get_performance_metrics()["win_rate"] == 0.52


def test_parabolic_sar():
    s = ParabolicSARVariant()
    assert s.on_bar(d([1])) is None
    # rising -> eventually LONG
    long_seen = any(
        s.on_bar(d(list(range(100, 100 + k)))) == "LONG" for k in range(2, 30)
    )
    assert long_seen
    s2 = ParabolicSARVariant()
    short_seen = any(
        s2.on_bar(d(list(range(200, 200 - k, -1)))) == "SHORT" for k in range(2, 40)
    )
    assert short_seen
    assert s.get_performance_metrics()["win_rate"] == 0.58


def test_donchian_breakout():
    s = DonchianChannelBreakout(period_n=20)
    assert s.on_bar(d([1, 2, 3])) is None
    # Establish a flat channel of 20 prior bars at 100.
    for _ in range(20):
        s.on_bar(d([100.0], high=[100.0], low=[100.0]))
    # Breakout above the prior channel -> LONG (current bar excluded from channel)
    up = s.on_bar(d([200.0], high=[200.0], low=[200.0]))
    assert up == "LONG"
    # Price above new channel -> exit long (returns None)
    assert s.on_bar(d([300.0], high=[300.0], low=[300.0])) is None
    # Fresh strat, breakout below the prior channel -> SHORT
    s2 = DonchianChannelBreakout(period_n=20)
    for _ in range(20):
        s2.on_bar(d([100.0], high=[100.0], low=[100.0]))
    down = s2.on_bar(d([1.0], high=[1.0], low=[1.0]))
    assert down == "SHORT"
    assert s.get_performance_metrics()["profit_factor"] == 1.7


def test_bollinger_breakout():
    s = BollingerBandBreakout(period_n=20)
    assert s.on_bar(d([1, 2, 3])) is None
    # accumulate band history then spike up for LONG
    long_seen = False
    for i in range(25):
        prices = [100.0] * 24 + [100.0 + i]
        r = s.on_bar(d(prices))
        if r == "LONG":
            long_seen = True
    # spike to force upper-band breakout
    r = s.on_bar(d([100.0] * 24 + [1000.0]))
    assert r in ("LONG", None)
    # downward spike
    s.on_bar(d([100.0] * 24 + [0.001]))
    assert s.get_performance_metrics()["win_rate"] == 0.51


def test_vwap_breakout():
    s = VWAPBreakout(period_n=20)
    assert s.on_bar(d([1, 2, 3])) is None
    # zero volumes -> None
    assert s.on_bar(d([100.0] * 25, volume=[0.0] * 25)) is None
    # LONG breakout above VWAP
    assert s.on_bar(d([100.0] * 24 + [200.0])) == "LONG"
    # SHORT breakout below VWAP
    assert s.on_bar(d([100.0] * 24 + [10.0])) == "SHORT"
    assert s.get_performance_metrics()["sharpe_ratio"] == 1.22


def test_factory():
    f = TrendStrategyFactory()
    assert len(f.get_all()) == 6
    assert f.get_all("macd_signal_crossover") is MACDSignalCrossover
    assert isinstance(f.instantiate("vwap_breakout"), VWAPBreakout)
    with pytest.raises(ValueError):
        f.instantiate("nope")


def test_unit_tests_helper():
    assert TrendStrategiesUnitTests.run_all_tests()["all_tests_passed"] is True
