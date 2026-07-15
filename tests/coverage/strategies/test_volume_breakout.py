from trading_system.strategies.trend.volume_breakout import (
    VolumeBreakoutStrategy,
    VolumeBreakoutConfig,
)


def _bars(n, close=100.0, vol=100.0):
    return [
        {"timestamp": i, "open": close, "high": close + 1, "low": close - 1,
         "close": close, "volume": vol}
        for i in range(n)
    ]


def test_volume_breakout():
    # init validation.
    s = VolumeBreakoutStrategy()
    try:
        s.init([])
        assert False
    except ValueError:
        pass
    try:
        s.init([{"close": 1}])
        assert False
    except ValueError:
        pass
    # missing volume field.
    try:
        s.init([{"close": 1, "price": 0} for _ in range(45)])
        assert False
    except ValueError:
        pass

    # _calculate_breakout_metrics empty.
    assert s._calculate_breakout_metrics([], [], []) == ([], 0.0, 0.0)

    data = _bars(45, close=100.0, vol=100.0)
    s.init(data)
    assert s.rolling_high_values

    # on_bar before init (rolling_high_values empty) -> else branch.
    s2 = VolumeBreakoutStrategy()
    assert s2.on_bar({"close": 100, "high": 100, "volume": 100}) is None

    # Invalid close.
    assert s.on_bar({"close": 0, "high": 1, "volume": 100}) is None
    assert s.on_bar({"close": float("nan"), "high": 1, "volume": 100}) is None

    # No breakout -> None.
    assert s.on_bar({"close": 100, "high": 100, "volume": 100}) is None

    # Breakout above resistance with high volume -> BUY.
    out = s.on_bar({"close": 5000, "high": 5000, "volume": 100000})
    assert out is not None and out["action"] == "BUY"

    # handle_signal BUY / SELL (positive + negative pnl).
    s.handle_signal({"action": "BUY", "entry_price": 5000})
    assert s.get_current_position() is not None
    s.handle_signal({"action": "SELL", "entry_price": 5100})
    assert s.num_successful_trades == 1
    s.handle_signal({"action": "BUY", "entry_price": 5000})
    s.handle_signal({"action": "SELL", "entry_price": 4900})
    assert s.num_failed_trades == 1

    empty = VolumeBreakoutStrategy()
    empty.init(_bars(45))
    assert empty.get_performance_metrics()["total_signals"] == 0
    assert s.get_performance_metrics()["win_rate"] >= 0
