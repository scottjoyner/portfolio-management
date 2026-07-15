from trading_system.strategies.trend.vwap_momentum import (
    VWAPMomentumStrategy,
    VWAPConfig,
)


def _bars(n, close=100.0, vol=1.0):
    return [
        {"timestamp": i, "open": close, "high": close + 1, "low": close - 1,
         "close": close, "volume": vol}
        for i in range(n)
    ]


def test_vwap_momentum():
    # init validation.
    s = VWAPMomentumStrategy()
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
    try:
        s.init([{"close": 1, "price": 0} for _ in range(35)])
        assert False
    except ValueError:
        pass

    data = _bars(35, close=100.0, vol=1.0)
    s.init(data)
    assert s.vwap_values

    # Invalid close.
    assert s.on_bar({"close": 0, "low": 0}) is None
    assert s.on_bar({"close": float("nan"), "low": 0}) is None

    # on_bar before init -> vwap_values empty -> else branch (current_vwap=close*0.98).
    s2 = VWAPMomentumStrategy()
    out = s2.on_bar({"close": 100, "low": 90})
    assert out is not None and out["action"] == "SELL"

    # current_vwap <= 0 branch -> no BUY/SELL.
    s3 = VWAPMomentumStrategy()
    s3.vwap_values = [-5.0]
    assert s3.on_bar({"close": 10, "low": 10}) is None

    # BUY: price pulls back within pullback threshold of VWAP.
    vwap = s.vwap_values[-1]
    out = s.on_bar({"close": vwap * 1.005, "low": vwap * 1.005})
    assert out is not None and out["action"] == "BUY"

    # SELL: price far below VWAP -> momentum exhaustion.
    out = s.on_bar({"close": 200, "low": 95})
    assert out is not None and out["action"] == "SELL"

    # Neither (price above, within band but not near) -> None.
    assert s.on_bar({"close": 200, "low": 150}) is None

    # handle_signal BUY / SELL.
    s.handle_signal({"action": "BUY", "entry_price": 100})
    assert s.get_current_position() is not None
    s.handle_signal({"action": "SELL", "entry_price": 110})
    assert s.num_successful_trades == 1
    s.handle_signal({"action": "BUY", "entry_price": 100})
    s.handle_signal({"action": "SELL", "entry_price": 90})
    assert s.num_failed_trades == 1

    empty = VWAPMomentumStrategy()
    empty.init(_bars(35))
    assert empty.get_performance_metrics()["total_signals"] == 0
    assert s.get_performance_metrics()["win_rate"] >= 0
