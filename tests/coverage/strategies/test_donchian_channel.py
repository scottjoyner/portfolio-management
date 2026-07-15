import math

from trading_system.strategies.trend.donchian_channel import (
    DonchianChannelTrendStrategy,
    DonchianChannelConfig,
)


def _bars(n, start=100.0, step=1.0):
    return [
        {"timestamp": i, "open": start + i * step, "high": start + i * step + 1,
         "low": start + i * step - 1, "close": start + i * step, "volume": 100.0}
        for i in range(n)
    ]


def test_donchian_channel():
    # init validation: empty / too short.
    s = DonchianChannelTrendStrategy()
    try:
        s.init([])
        assert False, "expected ValueError"
    except ValueError:
        pass
    try:
        s.init(_bars(5))
        assert False, "expected ValueError"
    except ValueError:
        pass

    # _calculate_donchian_bands empty path.
    assert s._calculate_donchian_bands([], []) == ([], [])

    # Normal init.
    data = _bars(25)
    s.init(data)
    assert s.high_values  # warm-up + full window covered

    # on_bar before init (high_values empty) -> else branch.
    s2 = DonchianChannelTrendStrategy()
    out = s2.on_bar({"close": 50, "high": 50, "low": 49, "volume": 100})
    assert out is None

    # Invalid close.
    assert s.on_bar({"close": 0}) is None
    assert s.on_bar({"close": float("nan")}) is None

    # No breakout -> None.  (Also seeds the rolling volume buffer.)
    base = data[-1]
    assert s.on_bar({"close": base["close"], "high": base["high"],
                     "low": base["low"], "volume": 100}) is None

    # Breakout with high volume -> BUY (volume_confirmed True; avg now ~100).
    out = s.on_bar({"close": 500, "high": 500, "low": 499, "volume": 100000})
    assert out is not None and out["action"] == "BUY"
    assert out["signal_type"] == "DONCHIAN_UPPER_BAND_BREAKOUT"

    # Breakout with low volume -> BUY but VOLUME_FILTERED.
    out2 = s.on_bar({"close": 500, "high": 500, "low": 499, "volume": 1})
    assert out2 is not None and out2["action"] == "BUY"
    assert out2["signal_type"].endswith("VOLUME_FILTERED")

    # handle_signal BUY -> position; SELL positive / negative pnl.
    s.handle_signal({"action": "BUY", "entry_price": 500})
    assert s.get_current_position() is not None
    s.handle_signal({"action": "SELL", "entry_price": 510})
    assert s.num_successful_trades == 1
    s.handle_signal({"action": "BUY", "entry_price": 500})
    s.handle_signal({"action": "SELL", "entry_price": 490})
    assert s.num_failed_trades == 1

    # performance metrics both branches.
    assert s.get_performance_metrics()["win_rate"] >= 0
    empty = DonchianChannelTrendStrategy()
    empty.init(_bars(25))
    assert empty.get_performance_metrics()["total_signals"] == 0
