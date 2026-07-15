from trading_system.strategies.trend.momentum_breakout import (
    SimpleMomentumBreakoutStrategy,
    MomentumPosition,
)


def _bars(n, close=100.0):
    return [
        {"timestamp": i, "open": close + i, "high": close + i + 2,
         "low": close + i - 2, "close": close + i}
        for i in range(n)
    ]


def test_momentum_breakout():
    s = SimpleMomentumBreakoutStrategy()
    # init validation.
    try:
        s.init([])
        assert False
    except ValueError:
        pass
    try:
        s.init(_bars(5))
        assert False
    except ValueError:
        pass

    s.init(_bars(25))
    # Override lookback bounds for deterministic control.
    s.lookback_high = 100.0
    s.lookback_low = 100.0

    # Invalid close.
    assert s.on_bar({"close": 0}) is None

    # Breakout above resistance -> BUY (no position).
    out = s.on_bar({"close": 200.0})
    assert out is not None and out["action"] == "BUY"
    s.handle_signal(out)
    assert s.position is not None
    entry = s.position.entry_price

    # Breakout below support -> SELL.
    s.lookback_low = 100.0
    out = s.on_bar({"close": 90.0})
    assert out is not None and out["action"] == "SELL"

    # Re-enter for the remaining exit paths.
    s.handle_signal({"action": "BUY", "entry_price": 200.0})
    # Hard stop-loss.
    out = s.on_bar({"close": 190.0})
    assert out is not None and out["signal_type"] == "STOP_LOSS_HIT"

    # Re-enter; trailing stop triggers on pullback from peak.
    s.handle_signal({"action": "BUY", "entry_price": 200.0})
    s.position.unrealized_pnl_pct = 2.5
    s._peak_pnl = 3.0
    out = s.on_bar({"close": 200.0})
    assert out is not None and out["signal_type"] == "TRAILING_STOP_EXIT"

    # Re-enter; trailing not armed (peak <= 2%) -> fall through to None.
    s.handle_signal({"action": "BUY", "entry_price": 200.0})
    s.position.unrealized_pnl_pct = 1.0
    s._peak_pnl = 1.0
    s.lookback_low = 50.0  # sell_threshold small so no breakout-below
    out = s.on_bar({"close": 200.0})
    assert out is None

    # handle_signal SELL with position.
    s.handle_signal({"action": "SELL", "entry_price": 210.0})
    assert s.num_successful_trades >= 1

    # Position helper methods.
    p = MomentumPosition(entry_price=100.0, entry_timestamp=0.0, quantity=1.0)
    p.calculate_unrealized_pnl(110.0)
    assert p.unrealized_pnl_pct == 10.0
    assert p.check_trailing_stop(100.0) is None

    # performance metrics.
    assert s.get_performance_metrics()["win_rate"] >= 0
    empty = SimpleMomentumBreakoutStrategy()
    empty.init(_bars(25))
    assert empty.get_performance_metrics()["total_signals"] == 0
