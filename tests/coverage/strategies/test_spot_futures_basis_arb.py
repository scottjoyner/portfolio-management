"""Coverage tests for the Spot-Futures Basis Arbitrage strategy."""
from __future__ import annotations

import math

from trading_system.strategies.arbitrage.spot_futures_basis_arb import (
    SpotFuturesBasisArbStrategy,
    SpotFuturesBasisArbConfig,
    SpotFuturesPosition,
)


def _bars(n, base=100.0, drift=0.0):
    out = []
    for i in range(n):
        c = base + drift * i
        out.append({"timestamp": i, "open": c, "high": c * 1.01,
                    "low": c * 0.99, "close": c, "volume": 1000 + i})
    return out


def _init(strategy, spot_drift=0.0, fut_drift=0.0, n=60):
    spot = _bars(n, base=100.0, drift=spot_drift)
    fut = _bars(n, base=100.0, drift=fut_drift)
    strategy.init(spot, fut)
    return strategy


def test_config_defaults():
    cfg = SpotFuturesBasisArbConfig()
    assert cfg.basis_threshold_pct == 1.5
    assert cfg.min_position_size_btc == 0.5
    s = SpotFuturesBasisArbStrategy(cfg)
    assert s.config is cfg


def test_position_dataclass():
    pos = SpotFuturesPosition(
        entry_spot_price=100.0, entry_futures_price=101.0,
        spot_quantity=1.0, futures_quantity=1.0,
        unrealized_pnl_pct=-5.0, stop_loss_hit=True,
        trailing_stop_hit=False, basis_at_entry_pct=1.0,
    )
    assert pos.unrealized_pnl_pct == -5.0
    assert pos.stop_loss_hit is True


def test_init_empty_spot_raises():
    s = SpotFuturesBasisArbStrategy()
    try:
        s.init([], [{"close": 1}])
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_init_empty_futures_raises():
    s = SpotFuturesBasisArbStrategy()
    try:
        s.init([{"close": 1}], [])
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_init_too_short_raises():
    s = SpotFuturesBasisArbStrategy()
    try:
        s.init(_bars(40), _bars(40))
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_init_valid():
    s = SpotFuturesBasisArbStrategy()
    _init(s)
    assert len(s.basis_values) == 60
    assert s.spot_position is None
    assert s.futures_position is None


def test_calculate_basis_empty():
    s = SpotFuturesBasisArbStrategy()
    assert s._calculate_basis([], [1, 2, 3]) == []
    assert s._calculate_basis([1, 2, 3], []) == []


def test_calculate_basis_zero_spot():
    s = SpotFuturesBasisArbStrategy()
    vals = s._calculate_basis([0.0, 100.0], [50.0, 110.0])
    # first spot price 0 -> basis 0.0 ; second -> (110-100)/100*100 = 10
    assert vals[0] == 0.0
    assert vals[1] == 10.0


def test_on_bar_missing_close():
    s = SpotFuturesBasisArbStrategy()
    _init(s)
    assert s.on_bar({"timestamp": 1, "volume": 10}) is None


def test_on_bar_nan_close():
    s = SpotFuturesBasisArbStrategy()
    _init(s)
    assert s.on_bar({"close": float("nan")}) is None


def test_on_bar_zero_close():
    s = SpotFuturesBasisArbStrategy()
    _init(s)
    assert s.on_bar({"close": 0.0}) is None


def test_on_bar_empty_basis():
    s = SpotFuturesBasisArbStrategy()
    # no init -> basis_values empty -> current_basis_pct 0 -> no signal
    assert s.on_bar({"close": 100.0}) is None


def test_on_bar_basis_below_threshold():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=0.0)  # basis ~0
    assert s.on_bar({"close": 100.0}) is None


def test_on_bar_contango_positive_basis():
    s = SpotFuturesBasisArbStrategy()
    # futures higher than spot -> positive basis
    _init(s, spot_drift=0.0, fut_drift=2.0)
    sig = s.on_bar({"close": 100.0})
    assert sig is not None
    assert sig["action"] == "BUY_LONG"
    assert sig["signal_type"] == "BASIS_CONVERGENCE_LONG_SPOT"
    assert sig["basis_pct"] > 0
    assert sig["stop_loss"] is not None


def test_on_bar_backwardation_negative_basis():
    s = SpotFuturesBasisArbStrategy()
    # futures lower than spot -> negative basis
    _init(s, spot_drift=2.0, fut_drift=0.0)
    sig = s.on_bar({"close": 100.0})
    assert sig is not None
    assert sig["action"] == "BUY_LONG"
    assert sig["signal_type"] == "BASIS_CONVERGENCE_LONG_FUTURES"
    assert sig["basis_pct"] < 0
    assert sig["stop_loss"] is not None


def test_on_bar_position_already_open():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    s.spot_position = object()  # pretend a position is open
    assert s.on_bar({"close": 100.0}) is None


def test_handle_signal_buy_long():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    sig = {
        "action": "BUY_LONG",
        "entry_spot_price": 100.0,
        "entry_futures_price": 102.0,
        "basis_pct": 2.0,
    }
    res = s.handle_signal(sig)
    assert res is None
    assert s.spot_position is not None
    assert s.spot_position.entry_spot_price == 100.0
    # futures_quantity divides by entry_futures_price
    assert s.spot_position.futures_quantity > 0


def test_handle_signal_close_position_profit():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    s.spot_position = SpotFuturesPosition(
        entry_spot_price=100.0, entry_futures_price=102.0,
        spot_quantity=1.0, futures_quantity=1.0,
        unrealized_pnl_pct=5.0,
    )
    res = s.handle_signal({"action": "CLOSE_POSITION"})
    assert res is None
    assert s.spot_position is None
    assert s.num_successful_trades == 1
    assert s.num_failed_trades == 0


def test_handle_signal_close_position_loss():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    s.spot_position = SpotFuturesPosition(
        entry_spot_price=100.0, entry_futures_price=102.0,
        spot_quantity=1.0, futures_quantity=1.0,
        unrealized_pnl_pct=-5.0,
    )
    s.handle_signal({"action": "CLOSE_POSITION"})
    assert s.spot_position is None
    assert s.num_failed_trades == 1
    assert s.num_successful_trades == 0


def test_handle_signal_close_without_position():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    # no spot_position -> inner block skipped
    assert s.handle_signal({"action": "CLOSE_POSITION"}) is None
    assert s.num_successful_trades == 0
    assert s.num_failed_trades == 0


def test_handle_signal_other_action():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    assert s.handle_signal({"action": "HOLD"}) is None


def test_get_current_position():
    s = SpotFuturesBasisArbStrategy()
    _init(s, spot_drift=0.0, fut_drift=2.0)
    assert s.get_current_position() is None
    s.spot_position = SpotFuturesPosition(
        entry_spot_price=100.0, entry_futures_price=102.0,
        spot_quantity=1.0, futures_quantity=1.0,
    )
    assert s.get_current_position() is s.spot_position


def test_performance_metrics_no_trades():
    s = SpotFuturesBasisArbStrategy()
    m = s.get_performance_metrics()
    assert m["total_signals"] == 0
    assert m["win_rate"] == 0.0
    assert m["successful_trades"] == 0
    assert m["failed_trades"] == 0


def test_performance_metrics_with_trades():
    s = SpotFuturesBasisArbStrategy()
    s.num_successful_trades = 3
    s.num_failed_trades = 1
    m = s.get_performance_metrics()
    assert m["total_signals"] == 4
    assert m["win_rate"] == 75.0
    assert m["successful_trades"] == 3
    assert m["failed_trades"] == 1
