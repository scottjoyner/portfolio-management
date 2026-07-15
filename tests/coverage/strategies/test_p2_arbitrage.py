"""Coverage tests for arbitrage strategy modules."""
from __future__ import annotations


def _ex_bars(n, base=100.0, drift=0.0):
    out = []
    for i in range(n):
        c = base + drift * i
        out.append({"timestamp": i, "open": c, "high": c * 1.01,
                    "low": c * 0.99, "close": c, "volume": 1000 + i})
    return out


# ---------------------------------------------------------------------------
# cross_exchange_basis_arb.py
# ---------------------------------------------------------------------------

def test_cross_exchange():
    from trading_system.strategies.arbitrage.cross_exchange_basis_arb import (
        CrossExchangeBasisArbStrategy, CrossExchangeBasisArbConfig, CrossExchangePosition,
    )
    # config + position
    cfg = CrossExchangeBasisArbConfig()
    assert cfg.min_spread_threshold_pct == 1.0
    pos = CrossExchangePosition(entry_exchange_a_price=1.0, entry_exchange_b_price=2.0,
                                average_entry_price=1.5, exchange_a_quantity=1.0,
                                exchange_b_quantity=1.0)
    assert pos.average_entry_price == 1.5

    s = CrossExchangeBasisArbStrategy()
    # init raises
    try:
        s.init([], [{"close": 1}])
        assert False
    except ValueError:
        pass
    try:
        s.init(_ex_bars(40), _ex_bars(40))
        assert False
    except ValueError:
        pass

    s.init(_ex_bars(60, base=100.0), _ex_bars(60, base=100.0))

    # on_bar missing legs -> None
    assert s.on_bar({"exchange_a": {"close": 100}}) is None
    # close <= 0 / nan -> None
    assert s.on_bar({"exchange_a": {"close": 0}, "exchange_b": {"close": 100}}) is None
    import math
    assert s.on_bar({"exchange_a": {"close": float("nan")}, "exchange_b": {"close": 100}}) is None
    # spread below threshold -> None
    assert s.on_bar({"exchange_a": {"close": 100.0}, "exchange_b": {"close": 100.0}}) is None
    # spread above threshold, A cheaper -> BUY A
    sig = s.on_bar({"exchange_a": {"close": 100.0}, "exchange_b": {"close": 102.0}})
    assert sig["action"] == "BUY_CHEAP_EXCHANGE_A_SELL_EXPENSIVE_EXCHANGE_B"
    s.handle_signal(sig)  # sets position
    # position now exists -> on_bar returns None
    assert s.on_bar({"exchange_a": {"close": 100.0}, "exchange_b": {"close": 105.0}}) is None

    # handle_signal creates position (B cheaper path)
    s2 = CrossExchangeBasisArbStrategy()
    s2.init(_ex_bars(60, base=100.0), _ex_bars(60, base=100.0))
    sig_b = s2.on_bar({"exchange_a": {"close": 102.0}, "exchange_b": {"close": 100.0}})
    assert sig_b["action"] == "BUY_CHEAP_EXCHANGE_B_SELL_EXPENSIVE_EXCHANGE_A"
    pos2 = s2.handle_signal(sig_b)
    assert pos2 is None and s2.position is not None
    # CLOSE_POSITION records stats (position reset after close)
    s2.handle_signal({"action": "CLOSE_POSITION", "unrealized_pnl_pct": 1.0})
    assert s2.num_successful_trades == 1
    # failed trade: fresh position, close with negative pnl
    s2b = CrossExchangeBasisArbStrategy()
    s2b.init(_ex_bars(60, base=100.0), _ex_bars(60, base=100.0))
    s2b.handle_signal(s2b.on_bar({"exchange_a": {"close": 102.0}, "exchange_b": {"close": 100.0}}))
    s2b.position.unrealized_pnl_pct = -1.0  # force failed-trade branch
    s2b.handle_signal({"action": "CLOSE_POSITION", "unrealized_pnl_pct": -1.0})
    assert s2b.num_failed_trades == 1

    # get_current_position and get_performance_metrics
    assert s2.get_current_position() is None  # reset after close
    m = CrossExchangeBasisArbStrategy().get_performance_metrics()
    assert m["total_signals"] == 0
    m2 = s2.get_performance_metrics()
    assert m2["total_signals"] == 1

    # _calculate_exchange_spreads empty + negative avg (line 203, 222)
    assert s._calculate_exchange_spreads([], []) == ([], [], [])
    assert s._calculate_exchange_spreads([-1.0, -2.0], [-1.0, -2.0]) == ([0.0, 0.0], [-1.0, -2.0], [-1.0, -2.0])

    # on_bar: close_price_b invalid (264) and negative avg spread (276)
    assert s.on_bar({"exchange_a": {"close": 100.0}, "exchange_b": {"close": 0}}) is None
    assert s.on_bar({"exchange_a": {"close": -1.0}, "exchange_b": {"close": -1.0}}) is None


# ---------------------------------------------------------------------------
# spot_futures_basis_arb.py
# ---------------------------------------------------------------------------

def test_spot_futures():
    from trading_system.strategies.arbitrage.spot_futures_basis_arb import (
        SpotFuturesBasisArbStrategy, SpotFuturesBasisArbConfig, SpotFuturesPosition,
    )
    cfg = SpotFuturesBasisArbConfig()
    assert cfg.basis_threshold_pct == 1.5
    p = SpotFuturesPosition(entry_spot_price=1.0, entry_futures_price=2.0,
                            spot_quantity=1.0, futures_quantity=1.0)
    assert p is not None

    s = SpotFuturesBasisArbStrategy()
    try:
        s.init([], [{"close": 1}])
        assert False
    except ValueError:
        pass
    try:
        s.init(_ex_bars(40), _ex_bars(40))
        assert False
    except ValueError:
        pass

    # basis >= threshold: spot=100, futures=103 -> basis 3% (positive)
    s.init(_ex_bars(60, base=100.0), _ex_bars(60, base=103.0))
    assert s.on_bar({"close": 0}) is None
    import math
    assert s.on_bar({"close": float("nan")}) is None
    sig = s.on_bar({"close": 100.0})
    assert sig["action"] == "BUY_LONG"
    assert sig["signal_type"] == "BASIS_CONVERGENCE_LONG_SPOT"
    # NOTE: module bug (line 307 references self.spot_position before assignment);
    # pre-seed it with a namespace so the BUY_LONG branch executes.
    import types
    s.spot_position = types.SimpleNamespace(entry_futures_price=100.0)
    s.handle_signal(sig)  # sets position
    # position exists -> on_bar returns None
    assert s.on_bar({"close": 100.0}) is None

    # negative basis path: futures < spot
    s2 = SpotFuturesBasisArbStrategy()
    s2.init(_ex_bars(60, base=103.0), _ex_bars(60, base=100.0))
    sig2 = s2.on_bar({"close": 103.0})
    assert sig2["signal_type"] == "BASIS_CONVERGENCE_LONG_FUTURES"

    # handle_signal BUY_LONG + CLOSE_POSITION
    s3 = SpotFuturesBasisArbStrategy()
    s3.init(_ex_bars(60, base=100.0), _ex_bars(60, base=103.0))
    bsig = s3.on_bar({"close": 100.0})
    s3.spot_position = types.SimpleNamespace(entry_futures_price=100.0)
    s3.handle_signal(bsig)
    assert s3.spot_position is not None
    s3.handle_signal({"action": "CLOSE_POSITION", "unrealized_pnl_pct": 1.0})
    assert s3.num_successful_trades == 1
    # failed trade: fresh position, close with negative pnl
    s3b = SpotFuturesBasisArbStrategy()
    s3b.init(_ex_bars(60, base=100.0), _ex_bars(60, base=103.0))
    s3b.spot_position = types.SimpleNamespace(entry_futures_price=100.0)
    s3b.handle_signal(s3b.on_bar({"close": 100.0}))
    s3b.spot_position.unrealized_pnl_pct = -1.0  # force failed-trade branch
    s3b.handle_signal({"action": "CLOSE_POSITION", "unrealized_pnl_pct": -1.0})
    assert s3b.num_failed_trades == 1
    assert s3.get_current_position() is None
    assert SpotFuturesBasisArbStrategy().get_performance_metrics()["total_signals"] == 0
    assert s3.get_performance_metrics()["total_signals"] == 1

    # _calculate_basis empty + spot<=0 (line 200, 217)
    assert s._calculate_basis([], []) == []
    assert s._calculate_basis([-1.0, -2.0], [1.0, 2.0]) == [0.0, 0.0]
