"""Coverage for EventTraderV4._paper_execute_impl — exit logic + entry gating."""
from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from coinbase.src.run_trader_v4 import EventTraderV4, PaperPosition  # noqa: E402


def _mktrader(**kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    t = EventTraderV4(mode=mode, products=["BTC-USD", "ETH-USD", "SOL-USD"], **kw)
    t._feed_mgr = None
    t.paper_cash = 1_000_000.0
    t.paper_last_trade_ts = {}
    t.paper_positions = {}
    t._signal_pulses = {}
    t._portfolio_risk = None
    pt = MagicMock()
    pt.is_disabled.return_value = False
    pt.is_strategy_disabled.return_value = False
    pt.get.return_value = None
    pt.kelly.return_value = 0.0
    pt.strategy_aggregate.return_value = {"trades": 0, "win_rate": 0.0}
    t._perf_tracker = pt
    return t


def _pos(side="LONG", entry=100.0, qty=1.0, age=1000, **kw):
    p = PaperPosition(
        product_id="BTC-USD", side=side, qty=qty, entry_price=entry,
        entry_ts=time.time() - age, strategy=kw.get("strategy", "ema_cross"),
        confidence=kw.get("confidence", 0.7), win_rate=0.65, sharpe=0.9,
    )
    p.entry_notional = entry * qty
    p.highest_price = kw.get("high", entry)
    p.lowest_price = kw.get("low", entry)
    p.initial_stop_dist = kw.get("stop", 5.0)
    p.stop_price = kw.get("stop_price", 0.0)
    p.atr_14 = kw.get("atr", 5.0)
    p.regime = kw.get("regime", "")
    p.breakeven_set = kw.get("be", False)
    p.trailing_activated = False
    p.trailing_take_price = 0.0
    p.leverage = 1.0
    p.trades = 1
    return p


def _opp(action="BUY", strat="ema_cross", conf=0.7, wr=0.65, sh=0.9, atr=5.0,
         regime="strong_uptrend", edge=20.0):
    return {
        "action": action, "strategy": strat, "confidence": conf, "win_rate": wr,
        "sharpe": sh, "atr_14": atr, "regime": regime, "edge_bps": edge, "price": 100.0,
    }


def _accounting_trader():
    """Paper trader with deterministic fills and no persistent/runtime writes."""
    with patch.object(EventTraderV4, "_load_paper_state", return_value=None):
        t = _mktrader(enable_leverage=True, max_leverage=2.0)
    t.paper_cash = 10_000.0
    t.paper_realized_pnl = 0.0
    t.paper_fees_paid = 0.0
    t.paper_trades = []
    t.strategy_stats = {}
    t._signal_type_counts = {}
    t._save_paper_state = MagicMock()
    t._record_trade_event = MagicMock()
    t._push_notification = MagicMock()
    t._update_trailing_volume = MagicMock()
    t._paper_trade_notional = MagicMock(return_value=1_000.0)
    t._paper_score_multiplier = MagicMock(return_value=1.0)
    t._btc_momentum_multiplier = MagicMock(return_value=1.0)
    t._paper_edge_model = MagicMock(return_value={
        "probability": 0.7, "gross_bps": 200.0, "fee_bps": 100.0,
        "latency_bps": 0.0, "net_bps": 100.0,
    })
    t._fee_tier = MagicMock(return_value=(1, 100.0, 100.0))
    t._effective_fee_bps = MagicMock(return_value=100.0)
    t._fill_model = MagicMock()
    t._fill_model.is_maker.return_value = False
    t._fill_model.estimate.side_effect = lambda _pid, _side, _qty, price, _vol: SimpleNamespace(
        entry_price=price, exit_price=price, partial_fill_pct=1.0,
    )
    t._scalping = MagicMock()
    return t


def _assert_cash_invariant(t, starting_cash):
    open_margin = sum(p.entry_notional / max(p.leverage, 1.0)
                      for p in t.paper_positions.values())
    open_costs = sum(p.fees_paid + p.cum_funding
                     for p in t.paper_positions.values())
    expected = starting_cash + t.paper_realized_pnl - open_margin - open_costs
    assert t.paper_cash == pytest.approx(expected)


def test_execute_impl_drawdown_breaker():
    t = _mktrader()
    t._paper_drawdown = MagicMock(return_value=0.9)
    t._paper_open_position = MagicMock()
    t._paper_close_position = MagicMock()
    t._paper_execute_impl("BTC-USD", 100.0, [_opp()])
    t._paper_open_position.assert_not_called()
    t._paper_close_position.assert_not_called()


def test_execute_impl_multi_signal_exit_long():
    t = _mktrader()
    t.paper_positions["BTC-USD"] = _pos(high=100.0, low=100.0, stop=50, atr=1, regime="strong_uptrend")
    t._last_price = {"BTC-USD": 100.0}
    opps = [_opp("SELL", conf=0.3), _opp("SELL", conf=0.3), _opp("SELL", conf=0.3),
            _opp("BUY", conf=0.9), _opp("BUY", conf=0.4)]
    t._paper_execute_impl("BTC-USD", 100.0, opps)
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_reverse_exit_long():
    t = _mktrader()
    t.paper_positions["BTC-USD"] = _pos(high=100.0, low=100.0, stop=50, atr=1, regime="strong_uptrend")
    t._last_price = {"BTC-USD": 100.0}
    opps = [_opp("SELL", conf=0.95), _opp("BUY", conf=0.3)]
    t._paper_execute_impl("BTC-USD", 100.0, opps)
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_trailing_stop_long():
    t = _mktrader()
    t.paper_positions["BTC-USD"] = _pos(high=110.0, low=100.0, stop=5, atr=5, regime="strong_uptrend")
    t._last_price = {"BTC-USD": 104.0}
    t._paper_execute_impl("BTC-USD", 104.0, [_opp("BUY", conf=0.4)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_trailing_take_long():
    t = _mktrader()
    t.paper_positions["BTC-USD"] = _pos(high=120.0, low=100.0, stop=50, atr=1, regime="strong_uptrend")
    t._last_price = {"BTC-USD": 109.0}
    t._paper_execute_impl("BTC-USD", 109.0, [_opp("BUY", conf=0.4)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_age_stop_long():
    t = _mktrader()
    t.paper_positions["BTC-USD"] = _pos(high=100.0, low=99.0, stop=50, atr=1, regime="strong_uptrend", age=80000)
    t._last_price = {"BTC-USD": 90.0}
    t._paper_execute_impl("BTC-USD", 90.0, [_opp("BUY", conf=0.4)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_timeout_long():
    t = _mktrader()
    t.paper_positions["BTC-USD"] = _pos(high=100.0, low=100.0, stop=50, atr=1, regime="strong_uptrend", age=200000)
    t._last_price = {"BTC-USD": 100.0}
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY", conf=0.4)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_entry_buy():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY")])
    assert "BTC-USD" in t.paper_positions


def test_execute_impl_skip_unknown_regime_no_streaming():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t.streaming = MagicMock()
    t.streaming.try_get.return_value = None
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY", regime="unknown", atr=0.0)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_atr_zero_enough_streaming():
    # atr<=0 with a favorable (non-unfavorable) regime: the streaming-data
    # allowance at the entry gate permits the entry.
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}

    class _S:
        closes = [1.0] * 40
    t.streaming = MagicMock()
    t.streaming.try_get.return_value = _S()
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY", regime="strong_uptrend", atr=0.0)])
    assert "BTC-USD" in t.paper_positions


def test_execute_impl_atr_zero_no_streaming():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t.streaming = MagicMock()
    t.streaming.try_get.return_value = None
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY", regime="strong_uptrend", atr=0.0)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_mean_reversion_in_trend():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY", strat="rsi_revert", regime="strong_uptrend")])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_disabled_strategy():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t._perf_tracker.is_disabled.return_value = True
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY")])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_global_disabled_strategy():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t._perf_tracker.is_strategy_disabled.return_value = True
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY")])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_insufficient_confluence():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    opps = [_opp("BUY", strat="ema_cross"), _opp("SELL", strat="rsi_revert"),
            _opp("SELL", strat="zscore_revert")]
    t._paper_execute_impl("BTC-USD", 100.0, opps)
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_cluster_exposure_skip():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0, "XRP-USD": 1.0, "ADA-USD": 1.0}
    existing = _pos(side="LONG", entry=1.0, qty=1e9, high=1.0, low=1.0, stop=1, atr=1, regime="strong_uptrend")
    existing.product_id = "XRP-USD"
    t.paper_positions["XRP-USD"] = existing
    t._correlation_clusters = {"large_cap": {"XRP", "ADA", "DOGE"}}
    t._max_cluster_exposure_pct = 0.30
    t._paper_execute_impl("ADA-USD", 1.0, [_opp("BUY", strat="ema_cross")])
    assert "ADA-USD" not in t.paper_positions


def test_execute_impl_short_macro_soften():
    t = _mktrader(enable_shorts=True)
    t._last_price = {"BTC-USD": 100.0}
    macro = SimpleNamespace(allows_new_shorts=False, bias="bullish", confidence=0.6)
    t._last_macro_signal = macro
    opp = _opp("SELL")
    t._paper_execute_impl("BTC-USD", 100.0, [opp])
    assert "BTC-USD" in t.paper_positions
    assert opp["confidence"] < 0.7


def test_execute_impl_pulse_penalty():
    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    t._signal_pulses["BTC-USD:ema_cross:BUY"] = SimpleNamespace(pulse_count=5, age_s=10)
    opp = _opp("BUY")
    t._paper_execute_impl("BTC-USD", 100.0, [opp])
    assert opp["confidence"] < 0.7


def test_execute_impl_leverage():
    t = _mktrader(enable_leverage=True)
    t._last_price = {"BTC-USD": 100.0}
    opp = _opp("BUY")
    t._paper_execute_impl("BTC-USD", 100.0, [opp])
    assert "leverage" in opp


def test_execute_impl_scale_in():
    t = _mktrader()
    pos = _pos(high=110.0, low=100.0, stop=10, atr=5, regime="strong_uptrend", entry=100.0, qty=1.0)
    t.paper_positions["BTC-USD"] = pos
    t._last_price = {"BTC-USD": 110.0}
    before = pos.qty
    t._paper_execute_impl("BTC-USD", 110.0, [_opp("BUY")])
    assert pos.qty > before


def test_execute_impl_trailing_stop_short():
    t = _mktrader(enable_shorts=True)
    t.paper_positions["BTC-USD"] = _pos(side="SHORT", entry=100.0, low=90.0, high=100.0,
                                        stop=5, atr=5, regime="strong_downtrend")
    t._last_price = {"BTC-USD": 96.0}
    t._paper_execute_impl("BTC-USD", 96.0, [_opp("SELL", conf=0.3)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_age_stop_short():
    t = _mktrader(enable_shorts=True)
    t.paper_positions["BTC-USD"] = _pos(side="SHORT", entry=100.0, low=100.0, high=100.0,
                                        stop=50, atr=1, regime="strong_downtrend", age=80000)
    t._last_price = {"BTC-USD": 111.0}
    t._paper_execute_impl("BTC-USD", 111.0, [_opp("SELL", conf=0.3)])
    assert "BTC-USD" not in t.paper_positions


def test_execute_impl_scale_in_short():
    t = _mktrader(enable_shorts=True)
    pos = _pos(side="SHORT", entry=100.0, low=90.0, high=100.0, stop=10, atr=5,
               regime="strong_downtrend", qty=1.0)
    t.paper_positions["BTC-USD"] = pos
    t._last_price = {"BTC-USD": 95.0}
    before = pos.qty
    t._paper_execute_impl("BTC-USD", 95.0, [_opp("SELL")])
    assert pos.qty > before


@pytest.mark.parametrize(("action", "scale_price"), [("BUY", 110.0), ("SELL", 90.0)])
def test_open_scale_close_preserves_cash_invariant(action, scale_price):
    t = _accounting_trader()
    starting_cash = t.paper_cash
    opp = _opp(action, regime="strong_uptrend" if action == "BUY" else "strong_downtrend")
    opp["leverage"] = 2.0

    t._paper_open_position("BTC-USD", 100.0, opp)
    pos = t.paper_positions["BTC-USD"]
    pos.initial_stop_dist = 5.0
    _assert_cash_invariant(t, starting_cash)

    t._last_price = {"BTC-USD": scale_price}
    t._paper_execute_impl("BTC-USD", scale_price, [opp])
    assert pos.trades == 2
    assert pos.fees_paid == pytest.approx(15.0)
    _assert_cash_invariant(t, starting_cash)

    t._paper_close_position(pos, scale_price, "accounting-test")
    t.paper_positions.pop("BTC-USD")
    _assert_cash_invariant(t, starting_cash)
    assert t._perf_tracker.record_trade.call_args.kwargs == {
        "backtest_win_rate": 0.65,
        "regime": t.health_status.get("market_regime", "unknown"),
    }


def test_trade_events_persisted_to_feed_cache():
    """A paper entry then exit must write labeled records to feed_cache
    (trade_events/<PRODUCT>.jsonl) so the run doubles as a backtest data factory."""
    import os
    import sys
    import tempfile

    root = tempfile.mkdtemp(prefix="trade_event_test_")
    os.environ["NAS_FEED_ROOT"] = root
    import data.feed_cache as fc
    # Point feed_cache at the temp root for this test without disturbing imports.
    fc._RESOLVED_ROOT = root
    import data.feed_cache as _fc
    _fc._RESOLVED_ROOT = root

    t = _mktrader()
    t._last_price = {"BTC-USD": 100.0}
    # entry
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("BUY", conf=0.7)])
    assert "BTC-USD" in t.paper_positions
    # exit via trailing timeout
    pos = t.paper_positions["BTC-USD"]
    pos.entry_ts = time.time() - 80000  # force age stop
    t._last_price = {"BTC-USD": 100.0}
    t._paper_execute_impl("BTC-USD", 100.0, [_opp("SELL", conf=0.3)])
    assert "BTC-USD" not in t.paper_positions

    from data.feed_cache import load_records
    events = load_records("trade_events", "BTC-USD")
    kinds = [e["kind"] for e in events]
    assert "entry" in kinds
    assert "exit" in kinds
    entry = next(e for e in events if e["kind"] == "entry")
    assert entry["side"] == "LONG"
    assert entry["strategy"]
    exit_ev = next(e for e in events if e["kind"] == "exit")
    assert "pnl" in exit_ev and "reason" in exit_ev
