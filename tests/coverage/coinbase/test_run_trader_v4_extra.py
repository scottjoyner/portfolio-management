"""
Additional coverage tests for ``coinbase.src.run_trader_v4.EventTraderV4``.

Targets large blocks not exercised by test_run_trader_v4.py:
  * ``start()`` paper startup path
  * ``_seed_history`` / ``_candle_refresh_loop`` / ``_polling_loop``
  * background loop methods (run one iteration each, no network)
  * more ``_paper_execute_impl`` exit/entry branches
  * more ``_live_execute`` branches
  * ``_drain_ticker_cache`` scalping/order-flow/onchain paths

Infinite loops are run for exactly one iteration by making ``time.sleep`` raise
after the first call (the loop's ``while not self._shutdown`` guard then exits).
No real network calls: collaborators are mocked.
"""
import os
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

logging = __import__("logging")
logging.disable(logging.CRITICAL)

from tests.coverage.coinbase.test_run_trader_v4 import (  # noqa: E402
    _make_trader, _make_streaming, BaseV4, EventTraderV4, PaperPosition,
    CoreHolding, SimpleNamespace as _SN, MagicMock as _M, patch as _patch,
)


class _LoopBreak(Exception):
    pass


def _run_one_iteration(loop_fn):
    """Run an infinite ``while not self._shutdown`` loop for a single body pass."""
    state = {"n": 0}

    def fake_sleep(seconds=0.0):
        state["n"] += 1
        if state["n"] >= 2:
            raise _LoopBreak

    with _patch("time.sleep", side_effect=fake_sleep):
        try:
            loop_fn()
        except _LoopBreak:
            pass


class TestStart(unittest.TestCase):
    def test_start_paper(self):
        t = _make_trader(["BTC-USD", "ETH-USD"], dry_run=True)
        t._feed_mgr = None  # avoid background refresh thread / network
        # Force a clean startup validation path: no paper state file present.
        for p in t._paper_state_path.parent.glob("paper_trader_v4_state.json*"):
            try:
                p.unlink()
            except Exception:
                pass
        t._load_knobs = lambda: None
        t.start()
        self.assertEqual(t.mode, "paper")
        self.assertIn("products", t.health_status)
        t._shutdown = True  # let the background loop threads exit


class TestSeedAndLoops(BaseV4):
    def setUp(self):
        super().setUp()
        self.t = _make_trader(["BTC-USD"])

    def test_seed_history(self):
        with _patch("coinbase.src.run_trader_v4.get_all_coinbase_pairs",
                    return_value=[{"id": "BTC-USD", "base": "BTC"}]), \
             _patch("coinbase.src.run_trader_v4.fetch_candles_batch_sync",
                    return_value={"BTC-USD": [[0, 0, 101, 99, 100, 1] for _ in range(40)]}):
            self.t._seed_history()

    def test_candle_refresh_loop(self):
        with _patch("coinbase.src.run_trader_v4.fetch_candles_batch_sync",
                    return_value={"BTC-USD": [[0, 0, 101, 99, 100, 1] for _ in range(40)]}):
            _run_one_iteration(self.t._candle_refresh_loop)

    def test_polling_loop(self):
        self.t._drain_ticker_cache = MagicMock()
        self.t._ws_feed = None
        _run_one_iteration(self.t._polling_loop)

    def test_scan_loop(self):
        self.t.scan_interval = 300
        self.t._run_scan = MagicMock()
        _run_one_iteration(self.t._scan_loop)
        self.t._run_scan.assert_called()

    def test_minute_scan_loop(self):
        self.t._tighten_all_position_stops = MagicMock()
        self.t._dca_core_holdings = MagicMock()
        self.t._rebalance_core_holdings = MagicMock()
        self.t._run_scan = MagicMock()
        self.t._minute_scan_products = MagicMock(return_value=[("BTC-USD", "BTC")])
        _run_one_iteration(self.t._minute_scan_loop)

    def test_full_scan_loop(self):
        self.t._run_scan = MagicMock()
        _run_one_iteration(self.t._full_scan_loop)
        self.t._run_scan.assert_called()

    def test_news_sentiment_loop(self):
        self.t._news_sentiment_scan = MagicMock()
        _run_one_iteration(self.t._news_sentiment_loop)
        self.t._news_sentiment_scan.assert_called()

    def test_macro_risk_loop(self):
        self.t._macro_risk_scan = MagicMock()
        _run_one_iteration(self.t._macro_risk_loop)
        self.t._macro_risk_scan.assert_called()

    def test_pair_trade_loop(self):
        self.t._pair_trade_scan = MagicMock()
        _run_one_iteration(self.t._pair_trade_loop)
        self.t._pair_trade_scan.assert_called()

    def test_onchain_loop(self):
        self.t._onchain_flow_scan = MagicMock()
        _run_one_iteration(self.t._onchain_loop)
        self.t._onchain_flow_scan.assert_called()

    def test_funding_loop(self):
        self.t._funding_scan = MagicMock()
        _run_one_iteration(self.t._funding_loop)
        self.t._funding_scan.assert_called()

    def test_macro_tf_loop(self):
        self.t._macro_tf_scan = MagicMock()
        _run_one_iteration(self.t._macro_tf_loop)
        self.t._macro_tf_scan.assert_called()

    def test_perf_save_loop(self):
        self.t._perf_tracker.auto_disable = MagicMock(return_value=0)
        self.t._perf_tracker.auto_disable_strategies = MagicMock(return_value=0)
        self.t._perf_tracker.auto_enable_strategies = MagicMock(return_value=0)
        self.t._perf_tracker.divergence_report = MagicMock(return_value=[])
        self.t._perf_tracker.save = MagicMock()
        _run_one_iteration(self.t._perf_save_loop)

    def test_watchdog_loop(self):
        self.t._ws_feed = None
        self.t._last_ticker_ts = 0
        self.t._last_eval_ts = 0
        _run_one_iteration(self.t._watchdog_loop)


class TestDrainTickerCache(BaseV4):
    def setUp(self):
        super().setUp()
        self.t = _make_trader(["BTC-USD", "ETH-USD"])
        self.fs = _make_streaming(closes=[float(100 + i) for i in range(60)])
        self.fs.update = lambda *a, **k: None
        self.t.streaming = self.fs
        self.t._slice_cache = {}
        self.t._feed_mgr = None
        self.t._cross_asset_regime = None
        self.t._order_flow.evaluate = MagicMock(return_value=None)
        self.t._scalping.get_signals = MagicMock(return_value=None)
        self.t._onchain_flow.get_signals = MagicMock(return_value=None)
        fake_ticker = SimpleNamespace(price=100.0, volume_24h=1e9, bid=99.9, ask=100.1)
        self.t._ticker_cache = SimpleNamespace(get_ticker=MagicMock(return_value=fake_ticker))
        self.t._last_price["BTC-USD"] = 95.0
        self.t._last_price["ETH-USD"] = 95.0
        self.t.min_change_pct = 0.0

    def test_drain_full(self):
        with _patch("rust_core.evaluate_all_opens_py", return_value=[("ema_cross", "BUY", 0.7, "r")]), \
             _patch("coinbase.src.run_trader_v4.batch_backtest_rust",
                    return_value={"ema_cross/BTC": _verdict()}):
            self.t._drain_ticker_cache()

    def test_drain_with_scalping_signal(self):
        self.t._scalping.get_signals = MagicMock(return_value={
            "product_id": "BTC-USD", "action": "BUY", "confidence": 0.7,
            "win_rate": 0.6, "sharpe": 1.0, "strategy": "scalp", "atr_14": 1.0,
            "regime": "strong_uptrend"})
        with _patch("rust_core.evaluate_all_opens_py", return_value=[]), \
             _patch.object(self.t, "_get_slices",
                           return_value=([1.0] * 60, [1.0] * 60)), \
             _patch.object(self.t, "_paper_execute") as pe:
            self.t._drain_ticker_cache()
            self.assertTrue(self.t._scalping.get_signals.called)
            self.assertTrue(pe.called)

    def test_drain_with_order_flow(self):
        of_sig = SimpleNamespace(to_opportunity=lambda: {
            "product_id": "BTC-USD", "action": "SELL", "confidence": 0.6,
            "win_rate": 0.6, "sharpe": 1.0, "strategy": "of", "atr_14": 1.0,
            "regime": "strong_uptrend"})
        self.t._order_flow.evaluate = MagicMock(return_value=of_sig)
        with _patch("rust_core.evaluate_all_opens_py", return_value=[]), \
             _patch.object(self.t, "_paper_execute") as pe:
            self.t._drain_ticker_cache()
            self.assertTrue(self.t._order_flow.evaluate.called)
            self.assertTrue(pe.called)


def _verdict(passed=True, win_rate=0.6, sharpe=1.0):
    from strategy_engine import BacktestVerdict
    return BacktestVerdict(
        strategy="ema_cross", currency="BTC", total_trades=10, winning_trades=6,
        losing_trades=4, win_rate=win_rate, total_return_pct=5.0, sharpe_ratio=sharpe,
        profit_factor=1.5, max_drawdown_pct=2.0, regime="trending", passed=passed, reason="ok",
    )


class TestPaperExecuteBranches(BaseV4):
    def setUp(self):
        super().setUp()
        self.t = _make_trader(["BTC-USD"])
        self.t.streaming = _make_streaming(closes=[float(100 + i) for i in range(60)])
        self.t._perf_tracker.is_disabled = lambda *a, **k: False
        self.t._perf_tracker.is_strategy_disabled = lambda *a, **k: False

    def _opp(self, **kw):
        base = {"action": "BUY", "confidence": 0.8, "win_rate": 0.7, "sharpe": 1.0,
                "strategy": "ema_cross", "atr_14": 2.0, "regime": "strong_uptrend",
                "is_long_horizon": False, "leverage": 1.0, "stop_dist": 5.0}
        base.update(kw)
        return base

    def test_exit_multi_signal_consensus(self):
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp(action="SELL")] * 5)
        # no position -> buy path; now open then exit
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertIn("BTC-USD", self.t.paper_positions)
        pos = self.t.paper_positions["BTC-USD"]
        pos.entry_ts = time.time() - 100000
        # many SELL signals -> consensus exit
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp(action="SELL")] * 5)
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_exit_reverse_signal(self):
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        pos = self.t.paper_positions["BTC-USD"]
        pos.entry_ts = time.time() - 100000
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp(action="SELL")])
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_trailing_take_profit_exit(self):
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        pos = self.t.paper_positions["BTC-USD"]
        pos.entry_ts = time.time() - 100000
        pos.highest_price = 130.0
        # pull back to trailing take level
        self.t._paper_execute_impl("BTC-USD", 115.0, [self._opp()])
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_timeout_exit(self):
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.t.max_hold_s = 1
        pos = self.t.paper_positions["BTC-USD"]
        pos.entry_ts = time.time() - 100000
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_mean_reversion_gate_in_trend(self):
        # open a mean-reversion strategy in a trending regime -> skipped
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp(
            strategy="rsi_revert", regime="strong_uptrend")])
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_strategy_disabled_gate(self):
        self.t._perf_tracker.is_disabled = lambda *a, **k: True
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_global_strategy_disabled_gate(self):
        self.t._perf_tracker.is_disabled = lambda *a, **k: False
        self.t._perf_tracker.is_strategy_disabled = lambda *a, **k: True
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_pulse_penalty_branch(self):
        # Build a hot pulse so the pulse-aware penalty branch runs.
        for _ in range(4):
            self.t._record_pulse("BTC-USD", "ema_cross", "BUY", 0.8, 100.0)
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertIn("BTC-USD", self.t.paper_positions)

    def test_confluence_all_same_entry(self):
        # 3 identical BUY opps: confluence is met, so a position is opened.
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()] * 3)
        self.assertIn("BTC-USD", self.t.paper_positions)

    def test_short_entry_enabled(self):
        self.t.enable_shorts = True
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp(action="SELL")])
        self.assertIn("BTC-USD", self.t.paper_positions)
        self.assertTrue(self.t.paper_positions["BTC-USD"].is_short)

    def test_cluster_exposure_within_cap(self):
        # Put a position in the same cluster; cluster exposure is still under cap
        # so the new BTC-USD entry is allowed.
        self.t.paper_positions["ETH-USD"] = PaperPosition(
            product_id="ETH-USD", side="LONG", qty=100.0, entry_price=1.0,
            entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
            sharpe=0.5, entry_notional=100.0)
        self.t._paper_cash = 10000.0
        self.t._paper_equity = lambda prices=None: 10000.0
        # BTC-USD is in large_cap cluster with ETH; cap at 30%
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertIn("BTC-USD", self.t.paper_positions)


class TestLiveExecuteBranches(BaseV4):
    def setUp(self):
        super().setUp()
        self.t = _make_trader(["BTC-USD"])
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._bracket_mgr._brackets = {}
        self.t._risk_mgr = MagicMock()
        self.t._risk_mgr.check_trade.return_value = (True, "ok")
        self.t._strategy_ranker = None
        self.t._cb_peak_equity = 10000.0
        self.t._live_cfg = SimpleNamespace(
            min_confidence=0.4, max_daily_loss_pct=0.05, max_consecutive_losses=3,
            bracket_stop_atr_mult=2.5, bracket_target_atr_mult=4.0,
            risk_per_trade_pct=0.02, max_notional_per_trade_usd=10000.0)
        self.t._btc_momentum_multiplier = lambda: 1.0

    def _opp(self, **kw):
        base = {"action": "BUY", "confidence": 0.8, "win_rate": 0.7, "sharpe": 1.0,
                "strategy": "ema_cross", "atr_14": 2.0, "regime": "strong_uptrend",
                "is_long_horizon": False, "leverage": 1.0}
        base.update(kw)
        return base

    def test_buy_high_volatility_multipliers(self):
        self.t._bracket_mgr.place_bracket.return_value = {
            "entry_order": SimpleNamespace(success=True), "status": "ok"}
        self.t._live_execute("BTC-USD", 100.0, [self._opp(regime="high_volatility")])

    def test_notional_too_small(self):
        self.t._live_execute("BTC-USD", 100.0, [self._opp(confidence=0.45)])

    def test_confluence_insufficient(self):
        self.t._live_execute("BTC-USD", 100.0, [self._opp()] * 3)

    def test_sell_skipped_live(self):
        self.t._live_execute("BTC-USD", 100.0, [self._opp(action="SELL")])

    def test_risk_fail(self):
        self.t._risk_mgr.check_trade.return_value = (False, "no")
        self.t._live_execute("BTC-USD", 100.0, [self._opp()])

    def test_strategy_ranker_gate(self):
        ranker = MagicMock()
        ranker.get_rank.return_value = 200
        self.t._strategy_ranker = ranker
        self.t._live_execute("BTC-USD", 100.0, [self._opp()])

    def test_bracket_failure(self):
        self.t._bracket_mgr.place_bracket.return_value = {
            "entry_order": {"success": False}, "status": "FAILED"}


if __name__ == "__main__":
    unittest.main()
