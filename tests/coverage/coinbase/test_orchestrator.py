"""Coverage tests for coinbase/src/orchestrator.py (ExecutionOrchestrator)."""

import os
import unittest
from types import SimpleNamespace
from unittest import mock

import coinbase.src.orchestrator as O
from coinbase.src.orchestrator import ExecutionOrchestrator, TradeMode, TradeSignal
from coinbase.src.protocols import Direction, InstrumentType, Opportunity


def make_opp(**kw):
    base = dict(
        product_id="BTC-USD",
        direction=Direction.LONG,
        instrument_type=InstrumentType.SPOT,
        entry_price=100.0,
        stop_price=90.0,
        target_price=120.0,
        risk_reward=2.0,
        confidence=0.8,
        reason="test",
        strategy_name="rsi_revert",
        base_size=1.0,
        atr=5.0,
        score=0.9,
        total_risk_pct=0.01,
        meta={},
    )
    base.update(kw)
    return Opportunity(**base)


def make_sig(strat="rsi_revert", direction=Direction.LONG, confidence=0.99, size=1.0,
             strategy_name=None):
    return TradeSignal(
        product_id="BTC-USD", direction=direction,
        entry_price=100, stop_price=90, target_price=120,
        size=size, confidence=confidence, reason="r",
        strategy_name=strategy_name or strat,
    )


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestEnvHelpers(unittest.TestCase):
    def test_env_bool(self):
        with mock.patch.dict(os.environ, {"X_Y": "true"}):
            self.assertTrue(ExecutionOrchestrator._env_bool("X_Y", False))
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(ExecutionOrchestrator._env_bool("X_Y", False))
            self.assertTrue(ExecutionOrchestrator._env_bool("X_Y", True))

    def test_env_float(self):
        with mock.patch.dict(os.environ, {"X_F": "3.5"}):
            self.assertEqual(ExecutionOrchestrator._env_float("X_F", 1.0), 3.5)
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(ExecutionOrchestrator._env_float("X_F", 1.0), 1.0)
        with mock.patch.dict(os.environ, {"X_F": "bad"}):
            self.assertEqual(ExecutionOrchestrator._env_float("X_F", 2.0), 2.0)

    def test_env_int(self):
        with mock.patch.dict(os.environ, {"X_I": "7"}):
            self.assertEqual(ExecutionOrchestrator._env_int("X_I", 1), 7)
        with mock.patch.dict(os.environ, {"X_I": "bad"}):
            self.assertEqual(ExecutionOrchestrator._env_int("X_I", 3), 3)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestConstruction(unittest.TestCase):
    def test_paper_no_cb(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None, dry_run=True)
        self.assertIsNone(o.cb)
        self.assertIsNone(o.exec_engine)
        self.assertEqual(o.mode, TradeMode.PAPER)

    def test_live_no_cb_falls_back(self):
        with mock.patch("coinbase.src.orchestrator.CBClient", side_effect=RuntimeError("no")):
            o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=None, dry_run=True)
        self.assertEqual(o.mode, TradeMode.PAPER)
        self.assertIsNotNone(o.exec_engine)

    def test_futures_dry_run_no_exec(self):
        o = ExecutionOrchestrator(mode=TradeMode.FUTURES, dry_run=True)
        self.assertIsNone(o.futures_exec)

    def test_futures_live_creates_exec(self):
        with mock.patch.dict(os.environ, {
            "COINBASE_API_KEY": "k", "COINBASE_API_SECRET": "s",
        }):
            with mock.patch("coinbase.src.futures_execution.CoinbaseFuturesExecutor") as FE:
                inst = FE.return_value
                inst.validate.return_value = None
                o = ExecutionOrchestrator(mode=TradeMode.FUTURES, dry_run=False)
                self.assertIsNotNone(o.futures_exec)
                self.assertTrue(inst.validate.called)

    def test_futures_live_validate_fails(self):
        with mock.patch.dict(os.environ, {
            "COINBASE_API_KEY": "k", "COINBASE_API_SECRET": "s",
        }):
            with mock.patch("coinbase.src.futures_execution.CoinbaseFuturesExecutor") as FE:
                inst = FE.return_value
                inst.validate.side_effect = RuntimeError("bad")
                with self.assertRaises(RuntimeError):
                    ExecutionOrchestrator(mode=TradeMode.FUTURES, dry_run=False)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestKillSwitch(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)

    def test_kill_switch_env(self):
        with mock.patch.dict(os.environ, {"TRADER_KILL_SWITCH": "true"}):
            self.assertTrue(self.o._kill_switch_active())

    def test_kill_switch_path(self):
        import tempfile
        d = tempfile.mkdtemp()
        p = os.path.join(d, "kill")
        with mock.patch.dict(os.environ, {"TRADER_KILL_SWITCH_PATH": p}, clear=False):
            self.assertFalse(self.o._kill_switch_active())
            open(p, "w").close()
            self.assertTrue(self.o._kill_switch_active())

    def test_blocked_result(self):
        r = self.o._blocked_result(make_sig(), "kill_switch")
        self.assertEqual(r["status"], "blocked")
        self.assertEqual(r["reason"], "kill_switch")
        self.assertEqual(r["side"], "LONG")


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestEdgeEstimate(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)

    def test_edge_arbitrage(self):
        self.assertIsInstance(
            self.o._estimated_live_edge_bps(make_sig(strat="arb")), float)

    def test_edge_mean_reversion(self):
        self.assertIsInstance(
            self.o._estimated_live_edge_bps(make_sig(strat="rsi_revert")), float)

    def test_edge_prediction_market(self):
        self.assertIsInstance(
            self.o._estimated_live_edge_bps(make_sig(strat="kalshi")), float)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestPreExecBlock(unittest.TestCase):
    def test_paper_no_block(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        self.assertIsNone(o._pre_execution_block_reason(make_sig()))

    def test_kill_switch_block(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        with mock.patch.object(o, "_kill_switch_active", return_value=True):
            self.assertEqual(o._pre_execution_block_reason(make_sig()), "kill_switch")

    def test_max_order_block(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        with mock.patch.dict(os.environ, {"MAX_NOTIONAL_PER_TRADE_USD": "10"}, clear=False):
            self.assertEqual(o._pre_execution_block_reason(
                make_sig(size=1000.0)), "max_order_notional")

    def test_challenge_only_block(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        sig = make_sig(strategy_name="s")
        sig.bucket_id = "other"
        with mock.patch.dict(os.environ, {"TRADER_LIVE_CHALLENGE_ONLY": "true"}, clear=False):
            self.assertEqual(o._pre_execution_block_reason(sig), "bucket_not_allowed")

    def _live_orchestrator(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o._available_live_cash_usd = lambda: 1e9
        o.live_allow_short = True
        o.live_max_open_positions = 100
        o.live_max_order_usd = 1e9
        o.live_max_total_notional_usd = 1e9
        return o

    def test_live_shorts_disabled(self):
        o = self._live_orchestrator()
        o.live_allow_short = False
        self.assertEqual(o._pre_execution_block_reason(
            make_sig(direction=Direction.SHORT)), "shorts_disabled_for_live_spot")

    def test_live_min_confidence(self):
        o = self._live_orchestrator()
        self.assertEqual(o._pre_execution_block_reason(
            make_sig(confidence=0.5)), "min_live_confidence")

    def test_live_insufficient_edge(self):
        o = self._live_orchestrator()
        with mock.patch.object(o, "_estimated_live_edge_bps", return_value=-5.0):
            self.assertEqual(o._pre_execution_block_reason(make_sig()), "insufficient_live_edge")

    def test_live_insufficient_cash(self):
        o = self._live_orchestrator()
        o._available_live_cash_usd = lambda: 10.0
        o.live_min_cash_reserve_usd = 100.0
        self.assertEqual(o._pre_execution_block_reason(make_sig()), "insufficient_live_cash")

    def test_live_max_open_positions(self):
        o = self._live_orchestrator()
        o.live_max_open_positions = 0
        self.assertEqual(o._pre_execution_block_reason(make_sig()), "live_max_open_positions")

    def test_live_max_order_usd(self):
        o = self._live_orchestrator()
        o.live_max_order_usd = 10.0
        self.assertEqual(o._pre_execution_block_reason(make_sig(size=100.0)), "live_max_order_usd")

    def test_live_max_total_notional(self):
        o = self._live_orchestrator()
        o.live_max_open_positions = 100
        o.live_max_total_notional_usd = 10.0
        o.state.open_positions["BTC-USD"] = {"size": 1, "entry": 100}
        self.assertEqual(o._pre_execution_block_reason(make_sig(size=100.0)),
                         "live_max_total_notional_usd")

    def test_live_ok(self):
        o = self._live_orchestrator()
        self.assertIsNone(o._pre_execution_block_reason(make_sig()))


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestAvailableCash(unittest.TestCase):
    def test_no_cb(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=None, dry_run=False)
        o.cb = None
        self.assertIsNone(o._available_live_cash_usd())

    def test_with_cb_usd(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o.cb.list_accounts.return_value = {"accounts": [
            {"currency": "USD", "available_balance": {"value": "5000"}, "available": "1"},
        ]}
        self.assertEqual(o._available_live_cash_usd(), 5000.0)

    def test_with_cb_data_key(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o.cb.list_accounts.return_value = {"data": [
            {"currency": "USD", "balance": "200"},
        ]}
        self.assertEqual(o._available_live_cash_usd(), 200.0)

    def test_with_cb_list(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o.cb.list_accounts.return_value = [{"currency": "USD", "available": "77"}]
        self.assertEqual(o._available_live_cash_usd(), 77.0)

    def test_no_usd(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o.cb.list_accounts.return_value = [{"currency": "ETH", "available": "1"}]
        self.assertIsNone(o._available_live_cash_usd())

    def test_exception(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o.cb.list_accounts.side_effect = RuntimeError("x")
        self.assertIsNone(o._available_live_cash_usd())


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestStateMethods(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)

    def test_set_risk_appetite(self):
        ctrl = mock.Mock()
        self.o.set_risk_appetite(ctrl)
        self.assertEqual(self.o.risk_appetite, ctrl)

    def test_set_liquidity_24h(self):
        self.o.liquidity_sizer.set_volume_24h = mock.Mock()
        self.o.set_liquidity_24h("BTC-USD", 123.0)
        self.o.liquidity_sizer.set_volume_24h.assert_called_with("BTC-USD", 123.0)

    def test_register_listener(self):
        fn = mock.Mock()
        self.o.register_listener(fn)
        self.assertIn(fn, self.o._listeners)

    def test_update_state(self):
        ctrl = mock.Mock()
        self.o.set_risk_appetite(ctrl)
        self.o.update_state(equity=1000.0, cash=900.0, open_positions={"BTC-USD": {}})
        self.assertEqual(self.o.state.equity, 1000.0)
        self.assertTrue(ctrl.update_equity.called)

    def test_apply_risk_appetite_profile(self):
        ctrl = mock.Mock()
        ctrl.get_profile.return_value = mock.Mock()
        self.o.set_risk_appetite(ctrl)
        self.o._apply_risk_appetite_profile()
        self.assertTrue(ctrl.get_profile.called)

    def test_update_market_profile(self):
        self.o.market_selector.evaluate = mock.Mock()
        prof = mock.Mock()
        self.o.update_market_profile(prof)
        self.assertTrue(self.o.market_selector.evaluate.called)

    def test_live_open_notional(self):
        self.o.state.open_positions = {
            "BTC-USD": {"size": 2, "entry": 100},
            "ETH-USD": {"size": 1, "entry": 50},
        }
        self.assertEqual(self.o._live_open_notional(), 250.0)

    def test_daily_reset(self):
        self.o.daily_reset()
        self.assertEqual(self.o.state.daily_trades, 0)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestProcessOpportunities(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        self.o.market_selector.is_enabled = mock.Mock(return_value=True)
        self.o.ranking_filter._ranking.rebalance_weights = mock.Mock(return_value={"rsi_revert": 0.5})
        self.o.ranking_filter._ranking.top_strategies = mock.Mock(return_value=["rsi_revert"])
        self.o.risk_mgr.check_trade = mock.Mock(return_value=(True, "ok"))

    def test_portfolio_check_fail(self):
        self.o.risk_mgr.check_portfolio = mock.Mock(
            return_value=SimpleNamespace(passed_checks=False, failures=["x"]))
        self.assertEqual(self.o.process_opportunities([make_opp()]), [])

    def test_market_selector_disabled_skipped(self):
        self.o.market_selector.is_enabled = mock.Mock(return_value=False)
        self.assertEqual(self.o.process_opportunities([make_opp()]), [])

    def test_ranking_filter_active(self):
        out = self.o.process_opportunities([make_opp()])
        self.assertEqual(len(out), 1)

    def test_news_adjust(self):
        self.o.news_risk.assess_product = mock.Mock(
            return_value=SimpleNamespace(article_count=2))
        out = self.o.process_opportunities([make_opp(), make_opp(strategy_name="other")])
        self.assertEqual(len(out), 1)

    def test_risk_check_fail(self):
        self.o.risk_mgr.check_trade = mock.Mock(return_value=(False, "bad"))
        self.assertEqual(self.o.process_opportunities([make_opp()]), [])

    def test_success(self):
        self.o.risk_mgr.check_trade = mock.Mock(return_value=(True, "ok"))
        out = self.o.process_opportunities([make_opp()])
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], TradeSignal)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestExecuteSignals(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)

    def _sig(self, **kw):
        return make_sig(**kw)

    def test_paper_execute(self):
        results = self.o.execute_signals([self._sig()])
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0]["success"])

    def test_block_reason(self):
        with mock.patch.object(self.o, "_pre_execution_block_reason", return_value="kill_switch"):
            results = self.o.execute_signals([self._sig()])
            self.assertEqual(results[0]["status"], "blocked")

    def test_max_orders_per_tick(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None, dry_run=True)
        o.state.cash = 1e9
        with mock.patch.dict(os.environ, {"TRADER_MAX_ORDERS_PER_TICK": "1"}, clear=False):
            results = o.execute_signals([self._sig(), self._sig(strategy_name="x")])
            self.assertEqual(results[1]["status"], "deferred")

    def test_max_notional_per_tick(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None, dry_run=True)
        with mock.patch.dict(os.environ, {"TRADER_MAX_NOTIONAL_PER_TICK": "1"}, clear=False):
            results = o.execute_signals([self._sig()])
            self.assertEqual(results[0]["status"], "deferred")

    def test_approval_execute(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE_APPROVAL, cb=mock.Mock(), dry_run=True)
        o.pending_file = "/tmp/approval_pending_test.json"
        o._available_live_cash_usd = lambda: 1e9
        o.live_allow_short = True
        o.live_max_open_positions = 100
        o.live_max_order_usd = 1e9
        o.live_max_total_notional_usd = 1e9
        results = o.execute_signals([make_sig(confidence=0.99)])
        self.assertTrue(results[0]["success"])
        self.assertEqual(results[0]["mode"], "approval")

    def test_futures_dry(self):
        o = ExecutionOrchestrator(mode=TradeMode.FUTURES, dry_run=True)
        o.live_max_order_usd = 1e9
        o.live_max_total_notional_usd = 1e9
        o._available_live_cash_usd = lambda: 1e9
        results = o.execute_signals([make_sig(confidence=0.99)])
        self.assertTrue(results[0]["success"])
        self.assertEqual(results[0]["mode"], "futures")

    def test_futures_live(self):
        o = ExecutionOrchestrator(mode=TradeMode.FUTURES, dry_run=True)
        o.dry_run = False
        o._available_live_cash_usd = lambda: 1e9
        o.live_allow_short = True
        o.live_max_open_positions = 100
        o.live_max_order_usd = 1e9
        o.live_max_total_notional_usd = 1e9
        o.futures_exec = mock.Mock()
        o.futures_exec.place_bracket.return_value = SimpleNamespace(
            success=True, order_id="o1", raw={}, error=None)
        o.state.open_positions["BTC-USD"] = {}
        results = o.execute_signals([make_sig(confidence=0.99)])
        self.assertEqual(results[0]["status"], "open")

    def test_futures_live_fail(self):
        o = ExecutionOrchestrator(mode=TradeMode.FUTURES, dry_run=True)
        o.dry_run = False
        o._available_live_cash_usd = lambda: 1e9
        o.live_allow_short = True
        o.live_max_open_positions = 100
        o.live_max_order_usd = 1e9
        o.live_max_total_notional_usd = 1e9
        o.futures_exec = mock.Mock()
        o.futures_exec.place_bracket.return_value = SimpleNamespace(
            success=False, order_id="o1", raw={}, error="boom")
        results = o.execute_signals([make_sig(confidence=0.99)])
        self.assertEqual(results[0]["status"], "failed")

    def test_live_execute_no_cb(self):
        o = ExecutionOrchestrator(mode=TradeMode.LIVE, cb=mock.Mock(), dry_run=False)
        o._available_live_cash_usd = lambda: 1e9
        o.live_allow_short = True
        o.exec_engine = mock.Mock()
        o.bracket_mgr = mock.Mock()
        o.live_max_order_usd = 1e9
        o.live_max_total_notional_usd = 1e9
        o.bracket_mgr.place_bracket.return_value = {"entry_order": SimpleNamespace(success=True)}
        results = o.execute_signals([make_sig(confidence=0.99)])
        self.assertTrue(results[0]["success"])

    def test_listener_called(self):
        fn = mock.Mock()
        self.o.register_listener(fn)
        self.o.execute_signals([self._sig()])
        self.assertTrue(fn.called)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestPaperExecute(unittest.TestCase):
    def test_paper_long(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.cash = 10000.0
        r = o._paper_execute(make_sig())
        self.assertTrue(r["success"])
        self.assertIn("BTC-USD", o.state.open_positions)

    def test_paper_long_insufficient_cash(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.cash = 10.0
        r = o._paper_execute(make_sig(size=1000.0))
        self.assertFalse(r["success"])

    def test_paper_short(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.cash = 10000.0
        r = o._paper_execute(make_sig(direction=Direction.SHORT))
        self.assertTrue(r["success"])

    def test_merged_position_state_same_dir(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.open_positions["BTC-USD"] = {"direction": "long", "size": 1.0, "entry": 100.0}
        merged = o._merged_position_state(make_sig(), "b")
        self.assertEqual(merged["direction"], "long")
        self.assertEqual(merged["size"], 2.0)

    def test_merged_position_state_opp_dir(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.open_positions["BTC-USD"] = {"direction": "short", "size": 1.0, "entry": 100.0}
        merged = o._merged_position_state(make_sig(), "b")
        self.assertEqual(merged["direction"], "long")

    def test_merged_position_state_empty(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        merged = o._merged_position_state(make_sig(size=0.0), "b")
        self.assertEqual(merged["entry"], 100.0)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestGenerateFeeVolume(unittest.TestCase):
    def test_none(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.volume_generator.generate_volume_opportunities = mock.Mock(return_value=None)
        self.assertIsNone(o.generate_fee_volume("BTC-USD", 100.0, 5.0))

    def test_signal(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        fake = SimpleNamespace(product_id="BTC-USD", direction=Direction.LONG,
                               entry_price=100, stop_price=90, target_price=120,
                               confidence=0.5, reason="r", strategy_name="v",
                               base_size=1.0, quote_size=100.0, atr=5.0, score=0.5,
                               meta={})
        o.volume_generator.generate_volume_opportunities = mock.Mock(return_value=fake)
        sig = o.generate_fee_volume("BTC-USD", 100.0, 5.0)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.product_id, "BTC-USD")


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestTradeResult(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)

    def test_record_trade_result(self):
        ctrl = mock.Mock()
        self.o.set_risk_appetite(ctrl)
        self.o.strategy_ranking.record_trade = mock.Mock()
        self.o.record_trade_result("rsi_revert", True, 1.0, 0.5)
        self.assertTrue(ctrl.record_trade.called)
        self.assertTrue(self.o.strategy_ranking.record_trade.called)

    def test_update_strategy_performance_win(self):
        self.o.update_strategy_performance("s", True, 1.0)
        p = self.o._strategy_performance["s"]
        self.assertEqual(p["wins"], 1)
        self.assertEqual(p["win_rate"], 1.0)

    def test_update_strategy_performance_loss(self):
        self.o.update_strategy_performance("s", False, -1.0)
        p = self.o._strategy_performance["s"]
        self.assertEqual(p["losses"], 1)
        self.assertEqual(p["win_rate"], 0.0)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestClosePosition(unittest.TestCase):
    def test_close_long(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.cash = 1000.0
        o.state.open_positions["BTC-USD"] = {
            "direction": "long", "size": 1.0, "entry": 100.0, "stop": 90.0}
        r = o.close_position("BTC-USD", 110.0, "signal")
        self.assertIsNotNone(r)
        self.assertGreater(r["pnl"], 0)

    def test_close_short(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.state.open_positions["BTC-USD"] = {
            "direction": "short", "size": 1.0, "entry": 100.0, "stop": 110.0}
        r = o.close_position("BTC-USD", 90.0, "signal")
        self.assertIsNotNone(r)
        self.assertGreater(r["pnl"], 0)

    def test_close_none(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        self.assertIsNone(o.close_position("BTC-USD", 1.0))

    def test_close_bucket_fail(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        o.bucket_ledger = mock.Mock()
        o.bucket_ledger.close_position.side_effect = RuntimeError("x")
        o.state.open_positions["BTC-USD"] = {
            "direction": "long", "size": 1.0, "entry": 100.0, "stop": 90.0}
        r = o.close_position("BTC-USD", 110.0, "signal")
        self.assertIsNotNone(r)


@mock.patch("coinbase.src.orchestrator.CBClient", mock.Mock())
class TestStatus(unittest.TestCase):
    def test_status(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        s = o.status()
        self.assertIn("mode", s)
        self.assertIn("equity", s)
        self.assertIn("execution_guards", s)
        self.assertIn("capital_buckets", s)

    def test_execution_guard_status(self):
        o = ExecutionOrchestrator(mode=TradeMode.PAPER, cb=None)
        g = o.execution_guard_status()
        self.assertIn("kill_switch_active", g)
        self.assertIn("max_orders_per_tick", g)


if __name__ == "__main__":
    unittest.main()
