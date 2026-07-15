import json
import unittest
from unittest import mock

import trading_system.ui.dashboard_server as ds


def patch(attr, **kw):
    return mock.patch.object(ds, attr, **kw)


class TestApiHealth(unittest.TestCase):
    def test_healthy(self):
        store = mock.MagicMock()
        store.stats.return_value = {"n": 1}
        with patch("_load_json", return_value={"marketIntelligence": {}}), \
                patch("_get_state_store", return_value=store), \
                mock.patch("os.path.exists", return_value=False):
            out = ds.api_health()
        self.assertIn(out["status"], ("healthy", "degraded"))
        self.assertEqual(out["components"]["state_store"], "ok")

    def test_store_none_and_op_empty(self):
        with patch("_load_json", return_value={}), \
                patch("_get_state_store", return_value=None), \
                mock.patch("os.path.exists", return_value=False):
            out = ds.api_health()
        self.assertEqual(out["components"]["state_store"], "unavailable")

    def test_store_stats_error(self):
        store = mock.MagicMock()
        store.stats.side_effect = RuntimeError("x")
        with patch("_load_json", return_value={}), \
                patch("_get_state_store", return_value=store), \
                mock.patch("os.path.exists", return_value=False):
            out = ds.api_health()
        self.assertTrue(out["components"]["state_store"].startswith("error"))

    def test_operator_state_error(self):
        calls = {"n": 0}

        def load(path, default=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("bad")
            return {}

        with patch("_load_json", side_effect=load), \
                patch("_get_state_store", return_value=None), \
                mock.patch("os.path.exists", return_value=False):
            out = ds.api_health()
        self.assertTrue(out["components"]["operator_state"].startswith("error"))

    def test_heartbeat_ok(self):
        with patch("_load_json", return_value={}), \
                patch("_get_state_store", return_value=None), \
                mock.patch("os.path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value=str(__import__("time").time())):
            out = ds.api_health()
        self.assertEqual(out["components"]["daemon_heartbeat"], "ok")

    def test_heartbeat_unreadable(self):
        with patch("_load_json", return_value={}), \
                patch("_get_state_store", return_value=None), \
                mock.patch("os.path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value="notafloat"):
            out = ds.api_health()
        self.assertEqual(out["components"]["daemon_heartbeat"], "unreadable")


class TestApiAccounts(unittest.TestCase):
    def test_from_operator_state(self):
        op = {"accounts": [{"id": "a", "cash": 100, "nav": 200}]}
        with patch("_load_json", return_value=op):
            out = ds.api_accounts()
        self.assertEqual(out["total_accounts"], 1)

    def test_from_coinbase(self):
        cb = mock.MagicMock()
        cb.get_balances.return_value = [{"currency": "BTC", "usd_value": 100, "balance": 1},
                                        {"currency": "X", "balance": 0}]
        with patch("_load_json", return_value={}), patch("_get_coinbase_cli", return_value=cb):
            out = ds.api_accounts()
        self.assertEqual(out["total_accounts"], 1)

    def test_coinbase_exception(self):
        cb = mock.MagicMock()
        cb.get_balances.side_effect = RuntimeError("x")
        with patch("_load_json", return_value={}), patch("_get_coinbase_cli", return_value=cb):
            out = ds.api_accounts()
        self.assertEqual(out["total_accounts"], 0)

    def test_no_coinbase(self):
        with patch("_load_json", return_value={}), patch("_get_coinbase_cli", return_value=None):
            out = ds.api_accounts()
        self.assertEqual(out["total_accounts"], 0)


class TestApiPositions(unittest.TestCase):
    def test_with_market_data(self):
        op = {
            "positions": [{"symbol": "BTC-USD", "quantity": 2, "averagePrice": 100,
                           "status": "open", "venue": "cb"}],
            "marketDataSnapshots": [{"symbol": "BTC-USD", "bid": 110}],
            "instruments": [{"symbol": "BTC-USD"}],
        }
        with patch("_load_json", return_value=op):
            out = ds.api_positions()
        self.assertEqual(out["total_positions"], 1)
        self.assertGreater(out["total_unrealized_pnl_usd"], 0)

    def test_without_prices(self):
        op = {"positions": [{"symbol": "X", "quantity": 1, "averagePrice": 0,
                             "unrealizedPnl": 5}], "marketDataSnapshots": [], "instruments": []}
        with patch("_load_json", return_value=op):
            out = ds.api_positions()
        self.assertEqual(out["total_positions"], 1)


class TestApiStrategies(unittest.TestCase):
    def test_full(self):
        store = mock.MagicMock()
        store.load_bt_cache.return_value = {"strat1:x": {"sharpe_ratio": 1.5, "win_rate": 0.6,
                                                         "total_trades": 10}}
        op = {"strategyTemplates": [{"id": "t1", "name": "T1"}],
              "strategies": [{"id": "s1", "name": "S1", "sharpe": 1.0, "winRate": 0.5,
                              "totalTrades": 3, "status": "active"}]}
        with patch("_get_state_store", return_value=store), patch("_load_json", return_value=op):
            out = ds.api_strategies()
        self.assertGreaterEqual(out["total_strategies"], 3)

    def test_store_none_and_exception(self):
        store = mock.MagicMock()
        store.load_bt_cache.side_effect = RuntimeError("x")
        with patch("_get_state_store", return_value=store), patch("_load_json", return_value={}):
            out = ds.api_strategies()
        self.assertEqual(out["total_strategies"], 0)


class TestApiApprovals(unittest.TestCase):
    def test_full(self):
        pending = {"tok1": {"status": "pending", "created_at": "2024", "currency": "BTC",
                            "size_usd": 100},
                   "tok2": {"status": "approved", "created_at": "2023"},
                   "tok3": {"status": "denied", "created_at": "2022"}}
        op = {"approvals": [{"id": "op1", "status": "pending_review"},
                            {"id": "op2", "status": "approved"},
                            {"id": "op3", "status": "rejected"},
                            {"id": "tok1"}]}

        def load(path, default=None):
            return dict(pending) if "pending_approvals" in path else dict(op)

        with patch("_load_json", side_effect=load):
            out = ds.api_approvals()
        self.assertGreaterEqual(out["summary"]["pending_count"], 1)


class TestApiUniverse(unittest.TestCase):
    def test_full(self):
        cb = mock.MagicMock()
        cb.get_products.return_value = {"products": [
            {"product_id": "BTC-USD", "volume_24h": 1000},
            {"product_id": "DEAD", "trading_disabled": True},
            "notadict",
            {"volume_24h": 0},
        ]}
        pm = mock.MagicMock()
        pm.search_all_categories.return_value = {"crypto": [1, 2], "sports": [3]}
        with patch("_get_coinbase_cli", return_value=cb), \
                patch("_get_prediction_client", return_value=pm), \
                patch("_graph_summary_for_products", return_value={"available": True}):
            out = ds.api_universe()
        self.assertEqual(out["coinbase_total"], 1)
        self.assertEqual(out["prediction_total"], 3)

    def test_cb_exception_and_no_pm(self):
        cb = mock.MagicMock()
        cb.get_products.side_effect = RuntimeError("x")
        with patch("_get_coinbase_cli", return_value=cb), \
                patch("_get_prediction_client", return_value=None):
            out = ds.api_universe()
        self.assertEqual(out["coinbase_total"], 0)

    def test_cb_bad_volume(self):
        cb = mock.MagicMock()
        cb.get_products.return_value = [{"product_id": "AAA-USD", "volume_24h": "notanumber"}]
        with patch("_get_coinbase_cli", return_value=cb), \
                patch("_get_prediction_client", return_value=None), \
                patch("_graph_summary_for_products", return_value={"available": False}):
            out = ds.api_universe()
        self.assertEqual(out["coinbase_total"], 1)

    def test_pm_timeout(self):
        cb = mock.MagicMock()
        cb.get_products.return_value = []
        pm = mock.MagicMock()
        fut = mock.MagicMock()
        from concurrent.futures import TimeoutError as FTE
        fut.result.side_effect = FTE()
        with patch("_get_coinbase_cli", return_value=cb), \
                patch("_get_prediction_client", return_value=pm), \
                mock.patch.object(ds._SHARED_EXECUTOR, "submit", return_value=fut):
            out = ds.api_universe()
        self.assertEqual(out["prediction_total"], 0)

    def test_pm_exception(self):
        cb = mock.MagicMock()
        cb.get_products.return_value = []
        pm = mock.MagicMock()
        fut = mock.MagicMock()
        fut.result.side_effect = RuntimeError("x")
        with patch("_get_coinbase_cli", return_value=cb), \
                patch("_get_prediction_client", return_value=pm), \
                mock.patch.object(ds._SHARED_EXECUTOR, "submit", return_value=fut):
            out = ds.api_universe()
        self.assertEqual(out["prediction_total"], 0)


class TestApiExecution(unittest.TestCase):
    def test_full(self):
        store = mock.MagicMock()
        store.load_trades.return_value = [{"type": "t", "side": "BUY", "currency": "BTC",
                                           "size_usd": 100}]
        store.load_snapshots.return_value = [{"total_value": 1000, "usdc_balance": 500,
                                              "holdings": {"BTC": {"currency": "BTC", "value": 200,
                                                                   "classification": "safe",
                                                                   "allocation_pct": 20,
                                                                   "product_id": "BTC-USD"},
                                                           "XYZ": {"currency": "XYZ", "value": 50}}}]
        policy = ds._normalize_capital_policy({"max_deployable_usd": 5000,
                                               "live_test_started_at": "2024-01-01T00:00:00Z"})
        approvals = {"tok": {"status": "pending"}}
        with patch("_get_state_store", return_value=store), \
                patch("_get_capital_policy", return_value=policy), \
                patch("_load_json", return_value=approvals), \
                patch("_compute_capital_in_play", return_value=100.0), \
                patch("_graph_summary_for_products", return_value={"available": True}), \
                patch("_load_capital_buckets", return_value={"buckets": []}):
            out = ds.api_execution()
        self.assertEqual(out["pending_count"], 1)
        self.assertIn("deployable_buy_power_usd", out)

    def test_no_store_bad_start(self):
        policy = ds._normalize_capital_policy({"live_test_started_at": "bad-date"})
        with patch("_get_state_store", return_value=None), \
                patch("_get_capital_policy", return_value=policy), \
                patch("_load_json", return_value={}), \
                patch("_compute_capital_in_play", return_value=0.0), \
                patch("_load_capital_buckets", return_value={"buckets": []}):
            out = ds.api_execution()
        self.assertEqual(out["pending_count"], 0)


class TestApiPerformance(unittest.TestCase):
    def test_full(self):
        store = mock.MagicMock()
        store.load_trades.return_value = [
            {"size_usd": 100, "fee": 1, "side": "BUY", "pnl_usd": 5},
            {"size_usd": 50, "fee": 0.5, "side": "SELL", "pnl_usd": -2},
        ]
        store.load_snapshots.return_value = [{"total_value": v} for v in
                                             [110, 108, 106, 104, 102, 100]]
        store.load_bt_cache.return_value = {"k": {"sharpe_ratio": 2.0, "max_drawdown_pct": -3}}
        op = {"backtests": [{"totalReturnPct": 10, "maxDrawdownPct": -5, "sharpe": 1.5}]}
        with patch("_get_state_store", return_value=store), patch("_load_json", return_value=op):
            out = ds.api_performance()
        self.assertEqual(out["summary_metrics"]["total_trades"], 2)

    def test_no_store_and_exception(self):
        store = mock.MagicMock()
        store.load_trades.side_effect = RuntimeError("x")
        with patch("_get_state_store", return_value=store), patch("_load_json", return_value={}):
            out = ds.api_performance()
        self.assertEqual(out["summary_metrics"]["total_trades"], 0)


class TestApiPriceEstimates(unittest.TestCase):
    def test_found_in_snapshots(self):
        op = {"marketDataSnapshots": [{"symbol": "BTC-USD", "bid": 100, "volume24h": 500}]}
        with patch("_load_json", return_value=op):
            out = ds.api_price_estimates("btc")
        self.assertEqual(out["instrument"], "BTC-USD")
        self.assertEqual(out["current_price_usd"], 100)

    def test_slash_formatting(self):
        with patch("_load_json", return_value={}), patch("_get_coinbase_cli", return_value=None):
            out = ds.api_price_estimates("eth/usd")
        self.assertEqual(out["instrument"], "ETH-USD")
        self.assertEqual(out["current_price_usd"], 50000)

    def test_coinbase_fallback(self):
        cb = mock.MagicMock()
        cb.get_price.return_value = {"price": 250}
        with patch("_load_json", return_value={}), patch("_get_coinbase_cli", return_value=cb):
            out = ds.api_price_estimates("SOL-USD")
        self.assertEqual(out["current_price_usd"], 250)

    def test_coinbase_exception(self):
        cb = mock.MagicMock()
        cb.get_price.side_effect = RuntimeError("x")
        with patch("_load_json", return_value={}), patch("_get_coinbase_cli", return_value=cb):
            out = ds.api_price_estimates("ABC-USD")
        self.assertEqual(out["current_price_usd"], 50000)

    def test_ask_price_used(self):
        op = {"marketDataSnapshots": [{"symbol": "BTC-USD", "bid": 0, "ask": 90}]}
        with patch("_load_json", return_value=op):
            out = ds.api_price_estimates("BTC-USD")
        self.assertEqual(out["current_price_usd"], 90)


class TestRiskDashboard(unittest.TestCase):
    def test_full(self):
        store = mock.MagicMock()
        store.load_trades.return_value = [
            {"currency": "BTC", "size_usd": 100, "side": "BUY"},
            {"currency": "ETH", "size_usd": 500, "side": "SELL"},
            {"currency": "", "size_usd": 10},
        ]
        op = {"emergencyCircuitBreaker": True}

        def load(path, default=None):
            if ".manual_ops" in path:
                return {"op1": {"a": 1}}
            return op

        with patch("_get_state_store", return_value=store), patch("_load_json", side_effect=load):
            out = ds.api_risk_dashboard()
        self.assertGreater(out["total_exposure_usd"], 0)
        self.assertTrue(out["system_alerts"]["emergency_breaker_active"])

    def test_no_store(self):
        store = mock.MagicMock()
        store.load_trades.side_effect = RuntimeError("x")
        with patch("_get_state_store", return_value=store), patch("_load_json", return_value={}):
            out = ds._get_risk_dashboard_data()
        self.assertEqual(out["total_exposure_usd"], 0)


class TestManualOperation(unittest.TestCase):
    def test_safe_op(self):
        with patch("_load_json", return_value={}), patch("_write_json", return_value=True):
            out = ds._execute_manual_operation("refresh", {"reason": "test"})
        self.assertTrue(out["success"])

    def test_dangerous_op(self):
        with patch("_load_json", return_value={}), patch("_write_json", return_value=True):
            out = ds._execute_manual_operation("close_all", {})
        self.assertFalse(out["success"])

    def test_exception(self):
        with patch("_load_json", side_effect=RuntimeError("boom")):
            out = ds._execute_manual_operation("x", {})
        self.assertIn("Error", out["message"])

    def test_api_execute_manual_operation(self):
        flask_mod = mock.MagicMock()
        flask_mod.request.get_json.return_value = {"type": "refresh", "params": {}}
        with mock.patch.dict("sys.modules", {"flask": flask_mod}), \
                patch("_execute_manual_operation", return_value={"success": True}):
            out = ds.api_execute_manual_operation()
        self.assertTrue(out["success"])

    def test_api_execute_manual_no_type(self):
        flask_mod = mock.MagicMock()
        flask_mod.request.get_json.return_value = {}
        with mock.patch.dict("sys.modules", {"flask": flask_mod}):
            out = ds.api_execute_manual_operation()
        self.assertFalse(out["success"])

    def test_api_execute_manual_error(self):
        with mock.patch.dict("sys.modules", {"flask": None}):
            out = ds.api_execute_manual_operation()
        self.assertFalse(out["success"])


class TestHypotheses(unittest.TestCase):
    def test_with_data(self):
        op = {"researchJobs": [{"label": "J1", "confidenceScore": 0.7}],
              "backtests": [{"strategyId": "S", "totalTrades": 5, "winRatePct": 60}]}
        with patch("_load_json", return_value=op):
            out = ds.api_hypotheses()
        self.assertGreaterEqual(out["total_hypotheses"], 2)

    def test_empty_defaults(self):
        with patch("_load_json", return_value={}):
            out = ds.api_hypotheses()
        self.assertEqual(out["total_hypotheses"], 3)


class TestMarketRegime(unittest.TestCase):
    def test_volatile(self):
        op = {"marketDataSnapshots": [{"volatilityScore": 80, "liquidityScore": 60,
                                       "spreadBps": 5}]}

        def load(path, default=None):
            if "signal_cache" in path or "unified_signal" in path:
                return {"signals": [{"direction": "LONG"}, {"direction": "SELL"}]}
            return op

        with patch("_load_json", side_effect=load):
            out = ds.api_market_regime()
        self.assertEqual(out["current_regime"]["state"], "volatile")

    def test_regime_levels(self):
        for vol, expected in [(60, "trending"), (40, "neutral"), (10, "quiet")]:
            op = {"marketDataSnapshots": [{"volatilityScore": vol}]}
            with patch("_load_json", return_value=op):
                out = ds.api_market_regime()
            self.assertEqual(out["current_regime"]["state"], expected)

    def test_empty(self):
        with patch("_load_json", return_value={}):
            out = ds.api_market_regime()
        self.assertEqual(out["symbols_tracked"], 0)


class TestMarketIntelligence(unittest.TestCase):
    def test_present(self):
        with patch("_load_json", return_value={"marketIntelligence": {"x": 1}}):
            self.assertEqual(ds.api_market_intelligence(), {"x": 1})

    def test_default(self):
        with patch("_load_json", return_value={}):
            out = ds.api_market_intelligence()
        self.assertIn("coinbase", out)


if __name__ == "__main__":
    unittest.main()
