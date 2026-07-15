import io
import json
import time
import unittest
from unittest import mock

import trading_system.ui.dashboard_server as ds


def patch(attr, **kw):
    return mock.patch.object(ds, attr, **kw)


class TestPaperTradesSettlement(unittest.TestCase):
    def test_paper_trades_missing(self):
        with mock.patch("pathlib.Path.exists", return_value=False):
            out = ds.api_paper_trades()
        self.assertEqual(out["total"], 0)

    def test_paper_trades_ok(self):
        tracker = mock.MagicMock()
        tracker.summary.return_value = {"s": 1}
        fake = mock.MagicMock()
        fake.SettlementTracker.return_value = tracker
        with mock.patch("pathlib.Path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text",
                           return_value=json.dumps([{"timestamp": "2024"}])), \
                mock.patch.dict("sys.modules", {"event_markets.settlement": fake}):
            out = ds.api_paper_trades()
        self.assertEqual(out["total"], 1)

    def test_paper_trades_bad_json(self):
        with mock.patch("pathlib.Path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value="{bad"):
            out = ds.api_paper_trades()
        self.assertEqual(out["total"], 0)

    def test_paper_trades_settlement_exception(self):
        with mock.patch("pathlib.Path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value=json.dumps([])), \
                mock.patch.dict("sys.modules", {"event_markets.settlement": None}):
            out = ds.api_paper_trades()
        self.assertEqual(out["settlement_summary"], {})

    def test_settlement_runs(self):
        tracker = mock.MagicMock()
        tracker.settle_open_trades.return_value = {"n": 1}
        tracker.summary.return_value = {"s": 1}
        fake = mock.MagicMock()
        fake.SettlementTracker.return_value = tracker
        ds._LAST_SETTLE_TS = 0.0
        with mock.patch.dict("sys.modules", {"event_markets.settlement": fake}):
            out = ds.api_settlement()
        self.assertEqual(out["summary"], {"s": 1})

    def test_settlement_throttled(self):
        tracker = mock.MagicMock()
        tracker.summary.return_value = {}
        fake = mock.MagicMock()
        fake.SettlementTracker.return_value = tracker
        ds._LAST_SETTLE_TS = time.time()
        with mock.patch.dict("sys.modules", {"event_markets.settlement": fake}):
            out = ds.api_settlement()
        self.assertIsNone(out["settled_now"])

    def test_settlement_exception(self):
        with mock.patch.dict("sys.modules", {"event_markets.settlement": None}):
            out = ds.api_settlement()
        self.assertIn("error", out)


class TestVenueBalances(unittest.TestCase):
    def setUp(self):
        ds._VENUE_BAL_CACHE["data"] = None
        ds._VENUE_BAL_CACHE["ts"] = 0.0

    def test_cache_hit(self):
        ds._VENUE_BAL_CACHE["data"] = {"cached": True}
        ds._VENUE_BAL_CACHE["ts"] = time.time()
        self.assertEqual(ds.api_venue_balances(), {"cached": True})

    def test_kalshi_ready(self):
        kc = mock.MagicMock(api_key_id="k", private_key_path="p")
        client = mock.MagicMock(_kalshi=kc)
        pmfake = mock.MagicMock()
        pmclient = mock.MagicMock()
        pmclient.is_configured.return_value = (True, None)
        pmclient.address.return_value = "0xabc"
        pmfake.PolymarketExecutionClient.return_value = pmclient
        with patch("_get_prediction_client", return_value=client), \
                mock.patch.dict("sys.modules", {"event_markets.polymarket_executor": pmfake}), \
                patch("_call_with_timeout",
                      side_effect=[{"balance_dollars": 100, "portfolio_value": 5000},
                                   [{"p": 1}], 50]):
            out = ds.api_venue_balances()
        self.assertTrue(out["kalshi"]["configured"])
        self.assertTrue(out["polymarket"]["configured"])

    def test_kalshi_balance_cents_and_exception(self):
        kc = mock.MagicMock(api_key_id="k", private_key_path="p")
        client = mock.MagicMock(_kalshi=kc)
        pmfake = mock.MagicMock()
        pmclient = mock.MagicMock()
        pmclient.is_configured.return_value = (False, "no wallet")
        pmfake.PolymarketExecutionClient.return_value = pmclient
        with patch("_get_prediction_client", return_value=client), \
                mock.patch.dict("sys.modules", {"event_markets.polymarket_executor": pmfake}), \
                patch("_call_with_timeout",
                      side_effect=[{"balance": 12345}, []]):
            out = ds.api_venue_balances()
        self.assertEqual(out["kalshi"]["balance_usd"], 123.45)
        self.assertEqual(out["polymarket"]["note"], "no wallet")

    def test_not_ready(self):
        with patch("_get_prediction_client", return_value=None), \
                mock.patch.dict("sys.modules", {"event_markets.polymarket_executor": None}):
            out = ds.api_venue_balances()
        self.assertFalse(out["kalshi"]["configured"])

    def test_kalshi_balance_exception(self):
        kc = mock.MagicMock(api_key_id="k", private_key_path="p")
        client = mock.MagicMock(_kalshi=kc)
        with patch("_get_prediction_client", return_value=client), \
                mock.patch.dict("sys.modules", {"event_markets.polymarket_executor": None}), \
                patch("_call_with_timeout", side_effect=RuntimeError("boom")):
            out = ds.api_venue_balances()
        self.assertIsNotNone(out["kalshi"]["error"])


class TestExecutor(unittest.TestCase):
    def test_get_executor_ok(self):
        fake = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"event_markets.executor": fake}), \
                patch("_get_prediction_client", return_value=mock.MagicMock()):
            self.assertIsNotNone(ds._get_arbitrage_executor())

    def test_get_executor_fail(self):
        with mock.patch.dict("sys.modules", {"event_markets.executor": None}):
            self.assertIsNone(ds._get_arbitrage_executor())

    def test_execution_status_none(self):
        with patch("_get_arbitrage_executor", return_value=None):
            out = ds.api_execution_status()
        self.assertFalse(out["available"])

    def test_execution_status_ok(self):
        ex = mock.MagicMock()
        ex.status.return_value = {"mode": "live"}
        with patch("_get_arbitrage_executor", return_value=ex):
            out = ds.api_execution_status()
        self.assertTrue(out["available"])


class TestCachedOpportunity(unittest.TestCase):
    def test_find_cross(self):
        ds.ARBITRAGE_CACHE["data"] = {"opportunities": [{"event_key": "e1"}]}
        self.assertIsNotNone(ds._find_cached_opportunity("e1"))
        self.assertIsNone(ds._find_cached_opportunity("nope"))
        ds.ARBITRAGE_CACHE["data"] = None

    def test_find_internal(self):
        ds.KALSHI_INTERNAL_CACHE["data"] = {"opportunities": [{"event_key": "e2"}]}
        self.assertIsNotNone(ds._find_cached_internal_opportunity("e2"))
        self.assertIsNone(ds._find_cached_internal_opportunity("nope"))
        ds.KALSHI_INTERNAL_CACHE["data"] = None


class TestExecuteArbitrage(unittest.TestCase):
    def test_no_confirm(self):
        self.assertFalse(ds.api_execute_arbitrage({})["ok"])

    def test_no_executor(self):
        with patch("_get_arbitrage_executor", return_value=None):
            out = ds.api_execute_arbitrage({"confirm": True})
        self.assertFalse(out["ok"])

    def test_no_event_key(self):
        with patch("_get_arbitrage_executor", return_value=mock.MagicMock()):
            out = ds.api_execute_arbitrage({"confirm": True})
        self.assertIn("event_key", out["error"])

    def test_opp_not_found(self):
        with patch("_get_arbitrage_executor", return_value=mock.MagicMock()), \
                patch("_find_cached_opportunity", return_value=None), \
                patch("_find_cached_internal_opportunity", return_value=None):
            out = ds.api_execute_arbitrage({"confirm": True, "event_key": "e"})
        self.assertIn("not found", out["error"])

    def test_invalid_notional(self):
        ex = mock.MagicMock()
        with patch("_get_arbitrage_executor", return_value=ex):
            out = ds.api_execute_arbitrage({"confirm": True, "opportunity": {"type": "x"},
                                           "notional": "abc"})
        self.assertIn("notional", out["error"])

    def test_execute_cross(self):
        ex = mock.MagicMock()
        ex.execute.return_value.to_dict.return_value = {"ok": True}
        with patch("_get_arbitrage_executor", return_value=ex):
            out = ds.api_execute_arbitrage({"confirm": True, "opportunity": {"type": "cross"},
                                           "notional": 100, "live": False})
        self.assertTrue(out["ok"])

    def test_execute_internal_via_kind(self):
        ex = mock.MagicMock()
        ex.execute_internal.return_value.to_dict.return_value = {"ok": True}
        with patch("_get_arbitrage_executor", return_value=ex), \
                patch("_find_cached_internal_opportunity", return_value={"type": "kalshi_internal"}):
            out = ds.api_execute_arbitrage({"confirm": True, "event_key": "e",
                                           "kind": "kalshi_internal", "notional": 10})
        self.assertTrue(out["ok"])

    def test_execute_exception(self):
        ex = mock.MagicMock()
        ex.execute.side_effect = RuntimeError("x")
        with patch("_get_arbitrage_executor", return_value=ex):
            out = ds.api_execute_arbitrage({"confirm": True, "opportunity": {"type": "cross"},
                                           "notional": 10})
        self.assertFalse(out["ok"])


class TestTradePlans(unittest.TestCase):
    def test_missing(self):
        with mock.patch("pathlib.Path.exists", return_value=False):
            self.assertEqual(ds.api_trade_plans()["total"], 0)

    def test_dict_payload(self):
        payload = {"plans": [{"priority": 2}, {"priority": 5}], "updated_at": "t",
                   "source": "opt", "total": 2}
        with mock.patch("pathlib.Path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value=json.dumps(payload)):
            out = ds.api_trade_plans()
        self.assertEqual(out["plans"][0]["priority"], 5)

    def test_list_payload(self):
        with mock.patch("pathlib.Path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value=json.dumps([{"priority": 1}])):
            out = ds.api_trade_plans()
        self.assertEqual(out["total"], 1)

    def test_bad_json(self):
        with mock.patch("pathlib.Path.exists", return_value=True), \
                mock.patch("pathlib.Path.read_text", return_value="{bad"):
            self.assertEqual(ds.api_trade_plans()["total"], 0)


class TestOperatorActions(unittest.TestCase):
    def test_action_ids(self):
        self.assertIn("refresh_market_data", ds._operator_action_ids())

    def test_load_queue_variants(self):
        with patch("_load_json", return_value=[1, 2]):
            self.assertEqual(ds._load_operator_actions_queue(), [1, 2])
        with patch("_load_json", return_value={"queue": [3]}):
            self.assertEqual(ds._load_operator_actions_queue(), [3])
        with patch("_load_json", return_value={"actions": [4]}):
            self.assertEqual(ds._load_operator_actions_queue(), [4])
        with patch("_load_json", return_value="bad"):
            self.assertEqual(ds._load_operator_actions_queue(), [])

    def test_save_queue(self):
        with mock.patch("pathlib.Path.mkdir"), patch("_write_json", return_value=True):
            self.assertTrue(ds._save_operator_actions_queue([]))

    def test_proxy_no_url(self):
        with patch("OPERATOR_ACTIONS_URL", new=""):
            self.assertIsNone(ds._proxy_operator_actions("/x"))

    def test_proxy_success(self):
        resp = mock.MagicMock()
        resp.read.return_value = b'{"ok": true}'
        resp.__enter__ = lambda s: resp
        resp.__exit__ = lambda s, *a: False
        with patch("OPERATOR_ACTIONS_URL", new="http://x"), \
                mock.patch.object(ds, "urlopen", return_value=resp):
            out = ds._proxy_operator_actions("/actions", method="POST", payload={"a": 1})
        self.assertTrue(out["ok"])

    def test_proxy_error(self):
        with patch("OPERATOR_ACTIONS_URL", new="http://x"), \
                mock.patch.object(ds, "urlopen", side_effect=OSError("no")):
            self.assertIsNone(ds._proxy_operator_actions("/actions"))

    def test_api_operator_actions_proxied(self):
        with patch("_proxy_operator_actions", return_value={"ok": True}):
            out = ds.api_operator_actions()
        self.assertEqual(out["backend"], "rust")

    def test_api_operator_actions_fallback(self):
        with patch("_proxy_operator_actions", return_value=None), \
                patch("_load_operator_actions_queue", return_value=[]):
            out = ds.api_operator_actions()
        self.assertEqual(out["backend"], "python-fallback")

    def test_queue_action_unknown(self):
        with self.assertRaises(ValueError):
            ds.queue_operator_action({"action": "nope"})

    def test_queue_action_proxied(self):
        with patch("_proxy_operator_actions", return_value={"ok": True}):
            out = ds.queue_operator_action({"action": "refresh_market_data"})
        self.assertEqual(out["backend"], "rust")

    def test_queue_action_fallback(self):
        with patch("_proxy_operator_actions", return_value=None), \
                patch("_load_operator_actions_queue", return_value=[]), \
                patch("_save_operator_actions_queue", return_value=True):
            out = ds.queue_operator_action({"action": "paper_smoke", "note": "n"})
        self.assertEqual(out["status"], "queued")

    def test_queue_action_save_fail(self):
        with patch("_proxy_operator_actions", return_value=None), \
                patch("_load_operator_actions_queue", return_value=[]), \
                patch("_save_operator_actions_queue", return_value=False):
            with self.assertRaises(OSError):
                ds.queue_operator_action({"action": "paper_smoke"})

    def test_bucket_presets(self):
        with patch("_bucket_preset_payloads", return_value=[{"name": "x"}]):
            out = ds.api_bucket_presets()
        self.assertEqual(out["presets"][0]["name"], "x")


def make_handler(path="/", body=None):
    h = ds.DashboardHandler.__new__(ds.DashboardHandler)
    h.path = path
    h._json_response = mock.MagicMock()
    if body is not None:
        raw = json.dumps(body).encode()
        h.headers = {"Content-Length": str(len(raw))}
        h.rfile = io.BytesIO(raw)
    return h


class TestDashboardHandlerGET(unittest.TestCase):
    def test_route_success(self):
        h = make_handler("/health")
        with patch("api_health", return_value={"ok": 1}):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_route_error(self):
        h = make_handler("/health")
        with patch("api_health", side_effect=RuntimeError("boom")):
            h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)

    def test_approve(self):
        h = make_handler("/approvals/approve/tok1")
        with patch("_update_approval", return_value=True):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_approve_missing(self):
        h = make_handler("/approvals/approve/")
        h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 404)

    def test_deny(self):
        h = make_handler("/approvals/deny/tok1")
        with patch("_update_approval", return_value=True):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_bucket_preset(self):
        h = make_handler("/capital/buckets/preset/challenge")
        with patch("_build_bucket_preset", return_value={"buckets": []}), \
                patch("_save_capital_buckets", return_value={"buckets": []}):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_bucket_preset_error(self):
        h = make_handler("/capital/buckets/preset/x")
        with patch("_build_bucket_preset", side_effect=RuntimeError("x")):
            h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)

    def test_price(self):
        h = make_handler("/evaluations/price/BTC-USD")
        with patch("api_price_estimates", return_value={"x": 1}):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_price_error(self):
        h = make_handler("/evaluations/price/BTC")
        with patch("api_price_estimates", side_effect=RuntimeError("x")):
            h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)

    def test_universe(self):
        h = make_handler("/market/universe")
        with patch("api_universe", return_value={}):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_universe_error(self):
        h = make_handler("/market/universe")
        with patch("api_universe", side_effect=RuntimeError("x")):
            h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)

    def test_execution_status(self):
        h = make_handler("/execution/status")
        with patch("api_execution", return_value={}):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_execution_status_error(self):
        h = make_handler("/execution/status")
        with patch("api_execution", side_effect=RuntimeError("x")):
            h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)

    def test_brackets(self):
        h = make_handler("/execution/brackets")
        with patch("_load_json", return_value={}):
            h.do_GET()
        h._json_response.assert_called_once()

    def test_brackets_error(self):
        h = make_handler("/execution/brackets")
        with patch("_load_json", side_effect=RuntimeError("x")):
            h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)

    def test_dashboard(self):
        h = make_handler("/dashboard")
        h._serve_dashboard = mock.MagicMock()
        h.do_GET()
        h._serve_dashboard.assert_called_once()

    def test_not_found(self):
        h = make_handler("/nope")
        h.do_GET()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 404)


class TestDashboardHandlerPOST(unittest.TestCase):
    def test_unknown(self):
        h = make_handler("/nope", body={})
        h.do_POST()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 404)

    def test_invalid_json(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.path = "/capital/config"
        h._json_response = mock.MagicMock()
        raw = b"{bad"
        h.headers = {"Content-Length": str(len(raw))}
        h.rfile = io.BytesIO(raw)
        h.do_POST()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 400)

    def test_bad_content_length(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.path = "/capital/config"
        h._json_response = mock.MagicMock()
        h.headers = {"Content-Length": "notanint"}
        h.rfile = io.BytesIO(b"")
        with patch("_save_capital_policy", return_value={}):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_capital_config(self):
        h = make_handler("/capital/config", body={"x": 1})
        with patch("_save_capital_policy", return_value={}):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_actions_run(self):
        h = make_handler("/actions/run", body={"action": "refresh_market_data"})
        with patch("queue_operator_action", return_value={"ok": True}):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_buckets_preset(self):
        h = make_handler("/capital/buckets/preset", body={"preset": "challenge"})
        with patch("_build_bucket_preset", return_value={}), \
                patch("_save_capital_buckets", return_value={}):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_brackets_cancel_no_id(self):
        h = make_handler("/execution/brackets/cancel", body={})
        h.do_POST()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 400)

    def test_brackets_cancel_not_found(self):
        h = make_handler("/execution/brackets/cancel", body={"bracket_id": "b"})
        with patch("_load_json", return_value={}):
            h.do_POST()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 404)

    def test_brackets_cancel_ok(self):
        h = make_handler("/execution/brackets/cancel", body={"bracket_id": "b"})
        with patch("_load_json", return_value={"b": {}}), patch("_write_json", return_value=True):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_brackets_cancel_all(self):
        h = make_handler("/execution/brackets/cancel-all", body={})
        with patch("_write_json", return_value=True):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_arbitrage_execute_ok(self):
        h = make_handler("/arbitrage/execute", body={"confirm": True})
        with patch("api_execute_arbitrage", return_value={"ok": True}):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_arbitrage_execute_fail(self):
        h = make_handler("/arbitrage/execute", body={})
        with patch("api_execute_arbitrage", return_value={"ok": False}):
            h.do_POST()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 400)

    def test_buckets_default(self):
        h = make_handler("/capital/buckets", body={"buckets": []})
        with patch("_save_capital_buckets", return_value={}):
            h.do_POST()
        h._json_response.assert_called_once()

    def test_exception(self):
        h = make_handler("/capital/config", body={"x": 1})
        with patch("_save_capital_policy", side_effect=RuntimeError("boom")):
            h.do_POST()
        _, kwargs = h._json_response.call_args
        self.assertEqual(kwargs.get("status"), 500)


class TestHandlerMisc(unittest.TestCase):
    def test_json_response(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.wfile = io.BytesIO()
        h.send_response = mock.MagicMock()
        h.send_header = mock.MagicMock()
        h.end_headers = mock.MagicMock()
        h._json_response('{"a": 1}', status=200)
        self.assertIn(b'"a": 1', h.wfile.getvalue())
        h.send_response.assert_called_once_with(200)

    def test_serve_dashboard_missing(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.wfile = io.BytesIO()
        h.send_response = mock.MagicMock()
        h.end_headers = mock.MagicMock()
        with mock.patch("os.path.exists", return_value=False):
            h._serve_dashboard()
        h.send_response.assert_called_with(404)

    def test_serve_dashboard_found(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.wfile = io.BytesIO()
        h.send_response = mock.MagicMock()
        h.send_header = mock.MagicMock()
        h.end_headers = mock.MagicMock()
        with mock.patch("os.path.exists", return_value=True), \
                mock.patch("builtins.open", mock.mock_open(read_data=b"<html>")):
            h._serve_dashboard()
        h.send_response.assert_called_with(200)

    def test_serve_dashboard_exception(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.wfile = io.BytesIO()
        h.send_response = mock.MagicMock()
        h.end_headers = mock.MagicMock()
        with mock.patch("os.path.exists", return_value=True), \
                mock.patch("builtins.open", side_effect=RuntimeError("x")):
            h._serve_dashboard()
        h.send_response.assert_called_with(500)

    def test_log_message_filtered(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        # filtered path returns without calling super
        self.assertIsNone(h.log_message("%s", "/health HTTP/1.1"))

    def test_log_message_passthrough(self):
        h = ds.DashboardHandler.__new__(ds.DashboardHandler)
        h.client_address = ("127.0.0.1", 1234)
        with mock.patch("http.server.BaseHTTPRequestHandler.log_message") as sup:
            h.log_message("%s", "/other")
        sup.assert_called_once()


class TestMain(unittest.TestCase):
    def test_parse_args(self):
        with mock.patch("sys.argv", ["prog", "--port", "9999", "--host", "127.0.0.1"]):
            args = ds.parse_args()
        self.assertEqual(args.port, 9999)

    def test_main(self):
        with mock.patch("sys.argv", ["prog"]), \
                mock.patch("os.chdir"), \
                mock.patch("socketserver.TCPServer.server_bind"), \
                mock.patch("socketserver.TCPServer.server_activate"), \
                mock.patch("socketserver.TCPServer.server_close"), \
                mock.patch("socketserver.BaseServer.serve_forever",
                           side_effect=KeyboardInterrupt):
            ds.main()


if __name__ == "__main__":
    unittest.main()
