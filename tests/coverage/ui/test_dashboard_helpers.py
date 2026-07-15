import io
import json
import os
import time
import unittest
from unittest import mock

import trading_system.ui.dashboard_server as ds


def patch(attr, **kw):
    return mock.patch.object(ds, attr, **kw)


class TestJsonHelpers(unittest.TestCase):
    def test_load_json_missing_default(self):
        self.assertEqual(ds._load_json("/no/such/file.json"), {})
        self.assertEqual(ds._load_json("/no/such/file.json", default=[]), [])

    def test_load_json_valid(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"a": 1}, f)
            path = f.name
        try:
            self.assertEqual(ds._load_json(path), {"a": 1})
        finally:
            os.unlink(path)

    def test_load_json_invalid(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            f.write("{not json")
            path = f.name
        try:
            self.assertEqual(ds._load_json(path), {})
            self.assertEqual(ds._load_json(path, default=[]), [])
        finally:
            os.unlink(path)

    def test_write_json_ok_and_fail(self):
        import tempfile
        path = os.path.join(tempfile.mkdtemp(), "out.json")
        self.assertTrue(ds._write_json(path, {"x": 1}))
        self.assertFalse(ds._write_json("/no/such/dir/x.json", {"x": 1}))


class TestStateStoreHelpers(unittest.TestCase):
    def test_get_state_store_ok(self):
        fake_mod = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"state_store": fake_mod}):
            store = ds._get_state_store()
        self.assertIsNotNone(store)

    def test_get_state_store_fail(self):
        with mock.patch.dict("sys.modules", {"state_store": None}):
            self.assertIsNone(ds._get_state_store())


class TestGraphHelpers(unittest.TestCase):
    def test_graph_summary_empty(self):
        out = ds._graph_summary_for_products([])
        self.assertFalse(out["available"])

    def test_graph_summary_no_store(self):
        with patch("_get_graph_store", return_value=None):
            out = ds._graph_summary_for_products(["BTC-USD"])
        self.assertFalse(out["available"])

    def test_graph_summary_success(self):
        sig = mock.MagicMock(product_id="BTC-USD", symbol="BTC", graph_score=0.9,
                             available_on_coinbase=True, reasons=["r"])
        store = mock.MagicMock()
        store.asset_signal.return_value = sig
        overlay_mod = mock.MagicMock()
        overlay_mod.graph_weight_overlays.return_value = {"BTC-USD": 1.2}
        with patch("_get_graph_store", return_value=store), \
                mock.patch.dict("sys.modules",
                                {"coinbase.src.graph.portfolio_overlay": overlay_mod}):
            out = ds._graph_summary_for_products(["BTC-USD"], limit=5)
        self.assertTrue(out["available"])
        self.assertEqual(out["top_assets"][0]["product_id"], "BTC-USD")

    def test_graph_summary_exception(self):
        store = mock.MagicMock()
        store.asset_signal.side_effect = RuntimeError("boom")
        with patch("_get_graph_store", return_value=store):
            out = ds._graph_summary_for_products(["BTC-USD"])
        self.assertFalse(out["available"])

    def test_get_graph_store_import_fail(self):
        with mock.patch.dict("sys.modules", {"coinbase.src.graph.neo4j_graph": None}):
            self.assertIsNone(ds._get_graph_store())

    def test_get_graph_store_cached_and_new(self):
        fake_mod = mock.MagicMock()
        store_instance = mock.MagicMock()
        fake_mod.CryptoGraphStore.return_value = store_instance
        with mock.patch.dict("sys.modules", {"coinbase.src.graph.neo4j_graph": fake_mod}):
            ds.GRAPH_CACHE["data"] = None
            ds.GRAPH_CACHE["ts"] = 0.0
            out = ds._get_graph_store()
            self.assertIs(out, store_instance)
            # cached path
            out2 = ds._get_graph_store()
            self.assertIs(out2, store_instance)
        ds.GRAPH_CACHE["data"] = None

    def test_get_graph_store_construct_exception(self):
        fake_mod = mock.MagicMock()
        fake_mod.CryptoGraphStore.side_effect = RuntimeError("no db")
        with mock.patch.dict("sys.modules", {"coinbase.src.graph.neo4j_graph": fake_mod}):
            ds.GRAPH_CACHE["data"] = None
            ds.GRAPH_CACHE["ts"] = 0.0
            self.assertIsNone(ds._get_graph_store())


class TestCapitalPolicy(unittest.TestCase):
    def test_normalize_defaults(self):
        out = ds._normalize_capital_policy(None)
        self.assertIn("targets", out)
        self.assertEqual(out["preset_name"], "custom")

    def test_normalize_custom(self):
        out = ds._normalize_capital_policy({
            "targets": {"reserve": 1, "core": 1, "opportunity": 2},
            "core_allowlist": "btc-usd, eth",
            "core_min_allocation_pct": 5,
            "max_deployable_usd": 1000,
        })
        self.assertEqual(sorted(out["core_allowlist"]), ["BTC", "ETH"])
        self.assertAlmostEqual(sum(out["targets"].values()), 1.0, places=5)

    def test_normalize_empty_allowlist(self):
        out = ds._normalize_capital_policy({"core_allowlist": []})
        self.assertTrue(out["core_allowlist"])

    def test_get_capital_policy_from_store(self):
        store = mock.MagicMock()
        store.get_meta.return_value = json.dumps({"preset_name": "x"})
        with patch("_get_state_store", return_value=store):
            out = ds._get_capital_policy()
        self.assertIn("targets", out)

    def test_get_capital_policy_store_exception(self):
        store = mock.MagicMock()
        store.get_meta.side_effect = RuntimeError("x")
        with patch("_get_state_store", return_value=store), \
                patch("_load_json", return_value={"capitalPolicy": {"preset_name": "op"}}):
            out = ds._get_capital_policy()
        self.assertIn("targets", out)

    def test_get_capital_policy_operator_state(self):
        with patch("_get_state_store", return_value=None), \
                patch("_load_json", return_value={"capitalPolicy": {"preset_name": "op"}}):
            out = ds._get_capital_policy()
        self.assertIn("targets", out)

    def test_get_capital_policy_default(self):
        with patch("_get_state_store", return_value=None), \
                patch("_load_json", return_value={}):
            out = ds._get_capital_policy()
        self.assertIn("targets", out)

    def test_build_preset_payload(self):
        out = ds._build_preset_payload()
        self.assertEqual(len(out), 3)

    def test_save_capital_policy(self):
        store = mock.MagicMock()
        with patch("_get_state_store", return_value=store), \
                patch("_load_json", return_value={}), \
                patch("_write_json", return_value=True):
            out = ds._save_capital_policy({"preset_name": "custom"})
        self.assertIn("updated_at", out)
        store.set_meta.assert_called_once()

    def test_save_capital_policy_store_exception(self):
        store = mock.MagicMock()
        store.set_meta.side_effect = RuntimeError("x")
        with patch("_get_state_store", return_value=store), \
                patch("_load_json", return_value={}), \
                patch("_write_json", return_value=True):
            out = ds._save_capital_policy({})
        self.assertIn("updated_at", out)


class TestCapitalBuckets(unittest.TestCase):
    def test_load_capital_buckets_empty(self):
        with patch("_load_json", return_value={}):
            out = ds._load_capital_buckets()
        self.assertEqual(out["buckets"], [])

    def test_load_capital_buckets_data(self):
        payload = {"buckets": [
            {"bucket_id": "b1", "name": "B1", "cash_usd": 100,
             "starting_balance_usd": 200, "realized_pnl_usd": 5,
             "volume_30d_usd": 5000, "target_volume_usd": 10000,
             "target_multiple": 2.0, "active": True,
             "positions": {"BTC": {"size": 1, "current_price": 100}},
             "allowed_strategies": ["a"]},
            "not_a_dict",
        ]}
        with patch("_load_json", return_value=payload):
            out = ds._load_capital_buckets()
        self.assertEqual(len(out["buckets"]), 1)
        self.assertGreater(out["total_value_usd"], 0)

    def test_load_capital_buckets_list_payload(self):
        with patch("_load_json", return_value=[{"bucket_id": "b", "positions": {}}]):
            out = ds._load_capital_buckets()
        self.assertEqual(len(out["buckets"]), 1)

    def test_load_capital_buckets_bad_type(self):
        with patch("_load_json", return_value={"buckets": "nope"}):
            out = ds._load_capital_buckets()
        self.assertEqual(out["buckets"], [])

    def test_bucket_preset_names_fallback(self):
        with mock.patch.dict("sys.modules", {"coinbase.src.capital_buckets": None}):
            names = ds._bucket_preset_names()
        self.assertIn("challenge", names)

    def test_bucket_preset_names_success(self):
        fake = mock.MagicMock()
        fake.bucket_preset_names.return_value = ["x"]
        with mock.patch.dict("sys.modules", {"coinbase.src.capital_buckets": fake}):
            self.assertEqual(ds._bucket_preset_names(), ["x"])

    def test_build_bucket_preset_success(self):
        fake = mock.MagicMock()
        fake.build_bucket_preset.return_value = {"buckets": []}
        with mock.patch.dict("sys.modules", {"coinbase.src.capital_buckets": fake}):
            out = ds._build_bucket_preset("challenge_5", {"starting_balance_usd": 5})
        self.assertEqual(out, {"buckets": []})

    def test_build_bucket_preset_fallbacks(self):
        with mock.patch.dict("sys.modules", {"coinbase.src.capital_buckets": None}):
            self.assertTrue(ds._build_bucket_preset("challenge_5")["buckets"])
            self.assertTrue(ds._build_bucket_preset("challenge")["buckets"])
            self.assertTrue(ds._build_bucket_preset("fee_tier")["buckets"])
            self.assertTrue(ds._build_bucket_preset("core")["buckets"])
            self.assertTrue(ds._build_bucket_preset("challenge_core_fee_tier")["buckets"])
            self.assertEqual(ds._build_bucket_preset("unknown")["buckets"], [])

    def test_bucket_preset_payloads(self):
        with patch("_bucket_preset_names", return_value=["challenge"]), \
                patch("_build_bucket_preset", return_value={"buckets": [{"name": "N"}]}):
            out = ds._bucket_preset_payloads()
        self.assertEqual(out[0]["label"], "N")

    def test_save_capital_buckets(self):
        payload = {"buckets": [
            {"bucket_id": "b1", "name": "B1", "starting_balance_usd": 100,
             "allowed_strategies": "a,b", "positions": {"X": {}}},
            "skip",
            {"id": "b2", "allowed_strategies": ["c"], "positions": "bad"},
        ]}
        with patch("_write_json", return_value=True), \
                patch("_load_capital_buckets", return_value={"buckets": []}):
            out = ds._save_capital_buckets(payload)
        self.assertEqual(out, {"buckets": []})

    def test_save_capital_buckets_list_and_bad(self):
        with patch("_write_json", return_value=True), \
                patch("_load_capital_buckets", return_value={"buckets": []}):
            ds._save_capital_buckets([{"bucket_id": "b"}])
            ds._save_capital_buckets("notalist")
            ds._save_capital_buckets({"buckets": "bad"})


class TestMiscHelpers(unittest.TestCase):
    def test_get_coinbase_cli(self):
        fake = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"portfolio_optimizer": fake}):
            self.assertIsNotNone(ds._get_coinbase_cli())
        with mock.patch.dict("sys.modules", {"portfolio_optimizer": None}):
            self.assertIsNone(ds._get_coinbase_cli())

    def test_update_approval(self):
        approvals = {"tok1": {"status": "pending"}}
        opstate = {"approvals": [{"id": "tok1", "status": "pending"}]}

        def fake_load(path, default=None):
            if "pending_approvals" in path:
                return dict(approvals)
            return {"approvals": [{"id": "tok1", "status": "pending"}]}

        with patch("_load_json", side_effect=fake_load), \
                patch("_write_json", return_value=True):
            self.assertTrue(ds._update_approval("tok1", "approved"))

    def test_update_approval_not_found(self):
        with patch("_load_json", return_value={}), patch("_write_json", return_value=True):
            self.assertFalse(ds._update_approval("nope", "approved"))

    def test_get_prediction_client(self):
        fake_mod = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"event_markets.unified_client": fake_mod,
                                             "dotenv": mock.MagicMock()}):
            self.assertIsNotNone(ds._get_prediction_client())

    def test_get_prediction_client_fail(self):
        with mock.patch.dict("sys.modules", {"event_markets.unified_client": None,
                                             "dotenv": None}):
            self.assertIsNone(ds._get_prediction_client())

    def test_get_event_arbitrage_scanner(self):
        fake_mod = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"event_markets.arbitrage": fake_mod}), \
                patch("_get_prediction_client", return_value=mock.MagicMock()):
            self.assertIsNotNone(ds._get_event_arbitrage_scanner())

    def test_get_event_arbitrage_scanner_no_client(self):
        fake_mod = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"event_markets.arbitrage": fake_mod}), \
                patch("_get_prediction_client", return_value=None):
            self.assertIsNotNone(ds._get_event_arbitrage_scanner())

    def test_get_event_arbitrage_scanner_fail(self):
        with mock.patch.dict("sys.modules", {"event_markets.arbitrage": None}):
            self.assertIsNone(ds._get_event_arbitrage_scanner())

    def test_ttl_cache(self):
        ds._ttl_cache_set("k1", "v1", ttl=10)
        self.assertEqual(ds._ttl_cache_get("k1"), "v1")
        self.assertIsNone(ds._ttl_cache_get("missing"))
        ds._ttl_cache_set("k2", "v2", ttl=-1)
        self.assertIsNone(ds._ttl_cache_get("k2"))

    def test_compute_capital_in_play(self):
        import datetime as dt
        start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
        store = mock.MagicMock()
        store.load_trades.return_value = [
            {"dry_run": 1},  # skipped
            {"timestamp": "bad"},  # bad ts skipped
            {"timestamp": "2023-01-01T00:00:00Z", "side": "BUY", "size_usd": 10},  # before start
            {"timestamp": "2024-06-01T00:00:00", "side": "BUY", "size_usd": 100},  # naive -> utc
            {"timestamp": "2024-06-01T00:00:00Z", "side": "SELL", "size_usd": 50},  # sell ignored
        ]
        ds._TTL_CACHE.clear()
        out = ds._compute_capital_in_play(store, hard_cap=1000, start_dt=start)
        self.assertEqual(out, 100.0)
        # cached path
        out2 = ds._compute_capital_in_play(store, hard_cap=1000, start_dt=start)
        self.assertEqual(out2, 100.0)

    def test_compute_capital_in_play_no_store(self):
        ds._TTL_CACHE.clear()
        self.assertEqual(ds._compute_capital_in_play(None, 0, None), 0.0)

    def test_call_with_timeout_success(self):
        self.assertEqual(ds._call_with_timeout(lambda: 42, 5), 42)

    def test_call_with_timeout_exception(self):
        def boom():
            raise RuntimeError("x")
        self.assertIsNone(ds._call_with_timeout(boom, 5))

    def test_call_with_timeout_timeout(self):
        def slow():
            time.sleep(0.5)
            return 1
        self.assertIsNone(ds._call_with_timeout(slow, 0.01))

    def test_get_regime(self):
        self.assertEqual(ds._get_regime(None), "neutral")
        self.assertEqual(ds._get_regime(10), "volatile")
        self.assertEqual(ds._get_regime(3), "trending")
        self.assertEqual(ds._get_regime(1), "neutral")
        self.assertEqual(ds._get_regime(0.1), "quiet")

    def test_compute_sharpe(self):
        self.assertEqual(ds._compute_sharpe([]), 0.0)
        self.assertEqual(ds._compute_sharpe([1]), 0.0)
        self.assertEqual(ds._compute_sharpe([1, 1, 1]), 0.0)
        self.assertNotEqual(ds._compute_sharpe([1, 2, 3, 4]), 0.0)

    def test_get_accumulator(self):
        fake = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"unified_signal_accumulator": fake}):
            self.assertIsNotNone(ds._get_accumulator())

    def test_get_accumulator_fail(self):
        with mock.patch.dict("sys.modules", {"unified_signal_accumulator": None}):
            self.assertIsNone(ds._get_accumulator())


if __name__ == "__main__":
    unittest.main()
