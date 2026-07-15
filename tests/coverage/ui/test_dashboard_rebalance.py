import json
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest import mock
from urllib.request import urlopen

import trading_system.ui.dashboard_server as ds

try:
    import coinbase.src.rebalance_engine as real_mod
except Exception:  # pragma: no cover - depends on rust_core availability
    real_mod = None


def patch(attr, **kw):
    return mock.patch.object(ds, attr, **kw)


class FakeStairStepList:
    """Emulates StairStepEngine.to_dict() returning state tuples (real API)."""
    def __init__(self):
        self._symbols = {"BTC-USD": object()}

    def to_dict(self):
        return {"BTC-USD": [0, 1, 0, 100.0, 5.0, "BUY"]}


class FakeStairStepDict:
    """Emulates a richer to_dict() with explicit step_levels/base_size."""
    def __init__(self):
        self._symbols = {"ETH-USD": object()}

    def to_dict(self):
        return {"ETH-USD": {
            "state": {"next_buy_index": 1, "filled_buys": 1, "filled_sells": 0,
                      "inventory_value": 50.0, "realized_pnl": 0.0, "last_action": "BUY"},
            "step_levels": [0.95, 0.90, 0.85],
            "base_size": 50.0,
        }}


class FakeTaker:
    low = 0.8
    high = 1.2
    steps = 3
    base_size_pct = 10.0
    budget = 1000.0


class FakeStairStepRich:
    """to_dict() returns a state tuple plus a taker exposing grid attrs."""
    def __init__(self):
        self._symbols = {"BTC-USD": FakeTaker()}

    def to_dict(self):
        return {"BTC-USD": [0, 1, 0, 100.0, 5.0, "BUY"]}


class FakeTakerBad:
    low = 0.8
    high = 1.2
    steps = 3
    base_size_pct = 10.0
    budget = "bad"  # triggers TypeError in base_size computation


class FakeStairStepBad:
    def __init__(self):
        self._symbols = {"BTC-USD": FakeTakerBad()}

    def to_dict(self):
        return {"BTC-USD": [0, 1, 0, 100.0, 5.0, "BUY"]}


class FakeTakerFlat:
    low = 1.0
    high = 1.0  # high == low -> step_levels stays None
    steps = 3
    base_size_pct = 10.0
    budget = 1000.0


class FakeStairStepFlat:
    def __init__(self):
        self._symbols = {"BTC-USD": FakeTakerFlat()}

    def to_dict(self):
        return {"BTC-USD": [0, 1, 0, 100.0, 5.0, "BUY"]}


class FakeStairStepNonDict:
    def __init__(self):
        self._symbols = {"BTC-USD": object()}

    def to_dict(self):
        return ["not", "a", "dict"]


class TestActivePreset(unittest.TestCase):
    def test_default(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(ds._active_rebalance_preset(None), "core_balanced")

    def test_env_and_validation(self):
        with mock.patch.dict("os.environ", {"REBALANCE_PRESET": "safe"}):
            self.assertEqual(ds._active_rebalance_preset(real_mod), "safe")
        with mock.patch.dict("os.environ", {"REBALANCE_PRESET": "nope"}):
            self.assertEqual(ds._active_rebalance_preset(real_mod), "core_balanced")


class TestApiRebalance(unittest.TestCase):
    def test_module_none(self):
        with patch("_get_rebalance_module", return_value=None):
            out = ds.api_rebalance()
        self.assertEqual(out["available"], False)
        self.assertEqual(out["presets"], [])
        self.assertIsNotNone(out["active_preset"])
        self.assertIsNone(out["current_drift"])
        self.assertIsNone(out["recommendation"])

    def test_full(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_rebalance_current_holdings", return_value=(1000.0, {"BTC-USD": 400, "ETH-USD": 350, "SOL-USD": 250})):
            out = ds.api_rebalance()
        self.assertEqual(out["available"], True)
        self.assertEqual(len(out["presets"]), 3)
        self.assertIn("core_balanced", {p["name"] for p in out["presets"]})
        self.assertIsNotNone(out["recommendation"])
        self.assertIn("max_drift", out["recommendation"])
        self.assertIn("turnover", out["recommendation"])
        self.assertIsInstance(out["current_drift"], float)

    def test_no_holdings(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_rebalance_current_holdings", return_value=(0.0, {})):
            out = ds.api_rebalance()
        self.assertEqual(out["available"], True)
        self.assertEqual(len(out["presets"]), 3)
        self.assertIsNone(out["recommendation"])
        self.assertIsNone(out["current_drift"])

    def test_compute_raises(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        bad_engine = mock.MagicMock()
        bad_engine.compute.side_effect = RuntimeError("boom")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_rebalance_engine", return_value=bad_engine), \
                patch("_rebalance_current_holdings", return_value=(1000.0, {"BTC-USD": 400})):
            out = ds.api_rebalance()
        self.assertIsNone(out["recommendation"])


class TestApiRebalancePresets(unittest.TestCase):
    def test_module_none(self):
        with patch("_get_rebalance_module", return_value=None):
            out = ds.api_rebalance_presets()
        self.assertEqual(out["available"], False)
        self.assertEqual(out["presets"], [])

    def test_full(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod):
            out = ds.api_rebalance_presets()
        self.assertEqual(out["available"], True)
        self.assertEqual(len(out["presets"]), 3)
        for p in out["presets"]:
            self.assertIn("name", p)
            self.assertIn("weights", p)
            self.assertIsInstance(p["weights"], dict)


class TestApiStairStep(unittest.TestCase):
    def test_module_none(self):
        with patch("_get_rebalance_module", return_value=None):
            out = ds.api_stairstep()
        self.assertEqual(out["available"], False)
        self.assertEqual(out["symbols"], [])

    def test_engine_none(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=None):
            out = ds.api_stairstep()
        self.assertEqual(out["available"], True)
        self.assertEqual(out["symbols"], [])

    def test_full_list(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=FakeStairStepList()):
            out = ds.api_stairstep()
        self.assertEqual(out["available"], True)
        self.assertEqual(len(out["symbols"]), 1)
        sym = out["symbols"][0]
        self.assertEqual(sym["symbol"], "BTC-USD")
        self.assertEqual(sym["state"]["filled_buys"], 1)
        self.assertEqual(sym["state"]["last_action"], "BUY")
        self.assertIsNone(sym["step_levels"])

    def test_full_dict(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=FakeStairStepDict()):
            out = ds.api_stairstep()
        self.assertEqual(len(out["symbols"]), 1)
        sym = out["symbols"][0]
        self.assertEqual(sym["symbol"], "ETH-USD")
        self.assertEqual(sym["step_levels"], [0.95, 0.90, 0.85])
        self.assertEqual(sym["base_size"], 50.0)
        self.assertEqual(sym["state"]["next_buy_index"], 1)

    def test_to_dict_raises(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        bad_engine = mock.MagicMock()
        bad_engine.to_dict.side_effect = RuntimeError("boom")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=bad_engine):
            out = ds.api_stairstep()
        self.assertEqual(out["available"], True)
        self.assertEqual(out["symbols"], [])


class TestRebalanceCoverage(unittest.TestCase):
    def test_current_holdings(self):
        holdings = {
            "BTC-USD": {"value": 400.0},
            "ETH": {"value": 350.0},
            "junk": "notadict",
            "SOL-USD": {"value": 0},
            "XRP-USD": {"current_value": 100.0},
        }
        with patch("_load_json", return_value={"holdings": holdings}):
            total, current = ds._rebalance_current_holdings()
        self.assertEqual(total, 850.0)
        self.assertEqual(current["ETH-USD"], 350.0)
        self.assertEqual(current["XRP-USD"], 100.0)
        self.assertNotIn("SOL-USD", current)

    def test_current_holdings_not_dict(self):
        with patch("_load_json", return_value={"holdings": "nope"}):
            total, current = ds._rebalance_current_holdings()
        self.assertEqual(total, 0.0)
        self.assertEqual(current, {})

    def test_module_import_fails(self):
        with mock.patch.dict("sys.modules", {"coinbase.src.rebalance_engine": None}):
            self.assertIsNone(ds._get_rebalance_module())

    def test_rebalance_engine_build_fails(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                mock.patch.object(real_mod.RebalanceEngine, "from_preset", side_effect=RuntimeError("x")):
            out = ds.api_rebalance()
        self.assertIsNone(out["recommendation"])

    def test_stairstep_engine_build_fails(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                mock.patch.object(real_mod, "StairStepEngine", side_effect=RuntimeError("x")):
            out = ds.api_stairstep()
        self.assertEqual(out["symbols"], [])

    def test_normalize_rich(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=FakeStairStepRich()):
            out = ds.api_stairstep()
        sym = out["symbols"][0]
        self.assertEqual(sym["symbol"], "BTC-USD")
        self.assertEqual(sym["step_levels"], [0.8, 0.933333, 1.066667, 1.2])
        self.assertEqual(sym["base_size"], 100.0)
        self.assertEqual(sym["state"]["filled_buys"], 1)

    def test_normalize_flat(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=FakeStairStepFlat()):
            out = ds.api_stairstep()
        self.assertIsNone(out["symbols"][0]["step_levels"])
        self.assertEqual(out["symbols"][0]["base_size"], 100.0)
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=FakeStairStepBad()):
            out = ds.api_stairstep()
        self.assertIsNone(out["symbols"][0]["base_size"])

    def test_stairstep_nondict(self):
        if real_mod is None:
            self.skipTest("rebalance_engine not importable")
        with patch("_get_rebalance_module", return_value=real_mod), \
                patch("_build_stairstep_engine", return_value=FakeStairStepNonDict()):
            out = ds.api_stairstep()
        self.assertEqual(out["symbols"], [])

    def test_normalize_else_and_missing(self):
        engine = FakeStairStepRich()
        entry = ds._normalize_stairstep_entry("MISSING", [1, 2, 3, 4, 5, "X"], engine)
        self.assertIsNone(entry["step_levels"])
        self.assertIsNone(entry["base_size"])
        self.assertEqual(entry["state"]["next_buy_index"], 1)
        weird = ds._normalize_stairstep_entry("X", "weird", engine)
        self.assertEqual(weird["state"], {})


class TestRebalanceEndpointsLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), ds.DashboardHandler)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def _get(self, path):
        url = f"http://127.0.0.1:{self.port}{path}"
        with urlopen(url, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())

    def test_rebalance(self):
        status, data = self._get("/strategies/rebalance")
        self.assertEqual(status, 200)
        self.assertIn("available", data)
        self.assertIn("presets", data)
        self.assertIn("active_preset", data)
        self.assertIn("current_drift", data)
        self.assertIn("recommendation", data)

    def test_rebalance_presets(self):
        status, data = self._get("/strategies/rebalance/presets")
        self.assertEqual(status, 200)
        self.assertIn("available", data)
        self.assertIn("presets", data)
        if data["presets"]:
            self.assertIn("weights", data["presets"][0])

    def test_stairstep(self):
        status, data = self._get("/strategies/stairstep")
        self.assertEqual(status, 200)
        self.assertIn("available", data)
        self.assertIn("symbols", data)
        for sym in data["symbols"]:
            self.assertIn("step_levels", sym)
            self.assertIn("base_size", sym)
            self.assertIn("state", sym)


if __name__ == "__main__":
    unittest.main()
