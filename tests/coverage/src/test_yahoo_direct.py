import asyncio
import json
import unittest
from datetime import datetime
from unittest import mock

import src.sources.yahoo_direct as yd
from src.sources.yahoo_direct import (
    fetch_history, fetch_close_series, try_get_tz, YahooDirectDataSource,
)


class _Ctx:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._payload


def _chart(n=3, with_meta=True, close_none_idx=None):
    ts = [1700000000 + i * 86400 for i in range(n)]
    closes = [1.5] * n
    if close_none_idx is not None:
        closes[close_none_idx] = None
    quotes = {
        "open": [1.0] * n,
        "high": [2.0] * n,
        "low": [0.5] * n,
        "close": closes,
        "volume": [100] * n,
    }
    result = {"timestamp": ts, "indicators": {"quote": [quotes]}}
    if with_meta:
        result["meta"] = {"exchangeTimezoneName": "America/New_York"}
    return {"chart": {"result": [result]}}


class TestYahooDirect(unittest.TestCase):
    def test_request_exception(self):
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        side_effect=RuntimeError("net")):
            self.assertIsNone(yd._request("http://x"))

    def test_request_success(self):
        payload = json.dumps({"chart": {"result": []}}).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            self.assertIsNotNone(yd._request("http://x"))

    def test_fetch_history_none(self):
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        side_effect=RuntimeError("net")):
            self.assertIsNone(fetch_history("AAPL"))

    def test_fetch_history_empty_result(self):
        payload = json.dumps({"chart": {"result": []}}).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            self.assertIsNone(fetch_history("AAPL"))

    def test_fetch_history_missing_result(self):
        payload = json.dumps({}).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            self.assertIsNone(fetch_history("AAPL"))

    def test_fetch_history_empty_timestamps(self):
        payload = json.dumps({"chart": {"result": [{"timestamp": [],
                                                    "indicators": {"quote": [{}]}}]}}).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            self.assertIsNone(fetch_history("AAPL"))

    def test_fetch_history_empty_quotes(self):
        payload = json.dumps({"chart": {"result": [{"timestamp": [1, 2],
                                                    "indicators": {"quote": [{}]}}]}}).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            self.assertIsNone(fetch_history("AAPL"))

    def test_fetch_history_success(self):
        payload = json.dumps(_chart(3)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            rows = fetch_history("AAPL")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["close"], 1.5)

    def test_fetch_history_skip_none_close(self):
        payload = json.dumps(_chart(3, close_none_idx=1)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            rows = fetch_history("AAPL")
        self.assertEqual(len(rows), 2)

    def test_fetch_close_series(self):
        payload = json.dumps(_chart(3)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            closes = fetch_close_series("AAPL")
        self.assertEqual(closes, [1.5, 1.5, 1.5])

    def test_fetch_close_series_none(self):
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        side_effect=RuntimeError("net")):
            self.assertEqual(fetch_close_series("AAPL"), [])

    def test_try_get_tz_none(self):
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        side_effect=RuntimeError("net")):
            self.assertIsNone(try_get_tz())

    def test_try_get_tz(self):
        payload = json.dumps(_chart(1)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            self.assertEqual(try_get_tz(), "America/New_York")

    def test_datasource_fetch_with_dates(self):
        payload = json.dumps(_chart(3)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            res = asyncio.run(YahooDirectDataSource().fetch(
                "AAPL", datetime(2024, 1, 1), datetime(2024, 1, 2)))
        self.assertEqual(len(res["data"]), 3)
        self.assertEqual(res["source"], "yahoo_direct")

    def test_datasource_fetch_no_dates(self):
        payload = json.dumps(_chart(3)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            res = asyncio.run(YahooDirectDataSource().fetch("AAPL"))
        self.assertEqual(len(res["data"]), 3)

    def test_datasource_fetch_error(self):
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        side_effect=RuntimeError("net")):
            res = asyncio.run(YahooDirectDataSource().fetch("AAPL"))
        self.assertIn("No data available", res["error"])

    def test_health_check_healthy(self):
        payload = json.dumps(_chart(1)).encode()
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        return_value=_Ctx(payload)):
            res = asyncio.run(YahooDirectDataSource().health_check())
        self.assertEqual(res["status"], "healthy")

    def test_health_check_unhealthy(self):
        with mock.patch("src.sources.yahoo_direct.urllib.request.urlopen",
                        side_effect=RuntimeError("net")):
            res = asyncio.run(YahooDirectDataSource().health_check())
        self.assertEqual(res["status"], "unhealthy")

    def test_health_check_exception(self):
        with mock.patch("src.sources.yahoo_direct.fetch_history",
                        side_effect=RuntimeError("boom")):
            res = asyncio.run(YahooDirectDataSource().health_check())
        self.assertEqual(res["status"], "unhealthy")

    def test_patch_yfinance_success_body(self):
        # At import time yfinance may be unavailable (env import error), so the
        # except branch is taken. Exercise the success body via a fake module.
        import sys
        import types
        fake_yf = types.ModuleType("yfinance")
        fake_data = types.ModuleType("yfinance.data")
        calls = {"raise": False}

        class YfData:
            @staticmethod
            def _get_cookie_and_crumb_basic(self, timeout):
                if calls["raise"]:
                    raise RuntimeError("blocked")
                return "cookie"

        fake_data.YfData = YfData
        fake_yf.data = fake_data
        with mock.patch.dict(sys.modules, {"yfinance": fake_yf}):
            yd.patch_yfinance()
            inst = YfData()
            # success path executes the try/return
            self.assertEqual(inst._get_cookie_and_crumb_basic(1), "cookie")
            # exception path executes the except branch
            calls["raise"] = True
            self.assertEqual(inst._get_cookie_and_crumb_basic(1), "")
        self.assertTrue(hasattr(YfData, "_get_cookie_and_crumb_basic"))

    def test_get_available_symbols(self):
        self.assertIn("BTC-USD", asyncio.run(YahooDirectDataSource().get_available_symbols("crypto")))
        self.assertIn("AAPL", asyncio.run(YahooDirectDataSource().get_available_symbols("stocks")))
        self.assertEqual(asyncio.run(YahooDirectDataSource().get_available_symbols()), [])


if __name__ == "__main__":
    unittest.main()
