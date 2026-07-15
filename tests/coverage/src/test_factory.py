import asyncio
import os
import unittest
from unittest import mock

from src.sources.factory import DataSourceFactory


def _ok(data):
    return {"symbol": "X", "data": data, "source": "x", "error": None}


def _err(msg):
    return {"symbol": "X", "data": [], "source": "x", "error": msg}


class TestFactory(unittest.TestCase):
    def test_create_source_unknown(self):
        f = DataSourceFactory()
        with self.assertRaises(ValueError):
            f.create_source("nope")

    def test_create_all_known(self):
        f = DataSourceFactory()
        for name in ["yahoo_direct", "yfinance", "alphavantage", "default"]:
            src = f.create_source(name)
            self.assertIsNotNone(src)

    def test_create_alphavantage_with_config_key(self):
        f = DataSourceFactory(config={"alphavantage": {"api_key": "SECRET"}})
        src = f.create_source("alphavantage")
        self.assertEqual(src.api_key, "SECRET")

    def test_create_alphavantage_without_config_key(self):
        f = DataSourceFactory(config={})
        with mock.patch.dict(os.environ, {"ALPHA_VANTAGE_API_KEY": "ENVKEY"}):
            src = f.create_source("alphavantage")
        self.assertEqual(src.api_key, "ENVKEY")

    def test_health_check_all(self):
        f = DataSourceFactory()
        res = asyncio.run(f.health_check_all())
        self.assertIn("yahoo_direct", res)
        self.assertIn("yfinance", res)
        self.assertIn("alphavantage", res)

    def test_health_check_all_exception(self):
        f = DataSourceFactory()
        with mock.patch("src.sources.yahoo_direct.YahooDirectDataSource.health_check",
                        side_effect=RuntimeError("down")):
            res = asyncio.run(f.health_check_all())
        self.assertEqual(res["yahoo_direct"]["status"], "unhealthy")
        self.assertIn("down", res["yahoo_direct"]["error"])

    def test_fetch_with_fallback_success_and_branches(self):
        f = DataSourceFactory()
        with mock.patch("src.sources.yahoo_direct.YahooDirectDataSource.fetch",
                        new=mock.AsyncMock(return_value=_err("yfinance library not installed"))), \
             mock.patch("src.sources.alphavantage.AlphaVantageDataSource.fetch",
                        new=mock.AsyncMock(return_value=_err("rate limited"))), \
             mock.patch("src.sources.yfinance.YFinanceDataSource.fetch",
                        new=mock.AsyncMock(side_effect=RuntimeError("boom"))), \
             mock.patch("src.sources.default.DefaultDataSource.fetch",
                        new=mock.AsyncMock(return_value=_ok([1, 2, 3]))):
            res = asyncio.run(f.fetch_with_fallback("AAPL", None, None))
        self.assertEqual(res["data"], [1, 2, 3])

    def test_fetch_with_fallback_all_fail(self):
        f = DataSourceFactory()
        with mock.patch("src.sources.yahoo_direct.YahooDirectDataSource.fetch",
                        new=mock.AsyncMock(return_value=_err("some error"))):
            res = asyncio.run(f.fetch_with_fallback(
                "AAPL", None, None, preferred_sources=["yahoo_direct"]))
        self.assertIn("error", res)
        self.assertIn("All sources failed", res["error"])

    def test_health_check_all_no_method(self):
        f = DataSourceFactory()

        class _NoHealth:
            pass

        with mock.patch("src.sources.yfinance.YFinanceDataSource", _NoHealth):
            res = asyncio.run(f.health_check_all())
        self.assertEqual(res["yfinance"]["status"], "unknown")


if __name__ == "__main__":
    unittest.main()
