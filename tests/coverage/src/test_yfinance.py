import asyncio
import unittest
from datetime import datetime
from unittest import mock

import src.sources.yfinance as yfmod
from src.sources.yfinance import YFinanceDataSource


class _Idx:
    def __init__(self, lo, hi):
        self._lo, self._hi = lo, hi

    def min(self):
        return self._lo

    def max(self):
        return self._hi


class FakeDF:
    def __init__(self, empty, records=None, idx=None):
        self.empty = empty
        self._records = records or []
        self.index = idx or _Idx("", "")

    def to_dict(self, orient):
        return self._records

    def __len__(self):
        return len(self._records)


def _make_yf(empty=True, records=None, idx=None, raise_on_history=False):
    yf = mock.MagicMock()
    ticker = mock.MagicMock()
    if raise_on_history:
        ticker.history.side_effect = RuntimeError("boom")
    else:
        ticker.history.return_value = FakeDF(empty, records, idx)
    yf.Ticker.return_value = ticker
    return yf


class TestYFinance(unittest.TestCase):
    def test_yf_not_installed(self):
        with mock.patch.object(yfmod, "yf", None):
            ds = YFinanceDataSource()
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertEqual(res["error"], "yfinance library not installed")

    def test_fetch_empty(self):
        yf = _make_yf(empty=True)
        with mock.patch.object(yfmod, "yf", yf):
            res = asyncio.run(YFinanceDataSource().fetch("AAPL"))
        self.assertEqual(res["error"], "No data available for AAPL")

    def test_fetch_success(self):
        yf = _make_yf(empty=False, records=[{"a": 1}], idx=_Idx("2024-01-01", "2024-01-02"))
        with mock.patch.object(yfmod, "yf", yf):
            res = asyncio.run(YFinanceDataSource().fetch("AAPL"))
        self.assertEqual(res["data"], [{"a": 1}])
        self.assertEqual(res["start"], "2024-01-01")

    def test_fetch_exception(self):
        yf = _make_yf(raise_on_history=True)
        with mock.patch.object(yfmod, "yf", yf):
            res = asyncio.run(YFinanceDataSource().fetch("AAPL"))
        self.assertEqual(res["error"], "boom")

    def test_health_check_healthy(self):
        yf = _make_yf(empty=False, records=[{"a": 1}], idx=_Idx("2024-01-01", "2024-01-02"))
        with mock.patch.object(yfmod, "yf", yf):
            res = asyncio.run(YFinanceDataSource().health_check())
        self.assertEqual(res["status"], "healthy")
        self.assertEqual(res["latency_ms"], 50)

    def test_health_check_no_latency(self):
        yf = _make_yf(empty=True)
        with mock.patch.object(yfmod, "yf", yf):
            res = asyncio.run(YFinanceDataSource().health_check())
        self.assertEqual(res["status"], "healthy")
        self.assertEqual(res["latency_ms"], 0)

    def test_health_check_exception(self):
        yf = _make_yf(raise_on_history=True)
        with mock.patch.object(yfmod, "yf", yf):
            res = asyncio.run(YFinanceDataSource().health_check())
        self.assertEqual(res["status"], "unhealthy")

    def test_get_available_symbols(self):
        ds = YFinanceDataSource()
        self.assertIn("BTC-USD", asyncio.run(ds.get_available_symbols("crypto")))
        self.assertIn("AAPL", asyncio.run(ds.get_available_symbols("stocks")))
        self.assertEqual(asyncio.run(ds.get_available_symbols()), [])

    def test_calculate_period_within_year(self):
        ds = YFinanceDataSource()
        p = ds._calculate_period(datetime(2024, 1, 1), datetime(2024, 1, 11))
        self.assertEqual(p, "10d")

    def test_calculate_period_beyond_year(self):
        ds = YFinanceDataSource()
        p = ds._calculate_period(datetime(2024, 1, 1), datetime(2025, 3, 1))
        self.assertEqual(p, "max")

    def test_calculate_period_no_dates(self):
        ds = YFinanceDataSource()
        self.assertEqual(ds._calculate_period(None, None), "max")


if __name__ == "__main__":
    unittest.main()
