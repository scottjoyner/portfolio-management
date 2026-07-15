import asyncio
import unittest
from datetime import datetime
from unittest import mock

from src.sources.alphavantage import AlphaVantageDataSource


class FakeResponse:
    def __init__(self, json_data, status_code=200, headers=None):
        self._json = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._json


def _series():
    return {
        "Meta Data": {
            "Time Series (Daily)": {
                "2024-01-02": {"1. open": "1", "2. high": "2",
                               "3. low": "3", "4. close": "4", "5. volume": "5"},
                "2024-01-01": {"1. open": "1", "2. high": "2",
                               "3. low": "3", "4. close": "4", "5. volume": "5"},
            }
        }
    }


class TestAlphaVantage(unittest.TestCase):
    def test_requests_not_installed(self):
        with mock.patch("src.sources.alphavantage.requests", None):
            ds = AlphaVantageDataSource(api_key="k")
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertEqual(res["error"], "requests library not installed")

    def test_fetch_rate_limited(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse(_series(), headers={"Retry-After": "30"})
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertIn("Retry after 30s", res["error"])

    def test_fetch_no_data_found(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse({"Note:": "No Data Found returned from the servers"})
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertEqual(res["error"], "No data available")

    def test_fetch_empty_timeseries(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse({"Meta Data": {"Time Series (Daily)": {}}})
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertEqual(res["error"], "No time series data returned")

    def test_fetch_success_with_dates(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse(_series())
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp) as m:
            res = asyncio.run(ds.fetch("AAPL", datetime(2024, 1, 1), datetime(2024, 1, 2)))
        self.assertEqual(len(res["data"]), 2)
        self.assertEqual(res["source"], "alphavantage")
        # dates provided -> interval param added
        _, kwargs = m.call_args
        self.assertEqual(kwargs["params"]["interval"], "1d")

    def test_fetch_success_no_dates(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse(_series())
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertEqual(res["start"], "2024-01-01")
        self.assertEqual(res["end"], "2024-01-02")

    def test_fetch_exception(self):
        ds = AlphaVantageDataSource(api_key="k")
        with mock.patch("src.sources.alphavantage.requests.get",
                        side_effect=RuntimeError("boom")):
            res = asyncio.run(ds.fetch("AAPL"))
        self.assertEqual(res["error"], "boom")

    def test_health_check_rate_limited(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse({}, status_code=429)
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.health_check())
        self.assertEqual(res["status"], "rate_limited")

    def test_health_check_note_unhealthy(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse({"Note": "No Data Found returned from the servers"})
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.health_check())
        self.assertEqual(res["status"], "unhealthy")

    def test_health_check_healthy(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse({"Global Quote": {"05. price": "150.0"}})
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.health_check())
        self.assertEqual(res["status"], "healthy")
        self.assertEqual(res["price"], 150.0)

    def test_health_check_unhealthy_status(self):
        ds = AlphaVantageDataSource(api_key="k")
        resp = FakeResponse({}, status_code=500)
        with mock.patch("src.sources.alphavantage.requests.get", return_value=resp):
            res = asyncio.run(ds.health_check())
        self.assertEqual(res["status"], "unhealthy")

    def test_health_check_exception(self):
        ds = AlphaVantageDataSource(api_key="k")
        with mock.patch("src.sources.alphavantage.requests.get",
                        side_effect=RuntimeError("down")):
            res = asyncio.run(ds.health_check())
        self.assertEqual(res["status"], "unhealthy")

    def test_get_available_symbols(self):
        ds = AlphaVantageDataSource(api_key="k")
        self.assertIn("BTCUSD", asyncio.run(ds.get_available_symbols("crypto")))
        self.assertIn("AAPL", asyncio.run(ds.get_available_symbols("stocks")))
        self.assertEqual(asyncio.run(ds.get_available_symbols()), [])

    def test_missing_api_key_warns(self):
        with self.assertLogs("src.sources.alphavantage", level="WARNING"):
            AlphaVantageDataSource(api_key="")


if __name__ == "__main__":
    unittest.main()
