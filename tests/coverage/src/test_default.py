import asyncio
import json
import os
import tempfile
import unittest
from unittest import mock
from datetime import datetime

from src.sources.default import DefaultDataSource


class TestDefault(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ds = DefaultDataSource(cache_dir=self.tmp)

    def test_init_creates_cache_dir(self):
        new = os.path.join(self.tmp, "sub", "dir")
        ds = DefaultDataSource(cache_dir=new)
        self.assertTrue(os.path.isdir(new))

    def test_fetch_cached_file(self):
        cache_file = os.path.join(self.tmp, "AAPL.json")
        with open(cache_file, "w") as f:
            json.dump([{"date": "2024-01-01", "close": 1.0}], f)
        res = asyncio.run(self.ds.fetch("AAPL"))
        self.assertEqual(res["source"], "default-cache")
        self.assertEqual(res["data"][0]["close"], 1.0)

    def test_fetch_mock_generated(self):
        res = asyncio.run(self.ds.fetch("AAPL"))
        self.assertEqual(res["source"], "default-mock")
        self.assertTrue(len(res["data"]) > 0)
        # cached for next time
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "AAPL.json")))

    def test_generate_mock_with_end_date(self):
        res = self.ds._generate_mock_data("AAPL", end_date=datetime(2024, 1, 10))
        self.assertEqual(len(res["data"]), 30)

    def test_generate_mock_unknown_symbol(self):
        res = self.ds._generate_mock_data("FOOCOIN")
        self.assertEqual(res["symbol"], "FOOCOIN")
        self.assertTrue(len(res["data"]) > 0)

    def test_fetch_exception(self):
        with mock.patch("src.sources.default.open", side_effect=OSError("boom")):
            res = asyncio.run(self.ds.fetch("AAPL"))
        self.assertEqual(res["source"], "default")
        self.assertIn("boom", res["error"])

    def test_health_check(self):
        res = asyncio.run(self.ds.health_check())
        self.assertEqual(res["status"], "healthy")

    def test_get_available_symbols(self):
        syms = asyncio.run(self.ds.get_available_symbols())
        self.assertIn("AAPL", syms)
        self.assertIn("BTC-USD", syms)


if __name__ == "__main__":
    unittest.main()
