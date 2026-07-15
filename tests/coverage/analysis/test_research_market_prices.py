import sys
import tempfile
import unittest
from unittest.mock import mock_open, patch

import trading_system.analysis.research_market_prices as rmp

# The module only imports `sys` inside the `__main__` guard, so `main()` would
# otherwise raise NameError. Inject it for testing. (Reported as a bug.)
rmp.sys = sys


class TestResearchMarketPrices(unittest.TestCase):
    def test_get_current_prices(self):
        # should not raise
        rmp.get_current_prices()

    def test_analyze_offline_no_file(self):
        with patch.object(rmp.os.path, "exists", return_value=False):
            out = rmp.analyze_offline("missing.json")
        self.assertIn("summary", out)

    def test_analyze_offline_with_file(self):
        data = {
            "AAPL": {"2020": 100.0, "2021": 150.0, "2022": 200.0},
            "EMPTY": {},
            "SHORT": {"2020": 100.0},
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            import json
            json.dump(data, f)
            path = f.name
        try:
            with patch.object(rmp.os.path, "exists", return_value=True):
                out = rmp.analyze_offline(path)
        finally:
            import os
            os.unlink(path)
        self.assertIn("AAPL", out)
        self.assertIn("current_price", out["AAPL"])
        # EMPTY and SHORT should be skipped (no results added)
        self.assertNotIn("EMPTY", out)
        self.assertNotIn("SHORT", out)

    def test_download_market_data_exists(self):
        with patch.object(rmp.os.path, "exists", return_value=True):
            self.assertTrue(rmp.download_market_data())

    def test_download_market_data_writes(self):
        m = mock_open()
        with patch.object(rmp.os.path, "exists", return_value=False), \
             patch.object(rmp, "open", m):
            rmp.download_market_data()
        self.assertTrue(m.called)

    def test_generate_report(self):
        report = rmp.generate_report()
        self.assertIn("FAIR MARKET PRICE RESEARCH REPORT", report)
        self.assertIn("AAPL", report)

    def test_main_download(self):
        m = mock_open()
        with patch.object(sys, "argv", ["x", "--download"]), \
             patch.object(rmp.os.path, "exists", return_value=False), \
             patch.object(rmp, "open", m):
            rmp.main()

    def test_main_analyze(self):
        with patch.object(sys, "argv", ["x", "--analyze", "--foo", "--input=bar.json"]), \
             patch.object(rmp.os.path, "exists", return_value=False):
            rmp.main()

    def test_main_report(self):
        m = mock_open()
        with patch.object(sys, "argv", ["x", "--report"]), \
             patch.object(rmp, "open", m):
            rmp.main()

    def test_main_unknown_action(self):
        with patch.object(sys, "argv", ["x", "--bogus"]):
            rmp.main()

    def test_main_interactive(self):
        m = mock_open()
        with patch.object(sys, "argv", ["x"]), \
             patch.object(rmp.os.path, "exists", return_value=False), \
             patch.object(rmp, "open", m), \
             patch.object(rmp, "input", side_effect=["5", "1", "2", "3", "4"]):
            rmp.main()


if __name__ == "__main__":
    unittest.main()
