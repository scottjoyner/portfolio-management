import asyncio
import os
import sys
import types
import unittest
from unittest.mock import patch

import trading_system.backtesters.prediction_markets as pm_mod
from trading_system.backtesters.prediction_markets import PredictionMarketsBacktester


def _fake_module(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


class _Alpaca:
    def __init__(self, paper_trading=False):
        pass


class _Coinbase:
    def __init__(self):
        pass


class TestPredictionMarkets(unittest.TestCase):
    def _inject(self, alpaca=_Alpaca, coinbase=_Coinbase):
        if alpaca is None:
            sys.modules["trading_system.connectors.alpaca"] = None
        else:
            sys.modules["trading_system.connectors.alpaca"] = _fake_module(
                "trading_system.connectors.alpaca", AlpacaConnector=alpaca
            )
        if coinbase is None:
            sys.modules["trading_system.connectors.coinbase"] = None
        else:
            sys.modules["trading_system.connectors.coinbase"] = _fake_module(
                "trading_system.connectors.coinbase", CoinbaseConnector=coinbase
            )

    def _restore(self):
        for m in ["trading_system.connectors.alpaca", "trading_system.connectors.coinbase"]:
            sys.modules.pop(m, None)

    def test_init(self):
        b = PredictionMarketsBacktester()
        self.assertIsNone(b.alpaca)
        self.assertIsNone(b.coinbase)
        self.assertIsNone(b.kalshi)
        self.assertIsNone(b.polymarket)

    def test_initialize_success(self):
        self._inject()
        try:
            os.environ["ALPACA_API_KEY"] = "realkey"
            b = PredictionMarketsBacktester()
            asyncio.run(b.initialize())
            self.assertIsNotNone(b.alpaca)
            self.assertIsNotNone(b.coinbase)
        finally:
            os.environ.pop("ALPACA_API_KEY", None)
            self._restore()

    def test_initialize_env_present(self):
        self._inject()
        try:
            os.environ["ALPACA_API_KEY"] = "realkey"
            with patch("pathlib.Path") as MockPath, \
                 patch("dotenv.load_dotenv") as ld:
                MockPath.return_value.exists.return_value = True
                b = PredictionMarketsBacktester()
                asyncio.run(b.initialize())
            ld.assert_called()
            self.assertIsNotNone(b.alpaca)
        finally:
            os.environ.pop("ALPACA_API_KEY", None)
            self._restore()

    def test_initialize_import_error(self):
        self._inject(alpaca=None, coinbase=None)
        try:
            b = PredictionMarketsBacktester()
            asyncio.run(b.initialize())
            self.assertIsNone(b.alpaca)
            self.assertIsNone(b.coinbase)
        finally:
            self._restore()

    def test_fetch_polymarket(self):
        b = PredictionMarketsBacktester()
        res = asyncio.run(b.fetch_historical_market_data("m1", "2024-01-01", "2024-02-01", "polymarket"))
        self.assertIn("data_points", res)

    def test_fetch_kalshi_with_client(self):
        b = PredictionMarketsBacktester()
        b.kalshi = _Coinbase()  # stand-in with async method
        async def _hist(mid):
            return {"history": mid}
        b.kalshi.get_market_history = _hist
        res = asyncio.run(b.fetch_historical_market_data("m1", "2024-01-01", "2024-02-01", "kalshi"))
        self.assertEqual(res, {"history": "m1"})

    def test_fetch_kalshi_no_client(self):
        b = PredictionMarketsBacktester()
        b.kalshi = None
        res = asyncio.run(b.fetch_historical_market_data("m1", "2024-01-01", "2024-02-01", "kalshi"))
        self.assertEqual(res, {})

    def test_fetch_kalshi_exception(self):
        b = PredictionMarketsBacktester()
        b.kalshi = _Coinbase()
        async def _boom(mid):
            raise RuntimeError("no history")
        b.kalshi.get_market_history = _boom
        res = asyncio.run(b.fetch_historical_market_data("m1", "2024-01-01", "2024-02-01", "kalshi"))
        self.assertEqual(res, {})

    def test_fetch_unknown_platform(self):
        b = PredictionMarketsBacktester()
        res = asyncio.run(b.fetch_historical_market_data("m1", "2024-01-01", "2024-02-01", "foo"))
        self.assertEqual(res, {})

    def test_generate_sample(self):
        b = PredictionMarketsBacktester()
        res = b._generate_sample_historical_data("m1", "2024-01-01", "2024-02-01")
        self.assertEqual(res["market_id"], "m1")
        self.assertEqual(len(res["data_points"]), 1)

    def test_calculate_no_data_points(self):
        b = PredictionMarketsBacktester()
        self.assertEqual(b.calculate_backtest_metrics({}), {"error": "No data points available"})

    def test_calculate_insufficient(self):
        b = PredictionMarketsBacktester()
        res = b.calculate_backtest_metrics({"data_points": [{"close": 0.5}]})
        self.assertEqual(res, {"error": "Insufficient price data"})

    def test_calculate_full(self):
        b = PredictionMarketsBacktester()
        data = {"data_points": [{"close": 0.5}, {"close": 0.6}, {"close": 0.55}]}
        res = b.calculate_backtest_metrics(data)
        self.assertEqual(res["data_points_analyzed"], 3)
        self.assertIn("total_return_percent", res)
        self.assertIn("price_range", res)

    def test_test_entrypoint(self):
        with patch.object(PredictionMarketsBacktester, "initialize", new=lambda self: asyncio.sleep(0)):
            asyncio.run(pm_mod.test_prediction_market_backtesting())


if __name__ == "__main__":
    unittest.main()
