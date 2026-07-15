import unittest
from unittest import mock

from coinbase.src.strategies.pair_trading import PairTradingStrategy, CORRELATED_PAIRS


class TestPairTrading(unittest.TestCase):
    def _feed(self, strat, prices, n=1):
        out = []
        for _ in range(n):
            out.extend(strat.on_prices(prices))
        return out

    def test_no_signal_when_prices_missing(self):
        s = PairTradingStrategy(min_history=2, lookback=10, z_entry=0.1, cooldown_s=0)
        # missing base price -> skip all
        sigs = s.on_prices({"BTC-USD": 2000.0})
        self.assertEqual(sigs, [])

    def test_no_signal_when_price_zero(self):
        s = PairTradingStrategy(min_history=2, lookback=10, z_entry=0.1, cooldown_s=0)
        sigs = s.on_prices({"ETH-USD": 0.0, "BTC-USD": 2000.0})
        self.assertEqual(sigs, [])

    def test_no_signal_before_min_history(self):
        s = PairTradingStrategy(min_history=5, lookback=10, z_entry=0.1, cooldown_s=0)
        # only 4 samples -> never enough
        for _ in range(4):
            s.on_prices({"ETH-USD": 30.0, "BTC-USD": 2000.0})
        self.assertEqual(s.on_prices({"ETH-USD": 30.0, "BTC-USD": 2000.0}), [])

    def test_sell_signal_on_ratio_spike(self):
        s = PairTradingStrategy(min_history=3, lookback=10, z_entry=0.1, cooldown_s=0)
        # baseline
        for _ in range(3):
            s.on_prices({"ETH-USD": 30.0, "BTC-USD": 2000.0})
        # spike up -> z > 0 -> SELL
        sigs = s.on_prices({"ETH-USD": 60.0, "BTC-USD": 2000.0})
        self.assertEqual(len(sigs), 1)
        sig = sigs[0]
        self.assertEqual(sig["action"], "SELL")
        self.assertEqual(sig["product_id"], "BTC-USD")
        self.assertEqual(sig["currency"], "ETH")
        self.assertEqual(sig["pair"], "ETH/BTC")
        self.assertEqual(sig["hedge_product"], "ETH-USD")
        self.assertGreaterEqual(sig["confidence"], 0.0)

    def test_buy_signal_on_ratio_drop(self):
        s = PairTradingStrategy(min_history=3, lookback=10, z_entry=0.1, cooldown_s=0)
        for _ in range(3):
            s.on_prices({"ETH-USD": 30.0, "BTC-USD": 2000.0})
        # drop -> z < 0 -> BUY (long underperformer = base)
        sigs = s.on_prices({"ETH-USD": 15.0, "BTC-USD": 2000.0})
        self.assertEqual(len(sigs), 1)
        sig = sigs[0]
        self.assertEqual(sig["action"], "BUY")
        self.assertEqual(sig["product_id"], "ETH-USD")
        self.assertEqual(sig["hedge_product"], "BTC-USD")

    def test_cooldown_skips_signal(self):
        s = PairTradingStrategy(min_history=3, lookback=10, z_entry=0.1, cooldown_s=1e9)
        for _ in range(3):
            s.on_prices({"ETH-USD": 30.0, "BTC-USD": 2000.0})
        sigs = s.on_prices({"ETH-USD": 60.0, "BTC-USD": 2000.0})
        self.assertEqual(len(sigs), 1)
        # within cooldown -> no new signal
        sigs2 = s.on_prices({"ETH-USD": 120.0, "BTC-USD": 2000.0})
        self.assertEqual(sigs2, [])

    def test_lookback_trim(self):
        s = PairTradingStrategy(min_history=2, lookback=3, z_entry=0.1, cooldown_s=0)
        for i in range(8):
            s.on_prices({"ETH-USD": 30.0 + i, "BTC-USD": 2000.0})
        hist = s._ratio_cache["ETH/BTC"]
        self.assertLessEqual(len(hist), 3)

    def test_get_z_score_empty(self):
        s = PairTradingStrategy(min_history=3, lookback=10)
        self.assertIsNone(s.get_z_score("ETH/BTC"))

    def test_get_z_score_computed(self):
        s = PairTradingStrategy(min_history=3, lookback=10, z_entry=0.1, cooldown_s=0)
        for _ in range(3):
            s.on_prices({"ETH-USD": 30.0, "BTC-USD": 2000.0})
        s.on_prices({"ETH-USD": 60.0, "BTC-USD": 2000.0})
        z = s.get_z_score("ETH/BTC")
        self.assertIsNotNone(z)
        self.assertIsInstance(z, float)

    def test_correlated_pairs_defined(self):
        self.assertGreater(len(CORRELATED_PAIRS), 0)


if __name__ == "__main__":
    unittest.main()
