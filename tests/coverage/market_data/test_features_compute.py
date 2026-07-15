import unittest

from trading_system.market_data.features.compute import FeatureSet, FeatureComputer


class TestFeatureSet(unittest.TestCase):
    def test_to_dict(self):
        fs = FeatureSet(product_id="BTC-USD", mid_price=100.0, spread_bps=5.0)
        d = fs.to_dict()
        self.assertEqual(d["product_id"], "BTC-USD")
        self.assertEqual(d["mid_price"], 100.0)
        self.assertEqual(d["buy_ratio_1m"], 0.5)


class TestFeatureComputer(unittest.TestCase):
    def test_ingest_trim(self):
        c = FeatureComputer("BTC-USD")
        for i in range(600):
            c.ingest_trade(100.0 + i, 1.0, "BUY" if i % 2 == 0 else "SELL")
        self.assertEqual(len(c._prices), 500)
        self.assertEqual(len(c._buys) + len(c._sells), 500)

    def test_ingest_buy_sell(self):
        c = FeatureComputer("BTC-USD")
        c.ingest_trade(100.0, 1.0, "BUY")
        c.ingest_trade(100.0, 2.0, "sell")
        self.assertEqual(len(c._buys), 1)
        self.assertEqual(len(c._sells), 1)

    def test_compute_with_bid_ask(self):
        c = FeatureComputer("BTC-USD")
        c.ingest_trade(100.0, 10.0, "BUY")
        fs = c.compute(bid=99.0, ask=101.0)
        self.assertEqual(fs.mid_price, 100.0)
        self.assertEqual(fs.spread_bps, 200.0)
        self.assertGreater(fs.microprice, 99.0)
        self.assertLess(fs.microprice, 101.0)
        self.assertEqual(fs.buy_ratio_1m, 1.0)

    def test_compute_no_bid_ask(self):
        c = FeatureComputer("BTC-USD")
        c.ingest_trade(100.0, 5.0, "SELL")
        fs = c.compute()  # bid=ask=0
        self.assertEqual(fs.mid_price, 100.0)
        self.assertEqual(fs.spread_bps, 0.0)
        self.assertEqual(fs.microprice, 100.0)
        self.assertEqual(fs.imbalance, 0.0)

    def test_compute_empty(self):
        c = FeatureComputer("BTC-USD")
        fs = c.compute(bid=1.0, ask=2.0)
        self.assertEqual(fs.mid_price, 0.0)
        self.assertEqual(fs.spread_bps, 0.0)
        self.assertEqual(fs.buy_ratio_1m, 0.5)
        self.assertEqual(fs.volatility_1m_bps, 0.0)

    def test_compute_volatility(self):
        c = FeatureComputer("BTC-USD")
        import math
        for i in range(25):
            c.ingest_trade(100.0 + 5.0 * math.sin(i / 3.0), 1.0, "BUY")
        fs = c.compute(bid=99.0, ask=101.0)
        self.assertGreaterEqual(fs.volatility_1m_bps, 0.0)
        self.assertEqual(fs.trade_count_1m, 25)


if __name__ == "__main__":
    unittest.main()
