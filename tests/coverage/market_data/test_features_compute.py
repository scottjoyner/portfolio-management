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
        # All four series are index-aligned with each other and bounded together.
        # They used to be partitioned lists that could each hold hundreds of
        # entries with no shared bound, so this asserted 1000.
        for series in (c._prices, c._volumes, c._buys, c._sells):
            self.assertEqual(len(series), 500)
        self.assertEqual(sum(c._buys) + sum(c._sells), 600 * 1.0 - 100 * 1.0)

    def test_ingest_buy_sell(self):
        c = FeatureComputer("BTC-USD")
        c.ingest_trade(100.0, 1.0, "BUY")
        c.ingest_trade(100.0, 2.0, "sell")
        # Both series are index-aligned with the two trades, so each has an
        # entry per trade and the two sums add up to the traded size. They used
        # to be partitioned, holding only their own side, which is what let the
        # buffers grow past the cap and put buy_ratio and volume_1m on different
        # trade windows.
        self.assertEqual(len(c._buys), 2)
        self.assertEqual(len(c._sells), 2)
        self.assertEqual(c._buys, [1.0, 0.0])
        self.assertEqual(c._sells, [0.0, 2.0])
        # Lower-case "sell" must still count as a sell.
        self.assertEqual(sum(c._sells), 2.0)

    def test_compute_with_bid_ask(self):
        c = FeatureComputer("BTC-USD")
        c.ingest_trade(100.0, 10.0, "BUY")
        fs = c.compute(bid=99.0, ask=101.0)
        self.assertEqual(fs.mid_price, 100.0)
        self.assertEqual(fs.spread_bps, 200.0)
        # One BUY and no SELL, so the microprice is weighted entirely toward the
        # bid -- it lands exactly on 99.0, not strictly inside the spread.
        self.assertEqual(fs.microprice, 99.0)
        self.assertEqual(fs.buy_ratio_1m, 1.0)

    def test_compute_no_bid_ask(self):
        c = FeatureComputer("BTC-USD")
        c.ingest_trade(100.0, 5.0, "SELL")
        fs = c.compute()  # bid=ask=0
        self.assertEqual(fs.mid_price, 100.0)
        self.assertEqual(fs.spread_bps, 0.0)
        self.assertEqual(fs.microprice, 100.0)
        # One SELL and no BUY is total sell pressure: (0 - 5) / 5 = -1.0.
        # This expected 0.0, which is only what you get when both sides are zero.
        self.assertEqual(fs.imbalance, -1.0)
        self.assertEqual(fs.buy_ratio_1m, 0.0)

    def test_compute_empty(self):
        """No trades and no book: everything degrades to a neutral reading.

        This passed bid=1.0/ask=2.0 and then expected mid_price 0.0, which the
        correct answer (1.5) could never satisfy. With no book the mid is 0.
        """
        c = FeatureComputer("BTC-USD")
        fs = c.compute()
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
