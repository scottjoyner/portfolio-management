import unittest

from trading_system.market_data.microstructure.features import (
    TopOfBook,
    TradePrint,
    MicrostructureFeatureBuilder,
    ToxicFlowEstimator,
)


class TestMicrostructureFeatures(unittest.TestCase):
    def test_microprice_normal(self):
        b = TopOfBook(bid_px=100.0, bid_sz=2.0, ask_px=101.0, ask_sz=2.0)
        mp = MicrostructureFeatureBuilder.microprice(b)
        # (101*2 + 100*2)/4 = 100.5
        self.assertAlmostEqual(mp, 100.5)

    def test_microprice_zero_denom(self):
        b = TopOfBook(bid_px=100.0, bid_sz=0.0, ask_px=101.0, ask_sz=0.0)
        mp = MicrostructureFeatureBuilder.microprice(b)
        self.assertAlmostEqual(mp, 100.5)

    def test_imbalance(self):
        b = TopOfBook(bid_px=100.0, bid_sz=3.0, ask_px=101.0, ask_sz=1.0)
        self.assertAlmostEqual(MicrostructureFeatureBuilder.imbalance(b), 0.5)
        bz = TopOfBook(bid_px=100.0, bid_sz=0.0, ask_px=101.0, ask_sz=0.0)
        self.assertEqual(MicrostructureFeatureBuilder.imbalance(bz), 0.0)

    def test_toxic_flow_buy(self):
        est = ToxicFlowEstimator(bucket_volume=5.0)
        # add 3 buy then 2 sell to reach 5 -> toxic = |3-2|/5 = 0.2
        r1 = est.update(TradePrint("BUY", 3.0, 100.0))
        self.assertEqual(r1, 0.0)
        r2 = est.update(TradePrint("sell", 2.0, 100.0))
        self.assertAlmostEqual(r2, 0.2)
        # after reset, buy only, below bucket
        r3 = est.update(TradePrint("BUY", 1.0, 100.0))
        self.assertEqual(r3, 0.0)

    def test_toxic_flow_small_bucket(self):
        est = ToxicFlowEstimator(bucket_volume=0.0)
        # bucket_volume clamped to 1e-6; single trade triggers reset
        r = est.update(TradePrint("BUY", 1.0, 100.0))
        self.assertGreaterEqual(r, 0.0)


if __name__ == "__main__":
    unittest.main()
