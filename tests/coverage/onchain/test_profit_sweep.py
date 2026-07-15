import unittest

from core.models.domain import CapitalBucketType
from onchain.strategies.treasury.profit_sweep import ProfitCaptureEngine, ProfitSweepPolicy


class TestProfitSweep(unittest.TestCase):
    def test_should_execute(self):
        e = ProfitCaptureEngine()
        self.assertTrue(e.should_execute(5.0))
        self.assertFalse(e.should_execute(0.5))

    def test_record_loss(self):
        e = ProfitCaptureEngine()
        self.assertEqual(e.record_realized(-10.0), {})
        self.assertEqual(e.realized_today, -10.0)

    def test_record_profit(self):
        e = ProfitCaptureEngine()
        r = e.record_realized(100.0)
        self.assertEqual(r[CapitalBucketType.LOCKED_RESERVE], 60.0)
        self.assertEqual(r[CapitalBucketType.HEDGING], 20.0)
        self.assertEqual(r[CapitalBucketType.CASH_BUFFER], 20.0)

    def test_record_profit_custom(self):
        e = ProfitCaptureEngine(policy=ProfitSweepPolicy(daily_lock_sweep_ratio=0.5, quarantine_experimental_ratio=0.1))
        r = e.record_realized(100.0)
        self.assertEqual(r[CapitalBucketType.LOCKED_RESERVE], 50.0)
        self.assertEqual(r[CapitalBucketType.CASH_BUFFER], 40.0)


if __name__ == "__main__":
    unittest.main()
