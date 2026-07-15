import unittest

from core.models.domain import CapitalBucket, CapitalBucketType

from portfolio.capital_buckets.service import CapitalBucketService


class TestCapitalBucketService(unittest.TestCase):
    def test_tradable_buckets(self):
        buckets = [
            CapitalBucket(name="active", bucket_type=CapitalBucketType.ACTIVE_TRADING, target_weight=0.5),
            CapitalBucket(name="reserve", bucket_type=CapitalBucketType.LOCKED_RESERVE, target_weight=0.3),
            CapitalBucket(name="mm", bucket_type=CapitalBucketType.MARKET_MAKING, target_weight=0.1),
            CapitalBucket(name="hedge", bucket_type=CapitalBucketType.HEDGING, target_weight=0.1, locked=True),
        ]
        svc = CapitalBucketService(buckets)
        tradable = svc.tradable_buckets()
        self.assertIn(CapitalBucketType.ACTIVE_TRADING, tradable)
        self.assertIn(CapitalBucketType.MARKET_MAKING, tradable)
        self.assertNotIn(CapitalBucketType.LOCKED_RESERVE, tradable)
        self.assertNotIn(CapitalBucketType.HEDGING, tradable)
        self.assertEqual(len(tradable), 2)

    def test_empty(self):
        svc = CapitalBucketService([])
        self.assertEqual(svc.tradable_buckets(), [])


if __name__ == "__main__":
    unittest.main()
