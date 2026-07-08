import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coinbase.src.capital_buckets import CapitalBucket


class TestVolumeGeneratorPositionAccounting(unittest.TestCase):
    def test_repeated_same_product_buys_accumulate_position_value(self):
        bucket = CapitalBucket(
            bucket_id="challenge",
            name="100 USDC Challenge",
            starting_balance_usd=100.0,
            cash_usd=100.0,
        )

        self.assertTrue(bucket.open_position("DOGE-USD", "long", 10.0, 0.10, "volume_generator"))
        self.assertTrue(bucket.open_position("DOGE-USD", "long", 10.0, 0.20, "volume_generator"))

        pos = bucket.positions["DOGE-USD"]
        self.assertAlmostEqual(pos.size, 20.0)
        self.assertAlmostEqual(pos.entry_price, 0.15)
        self.assertAlmostEqual(bucket.cash_usd, 97.0)
        self.assertAlmostEqual(bucket.volume_30d_usd, 3.0)
        self.assertAlmostEqual(bucket.total_value(), 101.0)


if __name__ == "__main__":
    unittest.main()
