import unittest
from decimal import Decimal

from onchain.dex.quoter.service import QuoteResult, quote_swap


class TestQuoter(unittest.TestCase):
    def test_quote_normal(self):
        r = quote_swap(Decimal("100"), Decimal("1000"), Decimal("1000"), 30)
        self.assertEqual(r.price_impact_bps, Decimal("1000"))
        self.assertGreater(r.estimated_amount_out, Decimal("0"))

    def test_quote_zero_reserves(self):
        r = quote_swap(Decimal("100"), Decimal("0"), Decimal("0"), 30)
        self.assertEqual(r.estimated_amount_out, Decimal("0"))
        self.assertEqual(r.price_impact_bps, Decimal("10000"))


if __name__ == "__main__":
    unittest.main()
