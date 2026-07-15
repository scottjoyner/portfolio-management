import unittest

from onchain.simulation.quote_refresh import quote_stale


class TestQuoteRefresh(unittest.TestCase):
    def test_stale(self):
        self.assertTrue(quote_stale(5000))

    def test_fresh(self):
        self.assertFalse(quote_stale(1000))

    def test_custom(self):
        self.assertTrue(quote_stale(100, 50))


if __name__ == "__main__":
    unittest.main()
