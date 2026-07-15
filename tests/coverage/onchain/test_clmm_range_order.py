import unittest

from onchain.strategies.amm_lp.clmm_range_order import range_order_action


class TestCLMMRangeOrder(unittest.TestCase):
    def test_in_range(self):
        self.assertEqual(range_order_action(True), "wait_fill")

    def test_out_of_range(self):
        self.assertEqual(range_order_action(False), "withdraw_and_settle")


if __name__ == "__main__":
    unittest.main()
