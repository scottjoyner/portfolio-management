import unittest
from decimal import Decimal

from trading_system.risk.limits.service import (
    PositionLimit,
    LimitManager,
)


class TestPositionLimit(unittest.TestCase):
    def test_allows_side_both(self):
        pl = PositionLimit("BTC-USD", Decimal("10"), Decimal("1000"), "both")
        self.assertTrue(pl.allows_side("buy"))
        self.assertTrue(pl.allows_side("sell"))

    def test_allows_side_specific(self):
        pl = PositionLimit("BTC-USD", Decimal("10"), Decimal("1000"), "buy")
        self.assertTrue(pl.allows_side("BUY"))
        self.assertFalse(pl.allows_side("sell"))

    def test_allows_side_other(self):
        pl = PositionLimit("BTC-USD", Decimal("10"), Decimal("1000"), "sell")
        self.assertTrue(pl.allows_side("sell"))
        self.assertFalse(pl.allows_side("buy"))


class TestLimitManager(unittest.TestCase):
    def setUp(self):
        self.mgr = LimitManager()
        self.mgr.set_limit(PositionLimit("BTC-USD", Decimal("10"), Decimal("1000"), "both"))

    def test_set_and_remove(self):
        self.assertIn("BTC-USD", self.mgr.limits)
        self.mgr.remove_limit("BTC-USD")
        self.assertNotIn("BTC-USD", self.mgr.limits)

    def test_remove_missing(self):
        self.mgr.remove_limit("NOPE")  # no error

    def test_update_position(self):
        self.mgr.update_position("ETH-USD", Decimal("5"))
        self.assertEqual(self.mgr.current_positions["ETH-USD"], Decimal("5"))

    def test_check_no_limit(self):
        ok, reason = self.mgr.check_order("ETH-USD", "buy", Decimal("1"), Decimal("100"))
        self.assertTrue(ok)
        self.assertEqual(reason, "no limit configured")

    def test_check_side_not_allowed(self):
        self.mgr.set_limit(PositionLimit("ETH-USD", Decimal("10"), Decimal("1000"), "buy"))
        ok, reason = self.mgr.check_order("ETH-USD", "sell", Decimal("1"), Decimal("100"))
        self.assertFalse(ok)
        self.assertIn("not allowed", reason)

    def test_check_notional_exceeds(self):
        ok, reason = self.mgr.check_order("BTC-USD", "buy", Decimal("5"), Decimal("1000"))
        self.assertFalse(ok)
        self.assertIn("exceeds limit", reason)

    def test_check_size_exceeds(self):
        self.mgr.update_position("BTC-USD", Decimal("9"))
        ok, reason = self.mgr.check_order("BTC-USD", "buy", Decimal("5"), Decimal("10"))
        self.assertFalse(ok)
        self.assertIn("would exceed limit", reason)

    def test_check_ok_buy(self):
        self.mgr.update_position("BTC-USD", Decimal("2"))
        ok, reason = self.mgr.check_order("BTC-USD", "buy", Decimal("3"), Decimal("100"))
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_check_ok_sell_reduces(self):
        self.mgr.update_position("BTC-USD", Decimal("8"))
        ok, reason = self.mgr.check_order("BTC-USD", "sell", Decimal("3"), Decimal("100"))
        self.assertTrue(ok)
        self.assertEqual(reason, "")


if __name__ == "__main__":
    unittest.main()
