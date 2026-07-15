import unittest
from datetime import datetime, timezone

from trading_system.risk.drawdown.service import DrawdownTracker, DrawdownMonitor


class TestDrawdownTracker(unittest.TestCase):
    def test_update_new_peak(self):
        t = DrawdownTracker()
        t.update(1000.0)
        self.assertEqual(t.peak_value, 1000.0)
        self.assertEqual(t.current_value, 1000.0)
        self.assertEqual(t.drawdown_pct, 0.0)
        self.assertFalse(t.halted)
        self.assertIsInstance(t.last_updated, datetime)

    def test_update_drawdown_halts(self):
        t = DrawdownTracker(halt_threshold=0.2)
        t.update(1000.0)
        t.update(700.0)  # 30% drawdown > 20% threshold
        self.assertAlmostEqual(t.drawdown_pct, 0.3)
        self.assertTrue(t.halted)

    def test_update_drawdown_not_halted_low_threshold(self):
        t = DrawdownTracker(halt_threshold=0.5)
        t.update(1000.0)
        t.update(900.0)
        self.assertAlmostEqual(t.drawdown_pct, 0.1)
        self.assertFalse(t.halted)

    def test_update_zero_peak(self):
        # peak stays 0 -> drawdown_pct not recomputed (peak_value > 0 is False)
        t = DrawdownTracker()
        t.update(0.0)
        self.assertEqual(t.peak_value, 0.0)
        self.assertEqual(t.drawdown_pct, 0.0)
        self.assertFalse(t.halted)

    def test_reset(self):
        t = DrawdownTracker()
        t.update(1000.0)
        t.reset()
        self.assertEqual(t.peak_value, 0.0)
        self.assertEqual(t.current_value, 0.0)
        self.assertEqual(t.drawdown_pct, 0.0)
        self.assertFalse(t.halted)
        self.assertIsNone(t.last_updated)


class TestDrawdownMonitor(unittest.TestCase):
    def setUp(self):
        self.mon = DrawdownMonitor()

    def test_get_tracker_creates(self):
        tr = self.mon.get_tracker("p1")
        self.assertIsInstance(tr, DrawdownTracker)
        self.assertIs(self.mon.get_tracker("p1"), tr)

    def test_set_halt_threshold(self):
        self.mon.set_halt_threshold("p1", 0.3)
        self.assertEqual(self.mon.get_tracker("p1").halt_threshold, 0.3)

    def test_update_and_is_halted(self):
        self.mon.update("p1", 1000.0)
        self.mon.update("p1", 500.0)
        self.assertTrue(self.mon.is_halted("p1"))

    def test_is_halted_missing(self):
        self.assertFalse(self.mon.is_halted("missing"))

    def test_reset_portfolio(self):
        self.mon.update("p1", 1000.0)
        self.mon.reset_portfolio("p1")
        self.assertNotIn("p1", self.mon.trackers)


if __name__ == "__main__":
    unittest.main()
