import unittest
from datetime import datetime, timezone

from trading_system.risk.kill_switch.service import KillSwitch, KillSwitchManager


class TestKillSwitch(unittest.TestCase):
    def test_engage(self):
        ks = KillSwitch()
        self.assertFalse(ks.active)
        ks.engage("operator", "manual")
        self.assertTrue(ks.active)
        self.assertEqual(ks.triggered_by, "operator")
        self.assertEqual(ks.reason, "manual")
        self.assertIsInstance(ks.triggered_at, datetime)

    def test_engage_custom_reason(self):
        ks = KillSwitch()
        ks.engage("auto", "drawdown breach")
        self.assertEqual(ks.reason, "drawdown breach")

    def test_disengage(self):
        ks = KillSwitch()
        ks.engage("operator")
        ks.disengage()
        self.assertFalse(ks.active)
        self.assertEqual(ks.triggered_by, "")
        self.assertEqual(ks.reason, "")
        self.assertIsNone(ks.triggered_at)


class TestKillSwitchManager(unittest.TestCase):
    def setUp(self):
        self.mgr = KillSwitchManager()

    def test_get_switch_creates(self):
        sw = self.mgr.get_switch("global")
        self.assertIsInstance(sw, KillSwitch)
        self.assertIs(self.mgr.get_switch("global"), sw)

    def test_engage_disengage(self):
        self.mgr.engage("trading", "risk")
        self.assertTrue(self.mgr.is_active("trading"))
        self.mgr.disengage("trading")
        self.assertFalse(self.mgr.is_active("trading"))

    def test_disengage_default_global(self):
        self.mgr.engage("global", "x")
        self.mgr.disengage()
        self.assertFalse(self.mgr.is_active("global"))

    def test_is_active_missing(self):
        self.assertFalse(self.mgr.is_active("never_created"))

    def test_set_and_check_auto_trigger_none(self):
        self.assertFalse(self.mgr.check_auto_trigger("loss", 5.0))

    def test_check_auto_trigger_exceeded(self):
        self.mgr.set_auto_trigger("drawdown", 0.2)
        self.assertTrue(self.mgr.check_auto_trigger("drawdown", 0.5))
        self.assertTrue(self.mgr.is_active("auto:drawdown"))

    def test_check_auto_trigger_not_exceeded(self):
        self.mgr.set_auto_trigger("drawdown", 0.2)
        self.assertFalse(self.mgr.check_auto_trigger("drawdown", 0.1))


if __name__ == "__main__":
    unittest.main()
