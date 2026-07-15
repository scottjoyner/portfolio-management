from __future__ import annotations

from unittest import TestCase

from onchain.bridges.settlement.service import SettlementTracker


class TestSettlementTracker(TestCase):
    def test_track_and_update(self):
        t = SettlementTracker()
        t.track("0xTX", "base", "pending")
        self.assertEqual(t.settlements["0xTX"]["chain"], "base")
        self.assertEqual(t.settlements["0xTX"]["status"], "pending")
        t.update("0xTX", "confirmed", 15)
        self.assertEqual(t.settlements["0xTX"]["status"], "confirmed")
        self.assertEqual(t.settlements["0xTX"]["confirmations"], 15)

    def test_update_unknown_noop(self):
        t = SettlementTracker()
        t.update("0xMISSING", "confirmed", 15)  # no error, no insert
        self.assertNotIn("0xMISSING", t.settlements)

    def test_is_settled_unknown(self):
        t = SettlementTracker()
        self.assertFalse(t.is_settled("0xMISSING"))

    def test_is_settled_not_confirmed(self):
        t = SettlementTracker()
        t.track("0xTX", "base", "pending")
        self.assertFalse(t.is_settled("0xTX"))

    def test_is_settled_confirmed_insufficient_confirmations(self):
        t = SettlementTracker()
        t.track("0xTX", "base", "confirmed")
        t.update("0xTX", "confirmed", 5)
        self.assertFalse(t.is_settled("0xTX", min_confirmations=12))

    def test_is_settled_confirmed_enough(self):
        t = SettlementTracker()
        t.track("0xTX", "base", "confirmed")
        t.update("0xTX", "confirmed", 20)
        self.assertTrue(t.is_settled("0xTX", min_confirmations=12))

    def test_is_settled_custom_min(self):
        t = SettlementTracker()
        t.track("0xTX", "base", "confirmed")
        t.update("0xTX", "confirmed", 3)
        self.assertTrue(t.is_settled("0xTX", min_confirmations=3))
