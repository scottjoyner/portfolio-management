"""Tests for trading_system.exchange.coinbase.reconciliation.service."""
import unittest

from core.models.domain import ExchangeTrustScore

from trading_system.exchange.coinbase.reconciliation.service import (
    ExchangeStateReconciler,
    ReconciliationSnapshot,
)


class TestReconciler(unittest.IsolatedAsyncioTestCase):
    def test_register_submit_key(self):
        r = ExchangeStateReconciler()
        self.assertTrue(r.register_submit_key("k1"))
        self.assertFalse(r.register_submit_key("k1"))
        self.assertTrue(r.register_submit_key("k2"))

    def test_apply_event_open(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        self.assertIn("o1", r.snapshot.open_orders_local)

    def test_apply_event_filled_removes(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        r.apply_event("e2", "o1", "FILLED")
        self.assertNotIn("o1", r.snapshot.open_orders_local)

    def test_apply_event_canceled_removes(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        r.apply_event("e2", "o1", "CANCELED")
        self.assertNotIn("o1", r.snapshot.open_orders_local)

    def test_apply_event_rejected_removes(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        r.apply_event("e2", "o1", "REJECTED")
        self.assertNotIn("o1", r.snapshot.open_orders_local)

    def test_apply_event_duplicate(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        r.apply_event("e1", "o1", "OPEN")
        self.assertEqual(r.snapshot.duplicate_events, 1)

    def test_reconcile_healthy(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        self.assertEqual(
            r.reconcile_open_orders({"o1"}), ExchangeTrustScore.HEALTHY)

    def test_reconcile_degraded_delta(self):
        r = ExchangeStateReconciler()
        self.assertEqual(
            r.reconcile_open_orders({"a", "b"}),
            ExchangeTrustScore.DEGRADED)

    def test_reconcile_degraded_duplicate(self):
        r = ExchangeStateReconciler()
        r.snapshot.duplicate_events = 1
        self.assertEqual(
            r.reconcile_open_orders(set()), ExchangeTrustScore.DEGRADED)

    def test_reconcile_untrusted_delta(self):
        r = ExchangeStateReconciler()
        self.assertEqual(
            r.reconcile_open_orders({f"o{i}" for i in range(5)}),
            ExchangeTrustScore.UNTRUSTED)

    def test_reconcile_untrusted_unknown_fills(self):
        r = ExchangeStateReconciler()
        r.snapshot.unknown_fills = 2
        self.assertEqual(
            r.reconcile_open_orders(set()), ExchangeTrustScore.UNTRUSTED)

    def test_record_unknown_fill(self):
        r = ExchangeStateReconciler()
        r.record_unknown_fill("f1")
        self.assertEqual(r.snapshot.unknown_fills, 1)

    def test_forensics_export(self):
        r = ExchangeStateReconciler()
        r.apply_event("e1", "o1", "OPEN")
        out = r.forensics_export()
        self.assertIn("snapshot", out)
        self.assertIn("order_events", out)
        self.assertEqual(out["processed_event_count"], 1)


if __name__ == "__main__":
    unittest.main()
