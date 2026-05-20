from core.models.domain import ExchangeTrustScore
from exchange.coinbase.reconciliation.service import ExchangeStateReconciler


def test_reconciliation_degraded_then_untrusted():
    reconciler = ExchangeStateReconciler()
    reconciler.apply_event("e1", "o1", "OPEN")
    trust = reconciler.reconcile_open_orders({"o2"})
    assert trust == ExchangeTrustScore.DEGRADED
    reconciler.record_unknown_fill("f1")
    reconciler.record_unknown_fill("f2")
    trust2 = reconciler.reconcile_open_orders({"o2"})
    assert trust2 == ExchangeTrustScore.UNTRUSTED
