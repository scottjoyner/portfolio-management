from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from core.models.domain import ExchangeTrustScore


@dataclass
class ReconciliationSnapshot:
    open_orders_remote: set[str] = field(default_factory=set)
    open_orders_local: set[str] = field(default_factory=set)
    unknown_fills: int = 0
    duplicate_events: int = 0
    stale_sequence_gaps: int = 0


class ExchangeStateReconciler:
    def __init__(self) -> None:
        self.processed_event_ids: set[str] = set()
        self.submit_keys: set[str] = set()
        self.snapshot = ReconciliationSnapshot()
        self.order_events: dict[str, list[str]] = defaultdict(list)

    def register_submit_key(self, submit_key: str) -> bool:
        if submit_key in self.submit_keys:
            return False
        self.submit_keys.add(submit_key)
        return True

    def apply_event(self, event_id: str, order_id: str, event_type: str) -> None:
        if event_id in self.processed_event_ids:
            self.snapshot.duplicate_events += 1
            return
        self.processed_event_ids.add(event_id)
        self.order_events[order_id].append(event_type)
        if event_type == "OPEN":
            self.snapshot.open_orders_local.add(order_id)
        elif event_type in {"FILLED", "CANCELED", "REJECTED"}:
            self.snapshot.open_orders_local.discard(order_id)

    def reconcile_open_orders(self, remote_open_orders: set[str]) -> ExchangeTrustScore:
        self.snapshot.open_orders_remote = remote_open_orders
        delta = self.snapshot.open_orders_local.symmetric_difference(remote_open_orders)
        if len(delta) >= 5 or self.snapshot.unknown_fills >= 2:
            return ExchangeTrustScore.UNTRUSTED
        if delta or self.snapshot.duplicate_events:
            return ExchangeTrustScore.DEGRADED
        return ExchangeTrustScore.HEALTHY

    def record_unknown_fill(self, _fill_id: str) -> None:
        self.snapshot.unknown_fills += 1

    def forensics_export(self) -> dict:
        return {
            "snapshot": self.snapshot.__dict__,
            "order_events": {k: list(v) for k, v in self.order_events.items()},
            "processed_event_count": len(self.processed_event_ids),
        }
