from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from brokers.base import BrokerAdapter, BrokerOrder


@dataclass
class BrokerRoutingDecision:
    broker: str
    product_id: str
    confidence: float
    reason: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


class BrokerRouter:
    def __init__(self) -> None:
        self._brokers: dict[str, BrokerAdapter] = {}
        self._preferred_order: list[str] = []

    def register(self, name: str, adapter: BrokerAdapter, preferred: bool = False) -> None:
        self._brokers[name] = adapter
        if preferred:
            self._preferred_order.append(name)

    def get(self, name: str) -> BrokerAdapter | None:
        return self._brokers.get(name)

    def list_brokers(self) -> list[str]:
        return list(self._brokers.keys())

    def route(self, product_id: str, mode: str = "paper") -> BrokerRoutingDecision:
        for name in self._preferred_order:
            adapter = self._brokers[name]
            if mode == "paper" and "paper" in name:
                return BrokerRoutingDecision(broker=name, product_id=product_id, confidence=1.0)
            if mode == "live" and name != "paper":
                return BrokerRoutingDecision(broker=name, product_id=product_id, confidence=0.95)
        if "paper" in self._brokers:
            return BrokerRoutingDecision(
                broker="paper", product_id=product_id, confidence=0.5,
                reason="no live broker registered, falling back to paper",
            )
        raise ValueError(f"no broker available for {product_id}")

    async def route_and_submit(self, order: BrokerOrder, mode: str = "paper") -> Any:
        decision = self.route(order.product_id, mode=mode)
        adapter = self._brokers[decision.broker]
        ok, msg = await adapter.preview_order(order)
        if not ok:
            raise ValueError(f"order preview failed: {msg}")
        return await adapter.submit_order(order)
