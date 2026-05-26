from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SettlementTracker:
    settlements: dict[str, dict] = field(default_factory=dict)

    def track(self, tx_hash: str, chain: str, status: str = "pending") -> None:
        self.settlements[tx_hash] = {"chain": chain, "status": status, "confirmations": 0}

    def update(self, tx_hash: str, status: str, confirmations: int = 0) -> None:
        if tx_hash in self.settlements:
            self.settlements[tx_hash]["status"] = status
            self.settlements[tx_hash]["confirmations"] = confirmations

    def is_settled(self, tx_hash: str, min_confirmations: int = 12) -> bool:
        entry = self.settlements.get(tx_hash)
        if not entry:
            return False
        return entry["status"] == "confirmed" and entry["confirmations"] >= min_confirmations
