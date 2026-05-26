from __future__ import annotations

from decimal import Decimal
from typing import Any


class Portfolio:
    def __init__(self, data: dict[str, Any]) -> None:
        self.uuid: str = data.get("uuid", "")
        self.name: str = data.get("name", "")
        self.type: str = data.get("type", "")
        self.deployed_balance: Decimal = Decimal(str(data.get("deployed_balance", {}).get("value", "0"))) if data.get("deployed_balance") else Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "type": self.type,
            "deployed_balance": float(self.deployed_balance),
        }

    @staticmethod
    def from_api(data: dict[str, Any]) -> Portfolio:
        return Portfolio(data)
