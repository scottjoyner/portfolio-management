from __future__ import annotations

from decimal import Decimal
from typing import Any


class Account:
    def __init__(self, data: dict[str, Any]) -> None:
        self.uuid: str = data.get("uuid", "")
        self.name: str = data.get("name", "")
        self.currency: str = data.get("currency", "")
        self.available_balance: Decimal = Decimal(str(data.get("available_balance", {}).get("value", "0")))
        self.hold: Decimal = Decimal(str(data.get("hold", {}).get("value", "0")))
        self.ledger_balance: Decimal = Decimal(str(data.get("ledger_balance", {}).get("value", "0")))

    @property
    def total_balance(self) -> Decimal:
        return self.available_balance + self.hold

    def to_dict(self) -> dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "currency": self.currency,
            "available_balance": float(self.available_balance),
            "hold": float(self.hold),
            "total_balance": float(self.total_balance),
        }

    @staticmethod
    def from_api(data: dict[str, Any]) -> Account:
        return Account(data)
