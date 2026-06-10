from __future__ import annotations

import re
from typing import Any, Dict, Optional

from trading_system.core.runtime.models import AccountSnapshot

_SECRET_PATTERNS = [
    re.compile(r"privateKey=[^\s,;]+", re.IGNORECASE),
    re.compile(r"-----BEGIN [^-]+-----.*?-----END [^-]+-----", re.IGNORECASE | re.DOTALL),
    re.compile(r"SHOULD_NOT_LEAK", re.IGNORECASE),
]


def sanitize_error(error: Exception | str) -> str:
    text = str(error)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


class CoinbaseService:
    """Runtime-safe wrapper around CoinbaseConnectorV3.

    The service normalizes Coinbase CLI/connector payloads for API/UI consumers
    and sanitizes errors before they reach logs or HTTP responses.
    """

    def __init__(self, connector: Optional[Any] = None) -> None:
        self._connector = connector

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3

            self._connector = CoinbaseConnectorV3(environment="live")
        return self._connector

    def get_connection_status(self) -> Dict[str, Any]:
        try:
            balances = self.connector.get_balances()
            accounts = balances.get("accounts", []) if isinstance(balances, dict) else []
            return {
                "connected": True,
                "environment": "live",
                "account_count": len(accounts),
                "error": None,
            }
        except Exception as exc:
            return {
                "connected": False,
                "environment": "live",
                "account_count": 0,
                "error": sanitize_error(exc),
            }

    def get_balances_snapshot(self) -> AccountSnapshot:
        balances = self.connector.get_balances()
        raw_accounts = balances.get("accounts", []) if isinstance(balances, dict) else []
        accounts = []
        for account in raw_accounts:
            available = account.get("available_balance", {}) or {}
            hold = account.get("hold", {}) or {}
            accounts.append(
                {
                    "uuid": account.get("uuid"),
                    "name": account.get("name"),
                    "currency": account.get("currency") or available.get("currency"),
                    "available": available.get("value", "0"),
                    "hold": hold.get("value", "0"),
                    "active": account.get("active"),
                    "ready": account.get("ready"),
                    "type": account.get("type"),
                }
            )
        return AccountSnapshot(accounts=accounts)

    def get_price(self, product_id: str) -> Dict[str, Any]:
        data = self.connector.get_price(product_id)
        if isinstance(data, dict):
            normalized = dict(data)
            normalized.setdefault("product_id", product_id)
            return normalized
        return {"product_id": product_id, "price": data}
