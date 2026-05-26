from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class ComplianceCheck:
    wallet: str
    action: str
    product: str | None = None
    passed: bool = False
    reason: str = ""
    checked_at: datetime | None = None


@dataclass
class ComplianceService:
    blocked_wallets: set[str] = field(default_factory=set)
    blocked_products: set[str] = field(default_factory=set)
    max_daily_trades: int = 1000

    def block_wallet(self, wallet: str) -> None:
        self.blocked_wallets.add(wallet.lower())

    def unblock_wallet(self, wallet: str) -> None:
        self.blocked_wallets.discard(wallet.lower())

    def block_product(self, product_id: str) -> None:
        self.blocked_products.add(product_id)

    def unblock_product(self, product_id: str) -> None:
        self.blocked_products.discard(product_id)

    def check(self, wallet: str, action: str, product: str | None = None) -> ComplianceCheck:
        check = ComplianceCheck(wallet=wallet, action=action, product=product, checked_at=datetime.now(timezone.utc))
        if wallet.lower() in self.blocked_wallets:
            check.reason = "wallet is blocked"
            return check
        if product and product in self.blocked_products:
            check.reason = f"product {product} is blocked"
            return check
        check.passed = True
        return check
