from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class WalletSafetyCheck:
    wallet: str
    chain: str
    is_allowed: bool = True
    reason: str = ""
    daily_tx_count: int = 0
    daily_volume_usd: Decimal = Decimal("0")
    max_daily_tx: int = 100
    max_daily_volume_usd: Decimal = Decimal("1000000")

    def check(self) -> bool:
        if self.daily_tx_count >= self.max_daily_tx:
            self.is_allowed = False
            self.reason = "daily transaction limit exceeded"
            return False
        if self.daily_volume_usd >= self.max_daily_volume_usd:
            self.is_allowed = False
            self.reason = "daily volume limit exceeded"
            return False
        return True


@dataclass
class WalletSafetyService:
    allowlist: set[str] = field(default_factory=set)
    blocklist: set[str] = field(default_factory=set)

    def allow_wallet(self, wallet: str) -> None:
        self.allowlist.add(wallet.lower())

    def block_wallet(self, wallet: str) -> None:
        self.blocklist.add(wallet.lower())

    def is_wallet_allowed(self, wallet: str) -> bool:
        addr = wallet.lower()
        if addr in self.blocklist:
            return False
        if self.allowlist and addr not in self.allowlist:
            return False
        return True
