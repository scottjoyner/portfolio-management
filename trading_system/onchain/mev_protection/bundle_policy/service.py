from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class BundlePolicy:
    max_bundles_per_block: int = 3
    min_priority_fee_wei: int = 0
    max_gas_price_wei: int = 0
    require_eth_balance: Decimal = Decimal("0.01")
    allowed_contracts: set[str] = field(default_factory=set)


@dataclass
class BundlePolicyEngine:
    policies: dict[str, BundlePolicy] = field(default_factory=dict)

    def set_policy(self, name: str, policy: BundlePolicy) -> None:
        self.policies[name] = policy

    def get_policy(self, name: str = "default") -> BundlePolicy:
        return self.policies.get(name, BundlePolicy())
