from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GasPolicy:
    max_gas_price_gwei: float = 100.0
    max_priority_fee_gwei: float = 2.0
    gas_limit_multiplier: float = 1.2
    eip1559_enabled: bool = True

    @property
    def max_gas_price_wei(self) -> int:
        return int(self.max_gas_price_gwei * 1e9)

    @property
    def max_priority_fee_wei(self) -> int:
        return int(self.max_priority_fee_gwei * 1e9)


@dataclass
class GasPolicyEngine:
    policies: dict[str, GasPolicy] = field(default_factory=dict)

    def set_policy(self, chain: str, policy: GasPolicy) -> None:
        self.policies[chain] = policy

    def get_policy(self, chain: str) -> GasPolicy:
        return self.policies.get(chain, GasPolicy())

    def clamp_gas_price(self, chain: str, suggested_gas_price_wei: int) -> int:
        policy = self.get_policy(chain)
        return min(suggested_gas_price_wei, policy.max_gas_price_wei)

    def adjusted_gas_limit(self, chain: str, estimated_gas: int) -> int:
        policy = self.get_policy(chain)
        return int(estimated_gas * policy.gas_limit_multiplier)
