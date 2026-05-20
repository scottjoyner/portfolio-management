from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AllowanceManager:
    allowances: dict[tuple[str, str, str, str], float] = field(default_factory=dict)

    def set_allowance(self, chain: str, wallet: str, token: str, spender: str, amount: float) -> None:
        self.allowances[(chain, wallet.lower(), token.lower(), spender.lower())] = amount

    def get_allowance(self, chain: str, wallet: str, token: str, spender: str) -> float:
        return self.allowances.get((chain, wallet.lower(), token.lower(), spender.lower()), 0.0)

    def minimize_approval(self, required_amount: float, max_cap: float) -> float:
        return max(0.0, min(required_amount, max_cap))

    def revoke(self, chain: str, wallet: str, token: str, spender: str) -> None:
        self.set_allowance(chain, wallet, token, spender, 0.0)
