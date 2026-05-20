from __future__ import annotations

from dataclasses import dataclass, field

from core.models.domain import CapitalBucketType


@dataclass
class ProfitSweepPolicy:
    hard_take_profit: float = 50.0
    min_route_net_edge: float = 1.0
    daily_lock_sweep_ratio: float = 0.6
    quarantine_experimental_ratio: float = 0.2


@dataclass
class ProfitCaptureEngine:
    policy: ProfitSweepPolicy = field(default_factory=ProfitSweepPolicy)
    realized_today: float = 0.0

    def should_execute(self, expected_net_edge: float) -> bool:
        return expected_net_edge >= self.policy.min_route_net_edge

    def record_realized(self, pnl: float) -> dict[CapitalBucketType, float]:
        self.realized_today += pnl
        if pnl <= 0:
            return {}
        lock = pnl * self.policy.daily_lock_sweep_ratio
        quarantine = pnl * self.policy.quarantine_experimental_ratio
        cash = max(0.0, pnl - lock - quarantine)
        return {
            CapitalBucketType.LOCKED_RESERVE: lock,
            CapitalBucketType.CASH_BUFFER: cash,
            CapitalBucketType.HEDGING: quarantine,
        }
