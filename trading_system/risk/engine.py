from __future__ import annotations

from dataclasses import dataclass, field

from core.models.domain import CapitalBucketType, ExchangeTrustScore, OrderIntent, RiskMode


@dataclass
class ModePolicy:
    max_order_notional: float
    max_open_orders: int
    max_mode_capital: float
    requires_explicit_enablement: bool
    allowed_products: set[str] = field(default_factory=set)


@dataclass
class RiskPolicy:
    stale_data_block: bool = True
    drawdown_halt_pct: float = 0.2
    stress_loss_limit: float = 50_000
    mode_policies: dict[RiskMode, ModePolicy] = field(default_factory=lambda: {
        RiskMode.ULTRA_CONSERVATIVE: ModePolicy(1_000, 10, 25_000, False),
        RiskMode.NORMAL: ModePolicy(5_000, 25, 100_000, False),
        RiskMode.AGGRESSIVE: ModePolicy(20_000, 50, 250_000, True),
        RiskMode.EXPERT_HIGH_RISK: ModePolicy(50_000, 100, 500_000, True),
        RiskMode.LAB_HFT: ModePolicy(15_000, 300, 150_000, True),
        RiskMode.MARKET_MAKING_PRO: ModePolicy(30_000, 200, 350_000, True),
        RiskMode.DERIVATIVES_EXPERT: ModePolicy(25_000, 75, 200_000, True),
        RiskMode.RESEARCH_ONLY: ModePolicy(0, 0, 0, True),
    })


class RiskEngine:
    def __init__(self, policy: RiskPolicy) -> None:
        self.policy = policy
        self.open_orders = 0
        self.kill_switch = False
        self.enabled_modes: set[RiskMode] = {RiskMode.ULTRA_CONSERVATIVE, RiskMode.NORMAL}
        self.exchange_trust_score = ExchangeTrustScore.HEALTHY
        self.live_drawdown_pct = 0.0
        self.realized_stress_loss = 0.0
        self.mode_capital_used: dict[RiskMode, float] = {m: 0.0 for m in policy.mode_policies}

    def set_exchange_trust(self, trust_score: ExchangeTrustScore) -> None:
        self.exchange_trust_score = trust_score

    def enable_mode(self, mode: RiskMode) -> None:
        self.enabled_modes.add(mode)

    def evaluate(self, intent: OrderIntent, mark_price: float, stale_data: bool = False) -> tuple[bool, str]:
        mode_policy = self.policy.mode_policies[intent.risk_mode]
        if self.kill_switch:
            return False, "global kill switch enabled"
        if intent.risk_mode == RiskMode.RESEARCH_ONLY:
            return False, "research-only mode not executable live"
        if stale_data and self.policy.stale_data_block:
            return False, "stale market data"
        if self.exchange_trust_score == ExchangeTrustScore.UNTRUSTED and not intent.reduce_only:
            return False, "exchange state untrusted; risk increasing orders halted"
        if intent.bucket == CapitalBucketType.LOCKED_RESERVE:
            return False, "locked reserve cannot be traded"
        if self.live_drawdown_pct >= self.policy.drawdown_halt_pct:
            return False, "drawdown halt activated"
        if self.realized_stress_loss >= self.policy.stress_loss_limit:
            return False, "stress loss limit exceeded"
        if mode_policy.requires_explicit_enablement and intent.risk_mode not in self.enabled_modes:
            return False, f"risk mode {intent.risk_mode.value} requires explicit enablement"
        if mode_policy.allowed_products and intent.product_id not in mode_policy.allowed_products:
            return False, "product not allowlisted for selected risk mode"
        if self.open_orders >= mode_policy.max_open_orders:
            return False, "max open orders reached"
        notional = intent.size * (intent.price or mark_price)
        if notional > mode_policy.max_order_notional:
            return False, "order exceeds max notional for risk mode"
        post_capital = self.mode_capital_used[intent.risk_mode] + notional
        if post_capital > mode_policy.max_mode_capital:
            return False, "mode capital ceiling exceeded"
        return True, "approved"
