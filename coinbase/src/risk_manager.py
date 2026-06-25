from __future__ import annotations
import math
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum


class RiskLimit(Enum):
    NONE = "none"
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"


@dataclass
class RiskProfile:
    max_leverage: float = 1.0
    max_positions: int = 10
    max_position_pct: float = 0.20
    max_correlation_pct: float = 0.35
    max_drawdown_pct: float = 0.15
    daily_loss_limit_pct: float = 0.05
    min_risk_reward: float = 1.5
    risk_per_trade_pct: float = 0.01
    var_confidence: float = 0.95
    # Allow the live runner to cap per-trade notional from the environment.
    max_notional_per_trade: float = float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "10000"))


RISK_TEMPLATES = {
    RiskLimit.CONSERVATIVE: RiskProfile(
        max_leverage=1.0, max_positions=6, max_position_pct=0.10,
        max_correlation_pct=0.20, max_drawdown_pct=0.10,
        daily_loss_limit_pct=0.03, min_risk_reward=2.0, risk_per_trade_pct=0.005,
    ),
    RiskLimit.MODERATE: RiskProfile(
        max_leverage=2.0, max_positions=10, max_position_pct=0.20,
        max_correlation_pct=0.35, max_drawdown_pct=0.15,
        daily_loss_limit_pct=0.05, min_risk_reward=1.5, risk_per_trade_pct=0.01,
    ),
    RiskLimit.AGGRESSIVE: RiskProfile(
        max_leverage=3.0, max_positions=15, max_position_pct=0.30,
        max_correlation_pct=0.50, max_drawdown_pct=0.25,
        daily_loss_limit_pct=0.08, min_risk_reward=1.2, risk_per_trade_pct=0.02,
    ),
}


@dataclass
class PositionRisk:
    product_id: str
    side: str
    size: float
    entry_price: float
    current_price: float
    stop_price: Optional[float] = None
    leverage: float = 1.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    risk_amount: float = 0.0
    risk_pct: float = 0.0
    r_multiple: float = 0.0
    var_95: float = 0.0

    @property
    def notional(self) -> float:
        return self.size * self.current_price / self.leverage

    @property
    def risk_if_stopped(self) -> float:
        if self.stop_price is None:
            return 0.0
        if self.side == "long":
            return (self.current_price - self.stop_price) * self.size / self.leverage
        return (self.stop_price - self.current_price) * self.size / self.leverage


@dataclass
class PortfolioRisk:
    total_equity: float = 0.0
    total_notional: float = 0.0
    total_leverage: float = 0.0
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    position_count: int = 0
    largest_position_pct: float = 0.0
    daily_pnl: float = 0.0
    daily_pnl_pct: float = 0.0
    var_95: float = 0.0
    correlation_risk: float = 0.0
    passed_checks: bool = True
    failures: List[str] = field(default_factory=list)


class RiskManager:
    def __init__(self, profile: Optional[RiskProfile] = None,
                 limit: RiskLimit = RiskLimit.MODERATE):
        self.profile = profile or RISK_TEMPLATES[limit]
        self.limit = limit
        self._peak_equity: float = 0.0
        self._daily_start_equity: float = 0.0
        self._daily_pnl: float = 0.0
        self._correlation_matrix: Dict[Tuple[str, str], float] = {}
        self._last_check_ts: float = 0.0

    def check_portfolio(self, positions: List[PositionRisk],
                        equity: float) -> PortfolioRisk:
        result = PortfolioRisk(total_equity=equity)
        result.position_count = len(positions)
        failures = []

        if not positions:
            result.passed_checks = True
            return result

        # Gross/net exposure
        long_notional = sum(p.notional for p in positions if p.side == "long")
        short_notional = sum(p.notional for p in positions if p.side == "short")
        result.gross_exposure = long_notional + short_notional
        result.net_exposure = long_notional - short_notional
        result.total_notional = result.gross_exposure
        result.total_leverage = result.gross_exposure / max(equity, 1e-9)

        # Largest position
        max_pos_notional = max((p.notional for p in positions), default=0.0)
        result.largest_position_pct = max_pos_notional / max(equity, 1e-9)

        # Daily PnL tracking
        if self._peak_equity == 0:
            self._peak_equity = equity
        self._peak_equity = max(self._peak_equity, equity)
        if self._daily_start_equity == 0:
            self._daily_start_equity = equity
        result.daily_pnl = equity - self._daily_start_equity
        result.daily_pnl_pct = result.daily_pnl / max(self._daily_start_equity, 1e-9)

        # VaR approximation
        var_total = sum(p.var_95 for p in positions)
        result.var_95 = var_total

        # Correlation risk
        corr_exposure = self._correlation_exposure(positions)
        result.correlation_risk = corr_exposure

        # Checks
        if result.total_leverage > self.profile.max_leverage:
            failures.append(f"Leverage {result.total_leverage:.2f}x > max {self.profile.max_leverage}x")
        if result.position_count > self.profile.max_positions:
            failures.append(f"Positions {result.position_count} > max {self.profile.max_positions}")
        if result.largest_position_pct > self.profile.max_position_pct:
            failures.append(f"Largest position {result.largest_position_pct:.1%} > max {self.profile.max_position_pct:.1%}")
        if result.daily_pnl_pct < -self.profile.daily_loss_limit_pct:
            failures.append(f"Daily loss {result.daily_pnl_pct:.2%} > limit {self.profile.daily_loss_limit_pct:.2%}")
        if equity < (1.0 - self.profile.max_drawdown_pct) * self._peak_equity:
            failures.append(f"Drawdown {(1 - equity / self._peak_equity):.1%} > max {self.profile.max_drawdown_pct:.1%}")
        if corr_exposure > self.profile.max_correlation_pct:
            failures.append(f"Correlation exposure {corr_exposure:.1%} > max {self.profile.max_correlation_pct:.1%}")

        result.failures = failures
        result.passed_checks = len(failures) == 0
        return result

    def check_trade(self, product_id: str, side: str, size: float,
                    entry: float, stop: Optional[float], target: Optional[float],
                    equity: float, positions: List[PositionRisk]) -> Tuple[bool, str]:
        rr = abs(target - entry) / max(abs(entry - stop), 1e-9) if stop and target else 0.0
        if rr < self.profile.min_risk_reward:
            return False, f"RR {rr:.2f} < min {self.profile.min_risk_reward}"
        notional = size * entry
        if notional > self.profile.max_notional_per_trade:
            return False, f"Notional ${notional:.0f} > max ${self.profile.max_notional_per_trade:.0f}"
        risk_amount = abs(entry - stop) * size / 1.0 if stop else 0.0
        risk_pct = risk_amount / max(equity, 1e-9)
        if risk_pct > self.profile.risk_per_trade_pct:
            return False, f"Risk {risk_pct:.2%} > max {self.profile.risk_per_trade_pct:.2%}"
        pos_notional = sum(p.notional for p in positions)
        if (pos_notional + notional) / max(equity, 1e-9) > self.profile.max_leverage:
            return False, "Would exceed max leverage"
        return True, "OK"

    def _correlation_exposure(self, positions: List[PositionRisk]) -> float:
        if len(positions) < 2:
            return 0.0
        n = len(positions)
        pids = [p.product_id for p in positions]
        weights = [p.notional for p in positions]
        total_w = sum(weights)
        if total_w <= 0:
            return 0.0
        weights = [w / total_w for w in weights]
        corr_sum = 0.0
        count = 0
        for i in range(n):
            for j in range(i + 1, n):
                key = (pids[i], pids[j])
                r = self._correlation_matrix.get(key, 0.5)
                corr_sum += weights[i] * weights[j] * abs(r)
                count += 1
        return corr_sum / max(count, 1) * n if n > 0 else 0.0

    def update_correlation(self, product_a: str, product_b: str, correlation: float):
        self._correlation_matrix[(product_a, product_b)] = correlation
        self._correlation_matrix[(product_b, product_a)] = correlation

    def update_daily_reset(self, equity: float):
        self._daily_start_equity = equity

    @staticmethod
    def compute_var(position: PositionRisk, confidence: float = 0.95) -> float:
        daily_vol = 0.02
        z = {0.95: 1.645, 0.99: 2.326}.get(confidence, 1.645)
        return position.notional * daily_vol * z * math.sqrt(position.leverage)


class KellySizer:
    @staticmethod
    def fraction(win_rate: float, avg_win: float, avg_loss: float) -> float:
        if avg_loss <= 0 or win_rate <= 0:
            return 0.0
        b = avg_win / avg_loss
        p = win_rate
        q = 1.0 - p
        if b <= 0:
            return 0.0
        return max(0.0, min(0.25, (p * b - q) / b))

    @staticmethod
    def half_kelly(win_rate: float, avg_win: float, avg_loss: float) -> float:
        return KellySizer.fraction(win_rate, avg_win, avg_loss) * 0.5

    @staticmethod
    def fractional_kelly(win_rate: float, avg_win: float, avg_loss: float, fraction: float = 0.5) -> float:
        """Backward-compatible alias used by the live trader."""
        return KellySizer.fraction(win_rate, avg_win, avg_loss) * max(0.0, min(1.0, fraction))

    @staticmethod
    def size_for_risk(equity: float, risk_pct: float, entry: float,
                      stop: float, leverage: float = 1.0) -> float:
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return 0.0
        risk_budget = equity * risk_pct * leverage
        return max(0.0, risk_budget / risk_per_unit)
