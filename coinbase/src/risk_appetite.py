from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum

from .risk_manager import RiskProfile, RiskLimit, RISK_TEMPLATES
from .regime import Regime


class DrawdownSeverity(Enum):
    NONE = "none"
    MILD = "mild"
    MODERATE = "moderate"
    SEVERE = "severe"


class ProfitLockState(Enum):
    OFF = "off"
    LOCKING = "locking"
    LOCKED = "locked"


class ConsecutiveLossState(Enum):
    NORMAL = "normal"
    CAUTION = "caution"
    REDUCED = "reduced"
    STOPPED = "stopped"


REGIME_RISK_MULTIPLIERS: Dict[str, float] = {
    Regime.STRONG_UPTREND.value: 1.3,
    Regime.WEAK_UPTREND.value: 1.1,
    Regime.RANGING.value: 0.8,
    Regime.WEAK_DOWNTREND.value: 0.6,
    Regime.STRONG_DOWNTREND.value: 0.3,
    Regime.HIGH_VOLATILITY.value: 0.5,
    Regime.LOW_VOLATILITY.value: 1.1,
    Regime.UNKNOWN.value: 0.7,
}

REGIME_MAX_LEVERAGE: Dict[str, float] = {
    Regime.STRONG_UPTREND.value: 3.0,
    Regime.WEAK_UPTREND.value: 2.0,
    Regime.RANGING.value: 1.5,
    Regime.WEAK_DOWNTREND.value: 1.0,
    Regime.STRONG_DOWNTREND.value: 1.0,
    Regime.HIGH_VOLATILITY.value: 1.0,
    Regime.LOW_VOLATILITY.value: 2.0,
    Regime.UNKNOWN.value: 1.0,
}


@dataclass
class RiskAppetiteSnapshot:
    score: float = 0.5
    regime_multiplier: float = 1.0
    drawdown_multiplier: float = 1.0
    profit_lock_multiplier: float = 1.0
    consecutive_loss_multiplier: float = 1.0
    compound_multiplier: float = 1.0
    volatility_multiplier: float = 1.0
    effective_leverage: float = 1.0
    effective_risk_per_trade: float = 0.01
    effective_max_position_pct: float = 0.2
    profile_label: str = "moderate"
    drawdown_severity: str = "none"
    profit_lock_active: bool = False
    consecutive_loss_state: str = "normal"
    is_compounding: bool = False
    gating_reasons: List[str] = field(default_factory=list)
    total_trades: int = 0
    recent_win_rate: float = 0.5


class DynamicRiskController:
    def __init__(self, base_limit: RiskLimit = RiskLimit.MODERATE):
        self.base_profile: RiskProfile = RISK_TEMPLATES[base_limit]
        self.base_limit = base_limit
        self._peak_equity: float = 0.0
        self._equity_history: List[float] = []
        self._pnl_history: List[float] = []
        self._trade_results: List[bool] = []
        self._consecutive_wins: int = 0
        self._consecutive_losses: int = 0
        self._total_trades: int = 0
        self._wins: int = 0
        self._avg_win: float = 0.02
        self._avg_loss: float = 0.01
        self._current_regime: str = Regime.UNKNOWN.value
        self._current_volatility: float = 0.02
        self._baseline_volatility: float = 0.02
        self._profit_lock_threshold: float = 0.10
        self._compound_threshold: float = 0.15
        self._max_consecutive_losses: int = 4
        self._drawdown_tiers: List[Tuple[float, float]] = [
            (0.05, 0.8),
            (0.10, 0.5),
            (0.15, 0.25),
            (0.20, 0.0),
        ]

    @property
    def state(self) -> RiskAppetiteSnapshot:
        return self.snapshot()

    def update_equity(self, equity: float):
        self._equity_history.append(equity)
        if len(self._equity_history) > 500:
            self._equity_history = self._equity_history[-500:]
        if equity > self._peak_equity:
            self._peak_equity = equity

    def record_trade(self, won: bool, r_multiple: float = 0.0):
        self._total_trades += 1
        self._trade_results.append(won)
        if len(self._trade_results) > 100:
            self._trade_results = self._trade_results[-100:]
        if won:
            self._consecutive_wins += 1
            self._consecutive_losses = 0
            self._wins += 1
            if r_multiple > 0:
                self._avg_win = self._avg_win * 0.9 + r_multiple * 0.1
        else:
            self._consecutive_losses += 1
            self._consecutive_wins = 0
            if r_multiple < 0:
                self._avg_loss = self._avg_loss * 0.9 + abs(r_multiple) * 0.1

    def update_regime(self, regime: str, volatility: float = 0.02):
        self._current_regime = regime
        self._current_volatility = volatility
        if self._baseline_volatility < 0.005:
            self._baseline_volatility = volatility

    def reset_daily(self, equity: float):
        self._pnl_history.clear()

    def snapshot(self) -> RiskAppetiteSnapshot:
        s = RiskAppetiteSnapshot()
        gating = []

        regime_mult = REGIME_RISK_MULTIPLIERS.get(self._current_regime, 0.7)
        s.regime_multiplier = regime_mult

        current_equity = self._equity_history[-1] if self._equity_history else 1.0
        drawdown_pct = max(0.0, (self._peak_equity - current_equity) / max(self._peak_equity, 1e-9))

        s.drawdown_severity = self._classify_drawdown(drawdown_pct)
        dd_mult = self._drawdown_multiplier(drawdown_pct)
        s.drawdown_multiplier = dd_mult
        if dd_mult < 1.0:
            gating.append(f"drawdown={drawdown_pct:.1%}")

        s.profit_lock_active = False
        profit_lock_mult = 1.0
        if self._peak_equity > 0:
            gain_pct = (current_equity - self._peak_equity) / max(self._peak_equity, 1e-9)
            if gain_pct > self._profit_lock_threshold:
                lock_tier = min(1.0, (gain_pct - self._profit_lock_threshold) / 0.20)
                profit_lock_mult = 1.0 - lock_tier * 0.4
                s.profit_lock_active = True
                gating.append(f"profit_lock(gain={gain_pct:.1%})")
        s.profit_lock_multiplier = profit_lock_mult

        s.consecutive_loss_state = self._classify_loss_streak()
        cl_mult = self._consecutive_loss_multiplier()
        s.consecutive_loss_multiplier = cl_mult
        if cl_mult < 1.0:
            gating.append(f"loss_streak={self._consecutive_losses}")

        compound_mult = 1.0
        s.is_compounding = False
        if self._consecutive_wins >= 3 and self._total_trades >= 10:
            recent_wr = self.recent_win_rate()
            if recent_wr > 0.6:
                compound_boost = min(0.3, (recent_wr - 0.6) * 1.5)
                compound_mult = 1.0 + compound_boost
                s.is_compounding = True
                gating.append(f"compounding(wr={recent_wr:.0%})")
        s.compound_multiplier = compound_mult

        vol_mult = 1.0
        if self._baseline_volatility > 0:
            vol_ratio = self._current_volatility / max(self._baseline_volatility, 1e-9)
            if vol_ratio > 1.5:
                vol_mult = max(0.3, 1.0 - (vol_ratio - 1.5) * 0.4)
            elif vol_ratio < 0.5:
                vol_mult = min(1.3, 1.0 + (0.5 - vol_ratio) * 0.3)
        s.volatility_multiplier = vol_mult

        product = regime_mult * dd_mult * profit_lock_mult * cl_mult * compound_mult * vol_mult
        s.score = max(0.05, min(1.0, product))

        label_map = {
            (0.0, 0.3): "conservative",
            (0.3, 0.6): "moderate",
            (0.6, 0.8): "aggressive",
            (0.8, 1.0): "high_risk",
        }
        s.profile_label = next(
            (l for (lo, hi), l in label_map.items() if lo <= s.score < hi),
            "moderate",
        )

        max_lev = REGIME_MAX_LEVERAGE.get(self._current_regime, 1.0)
        s.effective_leverage = round(max(1.0, min(max_lev, self.base_profile.max_leverage * s.score)), 2)
        s.effective_risk_per_trade = round(self.base_profile.risk_per_trade_pct * s.score, 4)
        s.effective_max_position_pct = round(self.base_profile.max_position_pct * s.score, 4)
        s.gating_reasons = gating
        s.total_trades = self._total_trades
        s.recent_win_rate = self.recent_win_rate()

        return s

    def get_profile(self) -> RiskProfile:
        s = self.snapshot()
        return RiskProfile(
            max_leverage=s.effective_leverage,
            max_positions=max(2, int(self.base_profile.max_positions * s.score)),
            max_position_pct=s.effective_max_position_pct,
            max_correlation_pct=self.base_profile.max_correlation_pct * s.score,
            max_drawdown_pct=self.base_profile.max_drawdown_pct,
            daily_loss_limit_pct=self.base_profile.daily_loss_limit_pct * (0.5 + s.score * 0.5),
            min_risk_reward=max(1.0, self.base_profile.min_risk_reward / max(s.score, 0.1)),
            risk_per_trade_pct=s.effective_risk_per_trade,
            var_confidence=self.base_profile.var_confidence,
            max_notional_per_trade=self.base_profile.max_notional_per_trade * s.score,
        )

    def size_adjustment(self) -> float:
        s = self.snapshot()
        return round(s.score, 4)

    def recent_win_rate(self, n: int = 20) -> float:
        recent = self._trade_results[-n:] if len(self._trade_results) >= n else self._trade_results
        if not recent:
            return 0.5
        return sum(recent) / len(recent)

    def _classify_drawdown(self, dd_pct: float) -> str:
        if dd_pct < 0.05:
            return DrawdownSeverity.NONE.value
        elif dd_pct < 0.10:
            return DrawdownSeverity.MILD.value
        elif dd_pct < 0.20:
            return DrawdownSeverity.MODERATE.value
        return DrawdownSeverity.SEVERE.value

    def _classify_loss_streak(self) -> str:
        if self._consecutive_losses == 0:
            return ConsecutiveLossState.NORMAL.value
        elif self._consecutive_losses <= 2:
            return ConsecutiveLossState.CAUTION.value
        elif self._consecutive_losses <= self._max_consecutive_losses:
            return ConsecutiveLossState.REDUCED.value
        return ConsecutiveLossState.STOPPED.value

    def _drawdown_multiplier(self, dd_pct: float) -> float:
        for threshold, mult in self._drawdown_tiers:
            if dd_pct >= threshold:
                return mult
        return 1.0

    def _consecutive_loss_multiplier(self) -> float:
        if self._consecutive_losses == 0:
            return 1.0
        if self._consecutive_losses <= 2:
            return 0.8
        elif self._consecutive_losses <= self._max_consecutive_losses:
            return 0.5
        return 0.0

    def summary(self) -> Dict[str, Any]:
        s = self.snapshot()
        return {
            "appetite_score": s.score,
            "profile_label": s.profile_label,
            "regime_multiplier": s.regime_multiplier,
            "drawdown_multiplier": s.drawdown_multiplier,
            "drawdown_severity": s.drawdown_severity,
            "profit_lock_multiplier": s.profit_lock_multiplier,
            "profit_lock_active": s.profit_lock_active,
            "loss_multiplier": s.consecutive_loss_multiplier,
            "loss_state": s.consecutive_loss_state,
            "compound_multiplier": s.compound_multiplier,
            "is_compounding": s.is_compounding,
            "volatility_multiplier": s.volatility_multiplier,
            "effective_leverage": s.effective_leverage,
            "effective_risk_per_trade": s.effective_risk_per_trade,
            "effective_max_position_pct": s.effective_max_position_pct,
            "consecutive_wins": self._consecutive_wins,
            "consecutive_losses": self._consecutive_losses,
            "recent_win_rate": round(self.recent_win_rate(), 3),
            "total_trades": self._total_trades,
            "peak_equity": self._peak_equity,
        }
