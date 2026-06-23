from __future__ import annotations
import time
import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple, Any

from .protocols import Direction, Opportunity


@dataclass
class FeeTier:
    min_volume: float
    maker_rate: float
    taker_rate: float


COINBASE_FEE_TIERS: List[FeeTier] = [
    FeeTier(0, 0.0060, 0.0120),
    FeeTier(1_000, 0.0035, 0.0075),
    FeeTier(10_000, 0.0025, 0.0040),
    FeeTier(50_000, 0.0015, 0.0025),
    FeeTier(100_000, 0.0010, 0.0020),
    FeeTier(1_000_000, 0.0008, 0.0018),
    FeeTier(20_000_000, 0.0005, 0.0015),
]


class FeeTracker:
    def __init__(self, initial_volume_30d: float = 0.0):
        self._initial_volume = initial_volume_30d
        self._trades_30d: List[Tuple[float, float]] = []

    @property
    def rolling_30d_volume(self) -> float:
        return self._initial_volume + sum(v for _, v in self._trades_30d)

    def get_current_tier(self) -> FeeTier:
        vol = self.rolling_30d_volume
        for tier in reversed(COINBASE_FEE_TIERS):
            if vol >= tier.min_volume:
                return tier
        return COINBASE_FEE_TIERS[0]

    def get_next_tier(self) -> Optional[FeeTier]:
        current = self.get_current_tier()
        for tier in COINBASE_FEE_TIERS:
            if tier.min_volume > current.min_volume:
                return tier
        return None

    def volume_to_next_tier(self) -> float:
        current = self.get_current_tier()
        next_tier = self.get_next_tier()
        if next_tier:
            return max(0.0, next_tier.min_volume - self.rolling_30d_volume)
        return 0.0

    def record_trade(self, volume_usd: float, timestamp: Optional[float] = None):
        self._trades_30d.append((timestamp or time.time(), volume_usd))
        self._prune()

    def fee_cost(self, trade_volume: float, is_maker: bool) -> float:
        tier = self.get_current_tier()
        rate = tier.maker_rate if is_maker else tier.taker_rate
        return trade_volume * rate

    def maker_rate(self) -> float:
        return self.get_current_tier().maker_rate

    def taker_rate(self) -> float:
        return self.get_current_tier().taker_rate

    def savings_to_next_tier(self, projected_monthly_volume: float) -> float:
        current = self.get_current_tier()
        next_tier = self.get_next_tier()
        if not next_tier:
            return 0.0
        maker_savings = (current.maker_rate - next_tier.maker_rate) * projected_monthly_volume * 0.5
        taker_savings = (current.taker_rate - next_tier.taker_rate) * projected_monthly_volume * 0.5
        return maker_savings + taker_savings

    def to_state(self) -> dict:
        return {"initial_volume_30d": self._initial_volume, "trades_30d": self._trades_30d}

    @classmethod
    def from_state(cls, state: Optional[dict] = None) -> FeeTracker:
        state = state or {}
        tracker = cls(initial_volume_30d=float(state.get("initial_volume_30d", 0.0)))
        trades = state.get("trades_30d", []) or []
        tracker._trades_30d = [(float(ts), float(v)) for ts, v in trades]
        tracker._prune()
        return tracker

    def _prune(self):
        cutoff = time.time() - 30 * 86400
        self._trades_30d = [(ts, v) for ts, v in self._trades_30d if ts > cutoff]

    def tier_display(self) -> str:
        tier = self.get_current_tier()
        next_tier = self.get_next_tier()
        base = (f"Tier ${tier.min_volume:,.0f}+: "
                f"maker={tier.maker_rate*100:.2f}% taker={tier.taker_rate*100:.2f}%")
        if next_tier:
            needed = self.volume_to_next_tier()
            base += (f" | ${needed:,.0f} to next "
                     f"(maker={next_tier.maker_rate*100:.2f}%)")
        return base


class FeeAwareSizer:
    def __init__(self, fee_tracker: Optional[FeeTracker] = None):
        self.fee_tracker = fee_tracker or FeeTracker()

    def effective_expected_return(self, expected_return_pct: float,
                                   trade_volume: float,
                                   is_maker: bool = False) -> float:
        fee_rate = self.fee_tracker.taker_rate() if not is_maker else self.fee_tracker.maker_rate()
        return expected_return_pct - fee_rate

    def volume_boost(self, trade_volume: float) -> float:
        needed = self.fee_tracker.volume_to_next_tier()
        if needed <= 0:
            return 1.0
        proximity = min(trade_volume / max(needed, 1), 1.0)
        return 1.0 + proximity * 0.4

    def size_with_fee_boost(self, opp: Opportunity) -> Opportunity:
        volume = opp.quote_size or opp.base_size * opp.entry_price
        boost = self.volume_boost(volume)
        opp.base_size *= boost
        opp.meta["fee_volume_boost"] = round(boost, 3)
        opp.meta["fee_tier"] = self.fee_tracker.get_current_tier().min_volume
        opp.meta["volume_to_next_tier"] = round(self.fee_tracker.volume_to_next_tier(), 2)
        return opp

    def should_generate_volume(self, min_savings: float = 50.0) -> Tuple[bool, float]:
        needed = self.fee_tracker.volume_to_next_tier()
        if needed <= 0:
            return False, 0.0
        projected = self.fee_tracker.rolling_30d_volume * 1.1
        savings = self.fee_tracker.savings_to_next_tier(projected)
        return savings > min_savings, savings


class VolumeGenerator:
    def __init__(self, fee_tracker: FeeTracker,
                 max_volume_per_day: float = 10000.0,
                 min_spread_bps: float = 5.0):
        self.fee_tracker = fee_tracker
        self.max_volume_per_day = max_volume_per_day
        self.min_spread_bps = min_spread_bps
        self._daily_volume: float = 0.0
        self._daily_reset_ts: float = time.time()

    def generate_volume_opportunities(self, product_id: str,
                                       current_price: float,
                                       atr: float) -> Optional[Opportunity]:
        self._daily_check()
        needed = self.fee_tracker.volume_to_next_tier()
        if needed <= 0:
            return None
        if self._daily_volume >= self.max_volume_per_day:
            return None

        tier = self.fee_tracker.get_current_tier()
        next_tier = self.fee_tracker.get_next_tier()
        if not next_tier:
            return None

        spread_savings = (tier.maker_rate - next_tier.maker_rate) * current_price * 0.5
        if spread_savings <= 0:
            return None

        gen_volume = min(needed, self.max_volume_per_day - self._daily_volume)
        gen_size = gen_volume / max(current_price, 1e-9)

        return Opportunity(
            product_id=product_id,
            direction=Direction.LONG,
            instrument_type=None,
            entry_price=current_price,
            stop_price=current_price * 0.99,
            target_price=current_price * 1.01,
            risk_reward=1.0,
            confidence=0.2,
            reason=f"VOLGEN: generate ${gen_volume:.0f} volume for fee tier",
            strategy_name="volume_generator",
            base_size=gen_size,
            quote_size=gen_volume,
            atr=atr,
            leverage=1.0,
            total_risk_pct=0.001,
            meta={"volume_to_generate": gen_volume, "volume_type": "fee_tier"},
        )

    def _daily_check(self):
        now = time.time()
        if now - self._daily_reset_ts > 86400:
            self._daily_volume = 0.0
            self._daily_reset_ts = now

    def record_generated(self, volume: float):
        self._daily_volume += volume
        self.fee_tracker.record_trade(volume)
