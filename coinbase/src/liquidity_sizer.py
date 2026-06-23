from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple, Callable

from .protocols import Direction, Opportunity


@dataclass
class LiquidityProfile:
    product_id: str = ""
    bid_depth: List[Tuple[float, float]] = field(default_factory=list)
    ask_depth: List[Tuple[float, float]] = field(default_factory=list)
    spread_bps: float = 0.0
    volume_24h: float = 0.0
    bid_vol_at_1pct: float = 0.0
    ask_vol_at_1pct: float = 0.0
    liquidity_score: float = 1.0
    max_bid_notional: float = 0.0
    max_ask_notional: float = 0.0

    @property
    def mid_price(self) -> float:
        if not self.bid_depth or not self.ask_depth:
            return 0.0
        return (self.bid_depth[0][0] + self.ask_depth[0][0]) / 2.0


LIQUIDITY_TIERS = {
    "deep": 0.8,
    "moderate": 0.5,
    "thin": 0.2,
    "illiquid": 0.05,
}


class MarketImpactModel:
    def __init__(self, impact_coeff: float = 1.5, participation_rate: float = 0.1):
        self.coeff = impact_coeff
        self.participation = participation_rate

    def estimate_impact_bps(self, notional: float, volume_24h: float,
                            price: float, side: str = "buy") -> float:
        if volume_24h <= 0 or price <= 0:
            return 10.0
        daily_notional = volume_24h * price
        participation = notional / max(daily_notional, 1e-9)
        perm_impact = self.coeff * math.sqrt(participation / 1000 * 100) if participation > 0 else 0
        temp_impact = self.participation * math.sqrt(participation * 10000)
        return min(50.0, perm_impact + temp_impact)

    def optimal_participation(self, notional: float, volume_24h: float,
                              price: float, max_impact_bps: float = 10.0) -> float:
        if volume_24h <= 0:
            return 0.01
        daily_notional = volume_24h * price
        target_participation = (max_impact_bps / max(self.coeff, 0.01)) ** 2 / 100 * 1000
        return min(0.3, max(0.001, target_participation / 100))


class OrderBookDepthEstimator:
    def __init__(self, default_ticks: int = 10):
        self.default_ticks = default_ticks
        self._depth_cache: Dict[str, LiquidityProfile] = {}
        self._price_provider: Optional[Callable[[str], Tuple[float, float, float]]] = None

    def set_price_provider(self, fn: Callable[[str], Tuple[float, float, float]]):
        self._price_provider = fn

    def estimate_depth(self, product_id: str, current_price: float,
                       volume_24h: float, atr: float) -> LiquidityProfile:
        cache_key = f"{product_id}_{current_price:.2f}"
        cached = self._depth_cache.get(cache_key)
        if cached:
            return cached

        tick_size = current_price * 0.0001
        atr_bps = atr / max(current_price, 1e-9) * 10000 if atr > 0 else 20.0
        depth_levels = max(5, min(20, int(atr_bps / 5)))

        spread_bps = max(0.5, atr_bps * 0.15)
        spread = current_price * (spread_bps / 10000)

        vol_per_tick = (volume_24h / 24 / 60) * (current_price / 10)
        depth_decay = 0.85

        bid_depth = []
        ask_depth = []
        cum_bid = 0.0
        cum_ask = 0.0

        for i in range(depth_levels):
            bid_price = current_price - spread / 2 - tick_size * i
            ask_price = current_price + spread / 2 + tick_size * i
            bid_size = vol_per_tick * (depth_decay ** i)
            ask_size = vol_per_tick * (depth_decay ** i)
            bid_depth.append((bid_price, bid_size))
            ask_depth.append((ask_price, ask_size))
            cum_bid += bid_size * bid_price
            cum_ask += ask_size * ask_price

        price_1pct = current_price * 0.01
        levels_1pct = max(1, int(price_1pct / max(tick_size, 1e-9)))
        bid_vol_1pct = sum(s for p, s in bid_depth[:levels_1pct])
        ask_vol_1pct = sum(s for p, s in ask_depth[:levels_1pct])

        daily_notional = volume_24h * current_price
        if daily_notional > 1_000_000:
            score = LIQUIDITY_TIERS["deep"]
        elif daily_notional > 100_000:
            score = LIQUIDITY_TIERS["moderate"]
        elif daily_notional > 10_000:
            score = LIQUIDITY_TIERS["thin"]
        else:
            score = LIQUIDITY_TIERS["illiquid"]

        profile = LiquidityProfile(
            product_id=product_id,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            spread_bps=spread_bps,
            volume_24h=volume_24h,
            bid_vol_at_1pct=bid_vol_1pct,
            ask_vol_at_1pct=ask_vol_1pct,
            liquidity_score=score,
            max_bid_notional=cum_bid * 0.5,
            max_ask_notional=cum_ask * 0.5,
        )

        if len(self._depth_cache) > 100:
            self._depth_cache.clear()
        self._depth_cache[cache_key] = profile
        return profile

    def max_liquid_size(self, product_id: str, side: str,
                        current_price: float, volume_24h: float,
                        atr: float, max_impact_bps: float = 15.0,
                        slippage_tolerance_bps: float = 30.0) -> float:
        profile = self.estimate_depth(product_id, current_price, volume_24h, atr)
        depth = profile.ask_depth if side == "buy" else profile.bid_depth

        cumulative = 0.0
        cumulative_notional = 0.0
        for price, size in depth:
            slippage = abs(price - current_price) / max(current_price, 1e-9) * 10000
            if slippage > slippage_tolerance_bps:
                break
            cumulative += size
            cumulative_notional += size * price

        max_participation = 0.05
        max_daily_notional = volume_24h * current_price
        max_by_notional = max_participation * max_daily_notional
        max_by_base = max_by_notional / max(current_price, 1e-9)

        impact_model = MarketImpactModel()
        impact_bps = impact_model.estimate_impact_bps(
            cumulative_notional or current_price * cumulative,
            volume_24h, current_price, side,
        )
        if impact_bps > max_impact_bps:
            scale = max_impact_bps / max(impact_bps, 0.01)
            cumulative *= scale

        return min(cumulative, max_by_base)


class LiquidityAwareSizer:
    def __init__(self, depth_estimator: Optional[OrderBookDepthEstimator] = None,
                 impact_model: Optional[MarketImpactModel] = None):
        self.depth_estimator = depth_estimator or OrderBookDepthEstimator()
        self.impact_model = impact_model or MarketImpactModel()
        self._volume_24h_cache: Dict[str, float] = {}

    def set_volume_24h(self, product_id: str, volume: float):
        self._volume_24h_cache[product_id] = volume

    def max_size_for_liquidity(self, product_id: str, side: str,
                                current_price: float, atr: float) -> float:
        vol_24h = self._volume_24h_cache.get(product_id, 100_000)
        return self.depth_estimator.max_liquid_size(
            product_id, side, current_price, vol_24h, atr
        )

    def size_with_liquidity(self, opp: Opportunity, atr: float) -> Opportunity:
        vol_24h = self._volume_24h_cache.get(opp.product_id, 100_000)
        profile = self.depth_estimator.estimate_depth(
            opp.product_id, opp.entry_price, vol_24h, atr
        )

        side = "buy" if opp.direction == Direction.LONG else "sell"
        max_size = self.max_size_for_liquidity(
            opp.product_id, side, opp.entry_price, atr
        )

        impact_bps = self.impact_model.estimate_impact_bps(
            opp.quote_size or opp.base_size * opp.entry_price,
            vol_24h, opp.entry_price, side,
        )

        capped = min(opp.base_size, max_size) if max_size > 0 else opp.base_size
        opp.base_size = capped
        opp.meta["liquidity_max_size"] = round(max_size, 4) if max_size > 0 else 0
        opp.meta["liquidity_score"] = round(profile.liquidity_score, 3)
        opp.meta["liquidity_spread_bps"] = round(profile.spread_bps, 2)
        opp.meta["estimated_impact_bps"] = round(impact_bps, 2)
        opp.meta["bid_vol_at_1pct"] = round(profile.bid_vol_at_1pct, 2)
        opp.meta["ask_vol_at_1pct"] = round(profile.ask_vol_at_1pct, 2)

        if impact_bps > 15.0:
            opp.confidence *= max(0.3, 1.0 - (impact_bps - 15.0) / 50.0)
            opp.meta["impact_penalty"] = True

        return opp

    def size_batch(self, opportunities: List[Opportunity],
                    atr: float) -> List[Opportunity]:
        return [self.size_with_liquidity(opp, atr) for opp in opportunities]

    def liquidation_size(self, product_id: str, current_price: float,
                          atr: float, max_slippage_bps: float = 20.0) -> float:
        vol_24h = self._volume_24h_cache.get(product_id, 100_000)
        return self.depth_estimator.max_liquid_size(
            product_id, "sell", current_price, vol_24h, atr,
            slippage_tolerance_bps=max_slippage_bps,
        )
