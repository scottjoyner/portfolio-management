from __future__ import annotations
import math
import random
from dataclasses import dataclass
from typing import Optional
from .protocols import Direction, BacktestFill, FillModel


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    spread_bps: float
    mid_price: float
    total_bid_volume: float
    total_ask_volume: float


class OrderBookSimulator:
    def __init__(self, mid_price: float, volume_24h: float,
                 avg_spread_bps: float = 3.0, depth_levels: int = 10):
        self.mid_price = mid_price
        self.volume_24h = volume_24h
        self.avg_spread_bps = avg_spread_bps
        self.depth_levels = depth_levels

    def snapshot(self) -> OrderBookSnapshot:
        spread = self.mid_price * self.avg_spread_bps / 10000
        bid = self.mid_price - spread / 2
        ask = self.mid_price + spread / 2
        vol_per_level = self.volume_24h / 24 / 3600 * 5
        bids = []
        asks = []
        decay = 0.7
        for i in range(self.depth_levels):
            level_vol = vol_per_level * (decay ** i) * random.uniform(0.8, 1.2)
            bids.append(OrderBookLevel(
                price=bid * (1 - i * self.avg_spread_bps / 10000),
                size=level_vol / max(bid, 1e-9),
            ))
            asks.append(OrderBookLevel(
                price=ask * (1 + i * self.avg_spread_bps / 10000),
                size=level_vol / max(ask, 1e-9),
            ))
        total_bid_vol = sum(l.size * l.price for l in bids)
        total_ask_vol = sum(l.size * l.price for l in asks)
        return OrderBookSnapshot(
            bids=bids, asks=asks,
            spread_bps=self.avg_spread_bps,
            mid_price=self.mid_price,
            total_bid_volume=total_bid_vol,
            total_ask_volume=total_ask_vol,
        )


class AdaptiveFillModel(FillModel):
    def __init__(self, fee_bps: float = 8.0, base_slippage_bps: float = 1.5,
                 impact_coeff: float = 2.0, partial_fill_threshold: float = 0.3):
        self.fee_bps = fee_bps
        self.base_slippage_bps = base_slippage_bps
        self.impact_coeff = impact_coeff
        self.partial_fill_threshold = partial_fill_threshold

    def fill(self, direction: Direction, price: float, size: float,
             bid: float, ask: float, volume: float) -> BacktestFill:
        book = OrderBookSimulator(price, volume, depth_levels=15)
        snapshot = book.snapshot()
        actual_spread = snapshot.spread_bps
        notional = price * size
        impact_bps = self._market_impact(notional, volume, price) if volume > 0 else 0.0
        total_slippage = self.base_slippage_bps + impact_bps
        if direction == Direction.LONG:
            fill_price = price * (1 + (self.fee_bps + total_slippage) / 10000)
        else:
            fill_price = price * (1 - (self.fee_bps + total_slippage) / 10000)
        fees = notional * (self.fee_bps / 10000)
        partial = False
        fill_pct = 1.0
        if actual_spread > 10:
            fill_pct = max(0.0, min(1.0, 1.0 - (actual_spread - 10) / 200))
        if actual_spread > 50:
            partial = True
            fill_pct = max(0.0, 1.0 - (actual_spread - 50) / 100)
        if notional > 0 and volume > 0:
            market_depth_ratio = notional / max(volume * price, 1e-9)
            if market_depth_ratio > self.partial_fill_threshold:
                partial = True
                fill_pct *= max(0.0, 1.0 - (market_depth_ratio - self.partial_fill_threshold))
        final_size = size * max(0.0, fill_pct)
        return BacktestFill(
            timestamp=0.0,
            price=fill_price,
            size=final_size,
            fees=fees,
            slippage=total_slippage,
            partial=partial,
        )

    @staticmethod
    def _market_impact(notional: float, volume_24h: float, price: float) -> float:
        adv = volume_24h * price
        if adv <= 0:
            return 2.0
        participation = notional / max(adv, 1e-9)
        return min(50.0, 5.0 * math.sqrt(participation * 100)) if participation > 0 else 0.0


class FillEngine:
    def __init__(self, model: Optional[FillModel] = None):
        self.model = model or AdaptiveFillModel()
        self._fills: list[BacktestFill] = []

    def execute(self, direction: Direction, price: float, size: float,
                bid: float, ask: float, volume: float) -> BacktestFill:
        fill = self.model.fill(direction, price, size, bid, ask, volume)
        self._fills.append(fill)
        return fill

    def stats(self) -> dict:
        if not self._fills:
            return {"avg_slippage_bps": 0.0, "total_fees": 0.0, "partial_fills": 0}
        total_fees = sum(f.fees for f in self._fills)
        avg_slip = sum(f.slippage for f in self._fills) / len(self._fills)
        partials = sum(1 for f in self._fills if f.partial)
        return {
            "avg_slippage_bps": round(avg_slip, 2),
            "total_fees": round(total_fees, 4),
            "partial_fills": partials,
            "total_fills": len(self._fills),
        }
