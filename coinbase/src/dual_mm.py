from __future__ import annotations
import math
import random
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
from enum import Enum

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


class MMState(Enum):
    SPAWN = "spawn"
    PLACED = "placed"
    ADJUSTING = "adjusting"


@dataclass
class Quote:
    product_id: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    spread_bps: float
    inventory_pct: float
    timestamp_bar: int


class DualMarketMaker:
    def __init__(self, max_spread_bps: float = 20.0, min_spread_bps: float = 2.0,
                 target_inventory: float = 0.0, inventory_limit: float = 0.3,
                 base_size_usd: float = 50.0, quote_refresh_bars: int = 2,
                 max_active_products: int = 3):
        self.max_spread_bps = max_spread_bps
        self.min_spread_bps = min_spread_bps
        self.target_inventory = target_inventory
        self.inventory_limit = inventory_limit
        self.base_size_usd = base_size_usd
        self.quote_refresh_bars = quote_refresh_bars
        self.max_active_products = max_active_products

        self._inventory: Dict[str, float] = {}
        self._last_quotes: Dict[str, Quote] = {}
        self._bars_since_quote: Dict[str, int] = {}
        self._total_pnl: float = 0.0
        self._total_spread_captures: int = 0
        self._bar_count: int = 0

    def record_trade(self, product_id: str, qty: float, price: float):
        self._inventory[product_id] = self._inventory.get(product_id, 0.0) + qty

    def generate_quotes(self, product_id: str, bar: Bar,
                        history: List[Bar]) -> Optional[Quote]:
        self._bar_count += 1
        bars_since = self._bars_since_quote.get(product_id, 999)
        if bars_since < self.quote_refresh_bars:
            self._bars_since_quote[product_id] = bars_since + 1
            return self._last_quotes.get(product_id)
        self._bars_since_quote[product_id] = 0

        mid = (bar.high + bar.low) / 2
        if mid <= 0:
            return None
        closes = [b.close for b in history] + [bar.close]

        atr = self._estimate_atr(
            [b.close for b in history] + [bar.close],
            [b.high for b in history] + [bar.high],
            [b.low for b in history] + [bar.low],
        )
        vola_bps = atr / max(mid, 1e-9) * 10000 if atr > 0 else 20

        dynamic_spread = min(self.max_spread_bps, max(self.min_spread_bps, vola_bps * 0.8))
        half_spread = dynamic_spread / 2 / 10000

        inventory = self._inventory.get(product_id, 0.0)
        inv_pct = abs(inventory) / max(abs(inventory), self.base_size_usd / mid) if abs(inventory) > 0 else 0

        inventory_skew = inventory * 0.3 if abs(inventory) > 0 else 0.0
        inventory_skew_bps = inventory_skew / max(mid, 1e-9) * 10000
        inventory_skew_pct = inventory_skew_bps / 10000

        bid = mid * (1 - half_spread) - inventory_skew_pct * mid
        ask = mid * (1 + half_spread) - inventory_skew_pct * mid

        bid_size = self.base_size_usd / max(bid, 1e-9)
        ask_size = self.base_size_usd / max(ask, 1e-9)

        if inv_pct > self.inventory_limit:
            lean_away = inv_pct * self.base_size_usd / max(mid, 1e-9)
            if inventory > 0:
                ask_size += lean_away
                bid_size *= 0.5
            else:
                bid_size += lean_away
                ask_size *= 0.5

        vol_ratio = 1.0
        if len(closes) >= 20:
            recent_win = closes[-21:] if len(closes) >= 21 else closes
            long_win = closes[-101:] if len(closes) >= 101 else closes
            recent_vol = sum(
                (recent_win[i] - recent_win[i-1]) ** 2 for i in range(1, len(recent_win))
            ) / max(1, len(recent_win) - 1)
            long_vol = sum(
                (long_win[i] - long_win[i-1]) ** 2 for i in range(1, len(long_win))
            ) / max(1, len(long_win) - 1)
            vol_ratio = math.sqrt(recent_vol / max(long_vol, 1e-20)) if long_vol > 0 else 1.0

        if vol_ratio > 2.0:
            dynamic_spread *= 1.5
            bid_size *= 0.5
            ask_size *= 0.5

        quote = Quote(
            product_id=product_id,
            bid_price=round(bid, 4), ask_price=round(ask, 4),
            bid_size=round(bid_size, 6), ask_size=round(ask_size, 6),
            spread_bps=round(dynamic_spread, 1),
            inventory_pct=round(inv_pct, 3),
            timestamp_bar=self._bar_count,
        )
        self._last_quotes[product_id] = quote
        return quote

    def record_fill(self, product_id: str, side: str, price: float,
                    size: float) -> Dict:
        qty = size if side == "buy" else -size
        self.record_trade(product_id, qty, price)
        inv = self._inventory.get(product_id, 0.0)
        return {
            "product_id": product_id, "side": side,
            "price": price, "size": size,
            "inventory": round(inv, 6),
            "inventory_pct": round(abs(inv) / max(self.base_size_usd / price, 1e-9), 3) if abs(inv) > 0 else 0,
        }

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i-1]),
                     abs(lows[-i] - closes[-i-1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0

    def summary(self) -> Dict:
        return {
            "spread_captures": self._total_spread_captures,
            "total_pnl": round(self._total_pnl, 2),
            "active_inventories": {
                pid: round(v, 6) for pid, v in self._inventory.items() if abs(v) > 1e-9
            },
        }


class MarketMakingStrategy(BaseStrategy):
    def __init__(self, mm: Optional[DualMarketMaker] = None):
        self.mm = mm or DualMarketMaker()
        self._name = "market_making"
        self._current_pid = "BTC-USD"

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self._current_pid = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        quote = self.mm.generate_quotes(self._current_pid, bar, history)
        if quote is None:
            return None

        mid = (bar.high + bar.low) / 2
        atr = self._estimate_atr(
            [b.close for b in history] + [bar.close],
            [b.high for b in history] + [bar.high],
            [b.low for b in history] + [bar.low],
        )
        if atr <= 0:
            return None

        inventory = self.mm._inventory.get(self._current_pid, 0.0)
        half_spread = quote.spread_bps / 2 / 10000

        entries = []
        bid_confidence = 0.35
        ask_confidence = 0.35

        if bar.volume > 0:
            vol_factor = min(2.0, bar.volume / max(self._avg_volume(history), 1))
            bid_confidence = min(0.5, bid_confidence * vol_factor)
            ask_confidence = min(0.5, ask_confidence * vol_factor)

        if inventory < self.mm.target_inventory:
            entries.append(BracketSetup(
                direction=Direction.LONG, entry_price=quote.bid_price,
                stop_price=round(quote.bid_price - atr * 0.8, 4),
                target_price=round(quote.ask_price * 1.001, 4),
                risk_reward=1.5, confidence=round(bid_confidence, 3),
                reason=f"MM_BID: {self._current_pid} spread={quote.spread_bps:.0f}bps",
                strategy_name=self._name, atr=atr,
                metadata={"mm_role": "maker", "side": "bid",
                          "inventory": round(inventory, 6),
                          "spread_bps": quote.spread_bps},
            ))

        if inventory > -self.mm.target_inventory:
            entries.append(BracketSetup(
                direction=Direction.SHORT, entry_price=quote.ask_price,
                stop_price=round(quote.ask_price + atr * 0.8, 4),
                target_price=round(quote.bid_price * 0.999, 4),
                risk_reward=1.5, confidence=round(ask_confidence, 3),
                reason=f"MM_ASK: {self._current_pid} spread={quote.spread_bps:.0f}bps",
                strategy_name=self._name, atr=atr,
                metadata={"mm_role": "maker", "side": "ask",
                          "inventory": round(inventory, 6),
                          "spread_bps": quote.spread_bps},
            ))

        if not entries:
            return None
        if len(entries) == 1:
            return entries[0]

        return random.choice(entries)

    @staticmethod
    def _avg_volume(history: List[Bar], window: int = 10) -> float:
        vols = [b.volume for b in history[-window:]] if history else [0]
        return sum(vols) / len(vols) if vols else 0

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i-1]),
                     abs(lows[-i] - closes[-i-1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
