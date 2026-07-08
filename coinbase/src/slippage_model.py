"""
Order book simulation and dynamic slippage/fee modeling.
"""

import logging
import time
import threading
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import deque
from enum import Enum

log = logging.getLogger(__name__)


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"


class Side(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class OrderBookLevel:
    price: float
    size: float
    orders: int = 1


@dataclass
class OrderBookSnapshot:
    """L2 order book snapshot."""
    product_id: str
    timestamp: float
    bids: List[OrderBookLevel] = field(default_factory=list)  # Sorted descending
    asks: List[OrderBookLevel] = field(default_factory=list)  # Sorted ascending
    
    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0
    
    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0.0
    
    @property
    def mid_price(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return self.best_bid or self.best_ask or 0.0
    
    @property
    def spread_bps(self) -> float:
        if self.mid_price > 0:
            return (self.best_ask - self.best_bid) / self.mid_price * 10000
        return 0.0
    
    def depth_at_bps(self, bps_from_mid: float, side: Side) -> float:
        """Get cumulative size within bps from mid."""
        if self.mid_price == 0:
            return 0.0
        levels = self.asks if side == Side.BUY else self.bids
        max_price = self.mid_price * (1 + bps_from_mid / 10000) if side == Side.BUY else self.mid_price * (1 - bps_from_mid / 10000)
        total = 0.0
        for level in levels:
            if side == Side.BUY:
                if level.price <= max_price:
                    total += level.size
                else:
                    break
            else:
                if level.price >= max_price:
                    total += level.size
                else:
                    break
        return total


class OrderBookCache:
    """Thread-safe order book cache with TTL."""
    
    def __init__(self, ttl_s: float = 5.0):
        self._books: Dict[str, OrderBookSnapshot] = {}
        self._lock = threading.RLock()
        self._ttl = ttl_s
    
    def update(self, snapshot: OrderBookSnapshot):
        with self._lock:
            self._books[snapshot.product_id] = snapshot
    
    def get(self, product_id: str) -> Optional[OrderBookSnapshot]:
        with self._lock:
            book = self._books.get(product_id)
            if book and time.time() - book.timestamp < self._ttl:
                return book
            return None
    
    def get_spread_bps(self, product_id: str) -> float:
        book = self.get(product_id)
        return book.spread_bps if book else 10.0  # Default 10bps
    
    def get_depth(self, product_id: str, bps: float, side: Side) -> float:
        book = self.get(product_id)
        return book.depth_at_bps(bps, side) if book else 0.0


@dataclass
class SlippageEstimate:
    """Slippage estimate for an order."""
    expected_fill_price: float
    slippage_bps: float
    fill_probability: float
    partial_fill_pct: float
    maker_probability: float
    queue_position_estimate: int
    estimated_latency_ms: float


class SlippageModel:
    """
    Dynamic slippage model based on order book, volatility, and order size.
    
    Factors:
    - Order book depth (within 10-50bps of mid)
    - Order size relative to 1-min volume
    - Recent volatility (ATR)
    - Spread
    - Maker/taker classification
    """
    
    def __init__(self, book_cache: OrderBookCache):
        self.book_cache = book_cache
        
        # Model parameters (tuned from historical data)
        self.base_market_impact = 0.0001  # 1bps per 1% of 1-min volume
        self.volatility_impact_mult = 0.5  # ATR multiplier
        self.spread_impact_mult = 0.3
        self.min_slippage_bps = 0.5
        self.max_slippage_bps = 200.0
        
        # Maker/taker model
        self.base_maker_prob = 0.3  # Base probability for limit orders
        self.queue_decay = 0.95  # Per level in queue
    
    def estimate_slippage(
        self,
        product_id: str,
        side: Side,
        order_type: OrderType,
        size_base: float,
        price: float,
        volume_1m: float,
        atr: float = 0.0,
        urgency: float = 1.0,  # 0=passive, 1=urgent
    ) -> SlippageEstimate:
        """Estimate slippage for an order."""
        
        book = self.book_cache.get(product_id)
        spread_bps = book.spread_bps if book else 10.0
        mid = book.mid_price if book else price
        
        # Size relative to 1-min volume
        vol_ratio = size_base / max(volume_1m, 1e-9) if volume_1m > 0 else 1.0
        vol_ratio = min(vol_ratio, 1.0)  # Cap at 100% of 1-min vol
        
        # Base market impact
        impact_bps = self.base_market_impact * vol_ratio * 10000  # Convert to bps
        
        # Volatility adjustment
        if atr > 0 and mid > 0:
            atr_pct = atr / mid
            impact_bps *= (1 + self.volatility_impact_mult * atr_pct * 100)
        
        # Spread cost (half spread for crossing)
        if order_type == OrderType.MARKET:
            impact_bps += spread_bps * self.spread_impact_mult
        
        # Urgency multiplier
        impact_bps *= (0.5 + 0.5 * urgency)
        
        # Clamp
        impact_bps = max(self.min_slippage_bps, min(self.max_slippage_bps, impact_bps))
        
        # Expected fill price
        if side == Side.BUY:
            fill_price = mid * (1 + impact_bps / 10000)
        else:
            fill_price = mid * (1 - impact_bps / 10000)
        
        # Maker probability for limit orders
        maker_prob = 0.0
        queue_pos = 0
        if order_type == OrderType.LIMIT:
            if book:
                # Estimate queue position based on price distance from mid
                if side == Side.BUY:
                    dist_bps = (mid - price) / mid * 10000 if mid > 0 else 0
                else:
                    dist_bps = (price - mid) / mid * 10000 if mid > 0 else 0
                
                # If price is at or better than best bid/ask, high maker prob
                if side == Side.BUY and price >= book.best_bid:
                    maker_prob = self.base_maker_prob * 2
                elif side == Side.SELL and price <= book.best_ask:
                    maker_prob = self.base_maker_prob * 2
                elif dist_bps <= 5:  # Within 5bps
                    maker_prob = self.base_maker_prob
                elif dist_bps <= 20:
                    maker_prob = self.base_maker_prob * 0.5
                else:
                    maker_prob = self.base_maker_prob * 0.1
                
                # Estimate queue position from depth at price
                if side == Side.BUY:
                    depth = book.depth_at_bps(dist_bps, Side.BUY)
                else:
                    depth = book.depth_at_bps(dist_bps, Side.SELL)
                queue_pos = int(depth / max(size_base, 1e-9))
            else:
                maker_prob = self.base_maker_prob
                queue_pos = 0
        
        # Partial fill probability
        if volume_1m > 0:
            partial_fill = min(1.0, volume_1m / (size_base * 10))
        else:
            partial_fill = 0.5
        
        # Latency estimate
        latency_ms = 50 + impact_bps * 2  # Base 50ms + impact
        
        return SlippageEstimate(
            expected_fill_price=fill_price,
            slippage_bps=impact_bps,
            fill_probability=partial_fill,
            partial_fill_pct=partial_fill,
            maker_probability=maker_prob,
            queue_position_estimate=queue_pos,
            estimated_latency_ms=latency_ms,
        )
    
    def estimate_market_order_cost(
        self,
        product_id: str,
        side: Side,
        size_base: float,
        price: float,
        volume_1m: float,
        atr: float = 0.0,
    ) -> Tuple[float, float, float]:
        """
        Estimate total cost of a market order.
        
        Returns: (fill_price, slippage_bps, fee_bps)
        """
        est = self.estimate_slippage(
            product_id, side, OrderType.MARKET, size_base, price, volume_1m, atr
        )
        # Fee depends on maker/taker
        return est.expected_fill_price, est.slippage_bps, est.maker_probability


# Global instances
_BOOK_CACHE: Optional[OrderBookCache] = None
_SLIPPAGE_MODEL: Optional[SlippageModel] = None
_CACHE_LOCK = threading.Lock()


def get_book_cache() -> OrderBookCache:
    global _BOOK_CACHE
    with _CACHE_LOCK:
        if _BOOK_CACHE is None:
            _BOOK_CACHE = OrderBookCache()
        return _BOOK_CACHE


def get_slippage_model() -> SlippageModel:
    global _SLIPPAGE_MODEL
    with _CACHE_LOCK:
        if _SLIPPAGE_MODEL is None:
            _SLIPPAGE_MODEL = SlippageModel(get_book_cache())
        return _SLIPPAGE_MODEL