from __future__ import annotations
import math
import time
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Callable
from .protocols import Direction, BacktestFill
from .fill_model import AdaptiveFillModel

log = logging.getLogger(__name__)


@dataclass
class ExecutionSlice:
    timestamp: float
    price: float
    size: float
    cumulative_size: float = 0.0
    fees: float = 0.0
    slippage_bps: float = 0.0


@dataclass
class ExecutionResult:
    slices: List[ExecutionSlice] = field(default_factory=list)
    total_size: float = 0.0
    avg_price: float = 0.0
    total_fees: float = 0.0
    avg_slippage_bps: float = 0.0
    duration_secs: float = 0.0
    completion_pct: float = 0.0

    @property
    def vwap(self) -> float:
        if not self.slices or self.total_size <= 0:
            return 0.0
        return sum(s.price * s.size for s in self.slices) / self.total_size

    @property
    def implementation_shortfall(self) -> float:
        if not self.slices:
            return 0.0
        arrival = self.slices[0].price
        return (self.vwap - arrival) / max(arrival, 1e-9) * 100


class TWAPAlgo:
    def __init__(self, total_size: float, duration_secs: float,
                 n_slices: int = 10, fill_model: Optional[AdaptiveFillModel] = None):
        self.total_size = total_size
        self.duration_secs = duration_secs
        self.n_slices = n_slices
        self.fill_model = fill_model or AdaptiveFillModel()
        self.slice_size = total_size / n_slices
        self.interval = duration_secs / n_slices

    def execute(self, direction: Direction, get_price: Callable,
                get_bid_ask: Optional[Callable] = None,
                get_volume: Optional[Callable] = None) -> ExecutionResult:
        result = ExecutionResult()
        cumulative = 0.0
        start = time.time()

        for i in range(self.n_slices):
            slice_start = time.time()
            price = get_price()
            bid, ask = get_bid_ask() if get_bid_ask else (price * 0.999, price * 1.001)
            vol = get_volume() if get_volume else 1000.0

            fill = self.fill_model.fill(direction, price, self.slice_size, bid, ask, vol)
            cumulative += fill.size

            result.slices.append(ExecutionSlice(
                timestamp=time.time(),
                price=fill.price,
                size=fill.size,
                cumulative_size=cumulative,
                fees=fill.fees,
                slippage_bps=fill.slippage,
            ))

            elapsed = time.time() - slice_start
            wait = max(0.0, self.interval - elapsed)
            if i < self.n_slices - 1 and wait > 0:
                time.sleep(wait)

        result.total_size = cumulative
        result.total_fees = sum(s.fees for s in result.slices)
        result.avg_slippage_bps = sum(s.slippage_bps for s in result.slices) / max(len(result.slices), 1)
        result.duration_secs = time.time() - start
        result.completion_pct = cumulative / max(self.total_size, 1e-9) * 100
        return result


class VWAPAlgo:
    def __init__(self, total_size: float, expected_volume_profile: List[float],
                 fill_model: Optional[AdaptiveFillModel] = None):
        self.total_size = total_size
        self.profile = expected_volume_profile
        self.fill_model = fill_model or AdaptiveFillModel()
        total_vol = sum(expected_volume_profile)
        self.slice_weights = [v / max(total_vol, 1e-9) for v in expected_volume_profile]
        self.n_slices = len(expected_volume_profile)

    def execute(self, direction: Direction, get_price: Callable,
                get_bid_ask: Optional[Callable] = None,
                get_volume: Optional[Callable] = None) -> ExecutionResult:
        result = ExecutionResult()
        cumulative = 0.0
        start = time.time()

        for i, weight in enumerate(self.slice_weights):
            slice_size = self.total_size * weight
            if slice_size <= 0:
                continue

            price = get_price()
            bid, ask = get_bid_ask() if get_bid_ask else (price * 0.999, price * 1.001)
            vol = get_volume() if get_volume else 1000.0

            fill = self.fill_model.fill(direction, price, slice_size, bid, ask, vol)
            cumulative += fill.size

            result.slices.append(ExecutionSlice(
                timestamp=time.time(),
                price=fill.price,
                size=fill.size,
                cumulative_size=cumulative,
                fees=fill.fees,
                slippage_bps=fill.slippage,
            ))

        result.total_size = cumulative
        result.total_fees = sum(s.fees for s in result.slices)
        result.avg_slippage_bps = sum(s.slippage_bps for s in result.slices) / max(len(result.slices), 1)
        result.duration_secs = time.time() - start
        result.completion_pct = cumulative / max(self.total_size, 1e-9) * 100
        return result


class IcebergAlgo:
    def __init__(self, total_size: float, visible_size: float,
                 price_delta_bps: float = 2.0,
                 fill_model: Optional[AdaptiveFillModel] = None):
        self.total_size = total_size
        self.visible_size = visible_size
        self.price_delta_bps = price_delta_bps
        self.fill_model = fill_model or AdaptiveFillModel()

    def execute(self, direction: Direction, get_price: Callable,
                get_bid_ask: Optional[Callable] = None,
                get_volume: Optional[Callable] = None,
                on_fill: Optional[Callable] = None) -> ExecutionResult:
        result = ExecutionResult()
        cumulative = 0.0
        start = time.time()
        remaining = self.total_size

        while remaining > 0:
            chunk = min(self.visible_size, remaining)
            price = get_price()
            bid, ask = get_bid_ask() if get_bid_ask else (price * 0.999, price * 1.001)

            if direction == Direction.LONG:
                limit_price = bid * (1 - self.price_delta_bps / 10000)
            else:
                limit_price = ask * (1 + self.price_delta_bps / 10000)

            vol = get_volume() if get_volume else 1000.0
            fill = self.fill_model.fill(direction, limit_price, chunk, bid, ask, vol)
            cumulative += fill.size
            remaining -= fill.size

            result.slices.append(ExecutionSlice(
                timestamp=time.time(),
                price=fill.price,
                size=fill.size,
                cumulative_size=cumulative,
                fees=fill.fees,
                slippage_bps=fill.slippage,
            ))

            if on_fill:
                on_fill(fill)

            if remaining > 0 and fill.size > 0:
                time.sleep(0.5)

        result.total_size = cumulative
        result.total_fees = sum(s.fees for s in result.slices)
        result.avg_slippage_bps = sum(s.slippage_bps for s in result.slices) / max(len(result.slices), 1)
        result.duration_secs = time.time() - start
        result.completion_pct = cumulative / max(self.total_size, 1e-9) * 100
        return result
