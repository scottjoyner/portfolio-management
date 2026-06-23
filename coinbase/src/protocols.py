from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any, Tuple, Callable
import math


class Direction(Enum):
    LONG = "long"
    SHORT = "short"


class InstrumentType(Enum):
    SPOT = "spot"
    PERP_FUTURES = "perp_futures"
    DATED_FUTURES = "dated_futures"


@dataclass
class Bar:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class BracketSetup:
    direction: Direction
    entry_price: float
    stop_price: float
    target_price: float
    risk_reward: float
    confidence: float
    reason: str
    strategy_name: str
    atr: float = 0.0
    instrument_type: InstrumentType = InstrumentType.SPOT
    leverage: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BacktestFill:
    timestamp: float
    price: float
    size: float
    fees: float = 0.0
    slippage: float = 0.0
    partial: bool = False


@dataclass
class BacktestPosition:
    product_id: str
    direction: Direction
    entry: BacktestFill
    size: float
    stop_price: float
    target_price: float
    instrument_type: InstrumentType = InstrumentType.SPOT
    leverage: float = 1.0
    opened_ts: float = 0.0
    closed_ts: Optional[float] = None
    exit_reason: Optional[str] = None
    exit_price: Optional[float] = None
    realized_pnl: Optional[float] = None
    r_multiple: Optional[float] = None


class BaseStrategy(ABC):
    @abstractmethod
    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        ...

    @abstractmethod
    def name(self) -> str:
        ...

    def set_product_id(self, product_id: str):
        pass


class FillModel(ABC):
    @abstractmethod
    def fill(self, direction: Direction, price: float, size: float,
             bid: float, ask: float, volume: float) -> BacktestFill:
        ...


# -- Concrete Fill Model --
class MarketFillModel(FillModel):
    def __init__(self, fee_bps: float = 8.0, slippage_bps: float = 1.5,
                 impact_coeff: float = 1.5, min_fill_pct: float = 0.95):
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self.impact_coeff = impact_coeff
        self.min_fill_pct = min_fill_pct

    def fill(self, direction: Direction, price: float, size: float,
             bid: float, ask: float, volume: float) -> BacktestFill:
        spread_bps = ((ask - bid) / max(bid, 1e-9)) * 10000 if ask > bid else 0.5
        notional = price * size
        impact_bps = self.impact_coeff * math.sqrt(
            notional / max(volume * price, 1e-9)
        ) * 100 if volume > 0 and price > 0 else 0.0
        total_bps = self.fee_bps + self.slippage_bps + impact_bps
        if direction == Direction.LONG:
            fill_price = price * (1 + total_bps / 10000)
        else:
            fill_price = price * (1 - total_bps / 10000)
        fees = notional * (self.fee_bps / 10000)
        partial = False
        if spread_bps > 50:
            fill_pct = max(0.0, 1.0 - (spread_bps - 50) / 200)
            if fill_pct < self.min_fill_pct:
                partial = True
                size *= fill_pct
        return BacktestFill(
            timestamp=0.0,
            price=fill_price,
            size=size,
            fees=fees,
            slippage=total_bps - self.fee_bps,
            partial=partial,
        )


# -- Adapter: strategy_engine.py on_bar -> BracketSetup --
class SignalToBracketAdapter:
    @staticmethod
    def convert(signal: Any, bar: Bar, history: List[Bar],
                atr: float, direction: Direction) -> BracketSetup:
        from strategy_engine import Signal
        if not isinstance(signal, Signal):
            raise TypeError(f"Expected Signal, got {type(signal)}")
        price = signal.price or bar.close
        stop_dist = atr * 2.0
        target_dist = atr * 3.0
        if direction == Direction.LONG:
            stop = price - stop_dist
            target = price + target_dist
        else:
            stop = price + stop_dist
            target = price - target_dist
        rr = abs(target - price) / max(abs(price - stop), 1e-9)
        return BracketSetup(
            direction=direction,
            entry_price=price,
            stop_price=stop,
            target_price=target,
            risk_reward=rr,
            confidence=signal.confidence,
            reason=signal.reason,
            strategy_name=signal.strategy or "signal_adapter",
            atr=atr,
        )


# -- Adapter: backtester.py generate_signatures -> BracketSetup --
class BacktesterToBracketAdapter:
    @staticmethod
    def convert(signal_tuple: Tuple[str, float], bar: Bar,
                history: List[Bar], atr: float) -> Optional[BracketSetup]:
        action, price = signal_tuple
        stop_dist = atr * 2.0
        target_dist = atr * 3.0
        if action == "BUY":
            direction = Direction.LONG
            stop = price - stop_dist
            target = price + target_dist
        elif action == "SELL":
            direction = Direction.SHORT
            stop = price + stop_dist
            target = price - target_dist
        else:
            return None
        rr = abs(target - price) / max(abs(price - stop), 1e-9)
        return BracketSetup(
            direction=direction,
            entry_price=price,
            stop_price=stop,
            target_price=target,
            risk_reward=rr,
            confidence=0.5,
            reason=f"backtester_{action}",
            strategy_name="backtester_adapter",
            atr=atr,
        )


# -- Unified Opportunity --
@dataclass
class Opportunity:
    product_id: str
    direction: Direction
    instrument_type: InstrumentType
    entry_price: float
    stop_price: float
    target_price: float
    risk_reward: float
    confidence: float
    reason: str
    strategy_name: str
    base_size: float = 0.0
    quote_size: float = 0.0
    atr: float = 0.0
    leverage: float = 1.0
    total_risk_pct: float = 0.01
    score: float = 0.0
    meta: Dict[str, Any] = field(default_factory=dict)

    def risk_per_unit(self) -> float:
        if self.direction == Direction.LONG:
            return abs(self.entry_price - self.stop_price)
        return abs(self.stop_price - self.entry_price)

    def compute_size(self, equity: float, risk_per_trade: float = 0.01) -> Opportunity:
        rpu = self.risk_per_unit()
        if rpu <= 0:
            self.base_size = 0.0
        else:
            risk_budget = equity * risk_per_trade * self.leverage
            self.base_size = max(0.0, risk_budget / rpu)
        self.quote_size = self.base_size * self.entry_price
        self.total_risk_pct = risk_per_trade
        return self


# -- Aggregator: combine multiple BracketSetup detections into ranked opportunities --
class OpportunityAggregator:
    def __init__(self, product_id: str, setups: List[BracketSetup],
                 current_price: float, atr: float):
        self.product_id = product_id
        self.setups = setups
        self.current_price = current_price
        self.atr = atr

    def best(self) -> Optional[Opportunity]:
        if not self.setups:
            return None
        best = max(self.setups, key=lambda s: s.confidence * s.risk_reward)
        return Opportunity(
            product_id=self.product_id,
            direction=best.direction,
            instrument_type=best.instrument_type,
            entry_price=best.entry_price,
            stop_price=best.stop_price,
            target_price=best.target_price,
            risk_reward=best.risk_reward,
            confidence=best.confidence,
            reason=best.reason,
            strategy_name=best.strategy_name,
            atr=best.atr or self.atr,
            leverage=best.leverage,
        )

    def all_ranked(self) -> List[Opportunity]:
        sorted_setups = sorted(
            self.setups, key=lambda s: s.confidence * s.risk_reward, reverse=True
        )
        return [
            Opportunity(
                product_id=self.product_id,
                direction=s.direction,
                instrument_type=s.instrument_type,
                entry_price=s.entry_price,
                stop_price=s.stop_price,
                target_price=s.target_price,
                risk_reward=s.risk_reward,
                confidence=s.confidence,
                reason=s.reason,
                strategy_name=s.strategy_name,
                atr=s.atr or self.atr,
                leverage=s.leverage,
            )
            for s in sorted_setups
        ]
