"""Python integration layer for the Rust rebalancing / stair-step core.

Wraps `rust_core.PyRebalancer` and `rust_core.PyStairStepProfitTaker` with a
clean dataclass API plus allocation presets and a convenience bot that the
trader loop can drive ("set an allocation and let it run").
"""

import math
import os
from dataclasses import dataclass, field
from typing import Optional

import rust_core


ALLOCATION_PRESETS = {
    "core_balanced": {
        "BTC-USD": 0.40,
        "ETH-USD": 0.25,
        "SOL-USD": 0.15,
        "XRP-USD": 0.10,
        "XLM-USD": 0.05,
        "MON-USD": 0.05,
    },
    "volatile_tilt": {
        "BTC-USD": 0.30,
        "ETH-USD": 0.20,
        "SOL-USD": 0.10,
        "XRP-USD": 0.10,
        "XLM-USD": 0.10,
        "MON-USD": 0.10,
        "PEPE-USD": 0.05,
        "BONK-USD": 0.05,
    },
    "safe": {
        "BTC-USD": 0.60,
        "ETH-USD": 0.30,
        "SOL-USD": 0.10,
    },
}

_ENGINE_KWARGS = ("drift_threshold", "profit_take_pct", "min_trade_notional")


@dataclass
class RebalanceOrder:
    asset: str
    side: str
    notional: float
    current_weight: float
    target_weight: float
    drift: float

    def to_dict(self) -> dict:
        return {
            "asset": self.asset,
            "side": self.side,
            "notional": self.notional,
            "current_weight": self.current_weight,
            "target_weight": self.target_weight,
            "drift": self.drift,
        }


@dataclass
class StairStepOrder:
    side: str
    price: float
    notional: float

    def to_dict(self) -> dict:
        return {"side": self.side, "price": self.price, "notional": self.notional}


@dataclass
class Recommendation:
    orders: list = field(default_factory=list)
    max_drift: float = 0.0
    turnover: float = 0.0

    def to_dict(self) -> dict:
        return {
            "orders": [o.to_dict() for o in self.orders],
            "max_drift": self.max_drift,
            "turnover": self.turnover,
        }


def _finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def _apply_env_overrides(base_name: str, base_weights: dict) -> tuple:
    name = os.environ.get("REBALANCE_PRESET", base_name)
    weights = dict(ALLOCATION_PRESETS.get(name, base_weights))
    raw = os.environ.get("REBALANCE_WEIGHTS")
    if raw:
        for pair in raw.split(","):
            pair = pair.strip()
            if not pair:
                continue
            sym, _, val = pair.partition("=")
            sym = sym.strip()
            try:
                weights[sym] = float(val.strip())
            except ValueError:
                continue
    return name, weights


class RebalanceEngine:
    def __init__(self, targets, drift_threshold=0.05, profit_take_pct=1.0,
                 min_trade_notional=1.0):
        self.targets = dict(targets)
        self.drift_threshold = drift_threshold
        self.profit_take_pct = profit_take_pct
        self.min_trade_notional = min_trade_notional
        self._rebalancer = rust_core.PyRebalancer(
            self.targets, drift_threshold, profit_take_pct, min_trade_notional)

    @classmethod
    def from_preset(cls, name, **overrides):
        base = ALLOCATION_PRESETS.get(name, {})
        if not base and name not in ALLOCATION_PRESETS:
            raise KeyError(f"unknown preset: {name}")
        env_name, env_weights = _apply_env_overrides(name, base)
        weights = dict(env_weights)
        kwargs = {k: overrides[k] for k in _ENGINE_KWARGS if k in overrides}
        for k in list(overrides):
            if k not in _ENGINE_KWARGS:
                weights[k] = overrides[k]
        return cls(weights, **kwargs)

    def compute(self, current_values, total=None) -> Recommendation:
        if total is None:
            total = sum(current_values.values())
        orders = []
        for asset, side, notional, cur_w, tgt_w, drift in self._rebalancer.compute_orders(
                current_values, total):
            orders.append(RebalanceOrder(
                asset=asset, side=side, notional=notional,
                current_weight=cur_w, target_weight=tgt_w, drift=drift))
        max_drift = self._rebalancer.max_abs_drift(current_values, total)
        turnover = sum(o.notional for o in orders)
        return Recommendation(orders=orders, max_drift=max_drift, turnover=turnover)


class StairStepEngine:
    def __init__(self):
        self._symbols = {}

    def add_symbol(self, symbol, low, high, steps, budget, take_profit_pct,
                   base_size_pct):
        self._symbols[symbol] = rust_core.PyStairStepProfitTaker(
            low, high, steps, budget, take_profit_pct, base_size_pct)

    def on_price(self, symbol, price) -> Optional[StairStepOrder]:
        taker = self._symbols.get(symbol)
        if taker is None or not _finite(price):
            return None
        result = taker.on_price(price)
        if result is None:
            return None
        side, px, notional = result
        return StairStepOrder(side=side, price=px, notional=notional)

    def state(self, symbol):
        taker = self._symbols.get(symbol)
        if taker is None:
            return (0, 0, 0, 0.0, 0.0, "INIT")
        return taker.state()

    def reset(self, symbol):
        taker = self._symbols.get(symbol)
        if taker is not None:
            taker.reset()

    def to_dict(self) -> dict:
        return {sym: list(self.state(sym)) for sym in self._symbols}


class RebalanceBot:
    def __init__(self, engine: Optional[RebalanceEngine] = None,
                 stair_step: Optional[StairStepEngine] = None):
        self.engine = engine or RebalanceEngine.from_preset("core_balanced")
        self.stair_step = stair_step or StairStepEngine()

    def recommend(self, current_book: dict) -> Recommendation:
        total = sum(current_book.values())
        return self.engine.compute(current_book, total)

    def on_price(self, symbol, price) -> Optional[StairStepOrder]:
        return self.stair_step.on_price(symbol, price)
