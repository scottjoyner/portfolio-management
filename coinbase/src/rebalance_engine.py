"""Rebalancing and stair-step engines with an optional Rust accelerator.

The compiled ``rust_core`` extension is used when available. Source checkouts,
CI, and smaller machines transparently use a deterministic Python implementation
of the same formulas instead of failing at import time.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Optional

try:
    import rust_core as _rust_core
    if not hasattr(_rust_core, "PyRebalancer"):
        _rust_core = None
except (ImportError, ModuleNotFoundError):
    _rust_core = None

RUST_ACCELERATOR_AVAILABLE = _rust_core is not None

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
            "orders": [order.to_dict() for order in self.orders],
            "max_drift": self.max_drift,
            "turnover": self.turnover,
        }


def _finite(value) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def _apply_env_overrides(base_name: str, base_weights: dict) -> tuple:
    name = os.environ.get("REBALANCE_PRESET", base_name)
    weights = dict(ALLOCATION_PRESETS.get(name, base_weights))
    raw = os.environ.get("REBALANCE_WEIGHTS")
    if raw:
        for pair in raw.split(","):
            pair = pair.strip()
            if not pair:
                continue
            symbol, _, value = pair.partition("=")
            try:
                weights[symbol.strip()] = float(value.strip())
            except ValueError:
                continue
    return name, weights


def _validate_engine(targets: dict, drift_threshold: float,
                     profit_take_pct: float, min_trade_notional: float) -> None:
    if not targets:
        raise ValueError("allocation must contain at least one asset")
    if any(not _finite(weight) or weight <= 0 for weight in targets.values()):
        raise ValueError("allocation weights must be finite and > 0")
    if not _finite(drift_threshold) or drift_threshold < 0:
        raise ValueError("drift_threshold must be >= 0")
    if not _finite(profit_take_pct) or not 0 < profit_take_pct <= 1:
        raise ValueError("profit_take_pct must be in (0, 1]")
    if not _finite(min_trade_notional) or min_trade_notional < 0:
        raise ValueError("min_trade_notional must be >= 0")


class RebalanceEngine:
    def __init__(self, targets, drift_threshold=0.05, profit_take_pct=1.0,
                 min_trade_notional=1.0):
        self.targets = dict(targets)
        self.drift_threshold = float(drift_threshold)
        self.profit_take_pct = float(profit_take_pct)
        self.min_trade_notional = float(min_trade_notional)
        _validate_engine(
            self.targets,
            self.drift_threshold,
            self.profit_take_pct,
            self.min_trade_notional,
        )
        self._rebalancer = (
            _rust_core.PyRebalancer(
                self.targets,
                self.drift_threshold,
                self.profit_take_pct,
                self.min_trade_notional,
            )
            if RUST_ACCELERATOR_AVAILABLE
            else None
        )

    @property
    def backend(self) -> str:
        return "rust" if self._rebalancer is not None else "python"

    @classmethod
    def from_preset(cls, name, **overrides):
        base = ALLOCATION_PRESETS.get(name, {})
        if not base and name not in ALLOCATION_PRESETS:
            raise KeyError(f"unknown preset: {name}")
        _, env_weights = _apply_env_overrides(name, base)
        weights = dict(env_weights)
        kwargs = {key: overrides[key] for key in _ENGINE_KWARGS if key in overrides}
        for key in list(overrides):
            if key not in _ENGINE_KWARGS:
                weights[key] = overrides[key]
        return cls(weights, **kwargs)

    def _normalized_targets(self) -> dict[str, float]:
        total = sum(self.targets.values())
        return {asset: weight / total for asset, weight in self.targets.items()}

    @staticmethod
    def _total_value(current_values: dict, total: float | None) -> float:
        if total is not None and _finite(total) and total > 0:
            return float(total)
        return sum(float(value) for value in current_values.values() if _finite(value))

    def _python_rows(self, current_values: dict, total: float | None) -> tuple[list[tuple], float]:
        effective_total = self._total_value(current_values, total)
        targets = self._normalized_targets()
        rows: list[tuple] = []
        max_drift = 0.0
        for asset, target_weight in targets.items():
            current_value = float(current_values.get(asset, 0.0) or 0.0)
            current_weight = current_value / effective_total if effective_total > 0 else 0.0
            drift = current_weight - target_weight
            max_drift = max(max_drift, abs(drift))
            if abs(drift) <= self.drift_threshold:
                continue
            delta = target_weight * effective_total - current_value
            if delta < 0:
                delta = -abs(delta) * self.profit_take_pct
            notional = abs(delta)
            if notional < self.min_trade_notional:
                continue
            rows.append((
                asset,
                "BUY" if delta > 0 else "SELL",
                notional,
                current_weight,
                target_weight,
                drift,
            ))
        return rows, max_drift

    def compute(self, current_values, total=None) -> Recommendation:
        values = dict(current_values)
        effective_total = self._total_value(values, total)
        if self._rebalancer is not None:
            rows = self._rebalancer.compute_orders(values, effective_total)
            max_drift = self._rebalancer.max_abs_drift(values, effective_total)
        else:
            rows, max_drift = self._python_rows(values, effective_total)
        orders = [
            RebalanceOrder(
                asset=asset,
                side=side,
                notional=notional,
                current_weight=current_weight,
                target_weight=target_weight,
                drift=drift,
            )
            for asset, side, notional, current_weight, target_weight, drift in rows
        ]
        return Recommendation(
            orders=orders,
            max_drift=max_drift,
            turnover=sum(order.notional for order in orders),
        )


class _PythonStairStepProfitTaker:
    def __init__(self, low, high, steps, budget, take_profit_pct, base_size_pct):
        if not all(_finite(value) for value in (low, high, budget, take_profit_pct, base_size_pct)):
            raise ValueError("stair-step inputs must be finite")
        if low >= high:
            raise ValueError("low must be < high")
        if int(steps) <= 0:
            raise ValueError("steps must be > 0")
        if budget <= 0:
            raise ValueError("budget must be > 0")
        if take_profit_pct <= 0:
            raise ValueError("take_profit_pct must be > 0")
        if not 0 < base_size_pct <= 1:
            raise ValueError("base_size_pct must be in (0, 1]")
        self.low = float(low)
        self.high = float(high)
        self.steps = int(steps)
        self.budget = float(budget)
        self.take_profit_pct = float(take_profit_pct)
        self.base_size_pct = float(base_size_pct)
        self.reset()

    def _level(self, index: int) -> float:
        return self.high - index * (self.high - self.low) / self.steps

    def _base_size(self) -> float:
        return self.budget * self.base_size_pct

    def on_price(self, price):
        if not _finite(price):
            self.last_action = "HOLD"
            return None
        price = float(price)
        if self.next_buy_index < self.steps and price <= self._level(self.next_buy_index):
            notional = self._base_size()
            self.buys.append(price)
            self.inventory_value += notional
            self.next_buy_index += 1
            self.filled_buys += 1
            self.last_action = "BUY"
            return ("BUY", price, notional)
        if self.buys:
            average = sum(self.buys) / len(self.buys)
            if price >= average * (1 + self.take_profit_pct):
                buy_price = self.buys.pop()
                notional = self._base_size()
                self.realized_pnl += (price - buy_price) / buy_price * notional
                self.inventory_value = max(0.0, self.inventory_value - notional)
                self.filled_sells += 1
                self.last_action = "SELL"
                return ("SELL", price, notional)
        self.last_action = "HOLD"
        return None

    def state(self):
        return (
            self.next_buy_index,
            self.filled_buys,
            self.filled_sells,
            self.inventory_value,
            self.realized_pnl,
            self.last_action,
        )

    def reset(self):
        self.next_buy_index = 0
        self.buys: list[float] = []
        self.inventory_value = 0.0
        self.realized_pnl = 0.0
        self.last_action = "INIT"
        self.filled_buys = 0
        self.filled_sells = 0


class StairStepEngine:
    def __init__(self):
        self._symbols = {}

    @property
    def backend(self) -> str:
        return "rust" if RUST_ACCELERATOR_AVAILABLE else "python"

    def add_symbol(self, symbol, low, high, steps, budget, take_profit_pct,
                   base_size_pct):
        cls = _rust_core.PyStairStepProfitTaker if RUST_ACCELERATOR_AVAILABLE else _PythonStairStepProfitTaker
        self._symbols[symbol] = cls(
            low, high, steps, budget, take_profit_pct, base_size_pct
        )

    def on_price(self, symbol, price) -> Optional[StairStepOrder]:
        taker = self._symbols.get(symbol)
        if taker is None or not _finite(price):
            return None
        result = taker.on_price(price)
        if result is None:
            return None
        side, fill_price, notional = result
        return StairStepOrder(side=side, price=fill_price, notional=notional)

    def state(self, symbol):
        taker = self._symbols.get(symbol)
        return taker.state() if taker is not None else (0, 0, 0, 0.0, 0.0, "INIT")

    def reset(self, symbol):
        taker = self._symbols.get(symbol)
        if taker is not None:
            taker.reset()

    def to_dict(self) -> dict:
        return {symbol: list(self.state(symbol)) for symbol in self._symbols}


class RebalanceBot:
    def __init__(self, engine: Optional[RebalanceEngine] = None,
                 stair_step: Optional[StairStepEngine] = None):
        self.engine = engine or RebalanceEngine.from_preset("core_balanced")
        self.stair_step = stair_step or StairStepEngine()

    def recommend(self, current_book: dict) -> Recommendation:
        return self.engine.compute(current_book, sum(current_book.values()))

    def on_price(self, symbol, price) -> Optional[StairStepOrder]:
        return self.stair_step.on_price(symbol, price)
