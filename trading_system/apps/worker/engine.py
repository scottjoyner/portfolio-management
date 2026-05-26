from __future__ import annotations

from typing import Any

from core.config.settings import Settings
from core.models.domain import OrderIntent, RiskMode
from risk.engine import RiskEngine, RiskPolicy
from strategies.registry.registry import load_strategies


class WorkerEngine:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.from_env()
        self.risk_engine = RiskEngine(RiskPolicy())
        self.strategies = load_strategies()
        self._product_map: dict[str, list] = {}

        for s in self.strategies:
            for product in s.metadata().get("products", ["BTC-USD"]):
                self._product_map.setdefault(product, []).append(s)

    def evaluate_market_state(self, product_id: str, market_state: dict[str, Any]) -> list[dict[str, Any]]:
        signals: list[dict[str, Any]] = []
        for strategy in self._product_map.get(product_id, []):
            if not strategy.metadata().get("enabled", True):
                continue
            try:
                signal = strategy.generate_signal(market_state)
                if signal is None:
                    continue
                signals.append({
                    "strategy_id": strategy.strategy_id,
                    "signal": signal,
                    "explanation": strategy.explain_trade(signal),
                })
            except Exception:
                continue
        return signals

    def evaluate_order(self, signal: dict[str, Any], market_state: dict[str, Any]) -> tuple[bool, str]:
        intent = OrderIntent(
            strategy_id=signal["strategy_id"],
            product_id=market_state.get("product_id", "BTC-USD"),
            side="buy",
            order_type="limit",
            size=1.0,
            price=market_state.get("price"),
            rationale=signal.get("explanation", ""),
            risk_mode=RiskMode.NORMAL,
        )
        return self.risk_engine.evaluate(intent, mark_price=market_state.get("price", 0.0))
