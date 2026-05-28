from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from core.config.settings import Settings
from core.models.domain import OrderIntent, RiskMode
from research.approval import ApprovalService
from risk.engine import RiskEngine, RiskPolicy
from strategies.registry.registry import load_strategies


class WorkerEngine:
    def __init__(self, settings: Settings | None = None, db: Session | None = None) -> None:
        self.settings = settings or Settings.from_env()
        self.risk_engine = RiskEngine(RiskPolicy())
        self.strategies = load_strategies()
        self._product_map: dict[str, list] = {}
        self._db_disabled: set[str] = set()
        self._db = db
        self._approval_svc = ApprovalService(db) if db else None

        for s in self.strategies:
            for product in s.metadata().get("products", ["BTC-USD"]):
                self._product_map.setdefault(product, []).append(s)

    def sync_disabled(self, disabled: set[str]) -> None:
        self._db_disabled = disabled
        self.risk_engine.disabled_strategies = disabled

    def evaluate_market_state(self, product_id: str, market_state: dict[str, Any], mode: str = "paper") -> list[dict[str, Any]]:
        signals: list[dict[str, Any]] = []
        for strategy in self._product_map.get(product_id, []):
            if strategy.strategy_id in self._db_disabled:
                continue
            if not strategy.metadata().get("enabled", True):
                continue
            if mode == "live" and self._approval_svc:
                ok, _ = self._approval_svc.check_strategy_approved(strategy.strategy_id)
                if not ok:
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

    def evaluate_order(self, signal: dict[str, Any], market_state: dict[str, Any], mode: str = "paper") -> tuple[bool, str]:
        if mode == "live" and self._approval_svc:
            ok, ref = self._approval_svc.check_trade_approved(
                signal["strategy_id"], market_state.get("product_id", "BTC-USD"),
            )
            if not ok:
                return False, f"trade not approved: {ref}"

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
