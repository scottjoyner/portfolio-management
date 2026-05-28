from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from apps.paper_exchange.engine import PaperExchangeEngine, PaperFill
from storage.postgres.models import StrategyRun
from storage.postgres.repository import OpsRepository


@dataclass
class IncubationReport:
    strategy_id: str
    task_id: str
    total_orders: int
    total_fills: int
    total_fill_value: Decimal
    avg_slippage_bps: float
    fill_rate: float
    realized_pnl: Decimal
    backtest_sharpe: float | None
    backtest_max_drawdown: float | None
    backtest_total_return: float | None
    slippage_drift_bps: float
    latency_drift_ms: float
    fill_quality_ratio: float
    started_at: datetime
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class IncubationService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.repo = OpsRepository(db)

    def track_run(self, task_id: str, engine: PaperExchangeEngine, backtest_assumptions: dict[str, Any] | None = None) -> IncubationReport:
        run = self.repo.get_strategy_run(task_id)
        if not run:
            raise ValueError(f"strategy run {task_id} not found")

        fills = engine.fills
        orders = len(fills)
        total_fills = sum(1 for f in fills if f.fill_id)
        total_value = sum(f.size * f.price for f in fills)
        avg_slippage = sum(float(f.slippage_bps) for f in fills) / max(len(fills), 1)
        fill_rate = total_fills / max(orders, 1)
        realized_pnl = sum(
            engine.positions[p].realized_pnl for p in engine.positions
        ) if hasattr(engine, 'positions') else Decimal("0")

        bt_assumptions = backtest_assumptions or {}
        bt_sharpe = bt_assumptions.get("backtest_sharpe")
        bt_max_dd = bt_assumptions.get("backtest_max_drawdown")
        bt_return = bt_assumptions.get("backtest_total_return")

        expected_slippage = float(bt_assumptions.get("expected_slippage_bps", 0))
        slippage_drift = avg_slippage - expected_slippage

        expected_latency = float(bt_assumptions.get("expected_latency_ms", 0))
        actual_latency = 0.0
        latency_drift = actual_latency - expected_latency

        expected_fill_rate = float(bt_assumptions.get("expected_fill_rate", 1.0))
        fill_quality_ratio = fill_rate / max(expected_fill_rate, 0.01)

        return IncubationReport(
            strategy_id=run.strategy_id,
            task_id=task_id,
            total_orders=orders,
            total_fills=total_fills,
            total_fill_value=total_value,
            avg_slippage_bps=avg_slippage,
            fill_rate=fill_rate,
            realized_pnl=realized_pnl,
            backtest_sharpe=bt_sharpe,
            backtest_max_drawdown=bt_max_dd,
            backtest_total_return=bt_return,
            slippage_drift_bps=slippage_drift,
            latency_drift_ms=latency_drift,
            fill_quality_ratio=fill_quality_ratio,
            started_at=run.started_at or run.queued_at,
        )

    def approve_for_live(self, task_id: str) -> tuple[bool, str]:
        run = self.repo.get_strategy_run(task_id)
        if not run:
            return False, "run not found"
        if run.mode != "paper":
            return False, f"run is in {run.mode} mode, not paper"

        self.repo.update_strategy_run(task_id, status="approved_for_live")
        from storage.postgres.models import StrategyConfig
        cfg = self.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == run.strategy_id).first()
        if not cfg:
            cfg = StrategyConfig(strategy_id=run.strategy_id)
            self.db.add(cfg)
        cfg.certification_status = "incubated"
        self.db.commit()
        return True, "approved"

    def shadow_payload(self, market_data: dict[str, Any], intent: dict[str, Any]) -> dict[str, Any]:
        return {
            "shadow_id": f"shadow-{intent.get('strategy_id', 'unknown')}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "strategy_id": intent.get("strategy_id"),
            "product_id": intent.get("product_id"),
            "side": intent.get("side"),
            "size": float(intent.get("size", 0)),
            "limit_price": float(intent.get("limit_price", 0)) if intent.get("limit_price") else None,
            "market_data_snapshot": {
                "price": market_data.get("price"),
                "spread_bps": market_data.get("spread_bps"),
                "volume_24h": market_data.get("volume_24h"),
                "timestamp": market_data.get("timestamp"),
            },
            "would_execute": True,
            "expected_slippage_bps": float(market_data.get("spread_bps", 5)) / 2,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": "shadow",
        }
