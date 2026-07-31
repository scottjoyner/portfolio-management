"""Fair-market-price estimation and position-quality helpers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class PriceTargetModel(Enum):
    """Price target models for fair-value estimation."""

    FUNDAMENTAL_BASED = "fundamental"
    TECHNICAL_ANALYSIS = "technical"
    CONSENSUS_AVERAGE = "consensus"
    ML_PREDICTIVE = "ml_predictive"


class AwaitableDict(dict[str, Any]):
    """Dictionary result that remains compatible with legacy ``await`` callers."""

    def __await__(self):
        async def _resolve() -> AwaitableDict:
            return self

        return _resolve().__await__()


@dataclass
class PositionQualityMetrics:
    """Position quality scoring."""

    risk_score: float
    alpha_score: float
    beta_exposure: float
    correlation_to_index: float
    volatility_regime: str

    def __await__(self):
        async def _resolve() -> PositionQualityMetrics:
            return self

        return _resolve().__await__()


class PriceEstimationEngine:
    """Deterministic fair-value and position-quality calculator.

    These calculations are CPU-only and complete synchronously. Returned values
    remain awaitable so existing asynchronous integrations can migrate without
    a flag day.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.price_source = str(
            self.config.get("price_source", "fundamental")
        )
        self.use_ml_models = bool(self.config.get("use_ml_models", False))
        self.max_position_notional = float(
            self.config.get("max_position_notional", 100_000.0)
        )
        if self.max_position_notional <= 0:
            raise ValueError("max_position_notional must be positive")

    def estimate_price(
        self,
        symbol: str,
        target_model: PriceTargetModel,
        price_data: Dict[str, Any],
    ) -> AwaitableDict:
        """Estimate deterministic buy, sell, and hold levels."""
        if not symbol or not str(symbol).strip():
            raise ValueError("symbol is required")
        if not isinstance(target_model, PriceTargetModel):
            raise ValueError("target_model must be a PriceTargetModel")

        try:
            current_price = float(price_data.get("current_price", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("current_price must be numeric") from exc
        if current_price <= 0:
            raise ValueError("current_price must be positive")

        confidence_by_model = {
            PriceTargetModel.FUNDAMENTAL_BASED: 0.60,
            PriceTargetModel.TECHNICAL_ANALYSIS: 0.55,
            PriceTargetModel.CONSENSUS_AVERAGE: 0.65,
            PriceTargetModel.ML_PREDICTIVE: 0.50,
        }
        confidence_score = confidence_by_model[target_model]

        return AwaitableDict(
            {
                "buy_level": round(current_price * 0.95, 2),
                "sell_level": round(current_price * 1.05, 2),
                "hold_level": round(current_price, 2),
                "confidence_score": round(confidence_score, 2),
                "model_used": target_model.value,
            }
        )

    def calculate_position_quality(
        self,
        position_data: Dict[str, Any],
    ) -> PositionQualityMetrics:
        """Calculate bounded position-quality metrics."""
        try:
            quantity = abs(float(position_data.get("quantity", 0) or 0))
            entry_price = float(position_data.get("entry_price", 0) or 0)
            current_price = float(position_data.get("current_price", 0) or 0)
            correlation = float(
                position_data.get("correlation_to_index", 0.7) or 0
            )
            beta = float(position_data.get("beta_exposure", 1.0) or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("position metrics must be numeric") from exc

        unrealized_return_pct = (
            (current_price - entry_price) / entry_price * 100
            if entry_price > 0
            else 0.0
        )
        reference_price = current_price if current_price > 0 else entry_price
        position_notional = quantity * max(reference_price, 0.0)
        size_risk = min(
            position_notional / self.max_position_notional,
            1.0,
        )
        return_risk = min(abs(unrealized_return_pct), 50.0) / 50.0

        volatility_regime = str(
            position_data.get("volatility_regime", "moderate")
        ).strip().lower()
        volatility_risk = {
            "low": 0.0,
            "moderate": 0.1,
            "high": 0.2,
            "extreme": 0.3,
        }.get(volatility_regime, 0.15)

        risk_score = min(
            1.0,
            0.2
            + 0.35 * size_risk
            + 0.25 * return_risk
            + volatility_risk,
        )

        return PositionQualityMetrics(
            risk_score=round(risk_score, 4),
            alpha_score=round(unrealized_return_pct * 100, 2),
            beta_exposure=max(-1.0, min(1.0, beta)),
            correlation_to_index=max(-1.0, min(1.0, correlation)),
            volatility_regime=volatility_regime,
        )
