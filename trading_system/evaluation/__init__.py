from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .agents import (
    ApprovalDrafter,
    BacktestCritic,
    CryptoOnchainAnalyst,
    FundamentalAnalyst,
    MarketAnalyst,
    PositionAuditor,
    RiskAnalyst,
    StrategyResearcher,
)
from .base import Action, AgentResult, BaseAgent, Evidence, Philosophy
from .pricing_models import (
    PositionQualityMetrics,
    PriceEstimationEngine,
    PriceTargetModel,
)

if TYPE_CHECKING:
    from .service import EvaluationService


def __getattr__(name: str) -> Any:
    """Load persistence-heavy services only when explicitly requested."""
    if name == "EvaluationService":
        from .service import EvaluationService

        return EvaluationService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Action",
    "AgentResult",
    "BaseAgent",
    "Evidence",
    "Philosophy",
    "PriceEstimationEngine",
    "PriceTargetModel",
    "PositionQualityMetrics",
    "EvaluationService",
    "PositionAuditor",
    "MarketAnalyst",
    "FundamentalAnalyst",
    "CryptoOnchainAnalyst",
    "RiskAnalyst",
    "StrategyResearcher",
    "BacktestCritic",
    "ApprovalDrafter",
]
