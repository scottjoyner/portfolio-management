from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Action(Enum):
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    REDUCE = "reduce"
    SELL = "sell"
    EXIT = "exit"
    WATCH = "watch"


class Philosophy(Enum):
    VALUE = "value"
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    MARKET_MAKING = "market_making"
    ARBITRAGE = "arbitrage"
    HEDGE = "hedge"
    MACRO = "macro"
    GROWTH = "growth"


@dataclass
class Evidence:
    source: str
    metric: str
    value: Any
    weight: float = 1.0


@dataclass
class AgentResult:
    agent_name: str
    instrument: str
    action: Action
    confidence: float
    rationale: str
    risk_score: float
    philosophy: Philosophy
    holding_period_hint: str
    evidence: list[Evidence] = field(default_factory=list)
    dissenting: str | None = None
    model_version: str = "1.0.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class BaseAgent(ABC):
    agent_name: str

    @abstractmethod
    def evaluate(self, instrument: str, market_data: dict[str, Any]) -> AgentResult:
        ...
