#!/usr/bin/env python3
"""Detect comparable-market price divergences between Kalshi and Polymarket."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOP_WORDS = {
    "a",
    "above",
    "an",
    "and",
    "be",
    "before",
    "by",
    "for",
    "in",
    "is",
    "of",
    "on",
    "the",
    "this",
    "to",
    "trade",
    "will",
    "year",
}
_MONTH_ALIASES = {
    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "sep": "september",
    "sept": "september",
    "oct": "october",
    "nov": "november",
    "dec": "december",
}


def _field(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    getter = getattr(item, "get", None)
    if callable(getter):
        return getter(key, default)
    return getattr(item, key, default)


def _probability(value: Any) -> float:
    """Normalize percentage-style or decimal-style prices to ``0..1``."""

    numeric = float(value or 0)
    if numeric > 1:
        numeric /= 100.0
    return max(0.0, min(1.0, numeric))


def _normalized_tokens(text: str) -> set[str]:
    normalized = text.lower().replace(",", "")
    normalized = re.sub(r"\$(\d+)k\b", lambda match: str(int(match.group(1)) * 1000), normalized)
    tokens: set[str] = set()
    for token in _TOKEN_RE.findall(normalized):
        token = _MONTH_ALIASES.get(token, token)
        if token not in _STOP_WORDS:
            tokens.add(token)
    return tokens


@dataclass(frozen=True)
class Opportunity:
    """A comparable-market price divergence with deterministic estimates."""

    kalshi_market_id: str
    polymarket_slug: str
    kalshi_price: float
    polymarket_price: float
    title_kalshi: str
    title_polymarket: str
    divergence: float
    arbitrage_potential_pct: float
    kalshi_volume: float = 0.0
    polymarket_volume: float = 0.0
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            object.__setattr__(self, "timestamp", datetime.now(timezone.utc))

    def potential_pnl(self, notional: float, estimated_round_trip_fee_pct: float = 0.003) -> float:
        """Estimate fee-adjusted PnL for a fully hedged notional amount."""

        edge = max(0.0, self.divergence - estimated_round_trip_fee_pct)
        return max(0.0, float(notional) * edge)

    def risk_score(self) -> float:
        """Return a conservative ``0..10`` liquidity confidence score."""

        available_volume = min(self.kalshi_volume, self.polymarket_volume)
        if available_volume <= 0:
            return 0.0
        return round(min(10.0, available_volume / 1_000.0), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kalshi_market_id": self.kalshi_market_id,
            "polymarket_slug": self.polymarket_slug,
            "kalshi_price": self.kalshi_price,
            "polymarket_price": self.polymarket_price,
            "title_kalshi": self.title_kalshi,
            "title_polymarket": self.title_polymarket,
            "divergence": self.divergence,
            "arbitrage_potential_pct": self.arbitrage_potential_pct,
            "kalshi_volume": self.kalshi_volume,
            "polymarket_volume": self.polymarket_volume,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }


class MarketMatcher:
    """Deterministic topic and keyword matcher for prediction markets."""

    def categorize_market(self, title: str) -> str:
        tokens = _normalized_tokens(title)
        if tokens & {"bitcoin", "btc", "crypto", "ethereum", "eth"}:
            return "bitcoin" if tokens & {"bitcoin", "btc"} else "crypto"
        if tokens & {"election", "president", "presidential", "trump", "biden", "harris"}:
            return "election"
        if tokens & {"super", "bowl", "nfl", "nba", "mlb", "nhl", "championship"}:
            return "sports"
        return "other"

    def keyword_similarity(self, left: str, right: str) -> float:
        left_tokens = _normalized_tokens(left)
        right_tokens = _normalized_tokens(right)
        if not left_tokens or not right_tokens:
            return 0.0
        overlap = len(left_tokens & right_tokens)
        union = len(left_tokens | right_tokens)
        jaccard = overlap / union if union else 0.0
        sequence = SequenceMatcher(None, " ".join(sorted(left_tokens)), " ".join(sorted(right_tokens))).ratio()
        return max(jaccard, sequence)

    def match_by_keyword(self, left: str, right: str, threshold: float = 0.55) -> bool:
        left_category = self.categorize_market(left)
        right_category = self.categorize_market(right)
        if left_category != right_category and "other" not in {left_category, right_category}:
            return False
        return self.keyword_similarity(left, right) >= threshold


class OpportunityDetector:
    """Detects comparable-market price divergences."""

    SIMILARITY_THRESHOLD = 0.55
    MIN_DIVERGENCE = 0.01

    def __init__(
        self,
        kalshi_markets: list[Any] | None = None,
        polymarket_events: list[Any] | None = None,
        kalshi_price_map: dict[str, float] | None = None,
        polymarket_price_map: dict[str, float] | None = None,
    ) -> None:
        self.kalshi_markets = list(kalshi_markets or [])
        self.polymarket_events = list(polymarket_events or [])
        self.kalshi_price_map = dict(kalshi_price_map or {})
        self.polymarket_price_map = dict(polymarket_price_map or {})
        self.opportunities: list[Opportunity] = []
        self.matcher = MarketMatcher()

    def from_dict(self, data: dict[str, Any]) -> None:
        self.kalshi_markets = list(data.get("markets", []))
        self.polymarket_events = list(data.get("events", []))
        self._rebuild_price_maps()
        self.opportunities = []

    def _rebuild_price_maps(self) -> None:
        self.kalshi_price_map = {
            str(_field(market, "market_id", _field(market, "id", ""))): _probability(_field(market, "bid", 0))
            for market in self.kalshi_markets
        }
        self.polymarket_price_map = {
            str(_field(event, "slug", _field(event, "id", ""))): _probability(_field(event, "bid", 0))
            for event in self.polymarket_events
        }

    def find_opportunities(
        self,
        kalshi_markets: list[Any],
        polymarket_events: list[Any],
    ) -> list[Opportunity]:
        """Compatibility entry point used by the original arbitrage service."""

        self.kalshi_markets = list(kalshi_markets or [])
        self.polymarket_events = list(polymarket_events or [])
        self._rebuild_price_maps()
        return self.detect_opportunities()

    def detect_opportunities(self) -> list[Opportunity]:
        self.opportunities = []
        for kalshi_market in self.kalshi_markets:
            for polymarket_event, _score in self._find_pm_matches(kalshi_market):
                kalshi_price = _probability(_field(kalshi_market, "bid", 0))
                polymarket_price = _probability(_field(polymarket_event, "bid", 0))
                divergence = abs(kalshi_price - polymarket_price)
                if divergence <= self.MIN_DIVERGENCE:
                    continue

                self.opportunities.append(
                    Opportunity(
                        kalshi_market_id=str(_field(kalshi_market, "market_id", _field(kalshi_market, "id", ""))),
                        polymarket_slug=str(_field(polymarket_event, "slug", _field(polymarket_event, "id", ""))),
                        kalshi_price=kalshi_price,
                        polymarket_price=polymarket_price,
                        title_kalshi=str(_field(kalshi_market, "title", "")),
                        title_polymarket=str(_field(polymarket_event, "question", "")),
                        divergence=divergence,
                        arbitrage_potential_pct=max(0.0, divergence - 0.003),
                        kalshi_volume=float(_field(kalshi_market, "volume", 0) or 0),
                        polymarket_volume=float(_field(polymarket_event, "volume", 0) or 0),
                    )
                )

        self.opportunities.sort(key=lambda opportunity: opportunity.arbitrage_potential_pct, reverse=True)
        return list(self.opportunities)

    def _find_pm_matches(self, kalshi_market: Any) -> list[tuple[Any, float]]:
        title = str(_field(kalshi_market, "title", ""))
        matches: list[tuple[Any, float]] = []
        for event in self.polymarket_events:
            question = str(_field(event, "question", ""))
            score = self.matcher.keyword_similarity(title, question)
            if self.matcher.match_by_keyword(title, question, self.SIMILARITY_THRESHOLD):
                matches.append((event, score))
        return matches

    def to_dict(self) -> dict[str, Any]:
        return {
            "opportunities": [opportunity.to_dict() for opportunity in self.opportunities],
            "kalshi_markets": self.to_dict_kalshi(),
            "polymarket_events": self.to_dict_polymarket(),
        }

    def to_dict_kalshi(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for market in self.kalshi_markets:
            if isinstance(market, dict):
                item = dict(market)
            else:
                item = dict(vars(market))
            item["market_id"] = str(_field(market, "market_id", _field(market, "id", "")))
            item["bid"] = _probability(_field(market, "bid", 0))
            result.append(item)
        return result

    def to_dict_polymarket(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for event in self.polymarket_events:
            if isinstance(event, dict):
                item = dict(event)
            else:
                item = dict(vars(event))
            item["slug"] = str(_field(event, "slug", _field(event, "id", "")))
            item["bid"] = _probability(_field(event, "bid", 0))
            result.append(item)
        return result

    def to_list(self) -> list[dict[str, Any]]:
        return [
            *self.to_dict_kalshi(),
            *self.to_dict_polymarket(),
            *(opportunity.to_dict() for opportunity in self.opportunities),
        ]

    @classmethod
    def from_list(cls, data: list[dict[str, Any]]) -> "OpportunityDetector":
        detector = cls(
            kalshi_markets=[item for item in data if "market_id" in item],
            polymarket_events=[item for item in data if "slug" in item],
        )
        detector._rebuild_price_maps()
        return detector

    @staticmethod
    def to_json(detector: "OpportunityDetector") -> str:
        from json import dumps

        return dumps(detector.to_dict(), indent=2)


__all__ = ["MarketMatcher", "Opportunity", "OpportunityDetector"]
